//! Inbound request routing: the four refusal checks, spawning a handler onto
//! the driver's `JoinSet`, and turning what it produced into a wire frame.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use serde_json::{Map, Value};
use tokio::task::{Id, JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::codec::ndjson::encode_frame_bytes;
use crate::error::{RemoteError, codes};
use crate::frame::{ErrorPayload, ErrorResponse, Frame, Request, Response};
use crate::port::PortTx;
use crate::validate::is_reserved_method_name;

use super::driver::Writer;
use super::handle::{Session, SessionState};
use super::handler::CallContext;
use super::shared::{Shared, lock};

/// One request this session is currently answering.
pub(super) struct ActiveRequest {
    pub(super) cancel: CancellationToken,
    pub(super) method: String,
}

/// The task-output type every handler invocation produces.
pub(super) type HandlerOutcome = Result<Value, RemoteError>;

/// Bookkeeping for every inbound request this driver is currently answering:
/// the spawned handler tasks, the request each is answering (with its
/// cancellation token), and the task-id → request-id reverse lookup a
/// panic-safe settlement needs. Grouped into one type since every one of
/// these fields changes together, on exactly the same two events (a request
/// arrives, a handler settles).
#[derive(Default)]
pub(super) struct RequestTracking {
    pub(super) tasks: JoinSet<HandlerOutcome>,
    pub(super) active: HashMap<String, ActiveRequest>,
    pub(super) by_task_id: HashMap<Id, String>,
}

fn respond_error<Tx: PortTx>(
    writer: &Writer<Tx>,
    id: String,
    code: &str,
    message: String,
    details: Option<Map<String, Value>>,
) {
    writer.enqueue(Frame::Err(ErrorResponse {
        id,
        error: ErrorPayload {
            code: code.to_string(),
            message,
            details,
        },
    }));
}

fn detail(key: &str, value: impl Into<Value>) -> Map<String, Value> {
    let mut details = Map::new();
    details.insert(key.to_string(), value.into());
    details
}

/// Routes one inbound `req` frame: the four refusal checks (not ready, a
/// duplicate id, a reserved method, no handler), then spawns the matched
/// handler onto `tasks`.
pub(super) fn on_request<Tx: PortTx>(
    shared: &Arc<Shared>,
    tracking: &mut RequestTracking,
    writer: &Writer<Tx>,
    request: Request,
) {
    let Request { id, method, params } = request;

    // Ready and remote are set together (see driver::on_hello), so treating
    // a missing remote the same as "not ready yet" is exact, not a fallback
    // for a state that should be provably unreachable.
    let remote = {
        let guard = lock(&shared.inner);
        (guard.state == SessionState::Ready)
            .then(|| guard.remote.clone())
            .flatten()
    };
    let Some(remote) = remote else {
        respond_error(
            writer,
            id,
            codes::UNAVAILABLE,
            "The session handshake has not completed; requests are refused until both hellos \
             have crossed."
                .to_string(),
            None,
        );
        return;
    };
    if tracking.active.contains_key(&id) {
        respond_error(
            writer,
            id.clone(),
            codes::INVALID_REQUEST,
            format!(
                "Request id \"{id}\" is already in flight; expected an id unique among the \
                 sender's pending requests."
            ),
            Some(detail("id", id)),
        );
        return;
    }
    if is_reserved_method_name(&method) {
        respond_error(
            writer,
            id,
            codes::INVALID_REQUEST,
            format!(
                "Method \"{method}\" is reserved; the rpc. segment belongs to the protocol and \
                 defines no method in wire 1.0."
            ),
            Some(detail("method", method)),
        );
        return;
    }
    let handler = lock(&shared.handlers)
        .get(&method)
        .map(|(_, handler)| Arc::clone(handler));
    let Some(handler) = handler else {
        respond_error(
            writer,
            id,
            codes::METHOD_UNSUPPORTED,
            format!("Method \"{method}\" has no handler on this peer."),
            Some(detail("method", method)),
        );
        return;
    };

    let cancel = CancellationToken::new();
    let context = CallContext {
        id: id.clone(),
        method: method.clone(),
        cancel: cancel.clone(),
        remote,
        session: Session {
            shared: Arc::clone(shared),
        },
    };
    shared.in_flight.fetch_add(1, Ordering::Relaxed);
    let abort_handle = tracking.tasks.spawn(handler.call(params, context));
    tracking.by_task_id.insert(abort_handle.id(), id.clone());
    tracking.active.insert(id, ActiveRequest { cancel, method });
}

/// A `cancel` frame for a request this side is (or was) answering: signals
/// the handler's token. A cancel for an id this side does not recognise (it
/// already settled, or never existed) is silently ignored, mirroring the
/// TypeScript SDK's optional-chained lookup.
pub(super) fn on_cancel(active: &HashMap<String, ActiveRequest>, id: &str) {
    if let Some(request) = active.get(id) {
        request.cancel.cancel();
    }
}

/// What one `JoinSet::join_next_with_id` produced: the settled handler's
/// result, or, on a panic, a message built from it. Turns that into a wire
/// frame and clears the request's bookkeeping.
pub(super) fn on_handler_settled<Tx: PortTx>(
    shared: &Shared,
    tracking: &mut RequestTracking,
    writer: &Writer<Tx>,
    settled: Result<(Id, HandlerOutcome), JoinError>,
) {
    let (task_id, outcome) = match settled {
        Ok((task_id, outcome)) => (task_id, outcome),
        Err(join_error) => {
            let task_id = join_error.id();
            let outcome = Err(RemoteError::new(codes::INTERNAL, panic_message(join_error)));
            (task_id, outcome)
        }
    };
    shared.in_flight.fetch_sub(1, Ordering::Relaxed);
    let Some(id) = tracking.by_task_id.remove(&task_id) else {
        return;
    };
    let Some(active_request) = tracking.active.remove(&id) else {
        return;
    };
    match outcome {
        Ok(value) => respond_result(shared, writer, &active_request.method, id, value),
        Err(error) => respond_error(
            writer,
            id,
            &error.code,
            error.message,
            error.details.map(|details| details.into_iter().collect()),
        ),
    }
}

fn respond_result<Tx: PortTx>(
    shared: &Shared,
    writer: &Writer<Tx>,
    method: &str,
    id: String,
    value: Value,
) {
    let frame = Frame::Res(Response {
        id: id.clone(),
        result: value,
    });
    let limit = shared.send_limit_bytes();
    match encode_frame_bytes(&frame, limit) {
        Ok(_) => writer.enqueue(frame),
        Err(_) => respond_error(
            writer,
            id,
            codes::FRAME_TOO_LARGE,
            format!(
                "The result of \"{method}\" encodes to more bytes than the session limit of {limit}."
            ),
            Some(detail("limit", u64::try_from(limit).unwrap_or(u64::MAX))),
        ),
    }
}

/// The message a panicking handler's `JoinError` carries, downcast from
/// whatever panic payload it produced.
fn panic_message(join_error: JoinError) -> String {
    let Ok(payload) = join_error.try_into_panic() else {
        return "The handler task was aborted before it could settle.".to_string();
    };
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "The handler panicked.".to_string()
}
