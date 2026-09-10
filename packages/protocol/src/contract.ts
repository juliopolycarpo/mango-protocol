import type { Static, TSchema } from 'typebox';
import type { TLocalizedValidationError } from 'typebox/error';
import Value from 'typebox/value';
import { RESERVED_ERROR_CODES, RemoteError } from './errors';
import { assertCatalog, type Catalog } from './schemas/catalog';
import { isReservedMethodName, isValidMethodName } from './schemas/common';
import type { EventFrame } from './schemas/frames';
import type { HandlerContext, RequestOptions, Session } from './session';
import type { ProtocolVersion } from './version';

export interface MethodDefinition<P extends TSchema = TSchema, R extends TSchema = TSchema> {
  readonly params: P;
  readonly result: R;
  /** Members of `hello.capabilities` the responder needs before serving this method. */
  readonly capabilities?: readonly string[];
  readonly description?: string;
  readonly deprecated?: boolean;
}

export interface EventDefinition<T extends TSchema = TSchema> {
  readonly payload: T;
  /** True when events on this topic carry a `streamId` and an `end` marker. */
  readonly stream?: boolean;
  readonly description?: string;
}

export type MethodMap = Readonly<Record<string, MethodDefinition>>;
export type EventMap = Readonly<Record<string, EventDefinition>>;

export interface ContractDefinition<M extends MethodMap, E extends EventMap> {
  readonly name: string;
  readonly version: string;
  readonly description?: string;
  /** Lowest wire version the contract needs. */
  readonly protocol?: ProtocolVersion;
  readonly methods: M;
  readonly events?: E;
  /** Schema of the `hello.capabilities` object this contract expects. */
  readonly capabilities?: TSchema;
}

export type MethodParams<M extends MethodMap, K extends keyof M> = Static<M[K]['params']>;
export type MethodResult<M extends MethodMap, K extends keyof M> = Static<M[K]['result']>;
export type EventPayload<E extends EventMap, K extends keyof E> = Static<E[K]['payload']>;

/** Typed request surface over one session. */
export interface ContractClient<M extends MethodMap> {
  request<K extends keyof M & string>(
    method: K,
    params: MethodParams<M, K>,
    options?: RequestOptions
  ): Promise<MethodResult<M, K>>;
}

export type ContractHandlers<M extends MethodMap> = {
  readonly [K in keyof M]: (
    params: MethodParams<M, K>,
    context: HandlerContext
  ) => MethodResult<M, K> | Promise<MethodResult<M, K>>;
};

export interface ServeOptions {
  /**
   * Runs before every handler with the method's declared capability list.
   * Throw a `RemoteError` (normally `DENIED`) to refuse; the SDK adds no policy
   * of its own because consent, authorisation and their audit belong to the
   * application.
   */
  readonly guard?: (method: string, capabilities: readonly string[]) => void | Promise<void>;
  /** Validate results against the schema before sending; off by default. */
  readonly validateResults?: boolean;
}

export interface ContractEvents<E extends EventMap> {
  emit<K extends keyof E & string>(
    topic: K,
    payload: EventPayload<E, K>,
    options?: { readonly streamId?: string; readonly end?: true }
  ): boolean;
  on<K extends keyof E & string>(
    topic: K,
    listener: (payload: EventPayload<E, K>, frame: EventFrame) => void
  ): () => void;
}

export interface Contract<M extends MethodMap, E extends EventMap> {
  readonly definition: ContractDefinition<M, E>;
  /** The catalog document, validated against `catalog.json`. */
  catalog(): Catalog;
  client(session: Session): ContractClient<M>;
  /** Registers every handler on the session; returns the function that removes them. */
  serve(session: Session, handlers: ContractHandlers<M>, options?: ServeOptions): () => void;
  events(session: Session): ContractEvents<E>;
  /** Throws `RemoteError` `INVALID_PARAMS` naming the failing path. */
  assertParams<K extends keyof M & string>(
    method: K,
    params: unknown
  ): asserts params is MethodParams<M, K>;
}

/**
 * Describes an application contract once and derives a typed client, a typed
 * handler map with parameter validation, typed events and the catalog
 * document from it.
 *
 * @example
 * const contract = defineContract({
 *   name: 'example', version: '1.0.0',
 *   methods: { 'text.echo': { params: Type.Object({ text: Type.String() }), result: Type.Object({ text: Type.String() }) } },
 *   events: { 'text.tick': { payload: Type.Object({ at: Type.Number() }) } },
 * });
 * const off = contract.serve(session, { 'text.echo': (params) => params });
 * const { text } = await contract.client(session).request('text.echo', { text: 'hi' });
 */
export function defineContract<M extends MethodMap, E extends EventMap = Record<never, never>>(
  definition: ContractDefinition<M, E>
): Contract<M, E> {
  for (const method of Object.keys(definition.methods)) assertContractName('method', method);
  for (const topic of Object.keys(definition.events ?? {})) assertContractName('topic', topic);

  function assertParams<K extends keyof M & string>(
    method: K,
    params: unknown
  ): asserts params is MethodParams<M, K> {
    const schema = definitionFor(definition, method).params;
    if (Value.Check(schema, params)) return;
    const first = firstViolation(schema, params);
    throw new RemoteError(
      RESERVED_ERROR_CODES.INVALID_PARAMS,
      `Parameters of "${method}" do not match the contract${first ? ` at ${first.path}: ${first.message}` : ''}.`,
      { method, ...(first ? { path: first.path, reason: first.message } : {}) }
    );
  }

  return {
    definition,
    catalog: () => buildCatalog(definition),
    client: (session) => ({
      request: (method, params, options) =>
        session.request(method, params, options) as Promise<MethodResult<M, typeof method>>,
    }),
    serve: (session, handlers, options = {}) => {
      const removers = Object.entries(definition.methods).map(([method, entry]) =>
        session.handle(method, async (params, context) => {
          if (options.guard) await options.guard(method, entry.capabilities ?? []);
          assertParams(method, params);
          const handler = handlers[method as keyof M];
          const result = await handler(params as never, context);
          if (options.validateResults && !Value.Check(entry.result, result)) {
            const first = firstViolation(entry.result, result);
            throw new RemoteError(
              RESERVED_ERROR_CODES.INTERNAL,
              `Result of "${method}" does not match the contract${first ? ` at ${first.path}: ${first.message}` : ''}.`,
              { method, ...(first ? { path: first.path, reason: first.message } : {}) }
            );
          }
          return result;
        })
      );
      return () => {
        for (const remove of removers) remove();
      };
    },
    events: (session) => ({
      emit: (topic, payload, options = {}) =>
        session.emit({
          topic,
          payload,
          ...(options.streamId !== undefined ? { streamId: options.streamId } : {}),
          ...(options.end ? { end: true as const } : {}),
        }),
      on: (topic, listener) =>
        session.onEvent((frame) => {
          if (frame.topic !== topic) return;
          listener(frame.payload as EventPayload<E, typeof topic>, frame);
        }),
    }),
    assertParams,
  };
}

function definitionFor<M extends MethodMap>(
  definition: ContractDefinition<M, EventMap>,
  method: string
): MethodDefinition {
  const entry = definition.methods[method];
  if (!entry) {
    throw new RemoteError(
      RESERVED_ERROR_CODES.METHOD_UNSUPPORTED,
      `Method "${method}" is not part of contract "${definition.name}".`,
      { method, contract: definition.name }
    );
  }
  return entry;
}

function assertContractName(kind: 'method' | 'topic', name: string): void {
  if (isValidMethodName(name) && !isReservedMethodName(name)) return;
  throw new TypeError(
    `Contract ${kind} "${name}" is not a valid name; expected two or more dot-separated lowercase segments outside the reserved rpc. namespace.`
  );
}

/** Strips TypeBox's symbol metadata so the catalog is plain JSON. */
/** A schema violation rendered as a JSON pointer plus TypeBox's message. */
interface Violation {
  readonly path: string;
  readonly message: string;
}

/** RFC 6901: `~` then `/`, so a slash does not produce a nested pointer. */
function escapePointerToken(token: string): string {
  return token.replaceAll('~', '~0').replaceAll('/', '~1');
}

/**
 * The first violation of `schema` by `value`, pointing at the offending
 * property when the error names one (`required`, `additionalProperties`
 * point at the container). The document root renders as `/`.
 */
function firstViolation(schema: TSchema, value: unknown): Violation | undefined {
  const error = Value.Errors(schema, value)[0];
  if (!error) return undefined;
  const property = offendingProperty(error);
  const pointer = property
    ? `${error.instancePath}/${escapePointerToken(property)}`
    : error.instancePath;
  return { path: pointer || '/', message: error.message };
}

function offendingProperty(error: TLocalizedValidationError): string | undefined {
  if (error.keyword === 'required') return error.params.requiredProperties[0];
  if (error.keyword === 'additionalProperties') return error.params.additionalProperties[0];
  return undefined;
}

function plainSchema(schema: TSchema): Record<string, unknown> {
  return JSON.parse(JSON.stringify(schema)) as Record<string, unknown>;
}

function buildCatalog<M extends MethodMap, E extends EventMap>(
  definition: ContractDefinition<M, E>
): Catalog {
  const catalog = {
    name: definition.name,
    version: definition.version,
    ...(definition.description !== undefined ? { description: definition.description } : {}),
    ...(definition.protocol !== undefined ? { protocol: definition.protocol } : {}),
    methods: Object.entries(definition.methods).map(([name, entry]) => ({
      name,
      ...(entry.description !== undefined ? { description: entry.description } : {}),
      params: plainSchema(entry.params),
      result: plainSchema(entry.result),
      ...(entry.capabilities !== undefined ? { capabilities: [...entry.capabilities] } : {}),
      ...(entry.deprecated !== undefined ? { deprecated: entry.deprecated } : {}),
    })),
    events: Object.entries(definition.events ?? {}).map(([topic, entry]) => ({
      topic,
      ...(entry.description !== undefined ? { description: entry.description } : {}),
      payload: plainSchema(entry.payload),
      ...(entry.stream !== undefined ? { stream: entry.stream } : {}),
    })),
    ...(definition.capabilities !== undefined
      ? { capabilities: plainSchema(definition.capabilities) }
      : {}),
  };
  assertCatalog(catalog);
  return catalog;
}
