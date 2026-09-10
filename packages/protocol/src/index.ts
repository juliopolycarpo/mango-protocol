/**
 * Mango Protocol SDK entry point: wire schemas, the codecs, version
 * negotiation, the close-code table, the error vocabulary, and the session over
 * any port.
 *
 * Everything reachable from here is browser-safe; no module imports `node:`.
 */

export * from './close';
export * from './codec';
export * from './errors';
export * from './port';
export * from './schemas';
export * from './session';
export * from './version';
