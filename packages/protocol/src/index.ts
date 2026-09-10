/**
 * Mango Protocol SDK entry point: wire schemas, the codecs, version
 * negotiation, the close-code table and the error vocabulary.
 *
 * Everything reachable from here is browser-safe; no module imports `node:`.
 */

export * from './close';
export * from './errors';
export * from './version';
