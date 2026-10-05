# OSC fork patches

This fork of Sofie Core is prepared for Eyevinn Open Source Cloud (OSC). Files added/changed vs upstream:

- `Dockerfile.osc`, `osc-entrypoint.sh` - OSC container build and platform wiring (PORT -> SOFIE_PORT, OSC_HOSTNAME -> ROOT_URL, DATABASE_URL -> MONGO_URL with a db name, volume at /data).
- **Polling replacement for MongoDB change streams** (needed for FerretDB, which has no change streams). Disabled unless `SOFIE_DB_CHANGE_POLLING=true`, so upstream behaviour is unchanged by default.
  - New: `packages/corelib/src/pollingChangeStream.ts` (`watchCollection()`, `PollingChangeStream`).
  - `packages/corelib/src/collectionChangeFeed.ts` - `createCollectionChangeStream` uses the polling stream when enabled (also skips the session `operationTime` capture, so no cluster time is needed).
  - `packages/job-worker/src/{main.ts,peripheralDevice.ts,db/collection.ts,workers/worker-set.ts}` - `collection.watch(...)` replaced by `watchCollection(collection, ...)` (grep for `OSC PATCH`).
  - Env: `SOFIE_DB_CHANGE_POLLING` (true/false), `SOFIE_DB_POLL_INTERVAL_MS` (default 2000, min 100).
  - Trade-offs: changes are seen after up to one poll interval; each watched collection is read in full every interval.

To rebase on upstream: re-apply the `OSC PATCH` call-site edits (one-line each) and keep `pollingChangeStream.ts`.
