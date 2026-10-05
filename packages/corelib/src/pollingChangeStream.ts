/**
 * OSC PATCH (eyevinn-osaas fork): polling replacement for MongoDB change streams.
 *
 * Upstream Sofie Core requires MongoDB change streams (replica set). Some MongoDB-wire-compatible
 * databases (e.g. FerretDB) do not implement them. When SOFIE_DB_CHANGE_POLLING=true, every place that
 * would call `collection.watch(...)` instead uses {@link watchCollection}, which returns a
 * {@link PollingChangeStream}: it periodically reads the collection, diffs against the previous read and
 * emits synthetic `change` events shaped like the subset of ChangeStreamDocument that Sofie consumes
 * (`operationType`, `documentKey._id`, `fullDocument`). Synthetic events carry no `clusterTime`.
 *
 * Environment:
 *  - SOFIE_DB_CHANGE_POLLING   "true"/"1" enables polling. Default: off (upstream behaviour, real change streams).
 *  - SOFIE_DB_POLL_INTERVAL_MS poll interval in ms (min 100, default 2000).
 *
 * Keep this file and its call sites small so the fork can be rebased on upstream.
 */
import { EventEmitter } from 'events'
import { createHash } from 'crypto'
import type { ChangeStream, Collection, Document } from 'mongodb'

export const DEFAULT_POLL_INTERVAL_MS = 2000
const MAX_CONSECUTIVE_FAILURES = 5

export function isChangePollingEnabled(): boolean {
	const v = process.env.SOFIE_DB_CHANGE_POLLING
	return v === 'true' || v === '1'
}

export function getChangePollIntervalMs(): number {
	const n = Number(process.env.SOFIE_DB_POLL_INTERVAL_MS)
	return Number.isFinite(n) && n >= 100 ? n : DEFAULT_POLL_INTERVAL_MS
}

/** Translate the `$match` stages of a change stream pipeline into a plain collection filter */
export function pipelineToFilter(pipeline: Document[] | undefined): Document {
	const filter: Document = {}
	for (const stage of pipeline ?? []) {
		const match = (stage as any)?.$match
		if (!match) continue
		for (const [key, value] of Object.entries(match)) {
			if (key === 'documentKey._id') filter['_id'] = value
			else if (key.startsWith('fullDocument.')) filter[key.slice('fullDocument.'.length)] = value
		}
	}
	return filter
}

function hashDoc(doc: Document): string {
	return createHash('sha1').update(JSON.stringify(doc)).digest('base64')
}

export interface PollingChangeStreamOptions {
	intervalMs?: number
	/** Emit an `insert` for every matching document on the first read (default: false, first read is a silent baseline) */
	emitInitial?: boolean
}

export class PollingChangeStream extends EventEmitter {
	readonly #collection: Collection<any>
	readonly #filter: Document
	readonly #intervalMs: number
	readonly #emitInitial: boolean

	#known = new Map<string, { id: unknown; hash: string }>()
	#initialised = false
	#closed = false
	#running = false
	#failures = 0
	#timer: ReturnType<typeof setTimeout> | undefined

	constructor(collection: Collection<any>, filter: Document, options?: PollingChangeStreamOptions) {
		super()
		this.#collection = collection
		this.#filter = filter
		this.#intervalMs = options?.intervalMs ?? getChangePollIntervalMs()
		this.#emitInitial = !!options?.emitInitial
	}

	get closed(): boolean {
		return this.#closed
	}

	/** Take the baseline read (throws if the read fails), then start polling. Used when the caller can await. */
	async start(): Promise<void> {
		await this.#poll()
		this.#schedule()
	}

	/** Start polling without awaiting the baseline (used by synchronous `watch()`-style callers) */
	startInBackground(): void {
		this.#timer = setTimeout(() => this.#tick(), 0)
	}

	async close(): Promise<void> {
		this.#closed = true
		if (this.#timer) clearTimeout(this.#timer)
		this.#timer = undefined
	}

	#schedule(): void {
		if (this.#closed) return
		this.#timer = setTimeout(() => this.#tick(), this.#intervalMs)
	}

	#tick(): void {
		if (this.#closed || this.#running) return
		this.#running = true
		this.#poll()
			.then(() => {
				this.#failures = 0
			})
			.catch((e) => {
				this.#failures++
				if (this.#failures >= MAX_CONSECUTIVE_FAILURES) this.#fail(e)
			})
			.finally(() => {
				this.#running = false
				this.#schedule()
			})
	}

	#fail(e: unknown): void {
		this.#closed = true
		// Mirror ChangeStream: 'error' if someone listens (avoids an unhandled 'error' throw), always 'end'
		if (this.listenerCount('error') > 0) this.emit('error', e instanceof Error ? e : new Error(String(e)))
		this.emit('end')
	}

	async #poll(): Promise<void> {
		const docs = await this.#collection.find(this.#filter).toArray()
		const next = new Map<string, { id: unknown; hash: string }>()
		const events: Document[] = []

		for (const doc of docs) {
			const key = String(doc._id)
			const hash = hashDoc(doc)
			next.set(key, { id: doc._id, hash })
			const prev = this.#known.get(key)
			if (!prev) {
				if (this.#initialised || this.#emitInitial)
					events.push({ operationType: 'insert', documentKey: { _id: doc._id }, fullDocument: doc })
			} else if (prev.hash !== hash) {
				events.push({ operationType: 'update', documentKey: { _id: doc._id }, fullDocument: doc })
			}
		}
		if (this.#initialised) {
			for (const [key, prev] of this.#known) {
				if (!next.has(key)) events.push({ operationType: 'delete', documentKey: { _id: prev.id } })
			}
		}

		this.#known = next
		this.#initialised = true
		if (this.#closed) return
		for (const ev of events) {
			try {
				this.emit('change', ev)
			} catch {
				// a throwing listener must not break polling
			}
		}
	}
}

/**
 * Drop-in for `collection.watch(pipeline, options)`. Returns a real change stream unless polling is
 * enabled (SOFIE_DB_CHANGE_POLLING), in which case a {@link PollingChangeStream} is returned that
 * implements the subset of `ChangeStream` Sofie uses (`on`, `close`, `closed`).
 */
export function watchCollection<T extends Document = Document>(
	collection: Collection<any>,
	pipeline: Document[],
	options?: Parameters<Collection<any>['watch']>[1],
	pollingOptions?: PollingChangeStreamOptions
): ChangeStream<T> {
	if (!isChangePollingEnabled()) return collection.watch(pipeline, options) as unknown as ChangeStream<T>

	const stream = new PollingChangeStream(collection, pipelineToFilter(pipeline), pollingOptions)
	stream.startInBackground()
	return stream as unknown as ChangeStream<T>
}
