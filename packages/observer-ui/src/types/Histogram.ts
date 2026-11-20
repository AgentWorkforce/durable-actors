/** Bucket `i` covers `fromMs + i * bucketMs` up to the next bucket. */
export interface Histogram<T> {
    fromMs: number
    bucketMs: number
    buckets: T[]
}
