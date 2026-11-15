/**
 * Where a slice of `text` that starts at `start` and holds at most `units` UTF-16 code units ends. A slice never
 * ends between the two halves of a surrogate pair, which would encode each half as U+FFFD.
 */
function sliceEnd(text: string, start: number, units: number): number {
    const end = Math.min(start + units, text.length)
    return end < text.length && isHighSurrogate(text.charCodeAt(end - 1)) ? end - 1 : end
}

function isHighSurrogate(unit: number): boolean {
    return unit >= 0xd800 && unit <= 0xdbff
}

export { sliceEnd }
