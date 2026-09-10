/** Option validation shared by the two codecs. */

/**
 * Reads one byte-ceiling option: the value when it is an integer at or above
 * the floor the spec allows, the fallback when it is absent.
 *
 * @example
 * resolveByteCeiling('maxFrameBytes', undefined, 16777216, 4096); // 16777216
 */
export function resolveByteCeiling(
  name: string,
  value: number | undefined,
  fallback: number,
  floor: number
): number {
  const ceiling = value ?? fallback;
  if (!Number.isInteger(ceiling) || ceiling < floor) {
    throw new RangeError(`${name} is ${ceiling}; expected an integer of at least ${floor}`);
  }
  return ceiling;
}
