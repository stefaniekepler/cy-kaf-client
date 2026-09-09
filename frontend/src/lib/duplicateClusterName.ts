export const DUPLICATE_NAME_SUFFIX = '-copy';

/**
 * Builds the name of a cluster duplicate: `<base>-copy`, or the first free
 * `<base>-copy-<n>` (n starting at 2) when that name is already taken.
 * Names are compared exactly, so casing differences never collide.
 */
export const nextDuplicateName = (
  base: string,
  existingNames: string[]
): string => {
  const taken = new Set(existingNames);
  const candidate = `${base}${DUPLICATE_NAME_SUFFIX}`;
  if (!taken.has(candidate)) return candidate;

  // At most taken.size + 1 numbered candidates can be occupied, so the loop
  // always terminates with a free name.
  for (let index = 2; index <= taken.size + 2; index += 1) {
    const numbered = `${candidate}-${index}`;
    if (!taken.has(numbered)) return numbered;
  }

  return `${candidate}-${taken.size + 2}`;
};
