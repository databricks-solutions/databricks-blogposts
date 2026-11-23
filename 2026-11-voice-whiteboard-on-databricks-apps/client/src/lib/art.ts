// Every SVG in assets/icons is bundled as text at build time, keyed by its file name.
// Databricks line art is recoloured to currentColor, so a node tints it with its accent.
const FILES = import.meta.glob('@/assets/icons/*.svg', {
  query: '?raw',
  import: 'default',
  eager: true,
}) as Record<string, string>

export const ART = new Map<string, string>(
  Object.entries(FILES).map(([path, svg]) => [path.slice(path.lastIndexOf('/') + 1, -4), svg]),
)

export function hasArt(key: string | null | undefined): key is string {
  return !!key && ART.has(key)
}

/** Databricks line pictograms (150-unit viewBox, 3-unit strokes): they need a heavier
 *  stroke when drawn at node size, or they read as pale smudges. */
export const LINE_ART = new Set(
  [...ART].filter(([, svg]) => svg.includes('viewBox="0 0 150 150"')).map(([key]) => key),
)
