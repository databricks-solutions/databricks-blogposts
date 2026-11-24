import type { CSSProperties } from 'react'
import { ART, LINE_ART } from './art'

/** A bundled icon by key. Renders nothing for an unknown key. */
export function ArtIcon({
  name,
  size,
  style,
}: {
  name: string
  size: number
  style?: CSSProperties
}) {
  const svg = ART.get(name)
  if (!svg) return null
  return (
    <span
      aria-hidden
      className={`inline-grid shrink-0 place-items-center [&>svg]:block [&>svg]:size-full ${LINE_ART.has(name) ? 'wb-line-art' : ''}`}
      style={{ width: size, height: size, ...style }}
      // Trusted: these are our own bundled files, sanitised at copy time (no scripts, no handlers).
      dangerouslySetInnerHTML={{ __html: svg }}
    />
  )
}
