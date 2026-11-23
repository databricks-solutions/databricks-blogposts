// Geometry the canvas is currently showing, shared with code outside its render
// (the shape palette, node resize handlers) that needs to convert between a
// point on screen and a node's stored position.

import type { Rect } from './layout'

export const live: {
  /** Final rectangle of every node, relative to its parent. */
  rects: Record<string, Rect>
  /** The automatic layout's slot for every node, before hand offsets. */
  auto: Record<string, Rect>
} = { rects: {}, auto: {} }
