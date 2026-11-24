// Local, per-tab canvas state: what is selected, what is being edited, and
// positions held by an in-progress drag or resize.

import { create } from 'zustand'
import type { Rect } from './layout'

interface UiState {
  selectedNodes: string[]
  selectedEdges: string[]
  editingNode: string | null
  editingEdge: string | null
  /** Keep the growing diagram in view while the AI draws. Off once the user pans. */
  follow: boolean
  transcriptOpen: boolean
  /** Rectangles the user is dragging or resizing right now; they win over the layout. */
  held: Record<string, Rect>
  heldVersion: number

  select: (nodes: string[], edges?: string[]) => void
  editNode: (id: string | null) => void
  editEdge: (id: string | null) => void
  setFollow: (on: boolean) => void
  toggleTranscript: () => void
  hold: (id: string, rect: Rect) => void
  release: (ids: string[]) => void
}

export const useUi = create<UiState>((set) => ({
  selectedNodes: [],
  selectedEdges: [],
  editingNode: null,
  editingEdge: null,
  follow: true,
  transcriptOpen: true,
  held: {},
  heldVersion: 0,

  select: (nodes, edges = []) => set({ selectedNodes: nodes, selectedEdges: edges }),
  editNode: (id) => set({ editingNode: id, editingEdge: null }),
  editEdge: (id) => set({ editingEdge: id, editingNode: null }),
  setFollow: (on) => set({ follow: on }),
  toggleTranscript: () => set((s) => ({ transcriptOpen: !s.transcriptOpen })),
  hold: (id, rect) =>
    set((s) => ({ held: { ...s.held, [id]: rect }, heldVersion: s.heldVersion + 1 })),
  release: (ids) =>
    set((s) => {
      const held = { ...s.held }
      for (const id of ids) delete held[id]
      return { held, heldVersion: s.heldVersion + 1 }
    }),
}))
