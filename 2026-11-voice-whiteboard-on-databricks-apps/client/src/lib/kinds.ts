import {
  AppWindow,
  Box,
  Cloud,
  Database,
  FileText,
  Folder,
  GitFork,
  Play,
  Sparkles,
  Square,
  StickyNote,
  User,
  Waves,
  type LucideIcon,
} from 'lucide-react'
import type { BNode, ColorName, Kind } from './types'

export interface Swatch {
  fill: string
  line: string
  ink: string
  /** Icon and detail colour: the tint's parent hue at a step that passes AA on the fill. */
  accent: string
}

// Depth comes from tint, not heavy fills: each swatch is a brand hue at ~12% over white.
export const SWATCHES: Record<ColorName, Swatch> = {
  white: { fill: '#FFFFFF', line: '#C4CCD6', ink: '#0B2026', accent: '#5A6F77' },
  oat: { fill: '#FAF9F7', line: '#DCE0E2', ink: '#0B2026', accent: '#5A6F77' },
  blue: { fill: '#E4EEF6', line: '#A7C7E1', ink: '#04355D', accent: '#0E538B' },
  green: { fill: '#E0F5EE', line: '#99DDC7', ink: '#095A35', accent: '#00875C' },
  yellow: { fill: '#FFF5E0', line: '#FFDD99', ink: '#7D5319', accent: '#7D5319' },
  lava: { fill: '#FFE7E4', line: '#FFAFA6', ink: '#801C17', accent: '#BD2B26' },
  maroon: { fill: '#F3E2E5', line: '#D69FAA', ink: '#4A121A', accent: '#730D21' },
  navy: { fill: '#E4EAEC', line: '#A4B9C0', ink: '#0B2026', accent: '#1B5162' },
}

export interface KindSpec {
  label: string
  icon: LucideIcon
  color: ColorName
  /** Outline: most kinds are a card; flowchart kinds get their conventional shape. */
  shape: 'card' | 'diamond' | 'pill' | 'note'
  dashed?: boolean
  w: number
  h: number
}

export const KIND_SPECS: Record<Kind, KindSpec> = {
  service: { label: 'Service', icon: Box, color: 'white', shape: 'card', w: 184, h: 56 },
  database: { label: 'Data store', icon: Database, color: 'blue', shape: 'card', w: 184, h: 56 },
  storage: { label: 'File storage', icon: Folder, color: 'blue', shape: 'card', w: 184, h: 56 },
  queue: { label: 'Stream / queue', icon: Waves, color: 'green', shape: 'card', w: 184, h: 56 },
  process: { label: 'Step', icon: Square, color: 'white', shape: 'card', w: 184, h: 56 },
  decision: { label: 'Decision', icon: GitFork, color: 'yellow', shape: 'diamond', w: 184, h: 104 },
  terminator: { label: 'Start / end', icon: Play, color: 'navy', shape: 'pill', w: 156, h: 48 },
  actor: { label: 'Person / team', icon: User, color: 'oat', shape: 'card', w: 184, h: 56 },
  external: {
    label: 'External system',
    icon: Cloud,
    color: 'oat',
    shape: 'card',
    dashed: true,
    w: 184,
    h: 56,
  },
  model: { label: 'AI / model', icon: Sparkles, color: 'lava', shape: 'card', w: 184, h: 56 },
  app: { label: 'App / dashboard', icon: AppWindow, color: 'maroon', shape: 'card', w: 184, h: 56 },
  document: { label: 'Document', icon: FileText, color: 'white', shape: 'card', w: 184, h: 56 },
  note: { label: 'Note', icon: StickyNote, color: 'yellow', shape: 'note', w: 184, h: 88 },
}

/**
 * The Databricks-style artwork a kind shows on its card (keys of client/src/assets/icons).
 * Kinds without an entry (flowchart shapes, notes) keep their lucide glyph or none.
 */
export const KIND_ART: Partial<Record<Kind, string>> = {
  service: 'server',
  database: 'delta-table',
  storage: 'cloud-storage',
  queue: 'streaming',
  process: 'workflows',
  actor: 'user',
  model: 'machine-learning',
  app: 'dashboards',
  document: 'file-document',
}

/** A node's artwork: its own icon when a speaker named a product, else its kind's. */
export function artFor(n: BNode): string | undefined {
  if (n.type === 'group') return undefined
  return n.icon || KIND_ART[n.kind]
}

export const GROUP_MIN = { w: 240, h: 132 }
export const GROUP_PAD = { top: 44, side: 22, bottom: 22 }

/** Node size is a pure function of the document, so every client lays out identically. */
export function nodeSize(n: BNode): { w: number; h: number } {
  if (n.type === 'group') return { w: n.w ?? GROUP_MIN.w, h: n.h ?? GROUP_MIN.h }
  const spec = KIND_SPECS[n.kind] ?? KIND_SPECS.service
  const extra = n.detail && spec.shape === 'card' ? 18 : 0
  return { w: n.w ?? spec.w, h: n.h ?? spec.h + extra }
}

export function swatchFor(n: BNode): Swatch {
  if (n.type === 'group') return SWATCHES[n.color ?? 'navy']
  return SWATCHES[n.color ?? (KIND_SPECS[n.kind] ?? KIND_SPECS.service).color]
}
