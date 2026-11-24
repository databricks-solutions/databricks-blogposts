// Wire types shared with the server (server/board.py and server/hub.py).

export const KINDS = [
  'service',
  'database',
  'storage',
  'queue',
  'process',
  'decision',
  'terminator',
  'actor',
  'external',
  'model',
  'app',
  'document',
  'note',
] as const
export type Kind = (typeof KINDS)[number]

export const COLORS = ['white', 'oat', 'blue', 'green', 'yellow', 'lava', 'maroon', 'navy'] as const
export type ColorName = (typeof COLORS)[number]

export type Direction = 'RIGHT' | 'DOWN'

export interface BNode {
  id: string
  type: 'shape' | 'group'
  kind: Kind
  label: string
  detail: string
  parent: string | null
  /**
   * How far a person has moved this node from the slot the automatic layout gives
   * it. Null means "exactly where the layout puts it". Storing an offset rather
   * than a position lets a hand-placed node travel with its neighbours when the
   * layout re-flows, instead of being left behind on top of something else.
   */
  x: number | null
  y: number | null
  w: number | null
  h: number | null
  pinned: boolean
  color: ColorName | null
  /** A product icon a speaker named (an icon key); absent or null means the kind's art. */
  icon?: string | null
  /** A person edited what this node is; the AI's tidy pass leaves it alone. */
  touched: boolean
  by: 'ai' | 'user'
}

export interface BEdge {
  id: string
  source: string
  target: string
  label: string
  dashed: boolean
  touched: boolean
  by: 'ai' | 'user'
}

export type NodePatch = Partial<BNode> & { id: string }
export type EdgePatch = Partial<BEdge> & { id: string }

export type Op =
  | { t: 'node.put'; node: NodePatch }
  | { t: 'node.del'; id: string }
  | { t: 'edge.put'; edge: EdgePatch }
  | { t: 'edge.del'; id: string }
  | { t: 'board.set'; patch: { title?: string; direction?: Direction } }
  | { t: 'board.reflow' }
  | { t: 'board.order'; ids: string[] }
  | { t: 'board.clear' }

export interface Peer {
  id: string
  name: string
  email: string
  color: string
  cursor: [number, number] | null
  selection: string[]
}

export interface Segment {
  id: number
  user: string
  color: string
  text: string
  state: 'transcribing' | 'heard' | 'drawn' | 'skipped' | 'failed'
  source: 'voice' | 'typed'
  t: number
  ops?: number
  ms: {
    audio?: number
    stt?: number
    gate?: number | null
    first?: number | null
    shape?: number | null
    total?: number
  }
}

export interface MicState {
  holder: { id: string; name: string; color: string } | null
  speaking: boolean
}

export interface SttState {
  state: 'unknown' | 'warming' | 'ready' | 'error'
  detail: string
  endpoint: string
}

export type AiPhase = 'idle' | 'drawing' | 'refining'

/** Who a board is shared with, as shown to one participant. */
export interface AccessView {
  role: 'owner' | 'editor' | null
  owner: { email: string; name: string }
  /** restricted: the owner and invited people. workspace: anyone signed in who has the link. */
  link: 'restricted' | 'workspace'
  people: { email: string; name: string; via: 'invite' | 'link' }[]
}

/** The autosave indicator. `at` is when the board was last written (epoch seconds). */
export interface SaveState {
  state: 'saved' | 'saving' | 'error'
  at: number
}

export interface BoardDoc {
  id: string
  title: string
  direction: Direction
  nodes: BNode[]
  edges: BEdge[]
  rev: number
}

export type ServerMsg =
  | {
      type: 'init'
      you: Peer
      board: BoardDoc
      peers: Peer[]
      mic: MicState
      transcript: Segment[]
      stt: SttState
      ai: { phase: AiPhase }
      models: { fast: string; refine: string }
      access: AccessView
      save: SaveState
    }
  | { type: 'peers'; peers: Peer[] }
  | { type: 'cursor'; id: string; cursor: [number, number] | null }
  | { type: 'select'; id: string; ids: string[] }
  | { type: 'ops'; ops: Op[]; origin: string; rev: number }
  | ({ type: 'mic' } & MicState)
  | { type: 'mic.denied'; holder: string }
  | { type: 'mic.asked'; from: { id: string; name: string } }
  | { type: 'vad'; speaking: boolean }
  | { type: 'transcript'; seg: Segment }
  | { type: 'transcript.drop'; id: number }
  | { type: 'ai'; phase: AiPhase; refine?: { ops: number; total: number | null } }
  | ({ type: 'stt' } & SttState)
  | { type: 'notice'; level: 'info' | 'error'; text: string }
  | { type: 'pong'; t: number }
  | { type: 'gone' }
  | { type: 'denied'; owner: string }
  | ({ type: 'access' } & AccessView)
  | ({ type: 'save' } & SaveState)

export type ClientMsg =
  | { type: 'ops'; ops: Op[] }
  | { type: 'cursor'; x: number | null; y: number | null }
  | { type: 'select'; ids: string[] }
  | { type: 'mic.request' }
  | { type: 'mic.release' }
  | { type: 'mic.ask' }
  | { type: 'mic.give'; to: string }
  | { type: 'vad'; speaking: boolean }
  | { type: 'say'; text: string }
  | { type: 'refine' }
  | { type: 'ping'; t: number }

export interface BoardSummary {
  id: string
  title: string
  owner: string
  owner_email: string
  updated: number
  nodes: number
  link: 'restricted' | 'workspace'
  mine: boolean
  people: { name: string; color: string }[]
  live: boolean
}

/** The lobby: the caller's own boards, and the ones other people shared with them. */
export interface BoardList {
  mine: BoardSummary[]
  shared: BoardSummary[]
}
