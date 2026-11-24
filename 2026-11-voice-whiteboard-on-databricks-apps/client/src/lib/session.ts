// Live session state for one board: the document, who is here, who holds the
// mic, the transcript, and the socket that keeps them in sync.

import { create } from 'zustand'
import { applyOps, emptyDoc, invertOps, type Doc } from './doc'
import type {
  AccessView,
  AiPhase,
  ClientMsg,
  MicState,
  Op,
  Peer,
  SaveState,
  Segment,
  ServerMsg,
  SttState,
} from './types'

export interface Notice {
  id: number
  level: 'info' | 'error'
  text: string
  action?: { label: string; run: () => void }
}

type Status = 'connecting' | 'open' | 'reconnecting' | 'gone' | 'denied'

interface SessionState {
  boardId: string | null
  status: Status
  /** Who owns the board, when access was refused. */
  deniedBy: string
  me: Peer | null
  /** Who the board is shared with, and my role on it. */
  access: AccessView | null
  /** Autosave: whether what is on screen has reached the database. */
  save: SaveState
  doc: Doc
  peers: Peer[]
  mic: MicState
  transcript: Segment[]
  ai: { phase: AiPhase; refine?: { ops: number; total: number | null } }
  stt: SttState
  models: { fast: string; refine: string }
  notices: Notice[]
  /** Node id -> time the AI last changed it, for the brief highlight. */
  flashes: Record<string, number>
  /** Time of the most recent AI change; the canvas follows the drawing while this is fresh. */
  lastAiChange: number
  undoStack: Op[][]
  redoStack: Op[][]

  connect: (boardId: string) => void
  disconnect: () => void
  send: (msg: ClientMsg) => void
  sendAudio: (frame: ArrayBuffer) => void
  /** Apply ops locally and send them. `inverse` overrides the computed undo step. */
  commit: (ops: Op[], opts?: { undoable?: boolean; inverse?: Op[] }) => void
  undo: () => void
  redo: () => void
  notify: (level: Notice['level'], text: string, action?: Notice['action']) => void
  dismiss: (id: number) => void
}

let socket: WebSocket | null = null
let reconnectTimer: number | undefined
let pingTimer: number | undefined
let attempts = 0
let noticeSeq = 0
let wanted: string | null = null

const INITIAL = {
  boardId: null,
  status: 'connecting' as Status,
  me: null,
  doc: emptyDoc(),
  peers: [],
  mic: { holder: null, speaking: false },
  transcript: [],
  ai: { phase: 'idle' as AiPhase },
  deniedBy: '',
  access: null as AccessView | null,
  save: { state: 'saved', at: 0 } as SaveState,
  stt: { state: 'unknown' as const, detail: '', endpoint: '' },
  models: { fast: '', refine: '' },
  notices: [],
  flashes: {},
  lastAiChange: 0,
  undoStack: [],
  redoStack: [],
}

export const useSession = create<SessionState>((set, get) => {
  const notify: SessionState['notify'] = (level, text, action) => {
    const id = ++noticeSeq
    set((s) => ({ notices: [...s.notices.slice(-3), { id, level, text, action }] }))
    window.setTimeout(() => get().dismiss(id), action ? 12000 : 5000)
  }

  const onMessage = (m: ServerMsg) => {
    switch (m.type) {
      case 'init': {
        const nodes = Object.fromEntries(m.board.nodes.map((n) => [n.id, n]))
        set({
          status: 'open',
          me: m.you,
          doc: {
            title: m.board.title,
            direction: m.board.direction,
            nodes,
            order: m.board.nodes.map((n) => n.id),
            edges: Object.fromEntries(m.board.edges.map((e) => [e.id, e])),
          },
          peers: m.peers,
          mic: m.mic,
          transcript: m.transcript,
          stt: m.stt,
          ai: m.ai,
          models: m.models,
          access: m.access,
          save: m.save,
        })
        break
      }
      case 'peers':
        set({ peers: m.peers })
        break
      case 'cursor':
        set((s) => ({
          peers: s.peers.map((p) => (p.id === m.id ? { ...p, cursor: m.cursor } : p)),
        }))
        break
      case 'select':
        set((s) => ({
          peers: s.peers.map((p) => (p.id === m.id ? { ...p, selection: m.ids } : p)),
        }))
        break
      case 'ops': {
        const fromAi = m.origin.startsWith('ai:')
        set((s) => {
          const next: Partial<SessionState> = { doc: applyOps(s.doc, m.ops) }
          if (fromAi) {
            const now = Date.now()
            const flashes = { ...s.flashes }
            for (const op of m.ops) if (op.t === 'node.put') flashes[op.node.id] = now
            next.flashes = flashes
            next.lastAiChange = now
          }
          return next
        })
        break
      }
      case 'mic':
        set({ mic: { holder: m.holder, speaking: m.speaking } })
        break
      case 'vad':
        set((s) => ({ mic: { ...s.mic, speaking: m.speaking } }))
        break
      case 'mic.denied':
        notify('info', `${m.holder} has the mic. One person talks at a time.`)
        break
      case 'mic.asked':
        notify('info', `${m.from.name} is asking for the mic.`, {
          label: 'Hand over',
          run: () => get().send({ type: 'mic.give', to: m.from.id }),
        })
        break
      case 'transcript':
        set((s) => {
          const i = s.transcript.findIndex((x) => x.id === m.seg.id)
          const transcript = [...s.transcript]
          if (i >= 0) transcript[i] = m.seg
          else transcript.push(m.seg)
          return { transcript: transcript.slice(-300) }
        })
        break
      case 'transcript.drop':
        set((s) => ({ transcript: s.transcript.filter((x) => x.id !== m.id) }))
        break
      case 'ai':
        set({ ai: { phase: m.phase, refine: m.refine } })
        break
      case 'stt':
        set({ stt: { state: m.state, detail: m.detail, endpoint: m.endpoint } })
        break
      case 'notice':
        notify(m.level, m.text)
        break
      case 'access':
        set({ access: { role: m.role, owner: m.owner, link: m.link, people: m.people } })
        break
      case 'save':
        set({ save: { state: m.state, at: m.at } })
        break
      case 'denied':
        wanted = null
        set({ status: 'denied', deniedBy: m.owner })
        break
      case 'gone':
        wanted = null
        set({ status: 'gone' })
        break
      case 'pong':
        break
    }
  }

  const open = () => {
    if (!wanted) return
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:'
    const as = new URLSearchParams(location.search).get('as')
    const ws = new WebSocket(
      `${proto}//${location.host}/ws/${wanted}${as ? `?as=${encodeURIComponent(as)}` : ''}`,
    )
    ws.binaryType = 'arraybuffer'
    socket = ws
    ws.onopen = () => {
      attempts = 0
      window.clearInterval(pingTimer)
      pingTimer = window.setInterval(() => get().send({ type: 'ping', t: Date.now() }), 20000)
    }
    ws.onmessage = (ev) => {
      if (typeof ev.data !== 'string') return
      try {
        onMessage(JSON.parse(ev.data) as ServerMsg)
      } catch (err) {
        console.error('bad message from server', err)
      }
    }
    ws.onclose = () => {
      window.clearInterval(pingTimer)
      if (socket !== ws) return
      socket = null
      if (!wanted) return
      set({ status: 'reconnecting', mic: { holder: null, speaking: false } })
      const delay = Math.min(5000, 400 * 2 ** attempts++)
      reconnectTimer = window.setTimeout(open, delay)
    }
  }

  return {
    ...INITIAL,
    notify,
    dismiss: (id) => set((s) => ({ notices: s.notices.filter((n) => n.id !== id) })),

    connect: (boardId) => {
      get().disconnect()
      wanted = boardId
      set({ ...INITIAL, boardId, doc: emptyDoc() })
      open()
    },
    disconnect: () => {
      wanted = null
      window.clearTimeout(reconnectTimer)
      window.clearInterval(pingTimer)
      const ws = socket
      socket = null
      ws?.close()
    },
    send: (msg) => {
      if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify(msg))
    },
    sendAudio: (frame) => {
      if (socket?.readyState === WebSocket.OPEN) socket.send(frame)
    },

    commit: (ops, opts) => {
      if (!ops.length) return
      const { doc, undoStack } = get()
      const undoable = opts?.undoable ?? true
      const inverse = undoable ? (opts?.inverse ?? invertOps(doc, ops)) : null
      set({
        doc: applyOps(doc, ops),
        ...(inverse?.length
          ? { undoStack: [...undoStack.slice(-99), inverse], redoStack: [] }
          : {}),
      })
      get().send({ type: 'ops', ops })
    },
    undo: () => {
      const { undoStack, redoStack, doc } = get()
      const step = undoStack.at(-1)
      if (!step) return
      const redo = invertOps(doc, step)
      set({
        doc: applyOps(doc, step),
        undoStack: undoStack.slice(0, -1),
        redoStack: [...redoStack, redo],
      })
      get().send({ type: 'ops', ops: step })
    },
    redo: () => {
      const { undoStack, redoStack, doc } = get()
      const step = redoStack.at(-1)
      if (!step) return
      const undo = invertOps(doc, step)
      set({
        doc: applyOps(doc, step),
        redoStack: redoStack.slice(0, -1),
        undoStack: [...undoStack, undo],
      })
      get().send({ type: 'ops', ops: step })
    },
  }
})

export const iHoldMic = (s: SessionState) => !!s.me && s.mic.holder?.id === s.me.id
