// Microphone capture, voice-activity segmentation, and WAV framing.
//
// Speed comes from cutting speech into short utterances at natural pauses and
// sending each one the moment it ends, so transcription and drawing overlap
// with the next thing the speaker says instead of waiting for them to finish.

const RATE = 16000
const FRAME = 320 // 20 ms
const SILENCE_MS = 420 // pause that ends an utterance
const MAX_MS = 7000 // force a cut in a long, unbroken stretch
const MIN_VOICED_MS = 260 // shorter blips are coughs and clicks
const PREROLL_FRAMES = 12 // keep 240 ms before speech onset so first syllables survive
const TAIL_FRAMES = 8

export interface CaptureHandlers {
  onLevel: (rms: number) => void
  onSpeaking: (speaking: boolean) => void
  onSegment: (frame: ArrayBuffer, durationMs: number) => void
}

const WORKLET = `
class Tap extends AudioWorkletProcessor {
  constructor() { super(); this.buf = new Float32Array(1024); this.n = 0 }
  process(inputs) {
    const ch = inputs[0] && inputs[0][0]
    if (ch) for (let i = 0; i < ch.length; i++) {
      this.buf[this.n++] = ch[i]
      if (this.n === this.buf.length) { this.port.postMessage(this.buf.slice(0)); this.n = 0 }
    }
    return true
  }
}
registerProcessor('tap', Tap)`

function encodeWav(samples: Float32Array, seq: number, durationMs: number): ArrayBuffer {
  // 12-byte header (magic, sequence, duration) followed by a PCM16 mono WAV.
  const buf = new ArrayBuffer(12 + 44 + samples.length * 2)
  const v = new DataView(buf)
  const ascii = (at: number, s: string) => {
    for (let i = 0; i < s.length; i++) v.setUint8(at + i, s.charCodeAt(i))
  }
  ascii(0, 'WBA1')
  v.setUint32(4, seq, true)
  v.setUint32(8, durationMs, true)
  const o = 12
  ascii(o, 'RIFF')
  v.setUint32(o + 4, 36 + samples.length * 2, true)
  ascii(o + 8, 'WAVEfmt ')
  v.setUint32(o + 16, 16, true)
  v.setUint16(o + 20, 1, true)
  v.setUint16(o + 22, 1, true)
  v.setUint32(o + 24, RATE, true)
  v.setUint32(o + 28, RATE * 2, true)
  v.setUint16(o + 32, 2, true)
  v.setUint16(o + 34, 16, true)
  ascii(o + 36, 'data')
  v.setUint32(o + 40, samples.length * 2, true)
  for (let i = 0; i < samples.length; i++) {
    const s = Math.max(-1, Math.min(1, samples[i]))
    v.setInt16(o + 44 + i * 2, s < 0 ? s * 0x8000 : s * 0x7fff, true)
  }
  return buf
}

export class MicCapture {
  private ctx: AudioContext | null = null
  private stream: MediaStream | null = null
  private node: AudioWorkletNode | null = null
  private pending = new Float32Array(0)
  private resampleCarry = new Float32Array(0)
  private preroll: Float32Array[] = []
  private seg: Float32Array[] = []
  private segRms: number[] = []
  private inSpeech = false
  private above = 0
  private silence = 0
  private voiced = 0
  private noise = 0.004
  private seq = 0
  private readonly handlers: CaptureHandlers

  constructor(handlers: CaptureHandlers) {
    this.handlers = handlers
  }

  async start(): Promise<void> {
    this.stream = await navigator.mediaDevices.getUserMedia({
      audio: {
        channelCount: 1,
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
      },
    })
    // Asking for a 16 kHz context lets the browser do the resampling with a proper filter.
    try {
      this.ctx = new AudioContext({ sampleRate: RATE })
    } catch {
      this.ctx = new AudioContext()
    }
    const url = URL.createObjectURL(new Blob([WORKLET], { type: 'application/javascript' }))
    try {
      await this.ctx.audioWorklet.addModule(url)
    } finally {
      URL.revokeObjectURL(url)
    }
    const source = this.ctx.createMediaStreamSource(this.stream)
    this.node = new AudioWorkletNode(this.ctx, 'tap')
    this.node.port.onmessage = (ev: MessageEvent<Float32Array>) => this.ingest(ev.data)
    source.connect(this.node)
    // The graph is only pulled when it reaches the destination, so route the tap
    // there through a muted gain (nothing is played back).
    const mute = this.ctx.createGain()
    mute.gain.value = 0
    this.node.connect(mute).connect(this.ctx.destination)
    if (this.ctx.state === 'suspended') await this.ctx.resume()
  }

  stop(): void {
    if (this.inSpeech) this.finish(this.seg.length)
    this.node?.disconnect()
    this.node = null
    this.stream?.getTracks().forEach((t) => t.stop())
    this.stream = null
    void this.ctx?.close()
    this.ctx = null
    this.handlers.onLevel(0)
  }

  private ingest(chunk: Float32Array): void {
    const sr = this.ctx?.sampleRate ?? RATE
    const mono = sr === RATE ? chunk : this.resample(chunk, sr)
    const joined = new Float32Array(this.pending.length + mono.length)
    joined.set(this.pending)
    joined.set(mono, this.pending.length)
    let at = 0
    while (joined.length - at >= FRAME) {
      this.frame(joined.slice(at, at + FRAME))
      at += FRAME
    }
    this.pending = joined.slice(at)
  }

  /** Box-filter decimation; only used when the browser would not give a 16 kHz context. */
  private resample(chunk: Float32Array, sr: number): Float32Array {
    const input = new Float32Array(this.resampleCarry.length + chunk.length)
    input.set(this.resampleCarry)
    input.set(chunk, this.resampleCarry.length)
    const ratio = sr / RATE
    const n = Math.floor(input.length / ratio)
    const out = new Float32Array(n)
    for (let i = 0; i < n; i++) {
      const a = Math.floor(i * ratio)
      const b = Math.min(input.length, Math.floor((i + 1) * ratio))
      let sum = 0
      for (let j = a; j < b; j++) sum += input[j]
      out[i] = sum / Math.max(1, b - a)
    }
    this.resampleCarry = input.slice(Math.floor(n * ratio))
    return out
  }

  private frame(f: Float32Array): void {
    let sum = 0
    for (let i = 0; i < f.length; i++) sum += f[i] * f[i]
    const rms = Math.sqrt(sum / f.length)
    this.handlers.onLevel(rms)
    const threshold = Math.min(0.05, Math.max(0.011, this.noise * 3.2))

    if (!this.inSpeech) {
      // Track the room's noise floor only while nobody is talking.
      if (rms < threshold) this.noise = this.noise * 0.96 + rms * 0.04
      this.preroll.push(f)
      if (this.preroll.length > PREROLL_FRAMES) this.preroll.shift()
      this.above = rms > threshold ? this.above + 1 : 0
      if (this.above >= 3) {
        this.inSpeech = true
        this.seg = [...this.preroll]
        this.segRms = this.seg.map(() => threshold)
        this.preroll = []
        this.silence = 0
        this.voiced = this.above
        this.handlers.onSpeaking(true)
      }
      return
    }

    this.seg.push(f)
    this.segRms.push(rms)
    if (rms > threshold * 0.7) {
      this.silence = 0
      this.voiced++
    } else {
      this.silence++
    }
    if (this.silence * 20 >= SILENCE_MS) {
      this.finish(this.seg.length - this.silence + TAIL_FRAMES)
      this.inSpeech = false
      this.above = 0
      this.handlers.onSpeaking(false)
    } else if (this.seg.length * 20 >= MAX_MS) {
      // No pause for a long while: cut at the quietest recent frame, most likely a word gap.
      const from = this.seg.length - 75
      let cut = this.seg.length
      let quietest = Infinity
      for (let i = from; i < this.seg.length - 5; i++) {
        if (this.segRms[i] < quietest) {
          quietest = this.segRms[i]
          cut = i + 1
        }
      }
      const rest = this.seg.slice(cut)
      const restRms = this.segRms.slice(cut)
      this.finish(cut)
      this.seg = rest
      this.segRms = restRms
      this.voiced = rest.length
    }
  }

  private finish(frames: number): void {
    const take = this.seg.slice(0, Math.max(0, Math.min(frames, this.seg.length)))
    const voicedMs = this.voiced * 20
    this.seg = []
    this.segRms = []
    this.voiced = 0
    this.silence = 0
    if (voicedMs < MIN_VOICED_MS || take.length === 0) return
    const samples = new Float32Array(take.length * FRAME)
    take.forEach((f, i) => samples.set(f, i * FRAME))
    const ms = take.length * 20
    this.handlers.onSegment(encodeWav(samples, this.seq++, ms), ms)
  }
}
