// The layout engine for the app: ELK running in a web worker.
//
// A dense board takes ELK a couple of hundred milliseconds, and the layout is
// recomputed as the AI draws. On the main thread that would freeze animation
// and input each time; in a worker the page stays smooth.

import ELK from 'elkjs/lib/elk-api'
import { setLayoutEngine } from './layout'

setLayoutEngine(
  new ELK({
    workerFactory: () => new Worker(new URL('elkjs/lib/elk-worker.min.js', import.meta.url)),
  }),
)
