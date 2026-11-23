import * as Tooltip from '@radix-ui/react-tooltip'
import { useCallback, useEffect, useState } from 'react'
import { BoardPage } from '@/pages/BoardPage'
import { Lobby } from '@/pages/Lobby'

const boardFromPath = () => /^\/b\/([a-z0-9]{6,24})\/?$/.exec(location.pathname)?.[1] ?? null

export default function App() {
  const [boardId, setBoardId] = useState(boardFromPath)

  // The URL is the state: a board link is shareable, and Back returns to the list.
  useEffect(() => {
    const onPop = () => setBoardId(boardFromPath())
    window.addEventListener('popstate', onPop)
    return () => window.removeEventListener('popstate', onPop)
  }, [])

  const go = useCallback((id: string | null) => {
    history.pushState(null, '', (id ? `/b/${id}` : '/') + location.search)
    setBoardId(id)
  }, [])

  return (
    <Tooltip.Provider delayDuration={350} skipDelayDuration={200}>
      {boardId ? (
        <BoardPage key={boardId} boardId={boardId} onHome={() => go(null)} />
      ) : (
        <Lobby onOpen={go} />
      )}
    </Tooltip.Provider>
  )
}
