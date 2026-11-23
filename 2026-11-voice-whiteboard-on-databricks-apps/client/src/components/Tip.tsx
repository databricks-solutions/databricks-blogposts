import * as Tooltip from '@radix-ui/react-tooltip'
import type { ReactNode } from 'react'

/** Tooltip for icon-only controls. The control itself still needs an aria-label. */
export function Tip({
  label,
  keys,
  side = 'bottom',
  children,
}: {
  label: string
  keys?: string
  side?: 'top' | 'right' | 'bottom' | 'left'
  children: ReactNode
}) {
  return (
    <Tooltip.Root>
      <Tooltip.Trigger asChild>{children}</Tooltip.Trigger>
      <Tooltip.Portal>
        <Tooltip.Content
          side={side}
          sideOffset={8}
          className="z-50 flex items-center gap-2 rounded-md bg-navy-900 px-2 py-1 text-[12px] text-white shadow-float"
        >
          {label}
          {keys && <kbd className="font-mono text-[10.5px] text-navy-300">{keys}</kbd>}
        </Tooltip.Content>
      </Tooltip.Portal>
    </Tooltip.Root>
  )
}

export function IconButton({
  label,
  keys,
  side,
  active,
  disabled,
  onClick,
  children,
  className = '',
}: {
  label: string
  keys?: string
  side?: 'top' | 'right' | 'bottom' | 'left'
  active?: boolean
  disabled?: boolean
  onClick?: () => void
  children: ReactNode
  className?: string
}) {
  return (
    <Tip label={label} keys={keys} side={side}>
      <button
        type="button"
        aria-label={label}
        aria-pressed={active}
        disabled={disabled}
        onClick={onClick}
        className={`grid size-8 place-items-center rounded-md text-nav transition-colors hover:bg-oat-medium hover:text-ink active:bg-line disabled:pointer-events-none disabled:opacity-35 ${
          active ? 'bg-tint-navy text-ink' : ''
        } ${className}`}
      >
        {children}
      </button>
    </Tip>
  )
}
