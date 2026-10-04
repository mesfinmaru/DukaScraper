/**
 * Compact dropdown filter for tables.
 *
 * The pages used a mix of pill rows, native `<select>` elements and hand-rolled
 * popovers, so filters looked different on every page and some needed a second
 * click to apply. `SelectFilter` is one control used everywhere: click to open,
 * click an option to select it, the selection shows in the trigger.
 *
 * Behaviour worth noting:
 *
 *  - Keyboard: Escape closes, arrow keys move the highlight, Enter selects.
 *    A native `<select>` would be simpler but cannot show the "All" label with
 *    an option count, and its popup cannot be styled to match the app.
 *  - Clicking outside closes it, so the user is never trapped in the menu.
 *  - The trigger is a real `<button>` with `aria-expanded`/`aria-haspopup`, so
 *    screen readers announce it as a control rather than as loose text.
 */
import { useEffect, useId, useRef, useState } from "react"
import { Check, ChevronDown } from "lucide-react"
import { cn } from "../utils"

export interface SelectOption<T extends string> {
  value: T
  label: string
  /** Optional trailing count, e.g. the number of rows with this value. */
  count?: number
}

export interface SelectFilterProps<T extends string> {
  label: string
  value: T
  options: readonly SelectOption<T>[]
  onChange: (value: T) => void
  className?: string
  /** Shown when the selected value has no match in `options`. */
  allLabel?: string
}

export function SelectFilter<T extends string>({
  label,
  value,
  options,
  onChange,
  className,
  allLabel = "All",
}: SelectFilterProps<T>) {
  const [open, setOpen] = useState(false)
  const [highlight, setHighlight] = useState(0)
  const rootRef = useRef<HTMLDivElement>(null)
  const listId = useId()

  const selected = options.find((o) => o.value === value)
  const triggerText = selected?.label ?? allLabel

  // Close when focus or a click lands outside the control.
  useEffect(() => {
    if (!open) return
    const onPointerDown = (e: PointerEvent) => {
      if (!rootRef.current?.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener("pointerdown", onPointerDown)
    return () => document.removeEventListener("pointerdown", onPointerDown)
  }, [open])

  // Keep the highlighted row aligned with the current selection on open.
  useEffect(() => {
    if (!open) return
    const idx = options.findIndex((o) => o.value === value)
    setHighlight(idx >= 0 ? idx : 0)
  }, [open, options, value])

  const choose = (next: T) => {
    onChange(next)
    setOpen(false)
  }

  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Escape") {
      setOpen(false)
      return
    }
    if (e.key === "ArrowDown") {
      e.preventDefault()
      if (!open) {
        setOpen(true)
        return
      }
      setHighlight((h) => Math.min(h + 1, options.length - 1))
      return
    }
    if (e.key === "ArrowUp") {
      e.preventDefault()
      setHighlight((h) => Math.max(h - 1, 0))
      return
    }
    if (e.key === "Enter" && open) {
      e.preventDefault()
      const opt = options[highlight]
      if (opt) choose(opt.value)
    }
  }

  return (
    <div ref={rootRef} className={cn("relative", className)}>
      <button
        type="button"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-controls={open ? listId : undefined}
        onClick={() => setOpen((v) => !v)}
        onKeyDown={onKeyDown}
        title={`${label}: ${triggerText}`}
        className="input flex w-full cursor-pointer items-center justify-between gap-2 py-1.5 text-left text-xs"
      >
        <span className="min-w-0">
          <span className="text-slate-500">{label}: </span>
          <span className="truncate font-medium text-slate-200">{triggerText}</span>
        </span>
        <ChevronDown
          className={cn(
            "h-3.5 w-3.5 shrink-0 text-slate-500 transition-transform",
            open && "rotate-180",
          )}
        />
      </button>

      {open && (
        <ul
          id={listId}
          role="listbox"
          aria-label={label}
          className="absolute top-full right-0 left-0 z-30 mt-1 max-h-72 overflow-auto rounded-lg border border-slate-700 bg-slate-900 py-1 shadow-xl"
        >
          {options.map((opt, i) => {
            const isSelected = opt.value === value
            return (
              <li key={opt.value}>
                <button
                  type="button"
                  role="option"
                  aria-selected={isSelected}
                  onMouseEnter={() => setHighlight(i)}
                  onClick={() => choose(opt.value)}
                  className={cn(
                    "flex w-full cursor-pointer items-center justify-between gap-2 px-3 py-1.5 text-left text-xs",
                    i === highlight ? "bg-slate-800 text-slate-100" : "text-slate-300",
                  )}
                >
                  <span className="flex min-w-0 items-center gap-1.5">
                    {isSelected ? (
                      <Check className="h-3 w-3 shrink-0 text-emerald-400" />
                    ) : (
                      <span className="w-3 shrink-0" />
                    )}
                    <span className="truncate">{opt.label}</span>
                  </span>
                  {opt.count !== undefined && (
                    <span className="shrink-0 font-mono text-[10px] text-slate-500">
                      {opt.count}
                    </span>
                  )}
                </button>
              </li>
            )
          })}
        </ul>
      )}
    </div>
  )
}
