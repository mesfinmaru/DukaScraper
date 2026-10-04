import { useCallback, useEffect, useRef, useState } from "react"
import type { ReactNode } from "react"
import { AlertTriangle, Check, Copy, Inbox, Loader2, X } from "lucide-react"
import type { JobStatus } from "../types"
import { cn, copyText } from "../utils"

export function Spinner({ className }: { className?: string }) {
  return <Loader2 className={cn("h-4 w-4 animate-spin", className)} aria-hidden />
}

export function LoadingBlock({ label = "Loading..." }: { label?: string }) {
  return (
    <div className="flex items-center justify-center gap-3 py-12 text-slate-500">
      <Spinner className="h-5 w-5" />
      <span className="text-sm">{label}</span>
    </div>
  )
}

export function ErrorBanner({ message, onRetry }: { message: string; onRetry?: () => void }) {
  return (
    <div className="msg msg-error items-center px-4 py-3 text-sm sm:justify-between">
      <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
      <p className="min-w-0 flex-1 break-words">{message}</p>
      {onRetry && (
        <button
          type="button"
          onClick={onRetry}
          className="btn-secondary shrink-0 px-3 py-1.5 text-xs"
        >
          Try again
        </button>
      )}
    </div>
  )
}

/** Inline message box. `tone` picks the palette; both themes get a readable
 *  pairing from the .msg-* rules in index.css. */
export function Msg({
  tone = "error",
  children,
  className,
}: {
  tone?: "error" | "success" | "warn"
  children: ReactNode
  className?: string
}) {
  return (
    <div
      role={tone === "error" ? "alert" : "status"}
      className={cn("msg", `msg-${tone}`, className)}
    >
      {children}
    </div>
  )
}

export function EmptyState({
  title,
  hint,
  icon,
}: {
  title: string
  hint?: string
  icon?: ReactNode
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 px-6 py-10 text-center">
      <div className="text-slate-600">{icon ?? <Inbox className="h-8 w-8" />}</div>
      <p className="text-sm font-medium text-slate-300">{title}</p>
      {hint && <p className="max-w-md text-xs text-slate-500">{hint}</p>}
    </div>
  )
}

const STATUS_STYLES: Record<JobStatus, { label: string; className: string }> = {
  pending: {
    label: "Pending",
    className: "border border-amber-500/40 bg-amber-500/10 text-amber-500",
  },
  running: {
    label: "Running",
    className: "border border-sky-500/40 bg-sky-500/10 text-sky-600",
  },
  paused: {
    label: "Paused",
    // Slate rather than amber: amber already means "waiting to start", and a
    // paused job is neither pending nor active.
    className: "border border-slate-500/40 bg-slate-500/10 text-slate-400",
  },
  completed: {
    label: "Completed",
    className: "border border-emerald-500/40 bg-emerald-500/10 text-emerald-600",
  },
  failed: {
    label: "Failed",
    className: "border border-rose-500/40 bg-rose-500/10 text-rose-500",
  },
  skipped: {
    label: "Skipped",
    className: "border border-slate-600 bg-slate-800/60 text-slate-300",
  },
  needs_review: {
    label: "Needs review",
    className: "border border-violet-500/40 bg-violet-500/10 text-violet-500",
  },
}

export function StatusBadge({ status }: { status: string }) {
  const style = STATUS_STYLES[status as JobStatus] ?? {
    label: status || "Unknown",
    className: "border border-slate-600 bg-slate-800/60 text-slate-300",
  }
  const animated = status === "running" || status === "pending"
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium whitespace-nowrap",
        style.className,
      )}
    >
      {animated && (
        <span className="relative flex h-1.5 w-1.5">
          <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-current opacity-60" />
          <span className="relative inline-flex h-1.5 w-1.5 rounded-full bg-current" />
        </span>
      )}
      {style.label}
    </span>
  )
}

export function WorkerBadge({ worker }: { worker: string }) {
  const map: Record<string, string> = {
    surface: "border border-sky-500/40 bg-sky-500/10 text-sky-600",
    deep: "border border-violet-500/40 bg-violet-500/10 text-violet-500",
    dark: "border border-slate-500 bg-slate-800/60 text-slate-300",
  }
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium capitalize whitespace-nowrap",
        map[worker.toLowerCase()] ?? "border border-slate-600 bg-slate-800/60 text-slate-300",
      )}
    >
      {worker}
    </span>
  )
}

export function CopyButton({ value, label }: { value: string; label?: string }) {
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    if (!copied) return
    const t = setTimeout(() => setCopied(false), 1500)
    return () => clearTimeout(t)
  }, [copied])

  return (
    <button
      type="button"
      onClick={() => {
        void copyText(value).then((ok) => ok && setCopied(true))
      }}
      title={`Copy ${label ?? "value"}`}
      className="inline-flex items-center gap-1.5 rounded-md border border-slate-700 bg-black px-2 py-1 text-[11px] font-medium text-slate-400 transition hover:border-slate-500 hover:text-slate-200"
    >
      {copied ? <Check className="h-3 w-3 text-emerald-500" /> : <Copy className="h-3 w-3" />}
      {copied ? "Copied" : (label ?? "Copy")}
    </button>
  )
}

const FOCUSABLE =
  'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'

export function Modal({
  open,
  onClose,
  title,
  children,
  hideClose,
}: {
  open: boolean
  onClose: () => void
  title: string
  children: ReactNode
  /** Confirmation dialogs send focus to their primary button instead, so the
   *  corner X would be a redundant stop in the tab order. */
  hideClose?: boolean
}) {
  const panelRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    // Remember where focus came from so closing returns the user to the button
    // they pressed - important for keyboard and screen-reader users.
    const restoreTo = document.activeElement as HTMLElement | null
    const { overflow } = document.body.style
    document.body.style.overflow = "hidden"

    // Wait a frame so the panel is laid out before we reach into it.
    const raf = requestAnimationFrame(() => {
      const panel = panelRef.current
      if (!panel) return
      const target =
        panel.querySelector<HTMLElement>("[data-autofocus]") ??
        panel.querySelector<HTMLElement>(FOCUSABLE)
      target?.focus()
    })

    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.stopPropagation()
        onClose()
        return
      }
      if (e.key !== "Tab") return
      // Cycle focus inside the dialog instead of escaping to the page behind.
      const panel = panelRef.current
      if (!panel) return
      const items = Array.from(panel.querySelectorAll<HTMLElement>(FOCUSABLE)).filter(
        (el) => el.offsetParent !== null,
      )
      if (items.length === 0) return
      const first = items[0]
      const last = items[items.length - 1]
      const active = document.activeElement
      if (e.shiftKey && (active === first || !panel.contains(active))) {
        e.preventDefault()
        last.focus()
      } else if (!e.shiftKey && active === last) {
        e.preventDefault()
        first.focus()
      }
    }

    document.addEventListener("keydown", onKey, true)
    return () => {
      cancelAnimationFrame(raf)
      document.removeEventListener("keydown", onKey, true)
      document.body.style.overflow = overflow
      restoreTo?.focus?.()
    }
  }, [open, onClose])

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/70 backdrop-blur-sm" onClick={onClose} />
      <div
        ref={panelRef}
        role="dialog"
        aria-modal="true"
        aria-label={title}
        className="card relative z-10 w-full max-w-md p-6 shadow-xl"
      >
        <div className="mb-4 flex items-center justify-between gap-3">
          <h2 className="text-base font-semibold text-slate-100">{title}</h2>
          {!hideClose && (
            <button
              type="button"
              onClick={onClose}
              aria-label="Close"
              className="rounded-md p-1 text-slate-500 transition hover:bg-slate-900 hover:text-slate-200"
            >
              <X className="h-4 w-4" />
            </button>
          )}
        </div>
        {children}
      </div>
    </div>
  )
}

export type ConfirmOptions = {
  title: string
  /** One short sentence. Keep it plain - the reader is not a developer. */
  message: ReactNode
  confirmLabel?: string
  cancelLabel?: string
  /** "danger" paints the confirm button red, for deletes and sign-outs. */
  tone?: "danger" | "default"
}

export function ConfirmDialog({
  options,
  onConfirm,
  onCancel,
}: {
  options: ConfirmOptions | null
  onConfirm: () => void
  onCancel: () => void
}) {
  if (!options) return null
  const {
    title,
    message,
    confirmLabel = "Yes",
    cancelLabel = "Cancel",
    tone = "default",
  } = options
  return (
    <Modal open onClose={onCancel} title={title} hideClose>
      <p className="text-sm leading-relaxed text-slate-300">{message}</p>
      <div className="mt-6 flex justify-end gap-2">
        <button type="button" className="btn-secondary" onClick={onCancel}>
          {cancelLabel}
        </button>
        <button
          type="button"
          data-autofocus
          className={tone === "danger" ? "btn-danger" : "btn-primary"}
          onClick={onConfirm}
        >
          {confirmLabel}
        </button>
      </div>
    </Modal>
  )
}

/**
 * Promise-based replacement for `window.confirm`.
 *
 *   const { confirm, confirmDialog } = useConfirm()
 *   if (!(await confirm({ title: "Delete user?", message: "...", tone: "danger" }))) return
 *   ...
 *   return <>{...}{confirmDialog}</>
 *
 * Call sites read the same as before, but the browser's native grey popup is
 * replaced by the app's own dialog.
 */
export function useConfirm() {
  const [options, setOptions] = useState<ConfirmOptions | null>(null)
  const resolver = useRef<((ok: boolean) => void) | null>(null)

  const confirm = useCallback((opts: ConfirmOptions) => {
    // A second confirm while one is open resolves the first as cancelled rather
    // than leaving its promise dangling forever.
    resolver.current?.(false)
    setOptions(opts)
    return new Promise<boolean>((resolve) => {
      resolver.current = resolve
    })
  }, [])

  const settle = useCallback((ok: boolean) => {
    setOptions(null)
    const resolve = resolver.current
    resolver.current = null
    resolve?.(ok)
  }, [])

  const confirmDialog = (
    <ConfirmDialog
      options={options}
      onConfirm={() => settle(true)}
      onCancel={() => settle(false)}
    />
  )

  return { confirm, confirmDialog }
}

export function PageHeader({
  title,
  description,
  actions,
}: {
  title: string
  description?: string
  actions?: ReactNode
}) {
  return (
    <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
      <div>
        <h1 className="text-2xl font-bold tracking-tight text-slate-100">{title}</h1>
        {description && <p className="mt-1 max-w-2xl text-sm text-slate-400">{description}</p>}
      </div>
      {actions && <div className="flex shrink-0 items-center gap-2">{actions}</div>}
    </div>
  )
}

export function InfoRow({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <dt className="text-xs font-medium tracking-wider text-slate-500 uppercase">{label}</dt>
      <dd className="text-sm break-all text-slate-200">{children}</dd>
    </div>
  )
}
