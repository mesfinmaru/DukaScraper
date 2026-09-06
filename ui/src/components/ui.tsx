import { useEffect, useState } from "react"
import type { ReactNode } from "react"
import { AlertTriangle, Check, Copy, Inbox, Loader2, X } from "lucide-react"
import type { JobStatus } from "../types"
import { cn, copyText } from "../utils"

export function Spinner({ className }: { className?: string }) {
  return <Loader2 className={cn("h-4 w-4 animate-spin", className)} aria-hidden />
}

export function LoadingBlock({ label = "Loading..." }: { label?: string }) {
  return (
    <div className="flex items-center justify-center gap-3 py-16 text-slate-500">
      <Spinner className="h-5 w-5" />
      <span className="text-sm">{label}</span>
    </div>
  )
}

export function ErrorBanner({ message, onRetry }: { message: string; onRetry?: () => void }) {
  return (
    <div className="flex flex-col gap-3 rounded-lg border border-rose-500/40 bg-rose-500/10 px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
      <div className="flex items-start gap-3">
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-rose-500" aria-hidden />
        <p className="text-sm break-words text-rose-300">{message}</p>
      </div>
      {onRetry && (
        <button
          type="button"
          onClick={onRetry}
          className="inline-flex shrink-0 items-center gap-1.5 rounded-lg border border-rose-500/50 bg-black px-3 py-1.5 text-xs font-semibold text-rose-400 transition hover:bg-rose-500/20"
        >
          Retry
        </button>
      )}
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
    <div className="flex flex-col items-center justify-center gap-2 px-6 py-16 text-center">
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

export function Modal({
  open,
  onClose,
  title,
  children,
}: {
  open: boolean
  onClose: () => void
  title: string
  children: ReactNode
}) {
  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose()
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [open, onClose])

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/70 backdrop-blur-sm" onClick={onClose} />
      <div className="card relative z-10 w-full max-w-md p-6 shadow-xl">
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-base font-semibold text-slate-100">{title}</h2>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            className="rounded-md p-1 text-slate-500 transition hover:bg-slate-900 hover:text-slate-200"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        {children}
      </div>
    </div>
  )
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
    <div className="mb-6 flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
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
