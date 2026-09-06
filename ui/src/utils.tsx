import type { ReactNode } from "react"

export function cn(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(" ")
}

export function formatBytes(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined || Number.isNaN(bytes)) return "-"
  if (bytes < 1024) return `${bytes} B`
  const units = ["KB", "MB", "GB", "TB"]
  let value = bytes / 1024
  let i = 0
  while (value >= 1024 && i < units.length - 1) {
    value /= 1024
    i += 1
  }
  return `${value.toFixed(value >= 100 ? 0 : 1)} ${units[i]}`
}

export function formatNumber(n: number | null | undefined): string {
  if (n === null || n === undefined || Number.isNaN(n)) return "-"
  return n.toLocaleString("en-US")
}

/**
 * Parse an ISO timestamp from the API.
 *
 * The backend stores naive UTC `TIMESTAMP` columns and serializes them via
 * `.isoformat()` without a timezone suffix. JavaScript's `new Date()` treats
 * such strings as *local* time, which skews every relative time by the local
 * UTC offset. Normalize by appending `Z` when no offset is present.
 */
function parseApiDate(iso: string | null | undefined): Date | null {
  if (!iso) return null
  // Already offset-aware (ends with Z or ±HH:MM) — parse as-is.
  if (/[zZ]$/.test(iso) || /[+-]\d{2}:?\d{2}$/.test(iso)) {
    const d = new Date(iso)
    return Number.isNaN(d.getTime()) ? null : d
  }
  const d = new Date(`${iso}Z`)
  return Number.isNaN(d.getTime()) ? null : d
}

export function formatDateTime(iso: string | null | undefined): string {
  const d = parseApiDate(iso)
  if (!d) return iso ?? "-"
  return d.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  })
}

export function relativeTime(iso: string | null | undefined): string {
  const d = parseApiDate(iso)
  if (!d) return "-"
  const sec = Math.round((Date.now() - d.getTime()) / 1000)
  if (sec < -30) return formatDateTime(iso) // clock skew / future date
  if (sec < 60) return `${Math.max(sec, 0)}s ago`
  const min = Math.floor(sec / 60)
  if (min < 60) return `${min}m ago`
  const hr = Math.floor(min / 60)
  if (hr < 24) return `${hr}h ago`
  const day = Math.floor(hr / 24)
  if (day < 30) return `${day}d ago`
  return formatDateTime(iso)
}

export function formatDuration(
  startIso: string | null | undefined,
  endIso: string | null | undefined,
): string {
  const start = parseApiDate(startIso)
  const end = parseApiDate(endIso)
  if (!start || !end) return "-"
  const ms = end.getTime() - start.getTime()
  if (!Number.isFinite(ms) || ms < 0) return "-"
  if (ms < 1000) return `${ms} ms`
  const sec = ms / 1000
  if (sec < 60) return `${sec.toFixed(1)} s`
  const min = Math.floor(sec / 60)
  const rem = Math.round(sec % 60)
  return `${min}m ${rem}s`
}

export function languageLabel(code: string | null | undefined): string {
  switch ((code ?? "").toLowerCase()) {
    case "am":
      return "Amharic"
    case "en":
      return "English"
    case "unknown":
      return "Unknown"
    default:
      return code ? code.toUpperCase() : "-"
  }
}

export async function copyText(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text)
    return true
  } catch {
    return false
  }
}

export function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
}

/** Renders `text` with every occurrence of any token wrapped in a <mark>. */
export function highlightText(text: string, tokens: string[]): ReactNode {
  const valid = tokens.map((t) => t.trim()).filter((t) => t.length >= 2)
  if (!valid.length || !text) return text
  const re = new RegExp(`(${valid.map(escapeRegExp).join("|")})`, "gi")
  const parts = text.split(re)
  return parts.map((part, i) =>
    re.test(part) ? <mark key={i}>{part}</mark> : <span key={i}>{part}</span>,
  )
}
