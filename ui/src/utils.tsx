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

/**
 * Turn a machine value into words: `cyber_threat` -> `Cyber threat`.
 *
 * Used for filter dropdown labels, which are built from the values the API
 * reports rather than a hardcoded list, so they arrive in snake_case.
 */
export function humanize(value: string): string {
  const spaced = value.replace(/_/g, " ").trim()
  return spaced.charAt(0).toUpperCase() + spaced.slice(1)
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

/**
 * Date without the time. Used where a full timestamp would widen a table
 * column for no gain (e.g. the admin user list, which has to fit every
 * column on screen at once).
 */
export function formatDateOnly(iso: string | null | undefined): string {
  const d = parseApiDate(iso)
  if (!d) return "-"
  return d.toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
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

/**
 * Human-readable duration between two API timestamps.
 *
 * Both bounds go through `parseApiDate`. The backend sends naive UTC
 * (`2026-10-02T09:14:03`, no offset), which `new Date()` would read as local
 * time and skew the result by the browser's UTC offset - 180 minutes for a
 * UTC+3 browser.
 *
 * The seconds are rounded into the minute total *before* being split, rather
 * than each being rounded independently: rounding them separately lets the
 * seconds reach 60 (119.6s -> "1m 60s") while the minute count stays put.
 */
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
  const totalSec = Math.round(ms / 1000)
  if (totalSec < 60) return `${(ms / 1000).toFixed(1)} s`
  const min = Math.floor(totalSec / 60)
  const rem = totalSec % 60
  const hr = Math.floor(min / 60)
  // Hours only once they exist, but "1h 0m" beats a bare minute count.
  if (hr >= 1) return `${hr}h ${min % 60}m`
  return `${min}m ${rem}s`
}

export function languageLabel(code: string | null | undefined): string {
  switch ((code ?? "").toLowerCase()) {
    case "am":
      return "Amharic"
    case "en":
      return "English"
    default:
      // The system reports only AM and EN. Anything else - including codes
      // stored before that policy existed - reads as Unknown rather than
      // surfacing a language the product does not support.
      return "Unknown"
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

/** Length of the emailed 6-digit verification code. */
export const OTP_CODE_LENGTH = 6

/** A blank six-digit entry state for the verification screens. */
export function emptyOtpDigits(): string[] {
  return Array(OTP_CODE_LENGTH).fill("")
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
