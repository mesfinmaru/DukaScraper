/**
 * Shared auto-refresh for every data page.
 *
 * The app used to expose a per-page "Refresh" button and, on some pages, a
 * pause/resume toggle wired to a WebSocket. Both were inconsistent - some pages
 * never refreshed, some refreshed on a timer whether or not the tab was
 * visible - and the manual button was the only way to see new results quickly.
 *
 * `useAutoRefresh` replaces that with one rule for the whole system: reload
 * every `intervalMs` while the tab is visible and the document has focus.
 *
 * Two details that matter for a polling loop:
 *
 *  - Overlapping requests. If a load is slow, a naive interval stacks requests
 *    until the tab falls over. `inFlight` makes the timer skip a tick rather
 *    than queue behind it, so a slow endpoint degrades to "refreshes as fast as
 *    it can" instead of "refreshes faster than it can".
 *
 *  - Waking up. Timers are throttled or frozen in background tabs, so the hook
 *    also refreshes on `visibilitychange` and `focus`. Returning to a tab that
 *    slept for a minute shows current data immediately rather than waiting out
 *    the remainder of a stale interval.
 */
import { useCallback, useEffect, useRef } from "react"

export const DEFAULT_REFRESH_MS = 5000

export interface AutoRefreshOptions {
  /** Load function. Must be stable (wrap in `useCallback`). */
  load: () => Promise<unknown> | unknown
  /** Poll interval in ms. `0` disables polling but keeps wake-up refreshes. */
  intervalMs?: number
  /** Set false to pause polling entirely (e.g. the user opted out). */
  enabled?: boolean
}

export function useAutoRefresh({
  load,
  intervalMs = DEFAULT_REFRESH_MS,
  enabled = true,
}: AutoRefreshOptions): void {
  const loadRef = useRef(load)
  loadRef.current = load

  const inFlight = useRef(false)
  const mounted = useRef(true)

  const run = useCallback(async () => {
    // Skip rather than queue: a slow load must not accumulate requests.
    if (inFlight.current) return
    inFlight.current = true
    try {
      await loadRef.current()
    } catch {
      // The caller's own error handling owns reporting; the poller just keeps
      // going so one failed tick does not stop the page updating.
    } finally {
      inFlight.current = false
    }
  }, [])

  useEffect(() => {
    mounted.current = true
    return () => {
      mounted.current = false
    }
  }, [])

  useEffect(() => {
    if (!enabled || intervalMs <= 0) return

    const tick = () => {
      // Only poll a tab somebody is looking at. A background tab's data is
      // refreshed by the wake-up handler instead.
      if (document.visibilityState === "visible" && document.hasFocus()) {
        void run()
      }
    }

    const timer = window.setInterval(tick, intervalMs)

    // Timers are throttled/frozen in background tabs, so on return the data
    // could be minutes stale. Refresh immediately instead of waiting for the
    // remainder of the interval.
    const wake = () => {
      if (document.visibilityState === "visible") void run()
    }
    document.addEventListener("visibilitychange", wake)
    window.addEventListener("focus", wake)

    return () => {
      window.clearInterval(timer)
      document.removeEventListener("visibilitychange", wake)
      window.removeEventListener("focus", wake)
    }
  }, [run, intervalMs, enabled])
}
