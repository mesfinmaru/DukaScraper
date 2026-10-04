import { useEffect, useState } from "react"
import { cn } from "../utils"
import { api, subscribeApiFailures, type ApiFailure } from "../api"

type HealthState = "loading" | "ready" | "degraded" | "down"

/**
 * The dot's colour, blink period and ring cadence are all driven by
 * [data-state] in index.css. This component only decides *which* state the
 * platform is in, so the animation can be retuned — or switched off entirely
 * for prefers-reduced-motion — without touching React.
 */

/**
 * Readiness dot for the whole platform — dot only, no surrounding pill, but it
 * blinks and pulses so a change in state is noticeable without watching the
 * header. See `.health-dot` in index.css for the severity-driven animation.
 *
 * Green ONLY when nothing is broken anywhere. Two sources of truth:
 *  - /ready poll  → infrastructure dependencies (postgres/kafka/minio/…).
 *  - request feed → every failed API call (5xx / network error) recorded by
 *    api.ts. Page-level breakage (dev-proxy 502, backend 500) never showed up
 *    in /ready, which is how the dot stayed green over a page full of errors.
 *
 * Amber = partially broken (a dependency or some API calls failing),
 * red = the API itself is unreachable, pulsing amber = still checking.
 * A short message (e.g. "analytics 502 · 2 deps down") sits beside the dot;
 * hover for the same summary as a tooltip.
 */
export function HealthPill({ online }: { online: boolean | null }) {
  const [state, setState] = useState<HealthState>(online === null ? "loading" : online ? "ready" : "down")
  const [failed, setFailed] = useState<string[]>([])
  const [reqFailures, setReqFailures] = useState<ApiFailure[]>([])

  // Latest failure per API service, updated live by api.ts.
  useEffect(() => subscribeApiFailures(setReqFailures), [])

  useEffect(() => {
    let cancelled = false
    const check = async () => {
      try {
        const res = await api.ready()
        if (cancelled) return
        setFailed(res.failed ?? [])
        setState(res.ready ? "ready" : "degraded")
      } catch {
        if (!cancelled) {
          setFailed([])
          setState("down")
        }
      }
    }
    void check()
    const t = setInterval(check, 20000)
    return () => {
      cancelled = true
      clearInterval(t)
    }
  }, [])

  // API liveness (from Layout's /health poll) is the floor: if the API is
  // unreachable, the dot must be RED even if a cached /ready said otherwise.
  useEffect(() => {
    if (online === false) {
      setState("down")
      setFailed([])
    }
  }, [online])

  const down = state === "down"
  const loading = state === "loading"

  // Short messages, most severe first. When the API is fully down, the
  // per-service "unreachable" entries are redundant noise — one line says it.
  const messages: string[] = []
  if (down) messages.push("API down")
  if (failed.length) messages.push(`${failed.length} dep${failed.length > 1 ? "s" : ""} down`)
  if (!down) {
    for (const f of reqFailures) {
      messages.push(f.status === 0 ? `${f.service} unreachable` : `${f.service} ${f.status}`)
    }
  }

  const dotState: HealthState = down ? "down" : loading ? "loading" : messages.length ? "degraded" : "ready"
  const tooltip = messages.length
    ? `${messages.join(" · ")} — see Monitoring for details`
    : "All platform dependencies are ready"

  return (
    <span title={tooltip} aria-live="polite" className="inline-flex items-center gap-1.5">
      <span className="health-dot shrink-0" data-state={dotState} />
      {messages.length > 0 && (
        <span
          className={cn(
            "max-w-[260px] truncate text-[10px] font-semibold tracking-wide",
            down ? "text-rose-600 health-label-down" : "text-amber-700 health-label-degraded",
          )}
        >
          {messages.join(" · ")}
        </span>
      )}
    </span>
  )
}
