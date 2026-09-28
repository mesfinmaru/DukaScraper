import { useEffect, useState } from "react"
import { cn } from "../utils"
import { api } from "../api"

type HealthState = "loading" | "ready" | "degraded" | "down"

/**
 * Colored readiness dot for the whole platform.
 *
 * Uses `/ready` (readiness) rather than `/health` (liveness): the API can be
 * alive while Kafka/Postgres/ClickHouse are down, and a liveness-based pill
 * showed green in exactly the states users need to know about.
 *
 * green = all dependencies ready, amber = degraded / starting,
 * red = API unreachable or not ready, pulsing amber = still checking.
 *
 * Colors are hardcoded (not theme tokens) so the state is readable in BOTH
 * themes — green/amber/red mean the same thing everywhere; only the pill's
 * surface adapts via the `light:` overrides in index.css.
 */
export function HealthPill({ online }: { online: boolean | null }) {
  const [state, setState] = useState<HealthState>(online === null ? "loading" : online ? "ready" : "down")
  const [failed, setFailed] = useState<string[]>([])

  useEffect(() => {
    let cancelled = false
    const check = async () => {
      try {
        const res = await api.ready()
        if (cancelled) return
        // network error inside api.ready() -> throw
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
  // unreachable, the pill must be RED even if a cached /ready said otherwise.
  useEffect(() => {
    if (online === false) {
      setState("down")
      setFailed([])
    }
  }, [online])

  const dotClass: Record<HealthState, string> = {
    loading: "bg-amber-500 animate-pulse",
    ready: "bg-emerald-600",
    degraded: "bg-amber-500",
    down: "bg-rose-600",
  }

  const tooltip =
    state === "ready"
      ? "All platform dependencies are ready"
      : state === "loading"
        ? "Checking platform readiness…"
        : state === "down"
          ? "API is offline or unreachable"
          : `Degraded — failing: ${failed.length ? failed.join(", ") : "some dependencies"}. See Monitoring.`

  const degraded =
    state === "degraded" || state === "down"

  return (
    <span
      title={tooltip}
      className={cn(
        "inline-flex items-center gap-2 rounded-full border px-2.5 py-1 shadow-sm transition-colors",
        state === "down"
          ? "border-rose-500/60 bg-rose-500/10"
          : state === "degraded"
            ? "border-amber-500/60 bg-amber-500/10"
            : "health-pill-neutral",
      )}
    >
      <span className={cn("h-2 w-2 shrink-0 rounded-full", dotClass[state])} />
      {degraded && (
        <span
          className={cn(
            "max-w-[220px] truncate text-[10px] font-semibold tracking-wide",
            state === "down" ? "text-rose-600 health-label-down" : "text-amber-700 health-label-degraded",
          )}
        >
          {state === "down" ? "API DOWN" : failed.length ? `${failed.length} DOWN` : "DEGRADED"}
        </span>
      )}
    </span>
  )
}
