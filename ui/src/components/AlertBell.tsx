/**
 * The nav badge for unread high-severity alerts.
 *
 * Mounted once in the header so the count updates regardless of which page the
 * operator is on — an alert badge that only refreshes while the Alerts page is
 * open tells you nothing while you are actually working.
 *
 * Polls on the shared `useAutoRefresh` hook, which already skips background
 * tabs and never stacks overlapping requests.
 */
import { useCallback, useState } from "react"
import { Link } from "react-router-dom"
import { Bell } from "lucide-react"
import { api } from "../api"
import { DEFAULT_REFRESH_MS, useAutoRefresh } from "../useAutoRefresh"
import { cn } from "../utils"

export function AlertBell() {
  const [unread, setUnread] = useState(0)

  const load = useCallback(async () => {
    try {
      const data = await api.getUnreadAlerts()
      setUnread(data.unread)
    } catch {
      // Leave the last known count rather than flashing the badge to zero on a
      // transient failure — a wrong "no alerts" is worse than a stale one.
    }
  }, [])

  useAutoRefresh({ load, intervalMs: DEFAULT_REFRESH_MS })

  return (
    <Link
      to="/alerts"
      title={unread > 0 ? `${unread} unread high-severity alert${unread === 1 ? "" : "s"}` : "Alerts"}
      aria-label={unread > 0 ? `Alerts, ${unread} unread` : "Alerts"}
      className={cn(
        "relative inline-flex items-center rounded-full p-2 transition",
        unread > 0
          ? "bg-rose-500/10 text-rose-400 hover:bg-rose-500/20"
          : "text-slate-400 hover:bg-white/5 hover:text-slate-100",
      )}
    >
      <Bell className="h-4 w-4" />
      {unread > 0 && (
        <span
          className={cn(
            "absolute -top-0.5 -right-0.5 flex h-4 min-w-4 items-center justify-center rounded-full px-1 text-[10px] font-bold",
            unread > 9 ? "bg-rose-600 text-white" : "bg-rose-500 text-white",
          )}
        >
          {unread > 99 ? "99+" : unread}
        </span>
      )}
    </Link>
  )
}