import { cn } from "../utils"

export function HealthPill({ online }: { online: boolean | null }) {
  const dot =
    online === null ? "bg-gray-400" : online ? "bg-emerald-500" : "bg-rose-500"
  const label = online === null ? "Checking" : online ? "API online" : "API offline"
  const text = online === false ? "text-rose-600" : "text-slate-200"
  return (
    <span
      className={cn(
        "inline-flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950 px-3 py-1.5 text-xs font-medium shadow-sm",
        text,
      )}
      title={online === false ? "FastAPI gateway unreachable" : "GET /health"}
    >
      <span className={cn("h-1.5 w-1.5 rounded-full", dot)} />
      {label}
    </span>
  )
}