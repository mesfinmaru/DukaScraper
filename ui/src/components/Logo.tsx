import { useTheme } from "../theme"
import { cn } from "../utils"

/**
 * DukaScraper wordmark.
 *
 * The logo PNG is black-on-transparent, so on dark themes we invert it to
 * render white; on light themes it is used as-is.
 */
export function Logo({ className }: { className?: string }) {
  const { theme } = useTheme()
  const dark = theme === "dark"

  return (
    <span className={cn("inline-flex items-center", className)}>
      <img
        src="/duka_logo.png"
        alt="DukaScraper"
        className={cn("h-full w-auto", dark ? "invert" : undefined)}
        draggable={false}
      />
    </span>
  )
}
