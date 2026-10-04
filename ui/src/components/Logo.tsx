import { useEffect, useState } from "react"
import { useTheme } from "../theme"
import { cn } from "../utils"

/**
 * DukaScraper wordmark.
 *
 * The logo PNG is black-on-transparent, so on dark themes we invert it to
 * render white; on light themes it is used as-is.
 *
 * While the bitmap is still loading — or if it fails outright — this renders
 * the word "DukaScraper" as text at the same visual weight. A brand mark that
 * silently collapses to an empty box while the network is slow reads as a
 * broken app; the text placeholder holds the layout and the identity, and the
 * image replaces it automatically on load without a layout shift.
 */
export function Logo({ className }: { className?: string }) {
  const { theme } = useTheme()
  const dark = theme === "dark"
  // Start as "not loaded": the <img> has not fired `load` yet on first paint,
  // so this avoids a flash of the text once a cached image arrives.
  const [loaded, setLoaded] = useState(false)
  const [failed, setFailed] = useState(false)

  // Reset when the artwork changes (e.g. a theme-specific asset), otherwise a
  // previously failed load would suppress the replacement indefinitely.
  useEffect(() => {
    setLoaded(false)
    setFailed(false)
  }, [dark])

  const showImage = loaded && !failed

  return (
    <span className={cn("inline-flex items-center", className)}>
      {showImage ? (
        <img
          src="/duka_logo.png"
          alt="DukaScraper"
          className={cn("h-full w-auto", dark ? "invert" : undefined)}
          draggable={false}
        />
      ) : (
        <span
          className={cn(
            "select-none font-semibold tracking-tight",
            dark ? "text-slate-100" : "text-slate-900",
          )}
        >
          DukaScraper
        </span>
      )}
      {/* Always mounted but hidden once the wordmark is visible: this is what
          reports load success/failure without remounting on every render. */}
      <img
        src="/duka_logo.png"
        alt=""
        aria-hidden="true"
        className="hidden"
        draggable={false}
        onLoad={() => setLoaded(true)}
        onError={() => setFailed(true)}
      />
    </span>
  )
}