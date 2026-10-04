import { createContext, useCallback, useContext, useEffect, useState } from "react"
import type { ReactNode } from "react"
import { Moon, Sun } from "lucide-react"
import { cn } from "./utils"

type Theme = "dark" | "light"

const THEME_KEY = "dukascraper.console.theme.v1"

function getInitialTheme(): Theme {
  try {
    const stored = localStorage.getItem(THEME_KEY)
    if (stored === "dark" || stored === "light") return stored
  } catch {
    /* ignore */
  }
  return "dark"
}

interface ThemeContextValue {
  theme: Theme
  toggleTheme: () => void
}

const ThemeContext = createContext<ThemeContextValue | null>(null)

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setTheme] = useState<Theme>(getInitialTheme)

  useEffect(() => {
    document.documentElement.classList.toggle("light", theme === "light")
    try {
      localStorage.setItem(THEME_KEY, theme)
    } catch {
      /* ignore */
    }
  }, [theme])

  const toggleTheme = useCallback(() => {
    setTheme((t) => (t === "dark" ? "light" : "dark"))
  }, [])

  return (
    <ThemeContext.Provider value={{ theme, toggleTheme }}>{children}</ThemeContext.Provider>
  )
}

// eslint-disable-next-line react-refresh/only-export-components
export function useTheme(): ThemeContextValue {
  const ctx = useContext(ThemeContext)
  if (!ctx) throw new Error("useTheme must be used within ThemeProvider")
  return ctx
}

export function ThemeToggle({ className }: { className?: string }) {
  const { theme, toggleTheme } = useTheme()
  const light = theme === "light"
  return (
    <button
      type="button"
      onClick={toggleTheme}
      title={light ? "Switch to dark mode" : "Switch to light mode"}
      aria-label={light ? "Switch to dark mode" : "Switch to light mode"}
      className={cn(
        "relative inline-flex h-7 w-[3.25rem] cursor-pointer items-center rounded-full border transition-all duration-300 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 focus-visible:ring-offset-2 focus-visible:ring-offset-black",
        light
          ? "border-indigo-400/60 bg-indigo-50/80"
          : "border-slate-700 bg-slate-900",
        className,
      )}
    >
      {/* Track icons */}
      <span className="pointer-events-none absolute left-1 flex h-4 w-4 items-center justify-center">
        <Moon
          className={cn(
            "h-3 w-3 transition-all duration-300",
            light ? "scale-75 opacity-30 text-slate-400" : "scale-100 opacity-100 text-slate-400",
          )}
        />
      </span>
      <span className="pointer-events-none absolute right-1 flex h-4 w-4 items-center justify-center">
        <Sun
          className={cn(
            "h-3 w-3 transition-all duration-300",
            light ? "scale-100 opacity-100 text-amber-500" : "scale-75 opacity-30 text-slate-600",
          )}
        />
      </span>
      {/* Thumb */}
      <span
        className={cn(
          "pointer-events-none absolute flex h-5 w-5 items-center justify-center rounded-full shadow-md transition-all duration-300",
          light
            ? "translate-x-[1.625rem] bg-white"
            : "translate-x-[0.125rem] bg-slate-700",
        )}
      >
        {light ? (
          <Sun className="h-3 w-3 text-amber-500" />
        ) : (
          <Moon className="h-3 w-3 text-slate-300" />
        )}
      </span>
    </button>
  )
}