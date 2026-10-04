import { useEffect, useRef } from "react"

import { OTP_CODE_LENGTH } from "../utils"

const CODE_LENGTH = OTP_CODE_LENGTH

/**
 * Six single-digit boxes for entering an emailed code.
 *
 * Shared by the two verification screens (first-login OTP and email
 * verification) so their behaviour cannot drift: auto-advance as digits land,
 * Backspace walking left, arrow keys, and pasting a whole code into any box.
 * Fires `onComplete` on its own once the last digit arrives, so the caller does
 * not need a separate submit button for the happy path.
 */
export function OtpInput({
  digits,
  onDigitsChange,
  onComplete,
  disabled = false,
  invalid = false,
}: {
  digits: string[]
  onDigitsChange: (next: string[]) => void
  onComplete: (code: string) => void
  disabled?: boolean
  /** Red border for a rejected code. */
  invalid?: boolean
}) {
  const inputsRef = useRef<(HTMLInputElement | null)[]>([])

  useEffect(() => {
    inputsRef.current[0]?.focus()
  }, [])

  const setDigit = (index: number, raw: string) => {
    const value = raw.replace(/\D/g, "")
    if (!value) {
      onDigitsChange(digits.map((d, i) => (i === index ? "" : d)))
      return
    }
    // Paste, or typing over digits starting at this box.
    const next = [...digits]
    for (let i = 0; i < value.length && index + i < CODE_LENGTH; i++) {
      next[index + i] = value[i]
    }
    onDigitsChange(next)
    const lastFilled = Math.min(index + value.length, CODE_LENGTH)
    if (lastFilled >= CODE_LENGTH && next.every(Boolean)) {
      onComplete(next.join(""))
    } else {
      inputsRef.current[lastFilled]?.focus()
    }
  }

  const onKeyDown = (index: number, e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Backspace" && !digits[index] && index > 0) {
      e.preventDefault()
      inputsRef.current[index - 1]?.focus()
    }
    if (e.key === "ArrowLeft" && index > 0) inputsRef.current[index - 1]?.focus()
    if (e.key === "ArrowRight" && index < CODE_LENGTH - 1) inputsRef.current[index + 1]?.focus()
  }

  return (
    <div className="flex justify-center gap-2">
      {digits.map((digit, i) => (
        <input
          key={i}
          ref={(el) => {
            inputsRef.current[i] = el
          }}
          value={digit}
          onChange={(e) => setDigit(i, e.target.value)}
          onKeyDown={(e) => onKeyDown(i, e)}
          disabled={disabled}
          inputMode="numeric"
          // Lets iOS/Android offer the code straight from the SMS/email.
          autoComplete={i === 0 ? "one-time-code" : "off"}
          maxLength={CODE_LENGTH}
          aria-label={`Digit ${i + 1}`}
          aria-invalid={invalid || undefined}
          className={
            "h-12 w-10 rounded-lg border bg-slate-950 text-center font-mono text-xl text-slate-100 " +
            "transition focus:ring-1 focus:outline-none disabled:opacity-50 " +
            (invalid
              ? "border-rose-500 focus:border-rose-500 focus:ring-rose-500/40"
              : "border-slate-700 focus:border-emerald-500 focus:ring-emerald-500/40")
          }
        />
      ))}
    </div>
  )
}
