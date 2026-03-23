import { useEffect, useRef, useState } from "react";
import { cn } from "@/utils";

export interface AnimatedNumberProps {
  /** Valor atual (número ou string numérica). Strings não numéricas são exibidas sem animação. */
  value: number | string;
  /** Formatação: recebe o número e retorna a string exibida (ex: toLocaleString, formatCurrency). */
  format?: (n: number) => string;
  /** Duração da animação em ms (por dígito no modo roll). */
  duration?: number;
  /** "roll" = dígitos rolando tipo odômetro (padrão); "count" = número subindo em valor. */
  variant?: "roll" | "count";
  className?: string;
}

/** Uma coluna com dígitos 0-9 que rola verticalmente (rolling number / odometer). */
function RollingDigit({
  digit,
  fromDigit,
  durationMs,
  className,
}: {
  digit: number;
  fromDigit: number | undefined;
  durationMs: number;
  className?: string;
}) {
  const [displayDigit, setDisplayDigit] = useState(fromDigit ?? digit);
  const isFirstRender = useRef(true);

  useEffect(() => {
    if (isFirstRender.current) {
      isFirstRender.current = false;
      setDisplayDigit(digit);
      return;
    }
    setDisplayDigit(digit);
  }, [digit]);

  const d = Math.min(9, Math.max(0, displayDigit));

  return (
    <span
      className={cn("inline-block overflow-hidden align-top", className)}
      style={{ height: "1em", lineHeight: 1 }}
      aria-hidden
    >
      <span
        className="inline-block tabular-nums transition-transform will-change-transform"
        style={{
          transform: `translateY(-${d}em)`,
          transitionDuration: `${durationMs}ms`,
          transitionTimingFunction: "cubic-bezier(0.25, 0.1, 0.25, 1)",
        }}
      >
        {[0, 1, 2, 3, 4, 5, 6, 7, 8, 9].map((n) => (
          <span key={n} className="block text-inherit" style={{ height: "1em", lineHeight: 1, textAlign: "center" }}>
            {n}
          </span>
        ))}
      </span>
    </span>
  );
}

/** Só interpreta como número strings puramente numéricas (evita "501/0" virar 5010 ao remover a barra). */
function parseAnimatedNumericInput(value: number | string): number {
  if (typeof value === "number") return value;
  const s = String(value).trim();
  if (s === "" || s === "—" || s === "-") return Number.NaN;
  if (s.includes("/")) return Number.NaN;
  const compact = s.replace(/\s/g, "");
  if (/^-?\d+$/.test(compact)) return parseInt(compact, 10);
  if (/^-?\d+[.,]\d+$/.test(compact)) return parseFloat(compact.replace(",", "."));
  return Number.NaN;
}

export function AnimatedNumber({
  value,
  format = (n) => n.toLocaleString("pt-BR"),
  duration = 420,
  variant = "roll",
  className,
}: AnimatedNumberProps) {
  const numericValue = parseAnimatedNumericInput(value);

  const prevFormattedRef = useRef<string>("");
  const target = Number.isFinite(numericValue) ? Math.round(numericValue) : 0;
  const currentStr = format(target);

  const [displayValue, setDisplayValue] = useState(target);
  const prevTarget = useRef<number | null>(null);
  const rafId = useRef<number>(0);
  const startTime = useRef(0);
  const startValue = useRef(target);

  useEffect(() => {
    if (variant === "roll" && currentStr) {
      prevFormattedRef.current = currentStr;
    }
  }, [variant, currentStr]);

  useEffect(() => {
    if (variant !== "count") return;
    if (prevTarget.current === target) return;
    const prev = prevTarget.current ?? displayValue;
    prevTarget.current = target;
    startValue.current = prev;
    startTime.current = performance.now();

    const tick = (now: number) => {
      const elapsed = now - startTime.current;
      const t = Math.min(elapsed / duration, 1);
      const eased = 1 - Math.pow(1 - t, 3);
      const current = Math.round(startValue.current + (target - startValue.current) * eased);
      setDisplayValue(current);
      if (t < 1) rafId.current = requestAnimationFrame(tick);
    };
    rafId.current = requestAnimationFrame(tick);
    return () => {
      if (rafId.current) cancelAnimationFrame(rafId.current);
    };
  }, [variant, target, duration]);

  if (!Number.isFinite(numericValue)) {
    return <span className={cn("tabular-nums", className)}>{String(value)}</span>;
  }

  if (variant === "roll") {
    const prevStr = prevFormattedRef.current;
    const chars: React.ReactNode[] = [];
    for (let i = 0; i < currentStr.length; i++) {
      const c = currentStr[i];
      if (c >= "0" && c <= "9") {
        const digit = parseInt(c, 10);
        const prevIdx = prevStr.length - currentStr.length + i;
        const fromDigit =
          prevIdx >= 0 && prevIdx < prevStr.length && prevStr[prevIdx] >= "0" && prevStr[prevIdx] <= "9"
            ? parseInt(prevStr[prevIdx], 10)
            : undefined;
        chars.push(
          <RollingDigit
            key={`${i}-${currentStr}`}
            digit={digit}
            fromDigit={fromDigit}
            durationMs={duration}
          />
        );
      } else {
        chars.push(
          <span key={`${i}-${c}`} className="tabular-nums">
            {c}
          </span>
        );
      }
    }
    return (
      <span className={cn("tabular-nums inline-flex items-baseline", className)} data-animated-number>
        {chars}
      </span>
    );
  }

  return (
    <span className={cn("tabular-nums inline-block", className)} data-animated-number>
      {format(displayValue)}
    </span>
  );
}
