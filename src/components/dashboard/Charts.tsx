import { useId } from "react";
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar, PieChart, Pie, Cell } from "recharts";

const CHART_COLORS = [
  "hsl(42, 92%, 56%)",
  "hsl(220, 65%, 18%)",
  "hsl(152, 60%, 42%)",
  "hsl(210, 92%, 55%)",
  "hsl(0, 72%, 51%)",
  "hsl(38, 92%, 50%)",
];

const CHART_ANIMATION = {
  begin: 0,
  duration: 650,
  easing: "ease-in-out" as const,
};

function getAreaAnimationId(data: { name: string; value: number }[]) {
  const signature = data.map((item) => `${item.name}:${item.value}`).join("|");
  let hash = 0;

  for (let index = 0; index < signature.length; index += 1) {
    hash = (hash * 31 + signature.charCodeAt(index)) | 0;
  }

  return Math.abs(hash);
}

interface MiniChartProps {
  data: { name: string; value: number }[];
  type?: "area" | "bar";
  color?: string;
  height?: number;
  /** Rótulo da métrica no tooltip (ex.: "Notas" em vez de "Value") */
  valueLabel?: string;
}

export function MiniChart({ data, type = "area", color = CHART_COLORS[0], height = 200, valueLabel }: MiniChartProps) {
  const gradientId = useId().replace(/:/g, "");
  const areaAnimationId = getAreaAnimationId(data);
  const tooltipFormatter = valueLabel ? (value: number) => [value, valueLabel] : undefined;
  const tooltipStyle = {
    background: "var(--ap-tooltip-bg)",
    color: "var(--ap-tooltip-text)",
    border: "1px solid var(--ap-tooltip-border)",
    borderRadius: "8px",
    boxShadow: "0 4px 12px rgba(0,0,0,0.15)",
    fontSize: "12px",
  };

  if (type === "bar") {
    return (
      <ResponsiveContainer width="100%" height={height}>
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="hsl(220, 15%, 90%)" opacity={0.3} />
          <XAxis dataKey="name" tick={{ fontSize: 11, fill: "hsl(220, 10%, 46%)" }} axisLine={false} tickLine={false} />
          <YAxis tick={{ fontSize: 11, fill: "hsl(220, 10%, 46%)" }} axisLine={false} tickLine={false} />
          <Tooltip
            contentStyle={tooltipStyle}
            formatter={tooltipFormatter}
          />
          <Bar dataKey="value" fill={color} radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={data}>
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor={color} stopOpacity={0.3} />
            <stop offset="95%" stopColor={color} stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="hsl(220, 15%, 90%)" opacity={0.3} />
        <XAxis dataKey="name" tick={{ fontSize: 11, fill: "hsl(220, 10%, 46%)" }} axisLine={false} tickLine={false} />
        <YAxis tick={{ fontSize: 11, fill: "hsl(220, 10%, 46%)" }} axisLine={false} tickLine={false} />
        <Tooltip
          contentStyle={tooltipStyle}
          formatter={tooltipFormatter}
        />
        <Area
          type="monotone"
          dataKey="value"
          stroke={color}
          strokeWidth={2}
          fillOpacity={1}
          fill={`url(#${gradientId})`}
          isAnimationActive
          animateNewValues
          animationId={areaAnimationId}
          animationBegin={CHART_ANIMATION.begin}
          animationDuration={CHART_ANIMATION.duration}
          animationEasing={CHART_ANIMATION.easing}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}

interface DonutChartProps {
  data: { name: string; value: number }[];
  height?: number;
}

export function DonutChart({ data, height = 200 }: DonutChartProps) {
  const tooltipStyle = {
    background: "var(--ap-tooltip-bg)",
    color: "var(--ap-tooltip-text)",
    border: "1px solid var(--ap-tooltip-border)",
    borderRadius: "8px",
    boxShadow: "0 4px 12px rgba(0,0,0,0.15)",
    fontSize: "12px",
  };
  return (
    <ResponsiveContainer width="100%" height={height}>
      <PieChart>
        <Pie
          data={data}
          cx="50%"
          cy="50%"
          innerRadius={50}
          outerRadius={75}
          paddingAngle={4}
          dataKey="value"
          stroke="none"
        >
          {data.map((_, index) => (
            <Cell key={`cell-${index}`} fill={CHART_COLORS[index % CHART_COLORS.length]} />
          ))}
        </Pie>
        <Tooltip
          contentStyle={tooltipStyle}
        />
      </PieChart>
    </ResponsiveContainer>
  );
}
