import { addDays, endOfMonth, format, startOfDay, startOfMonth, subDays, isBefore } from "date-fns";
import type { Robot } from "@/services/robotsService";
import { buildRobotExecutionSnapshot } from "@/lib/robotConfigSchemas";
import type { Json, RobotExecutionMode } from "@/types/database";

export function getNextRobotRunAt(rule: {
  run_at_time?: string | null;
  run_at_date?: string | null;
  last_run_at?: string | null;
} | null): Date | null {
  if (!rule?.run_at_time) return null;
  const now = new Date();
  const timeStr = String(rule.run_at_time).trim().slice(0, 5);
  const parts = timeStr.split(":");
  const hours = Math.min(23, Math.max(0, parseInt(parts[0] ?? "0", 10) || 0));
  const minutes = Math.min(59, Math.max(0, parseInt(parts[1] ?? "0", 10) || 0));

  let runDate: Date;
  if (rule.last_run_at) {
    runDate = addDays(startOfDay(new Date(rule.last_run_at)), 1);
  } else if (rule.run_at_date) {
    const rawDate = String(rule.run_at_date).trim().slice(0, 10);
    runDate = /^\d{4}-\d{2}-\d{2}$/.test(rawDate)
      ? new Date(`${rawDate}T12:00:00`)
      : startOfDay(now);
  } else {
    runDate = startOfDay(now);
  }

  const next = new Date(
    runDate.getFullYear(),
    runDate.getMonth(),
    runDate.getDate(),
    hours,
    minutes,
    0,
    0,
  );

  if (isBefore(next, now)) {
    const tomorrow = addDays(startOfDay(now), 1);
    return new Date(
      tomorrow.getFullYear(),
      tomorrow.getMonth(),
      tomorrow.getDate(),
      hours,
      minutes,
      0,
      0,
    );
  }
  return next;
}

export function formatCountdownLabel(ms: number): string {
  if (ms <= 0) return "0h 0m 0s";
  const seconds = Math.floor((ms / 1000) % 60);
  const minutes = Math.floor((ms / 60000) % 60);
  const hours = Math.floor(ms / 3600000);
  return `${hours}h ${minutes}m ${seconds}s`;
}

function isFiscalNotesRobot(robot: Robot): boolean {
  const segmentPath = (robot.segment_path || "").toUpperCase();
  return Boolean(
    robot.is_fiscal_notes_robot ||
      robot.fiscal_notes_kind ||
      robot.notes_mode ||
      segmentPath.includes("FISCAL/NFS") ||
      segmentPath.includes("FISCAL/NFE") ||
      segmentPath.includes("FISCAL/NFC"),
  );
}

export function computePeriodForRobot(robot: Robot, today: Date): { periodStart: string; periodEnd: string } {
  const yesterday = subDays(today, 1);
  const yesterdayIso = format(yesterday, "yyyy-MM-dd");

  if (robot.date_execution_mode === "competencia") {
    return {
      periodStart: format(startOfMonth(today), "yyyy-MM-dd"),
      periodEnd: format(endOfMonth(today), "yyyy-MM-dd"),
    };
  }

  if (robot.date_execution_mode === "interval") {
    if (robot.last_period_end) {
      return { periodStart: yesterdayIso, periodEnd: yesterdayIso };
    }
    if (robot.initial_period_start && robot.initial_period_end) {
      return {
        periodStart: robot.initial_period_start,
        periodEnd: robot.initial_period_end,
      };
    }
  }

  return { periodStart: yesterdayIso, periodEnd: yesterdayIso };
}

function getStoredIntervalPeriod(robot: Robot): { periodStart: string; periodEnd: string } | null {
  if (robot.date_execution_mode !== "interval") return null;
  if (!robot.initial_period_start || !robot.initial_period_end) return null;
  return {
    periodStart: robot.initial_period_start,
    periodEnd: robot.initial_period_end,
  };
}

export function computeScheduledPeriodForRobot(robot: Robot, today: Date): { periodStart: string; periodEnd: string } {
  if (isFiscalNotesRobot(robot) && robot.date_execution_mode === "interval") {
    const yesterday = format(subDays(today, 1), "yyyy-MM-dd");
    return { periodStart: yesterday, periodEnd: yesterday };
  }
  return computePeriodForRobot(robot, today);
}

export function computeManualPeriodForRobot(robot: Robot, today: Date): { periodStart: string; periodEnd: string } {
  if (isFiscalNotesRobot(robot)) {
    const storedInterval = getStoredIntervalPeriod(robot);
    if (storedInterval) return storedInterval;
  }
  return computePeriodForRobot(robot, today);
}

export function computeManualPeriodForRobotAtDate(
  robot: Robot,
  referenceDate: Date,
): { periodStart: string; periodEnd: string } {
  if (robot.date_execution_mode === "competencia") {
    return {
      periodStart: format(startOfMonth(referenceDate), "yyyy-MM-dd"),
      periodEnd: format(endOfMonth(referenceDate), "yyyy-MM-dd"),
    };
  }

  if (robot.date_execution_mode === "interval") {
    const selectedDay = format(startOfDay(referenceDate), "yyyy-MM-dd");
    return { periodStart: selectedDay, periodEnd: selectedDay };
  }

  return computeManualPeriodForRobot(robot, referenceDate);
}

export function buildJobPayloadForRobot(
  robot: Robot,
  companyIds: string[],
  periodStart: string,
  periodEnd: string,
  extraPayload?: Record<string, Json>,
): Record<string, Json> {
  return {
    ...buildRobotExecutionSnapshot(robot),
    company_ids: companyIds,
    period_start: periodStart,
    period_end: periodEnd,
    ...(extraPayload ?? {}),
  };
}

export function buildScheduleSettings(robots: Robot[]): Record<string, Json> {
  return {
    robot_snapshots: Object.fromEntries(
      robots.map((robot) => [robot.technical_id, buildRobotExecutionSnapshot(robot)]),
    ),
  };
}

export function normalizeExecutionMode(value: string | null | undefined): RobotExecutionMode {
  return value === "parallel" ? "parallel" : "sequential";
}

