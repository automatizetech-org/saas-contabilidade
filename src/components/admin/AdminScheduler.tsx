import { useState, useEffect, useCallback, useMemo, useRef } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { getCompaniesForUser, getCompanyRobotConfigsForSelection } from "@/services/companiesService"
import { getRobots, updateRobot } from "@/services/robotsService"
import { createExecutionRequest } from "@/services/executionRequestsService"
import {
  getScheduleRules,
  getActiveScheduleRules,
  createScheduleRule,
  updateScheduleRule,
  pauseScheduleRule,
} from "@/services/scheduleRulesService"
import {
  getPendingOrRunningExecutionRequests,
  cancelPendingByRobotTechnicalIds,
  cancelPendingByScheduleRuleId,
  deleteAllByRobotTechnicalIds,
  deleteAllByScheduleRuleId,
  markRunningAsCancelledByRobotTechnicalIds,
  markRunningAsCancelledByScheduleRuleId,
} from "@/services/executionRequestsService"
import { upsertRobotDisplayConfig } from "@/services/robotDisplayConfigService"
import { GlassCard } from "@/components/dashboard/GlassCard"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Checkbox } from "@/components/ui/checkbox"
import { CalendarClock, Play, Loader2, Bot, Building2, Square, GripVertical, Search } from "lucide-react"
import { toast } from "sonner"
import { format, startOfMonth, endOfMonth, addDays, subDays, startOfDay, isBefore } from "date-fns"
import { ptBR } from "date-fns/locale"
import type { Robot } from "@/services/robotsService"
import type { ScheduleRule } from "@/services/scheduleRulesService"
import { getCommonRobotNotesMode, getRobotNotesMode } from "@/lib/robotNotes"
import type { Json, RobotExecutionMode } from "@/types/database"
import { buildRobotExecutionSnapshot } from "@/lib/robotConfigSchemas"
import { getEligibleCompanyIdsForRobot, indexCompanyRobotConfigs } from "@/lib/robotEligibility"

const DEBOUNCE_MS = 800

/** Retorna o próximo horário de execução (em Date) com base na regra ativa e no horário de Brasília. */
function getNextRunAt(rule: ScheduleRule | null): Date | null {
  if (!rule?.run_at_time) return null
  const now = new Date()
  const timeStr = String(rule.run_at_time).trim().slice(0, 5)
  const parts = timeStr.split(":")
  const h = Math.min(23, Math.max(0, parseInt(parts[0], 10) || 0))
  const m = Math.min(59, Math.max(0, parseInt(parts[1], 10) || 0))

  let runDate: Date
  if (rule.last_run_at) {
    const last = new Date(rule.last_run_at)
    const lastDay = startOfDay(last)
    runDate = addDays(lastDay, 1)
  } else if (rule.run_at_date) {
    const raw = String(rule.run_at_date).trim()
    const dateOnly = raw.slice(0, 10)
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateOnly)) {
      runDate = startOfDay(now)
    } else {
      runDate = new Date(dateOnly + "T12:00:00")
    }
  } else {
    runDate = startOfDay(now)
  }

  const next = new Date(runDate.getFullYear(), runDate.getMonth(), runDate.getDate(), h, m, 0, 0)

  if (isBefore(next, now)) {
    const tomorrow = addDays(startOfDay(now), 1)
    return new Date(tomorrow.getFullYear(), tomorrow.getMonth(), tomorrow.getDate(), h, m, 0, 0)
  }
  return next
}

/** Formata diferença em ms para "Xh Ym Zs" (sempre 3 partes, zeros quando < 1). */
function formatCountdown(ms: number): string {
  if (ms <= 0) return "0h 0m 0s"
  const s = Math.floor((ms / 1000) % 60)
  const m = Math.floor((ms / 60000) % 60)
  const h = Math.floor(ms / 3600000)
  return `${h}h ${m}m ${s}s`
}

function computePeriodForRobot(robot: Robot, today: Date): { periodStart: string; periodEnd: string } {
  const yesterday = subDays(today, 1)
  const y = format(yesterday, "yyyy-MM-dd")
  if (robot.date_execution_mode === "competencia") {
    const start = startOfMonth(today)
    const end = endOfMonth(today)
    return { periodStart: format(start, "yyyy-MM-dd"), periodEnd: format(end, "yyyy-MM-dd") }
  }
  if (robot.date_execution_mode === "interval") {
    if (robot.last_period_end) {
      return { periodStart: y, periodEnd: y }
    }
    if (robot.initial_period_start && robot.initial_period_end) {
      return {
        periodStart: robot.initial_period_start,
        periodEnd: robot.initial_period_end,
      }
    }
  }
  return { periodStart: y, periodEnd: y }
}

function getStoredIntervalPeriod(robot: Robot): { periodStart: string; periodEnd: string } | null {
  if (robot.date_execution_mode !== "interval") return null
  if (!robot.initial_period_start || !robot.initial_period_end) return null
  return {
    periodStart: robot.initial_period_start,
    periodEnd: robot.initial_period_end,
  }
}

function isFiscalNotesRobot(robot: Robot): boolean {
  const segmentPath = (robot.segment_path || "").toUpperCase()
  return Boolean(
    robot.is_fiscal_notes_robot ||
    robot.fiscal_notes_kind ||
    robot.notes_mode ||
    segmentPath.includes("FISCAL/NFS") ||
    segmentPath.includes("FISCAL/NFE") ||
    segmentPath.includes("FISCAL/NFC")
  )
}

function computeScheduledPeriodForRobot(robot: Robot, today: Date): { periodStart: string; periodEnd: string } {
  if (isFiscalNotesRobot(robot) && robot.date_execution_mode === "interval") {
    const yesterday = format(subDays(today, 1), "yyyy-MM-dd")
    return { periodStart: yesterday, periodEnd: yesterday }
  }
  return computePeriodForRobot(robot, today)
}

function computeManualPeriodForRobot(robot: Robot, today: Date): { periodStart: string; periodEnd: string } {
  if (isFiscalNotesRobot(robot)) {
    const storedInterval = getStoredIntervalPeriod(robot)
    if (storedInterval) return storedInterval
  }
  return computePeriodForRobot(robot, today)
}

function buildJobPayloadForRobot(
  robot: Robot,
  companyIds: string[],
  periodStart: string,
  periodEnd: string
): Record<string, Json> {
  return {
    ...buildRobotExecutionSnapshot(robot),
    company_ids: companyIds,
    period_start: periodStart,
    period_end: periodEnd,
  }
}

function buildScheduleSettings(robots: Robot[]): Record<string, Json> {
  return {
    robot_snapshots: Object.fromEntries(
      robots.map((robot) => [
        robot.technical_id,
        buildRobotExecutionSnapshot(robot),
      ])
    ),
  }
}

export function AdminScheduler({
  isSuperAdmin,
  robots: robotsProp,
}: {
  isSuperAdmin: boolean
  robots?: Robot[]
}) {
  const queryClient = useQueryClient()
  const [companyIds, setCompanyIds] = useState<Set<string>>(new Set())
  const [robotIdsOrdered, setRobotIdsOrdered] = useState<string[]>([])
  const [allRobots, setAllRobots] = useState(false)
  const [runAtDate, setRunAtDate] = useState(format(new Date(), "yyyy-MM-dd"))
  const [runAtTime, setRunAtTime] = useState("08:00")
  const [runDaily, setRunDaily] = useState(false)
  const [executionMode, setExecutionMode] = useState<RobotExecutionMode>("sequential")
  const [submitting, setSubmitting] = useState(false)
  const [companySearch, setCompanySearch] = useState("")
  const [activeRuleId, setActiveRuleId] = useState<string | null>(null)
  const [countdownMs, setCountdownMs] = useState<number>(0)
  const [draggedId, setDraggedId] = useState<string | null>(null)
  const lastNextRunAtRef = useRef<Date | null>(null)
  /** Ref para garantir que o modo de execução usado ao clicar "Executar agora" seja o atual (evita estado desatualizado). */
  const executionModeRef = useRef<RobotExecutionMode>(executionMode)
  useEffect(() => {
    executionModeRef.current = executionMode
  }, [executionMode])

  const { data: companies = [] } = useQuery({
    queryKey: ["admin-companies-scheduler"],
    queryFn: () => getCompaniesForUser("all"),
  })

  const { data: queriedRobots = [] } = useQuery({
    queryKey: ["admin-robots"],
    queryFn: getRobots,
    refetchOnWindowFocus: true,
    refetchInterval: 5000,
    enabled: !robotsProp,
    staleTime: 5000,
  })
  const robots = robotsProp ?? queriedRobots

  const { data: scheduleRules = [], isLoading: loadingRules } = useQuery({
    queryKey: ["schedule-rules"],
    queryFn: getScheduleRules,
    refetchOnWindowFocus: true,
  })

  const { data: activeRules = [] } = useQuery({
    queryKey: ["schedule-rules-active"],
    queryFn: getActiveScheduleRules,
    refetchOnWindowFocus: true,
  })

  const activeRule = activeRules.length > 0 ? activeRules[0] : null

  const { data: pendingOrRunningExecutions = [] } = useQuery({
    queryKey: ["execution-requests-pending-running", activeRule?.id],
    queryFn: () => getPendingOrRunningExecutionRequests(activeRule?.id ?? null),
    enabled: !!activeRule?.id,
    refetchOnWindowFocus: true,
    refetchInterval: activeRule?.id ? 2000 : false,
  })

  const nextRunAt = activeRule ? getNextRunAt(activeRule) : null

  const hasPendingOrRunning = pendingOrRunningExecutions.length > 0
  const isExecutingNow = hasPendingOrRunning

  if (activeRule && !isExecutingNow && nextRunAt) {
    lastNextRunAtRef.current = nextRunAt
  } else if (!activeRule || isExecutingNow) {
    lastNextRunAtRef.current = null
  }

  const displayNextRunAt = !isExecutingNow
    ? (nextRunAt ?? (activeRule ? lastNextRunAtRef.current : null))
    : null

  useEffect(() => {
    if (!displayNextRunAt) {
      setCountdownMs(0)
      return
    }
    const tick = () => {
      const ms = displayNextRunAt.getTime() - Date.now()
      setCountdownMs(Math.max(0, ms))
    }
    tick()
    const id = setInterval(tick, 1000)
    return () => clearInterval(id)
  }, [displayNextRunAt?.toISOString() ?? null, isExecutingNow])

  useEffect(() => {
    if (activeRule && !activeRuleId) {
      setActiveRuleId(activeRule.id)
      setCompanyIds(new Set(activeRule.company_ids || []))
      const ids = activeRule.robot_technical_ids?.filter((id) => id !== "all") || []
      if (activeRule.robot_technical_ids?.includes("all")) {
        setAllRobots(true)
        setRobotIdsOrdered(robots.map((r) => r.technical_id))
      } else {
        setAllRobots(false)
        setRobotIdsOrdered(ids.length > 0 ? ids : [])
      }
      if (activeRule.run_at_date) setRunAtDate(activeRule.run_at_date)
      const t = String(activeRule.run_at_time).slice(0, 5)
      setRunAtTime(t)
      setExecutionMode((activeRule.execution_mode === "parallel" ? "parallel" : "sequential") as RobotExecutionMode)
      setRunDaily(true)
    }
  }, [activeRule?.id])

  const selectedRobots = useMemo(
    () =>
      allRobots
        ? robots
        : robotIdsOrdered
            .map((id) => robots.find((r) => r.technical_id === id))
            .filter((r): r is Robot => !!r),
    [allRobots, robotIdsOrdered, robots]
  )
  const commonSelectedRobotNotesMode = useMemo(
    () => getCommonRobotNotesMode(selectedRobots),
    [selectedRobots]
  )
  const filteredCompanies = useMemo(() => {
    const q = companySearch.trim().toLowerCase()
    if (!q) return companies
    return companies.filter((company) => company.name.toLowerCase().includes(q))
  }, [companies, companySearch])

  const persistDisplayConfig = useCallback(() => {
    if (companyIds.size === 0) return
    const robotsToUpdate = selectedRobots.length > 0 ? selectedRobots : robots
    if (robotsToUpdate.length === 0) return
    Promise.all(
      robotsToUpdate.map((robot) =>
        upsertRobotDisplayConfig({
          robotTechnicalId: robot.technical_id,
          companyIds: Array.from(companyIds),
          notesMode: getRobotNotesMode(robot) ?? undefined,
        })
      )
    ).catch(() => {})
  }, [companyIds, selectedRobots, robots])

  useEffect(() => {
    const t = setTimeout(persistDisplayConfig, DEBOUNCE_MS)
    return () => clearTimeout(t)
  }, [companyIds, selectedRobots, persistDisplayConfig])

  const persistScheduleRule = useCallback(() => {
    if (!activeRule?.id || !runDaily || companyIds.size === 0 || (robotIdsOrdered.length === 0 && !allRobots)) return
    const robotsForSnapshot =
      (allRobots ? robots : robotIdsOrdered.map((id) => robots.find((robot) => robot.technical_id === id)).filter((robot): robot is Robot => Boolean(robot)))
    const payload = {
      companyIds: Array.from(companyIds),
      robotTechnicalIds: allRobots ? robots.map((r) => r.technical_id) : robotIdsOrdered,
      notesMode: commonSelectedRobotNotesMode ?? undefined,
      runAtDate,
      runAtTime: runAtTime.slice(0, 5),
      runDaily: true,
      executionMode,
      settings: buildScheduleSettings(robotsForSnapshot),
    }
    updateScheduleRule(activeRule.id, payload)
      .then(() => {
        queryClient.invalidateQueries({ queryKey: ["schedule-rules"] })
        queryClient.invalidateQueries({ queryKey: ["schedule-rules-active"] })
      })
      .catch(() => {})
  }, [activeRule?.id, runDaily, runAtDate, runAtTime, companyIds, robotIdsOrdered, allRobots, robots, commonSelectedRobotNotesMode, executionMode, queryClient])

  useEffect(() => {
    if (!activeRule?.id || !runDaily) return
    const ruleDate = (activeRule.run_at_date || "").slice(0, 10)
    const ruleTime = String(activeRule.run_at_time).slice(0, 5)
    const dateChanged = runAtDate.slice(0, 10) !== ruleDate
    const timeChanged = runAtTime.slice(0, 5) !== ruleTime
    const companiesChanged =
      companyIds.size !== (activeRule.company_ids?.length ?? 0) ||
      Array.from(companyIds).some((id) => !(activeRule.company_ids || []).includes(id))
    const robotsChanged =
      (activeRule.robot_technical_ids?.includes("all") && !allRobots) ||
      (!activeRule.robot_technical_ids?.includes("all") && allRobots) ||
      (robotIdsOrdered.length !== (activeRule.robot_technical_ids?.filter((x) => x !== "all").length ?? 0)) ||
      robotIdsOrdered.some((id) => !(activeRule.robot_technical_ids || []).includes(id))
    const executionModeChanged = (activeRule.execution_mode === "parallel" ? "parallel" : "sequential") !== executionMode
    if (!dateChanged && !timeChanged && !companiesChanged && !robotsChanged && !executionModeChanged) return
    const t = setTimeout(persistScheduleRule, DEBOUNCE_MS)
    return () => clearTimeout(t)
  }, [activeRule?.id, activeRule?.run_at_date, activeRule?.run_at_time, activeRule?.company_ids, activeRule?.robot_technical_ids, activeRule?.execution_mode, runDaily, runAtDate, runAtTime, companyIds, robotIdsOrdered, allRobots, executionMode, persistScheduleRule])

  const toggleCompany = (id: string) => {
    setCompanyIds((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const toggleRobot = (technicalId: string) => {
    setRobotIdsOrdered((prev) => {
      if (prev.includes(technicalId)) return prev.filter((id) => id !== technicalId)
      return [...prev, technicalId]
    })
  }

  const toggleAllRobots = (checked: boolean) => {
    setAllRobots(checked)
    if (checked) setRobotIdsOrdered(robots.map((r) => r.technical_id))
  }

  const moveRobot = (fromIndex: number, toIndex: number) => {
    if (fromIndex === toIndex) return
    setRobotIdsOrdered((prev) => {
      const next = [...prev]
      const [removed] = next.splice(fromIndex, 1)
      next.splice(toIndex, 0, removed)
      return next
    })
  }

  const handleDragStart = (e: React.DragEvent, technicalId: string) => {
    setDraggedId(technicalId)
    e.dataTransfer.effectAllowed = "move"
    e.dataTransfer.setData("text/plain", technicalId)
  }
  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault()
    e.dataTransfer.dropEffect = "move"
  }
  const handleDrop = (e: React.DragEvent, dropIndex: number) => {
    e.preventDefault()
    const technicalId = e.dataTransfer.getData("text/plain")
    setDraggedId(null)
    if (!technicalId) return
    const fromIndex = robotIdsOrdered.indexOf(technicalId)
    if (fromIndex === -1) return
    moveRobot(fromIndex, dropIndex)
  }
  const handleDragEnd = () => setDraggedId(null)

  const selectAllCompanies = () => {
    const visibleIds = filteredCompanies.map((company) => company.id)
    const allVisibleSelected = visibleIds.length > 0 && visibleIds.every((id) => companyIds.has(id))
    setCompanyIds((prev) => {
      const next = new Set(prev)
      if (allVisibleSelected) visibleIds.forEach((id) => next.delete(id))
      else visibleIds.forEach((id) => next.add(id))
      return next
    })
  }

  const handleRun = async () => {
    if (companyIds.size === 0) {
      toast.error("Selecione ao menos uma empresa.")
      return
    }
    if (robotIdsOrdered.length === 0) {
      toast.error("Selecione ao menos um robô ou marque 'Todos os robôs'.")
      return
    }
    setSubmitting(true)
    try {
      const selectedCompanyIds = Array.from(companyIds)
      if (runDaily) {
        const executionGroupId = crypto.randomUUID()
        const modeToUse = executionModeRef.current ?? executionMode
        const robotsForSnapshot =
          (allRobots ? robots : robotIdsOrdered.map((id) => robots.find((robot) => robot.technical_id === id)).filter((robot): robot is Robot => Boolean(robot)))
        const payload = {
          companyIds: selectedCompanyIds,
          robotTechnicalIds: robotIdsOrdered.length > 0 ? robotIdsOrdered : ["all"],
          notesMode: commonSelectedRobotNotesMode ?? undefined,
          runAtDate,
          runAtTime: runAtTime.slice(0, 5),
          runDaily: true,
          executionMode: modeToUse,
          settings: buildScheduleSettings(robotsForSnapshot),
        }
        let ruleId: string
        if (scheduleRules.length > 0) {
          await updateScheduleRule(scheduleRules[0].id, payload)
          ruleId = scheduleRules[0].id
        } else {
          const created = await createScheduleRule(payload)
          ruleId = created.id
        }
        await cancelPendingByScheduleRuleId(ruleId)
        const technicalIds =
          payload.robotTechnicalIds.includes("all")
            ? robots.map((r) => r.technical_id)
            : payload.robotTechnicalIds.filter((id) => id !== "all")
        for (const tid of technicalIds) {
          const robot = robots.find((r) => r.technical_id === tid)
          if (robot) await updateRobot(robot.id, { last_period_end: null })
        }
        queryClient.invalidateQueries({ queryKey: ["admin-robots"] })
        const dateStr = runAtDate.slice(0, 10)
        const timeStr = runAtTime.slice(0, 5)
        const scheduledTime = new Date(`${dateStr}T${timeStr}:00`)
        const now = Date.now()
        const shouldRunNow = now >= scheduledTime.getTime()
        if (shouldRunNow) {
          const list = (robotIdsOrdered.length > 0 ? robotIdsOrdered : robots.map((r) => r.technical_id))
            .map((id) => robots.find((r) => r.technical_id === id))
            .filter((r): r is Robot => !!r)
          const today = new Date()
          const companyConfigs = await getCompanyRobotConfigsForSelection({
            companyIds: selectedCompanyIds,
            robotTechnicalIds: list.map((robot) => robot.technical_id),
          })
          const companyConfigsByRobot = indexCompanyRobotConfigs(companyConfigs)
          let createdJobs = 0
          for (const [index, robot] of list.entries()) {
            const { periodStart, periodEnd } = computeScheduledPeriodForRobot(robot, today)
            const eligibleCompanyIds = getEligibleCompanyIdsForRobot({
              robot,
              selectedCompanyIds,
              companies,
              companyConfigsByRobot,
            })
            if (eligibleCompanyIds.length === 0) continue
            await createExecutionRequest({
              companyIds: eligibleCompanyIds,
              robotTechnicalIds: [robot.technical_id],
              periodStart,
              periodEnd,
              notesMode: getRobotNotesMode(robot) ?? undefined,
              scheduleRuleId: ruleId,
              executionMode: modeToUse,
              executionGroupId,
              executionOrder: index,
              jobPayload: buildJobPayloadForRobot(robot, eligibleCompanyIds, periodStart, periodEnd),
              source: "scheduler",
            })
            createdJobs += 1
          }
          if (createdJobs === 0) {
            throw new Error("Nenhuma empresa elegivel restou para os robos selecionados. Verifique filtros por cidade e vinculacao dos robos.")
          }
          await updateScheduleRule(ruleId, {
            ...payload,
            lastRunAt: new Date().toISOString(),
          })
          queryClient.invalidateQueries({ queryKey: ["execution-requests-pending-running"] })
          toast.success("Agendamento ativado e execução disparada agora (horário já era para rodar). Próxima em 24h.")
        } else {
          toast.success("Agendamento ativado. Execução na data/hora definida e depois a cada 24h.")
        }
        if (companyIds.size > 0) {
          await Promise.all(
            selectedRobots.map((robot) =>
              upsertRobotDisplayConfig({
                robotTechnicalId: robot.technical_id,
                companyIds: Array.from(companyIds),
                notesMode: getRobotNotesMode(robot) ?? undefined,
              })
            )
          )
        }
        queryClient.invalidateQueries({ queryKey: ["schedule-rules"] })
        queryClient.invalidateQueries({ queryKey: ["schedule-rules-active"] })
      } else {
        const modeToUse = executionModeRef.current ?? executionMode
        const executionGroupId = crypto.randomUUID()
        const list = robotIdsOrdered
          .map((id) => robots.find((r) => r.technical_id === id))
          .filter((r): r is Robot => !!r)
        const today = new Date()
        const companyConfigs = await getCompanyRobotConfigsForSelection({
          companyIds: selectedCompanyIds,
          robotTechnicalIds: list.map((robot) => robot.technical_id),
        })
        const companyConfigsByRobot = indexCompanyRobotConfigs(companyConfigs)
        let createdJobs = 0
        for (const [index, robot] of list.entries()) {
          const { periodStart, periodEnd } = computeManualPeriodForRobot(robot, today)
          const eligibleCompanyIds = getEligibleCompanyIdsForRobot({
            robot,
            selectedCompanyIds,
            companies,
            companyConfigsByRobot,
          })
          if (eligibleCompanyIds.length === 0) continue
          await createExecutionRequest({
            companyIds: eligibleCompanyIds,
            robotTechnicalIds: [robot.technical_id],
            periodStart,
            periodEnd,
            notesMode: getRobotNotesMode(robot) ?? undefined,
            executionMode: modeToUse,
            executionGroupId,
            executionOrder: index,
            jobPayload: buildJobPayloadForRobot(robot, eligibleCompanyIds, periodStart, periodEnd),
            source: "manual",
          })
          createdJobs += 1
        }
        if (createdJobs === 0) {
          throw new Error("Nenhuma empresa elegivel restou para os robos selecionados. Verifique filtros por cidade e vinculacao dos robos.")
        }
        if (companyIds.size > 0) {
          await Promise.all(
            selectedRobots.map((robot) =>
              upsertRobotDisplayConfig({
                robotTechnicalId: robot.technical_id,
                companyIds: Array.from(companyIds),
                notesMode: getRobotNotesMode(robot) ?? undefined,
              })
            )
          )
        }
        queryClient.invalidateQueries({ queryKey: ["execution-requests"] })
        queryClient.invalidateQueries({ queryKey: ["execution-requests-running"] })
        queryClient.invalidateQueries({ queryKey: ["execution-requests-pending-running"] })
        toast.success("Execução disparada. Os robôs processarão a fila.")
      }
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Falha ao disparar.")
    } finally {
      setSubmitting(false)
    }
  }

  const handleStop = async () => {
    if (!activeRule?.id) return
    setSubmitting(true)
    try {
      const technicalIds =
        activeRule.robot_technical_ids?.includes("all")
          ? robots.map((r) => r.technical_id)
          : (activeRule.robot_technical_ids || []).filter((id): id is string => id !== "all")
      await cancelPendingByScheduleRuleId(activeRule.id)
      await markRunningAsCancelledByScheduleRuleId(activeRule.id)
      await cancelPendingByRobotTechnicalIds(technicalIds)
      await markRunningAsCancelledByRobotTechnicalIds(technicalIds)
      await deleteAllByScheduleRuleId(activeRule.id)
      await deleteAllByRobotTechnicalIds(technicalIds)
      await pauseScheduleRule(activeRule.id)
      for (const tid of technicalIds) {
        const robot = robots.find((r) => r.technical_id === tid)
        if (robot) await updateRobot(robot.id, { last_period_end: null })
      }
      setActiveRuleId(null)
      queryClient.invalidateQueries({ queryKey: ["schedule-rules"] })
      queryClient.invalidateQueries({ queryKey: ["schedule-rules-active"] })
      queryClient.invalidateQueries({ queryKey: ["admin-robots"] })
      queryClient.invalidateQueries({ queryKey: ["execution-requests"] })
      queryClient.invalidateQueries({ queryKey: ["execution-requests-running"] })
      queryClient.invalidateQueries({ queryKey: ["execution-requests-pending-running"] })
      toast.success("Agendamento pausado. Horário e período zerados; ao reativar, a próxima execução será na data/hora configurada.")
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Falha ao pausar.")
    } finally {
      setSubmitting(false)
    }
  }

  const isActive = activeRule != null

  if (!isSuperAdmin) return null

  return (
    <GlassCard className="overflow-hidden">
      <div className="p-4 border-b border-border flex items-center gap-2">
        <CalendarClock className="h-4 w-4 text-primary-icon" />
        <h3 className="text-sm font-semibold font-display">Agendador — Executar robôs</h3>
      </div>
      <div className="p-4 space-y-4">
        {isActive && (
          <>
            {isExecutingNow && (
              <div className="rounded-md bg-amber-500/20 border border-amber-500/50 px-3 py-2 text-xs text-amber-700 dark:text-amber-400 font-medium flex items-center gap-2">
                <Loader2 className="h-3.5 w-3.5 animate-spin shrink-0" />
                Executando agora — robôs na fila ou processando (na ordem configurada). O contador das próximas 24h só aparece quando todos terminarem.
              </div>
            )}
            {!isExecutingNow && (
              <div className="rounded-md bg-muted/50 border border-border px-3 py-2 text-xs text-muted-foreground">
                Aguardando — rotina ativa. A repetição diária (horário de Brasília) é enfileirada pelo
                servidor local com o conector; mantenha a VM rodando nesse horário.
              </div>
            )}
            <div className="rounded-md bg-primary/10 border border-primary-icon/30 px-3 py-2 text-xs text-primary-icon">
              Próxima execução: {displayNextRunAt ? format(displayNextRunAt, "dd/MM/yyyy") : (activeRule.run_at_date ? format(new Date(activeRule.run_at_date), "dd/MM/yyyy") : "—")} às {String(activeRule.run_at_time).slice(0, 5)}. Repete a cada 24h.
            </div>
          </>
        )}

        {/* Contador: estável com ref para não sumir no refetch; oculto durante execução */}
        {displayNextRunAt && !isExecutingNow && (
          <div className="rounded-lg border border-primary-icon/30 bg-primary/5 overflow-hidden">
            <div className="px-3 pt-3 pb-1">
              <p className="text-[10px] uppercase tracking-wider text-primary-icon/80 font-medium">
                Próxima execução em
              </p>
              <p
                className="font-mono text-xl tabular-nums text-primary-icon mt-0.5"
                style={{ fontVariantNumeric: "tabular-nums" }}
              >
                {formatCountdown(countdownMs)}
              </p>
            </div>
            <div className="px-3 pb-3 pt-0">
              <p className="text-[10px] text-muted-foreground">
                {format(displayNextRunAt, "EEEE, d 'de' MMMM 'às' HH:mm", { locale: ptBR })}
              </p>
            </div>
          </div>
        )}

        <div>
          <div className="flex items-center justify-between mb-2">
            <Label className="text-xs font-medium flex items-center gap-1">
              <Building2 className="h-3.5 w-3.5" />
              Empresas
            </Label>
            <Button type="button" variant="ghost" size="sm" className="text-[10px] h-7" onClick={selectAllCompanies}>
              {filteredCompanies.length > 0 && filteredCompanies.every((company) => companyIds.has(company.id)) ? "Desmarcar visíveis" : "Marcar visíveis"}
            </Button>
          </div>
          <div className="relative mb-2">
            <Search className="absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={companySearch}
              onChange={(event) => setCompanySearch(event.target.value)}
              placeholder="Buscar empresa..."
              className="h-8 pl-8 text-xs"
            />
          </div>
          <div className="max-h-32 overflow-y-auto rounded border border-border bg-muted/30 p-2 space-y-1">
            {filteredCompanies.length === 0 ? (
              <p className="text-xs text-muted-foreground">Nenhuma empresa cadastrada.</p>
            ) : (
              filteredCompanies.map((c) => (
                <label key={c.id} className="flex items-center gap-2 cursor-pointer hover:bg-muted/50 rounded px-2 py-1">
                  <Checkbox checked={companyIds.has(c.id)} onCheckedChange={() => toggleCompany(c.id)} />
                  <span className="text-xs truncate">{c.name}</span>
                </label>
              ))
            )}
          </div>
        </div>

        <div>
          <Label className="text-xs font-medium flex items-center gap-1 mb-2">
            <Bot className="h-3.5 w-3.5" />
            Robôs vinculados
          </Label>
          <p className="text-[10px] text-muted-foreground mb-2">
            Arraste para definir a ordem de execução (o primeiro da lista roda primeiro).
          </p>
          <label className="flex items-center gap-2 cursor-pointer mb-2">
            <Checkbox checked={allRobots} onCheckedChange={(c) => toggleAllRobots(!!c)} />
            <span className="text-xs font-medium">Todos os robôs</span>
          </label>
          <div className="max-h-48 overflow-y-auto rounded border border-border bg-muted/30 p-2 space-y-1">
            {robots.length === 0 ? (
              <p className="text-xs text-muted-foreground">Nenhum robô vinculado.</p>
            ) : (
              <>
                {robotIdsOrdered.map((technicalId, index) => {
                  const r = robots.find((x) => x.technical_id === technicalId)
                  if (!r) return null
                  return (
                    <div
                      key={r.id}
                      draggable
                      onDragStart={(e) => handleDragStart(e, technicalId)}
                      onDragOver={handleDragOver}
                      onDrop={(e) => handleDrop(e, index)}
                      onDragEnd={handleDragEnd}
                      className={`flex items-center gap-2 rounded px-2 py-1.5 group cursor-grab active:cursor-grabbing ${draggedId === technicalId ? "opacity-50 bg-primary/20" : "hover:bg-muted/50"}`}
                    >
                      <GripVertical className="h-3.5 w-3.5 shrink-0 text-muted-foreground" aria-hidden />
                      <Checkbox
                        checked
                        onCheckedChange={() => toggleRobot(technicalId)}
                        className="shrink-0"
                      />
                      <span className="text-xs truncate flex-1">{r.display_name}</span>
                    </div>
                  )
                })}
                {robots
                  .filter((r) => !robotIdsOrdered.includes(r.technical_id))
                  .map((r) => (
                    <label
                      key={r.id}
                      className="flex items-center gap-2 cursor-pointer hover:bg-muted/50 rounded px-2 py-1.5"
                    >
                      <span className="w-3.5 shrink-0" />
                      <Checkbox
                        checked={false}
                        onCheckedChange={() => toggleRobot(r.technical_id)}
                        className="shrink-0"
                      />
                      <span className="text-xs truncate text-muted-foreground">Adicionar: {r.display_name}</span>
                    </label>
                  ))}
              </>
            )}
          </div>
        </div>

        <div className="grid grid-cols-2 gap-2">
          <div>
            <Label className="text-[10px] text-muted-foreground">Data e hora de execução</Label>
            <input
              type="date"
              value={runAtDate}
              onChange={(e) => setRunAtDate(e.target.value)}
              className="mt-0.5 w-full rounded border border-input bg-background px-2 py-1.5 text-xs"
            />
          </div>
          <div>
            <Label className="text-[10px] text-muted-foreground">Horário</Label>
            <input
              type="time"
              value={runAtTime}
              onChange={(e) => setRunAtTime(e.target.value.slice(0, 5))}
              className="mt-0.5 w-full rounded border border-input bg-background px-2 py-1.5 text-xs"
            />
          </div>
        </div>

        <label className="flex items-center gap-2 cursor-pointer">
          <input type="checkbox" checked={runDaily} onChange={(e) => setRunDaily(e.target.checked)} className="rounded border-input" />
          <span className="text-xs font-medium">Rodar automaticamente a cada 24h neste horário</span>
        </label>
        <div className="rounded-md border border-border bg-muted/30 px-3 py-3 space-y-2">
          <Label className="text-xs font-medium">Modo de execução dos robôs</Label>
          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="radio"
              name="execution-mode"
              checked={executionMode === "sequential"}
              onChange={() => {
                executionModeRef.current = "sequential"
                setExecutionMode("sequential")
              }}
              className="rounded-full border-input"
            />
            <span className="text-xs">Um por vez, respeitando a ordem da lista</span>
          </label>
          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="radio"
              name="execution-mode"
              checked={executionMode === "parallel"}
              onChange={() => {
                executionModeRef.current = "parallel"
                setExecutionMode("parallel")
              }}
              className="rounded-full border-input"
            />
            <span className="text-xs">Todos os robôs selecionados de uma vez</span>
          </label>
        </div>
        {runDaily && (
          <div className="rounded-md bg-muted/50 border border-border px-3 py-2 text-[10px] text-muted-foreground">
            <p>Primeira execução na data e hora acima. Depois, o sistema repete a cada 24 horas. O período de cada robô segue o modo configurado em Admin → Robôs (por competência ou por intervalo).</p>
          </div>
        )}

        <div className="flex gap-2">
          {isActive ? (
            <Button type="button" variant="destructive" onClick={handleStop} disabled={submitting} className="flex-1">
              {submitting ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Square className="h-4 w-4 mr-2" />}
              Parar agendamento
            </Button>
          ) : (
            <Button
              type="button"
              onClick={handleRun}
              disabled={submitting || companyIds.size === 0 || robotIdsOrdered.length === 0}
              className="flex-1"
            >
              {submitting ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin mr-2" />
                  Salvando...
                </>
              ) : (
                <>
                  <Play className="h-4 w-4 mr-2" />
                  Executar
                </>
              )}
            </Button>
          )}
        </div>
      </div>
    </GlassCard>
  )
}
