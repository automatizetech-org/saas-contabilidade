import { getRecentExecutionRequests } from "./executionRequestsService"
import { getRobots } from "./robotsService"
import { supabase } from "./supabaseClient"

export type OperationsOverview = {
  eventosHoje: number
  eventosOntem: number
  falhas: number
  robots: number
  taxaSucesso: number
}

export async function getOperationsOverview(): Promise<OperationsOverview> {
  try {
    const { data, error } = await supabase.rpc("get_operations_overview_summary")
    if (error) throw error
    const payload = (data ?? {}) as Partial<OperationsOverview>
    return {
      eventosHoje: Number(payload.eventosHoje ?? 0),
      eventosOntem: Number(payload.eventosOntem ?? 0),
      falhas: Number(payload.falhas ?? 0),
      robots: Number(payload.robots ?? 0),
      taxaSucesso: Number(payload.taxaSucesso ?? 0),
    }
  } catch {
    const [robots, executions] = await Promise.all([
      getRobots(),
      getRecentExecutionRequests(200),
    ])

    const now = new Date()
    const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate())
    const yesterdayStart = new Date(todayStart)
    yesterdayStart.setDate(yesterdayStart.getDate() - 1)

    let eventosHoje = 0
    let eventosOntem = 0
    let successCount = 0
    let failCount = 0

    for (const execution of executions.filter((item) => item.status === "completed" || item.status === "failed")) {
      const timestamp = new Date(execution.completed_at ?? execution.created_at).getTime()
      if (timestamp >= todayStart.getTime()) eventosHoje += 1
      else if (timestamp >= yesterdayStart.getTime() && timestamp < todayStart.getTime()) eventosOntem += 1
      if (execution.status === "completed") successCount += 1
      if (execution.status === "failed") failCount += 1
    }

    const total = successCount + failCount
    return {
      eventosHoje,
      eventosOntem,
      falhas: failCount,
      robots: robots.length,
      taxaSucesso: total > 0 ? Math.round((successCount / total) * 1000) / 10 : 0,
    }
  }
}
