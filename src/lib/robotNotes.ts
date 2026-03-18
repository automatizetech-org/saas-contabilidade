import type { Tables } from "@/types/database"

export type RobotNotesMode = Tables<"robots">["notes_mode"]
export type FiscalNotesKind = NonNullable<Tables<"robots">["fiscal_notes_kind"]>
export type RobotRecord = Tables<"robots">

export type RobotNotesOption = {
  value: NonNullable<RobotNotesMode>
  label: string
  description: string
}

const NFS_OPTIONS: RobotNotesOption[] = [
  { value: "recebidas", label: "Recebidas", description: "Baixa NFS tomadas." },
  { value: "emitidas", label: "Emitidas", description: "Baixa NFS prestadas." },
  { value: "both", label: "Emitidas + Recebidas", description: "Baixa NFS tomadas e prestadas." },
]

const NFE_NFC_OPTIONS: RobotNotesOption[] = [
  { value: "modelo_55", label: "Modelo 55", description: "Baixa NF-e." },
  { value: "modelo_65", label: "Modelo 65", description: "Baixa NFC-e." },
  { value: "modelos_55_65", label: "Modelos 55 + 65", description: "Baixa NF-e e NFC-e." },
]

export function getNotesModeOptions(kind: FiscalNotesKind): RobotNotesOption[] {
  return kind === "nfe_nfc" ? NFE_NFC_OPTIONS : NFS_OPTIONS
}

export function getDefaultNotesMode(kind: FiscalNotesKind): NonNullable<RobotNotesMode> {
  return kind === "nfe_nfc" ? "modelo_55" : "recebidas"
}

export function isNotesModeCompatible(
  kind: FiscalNotesKind,
  mode: RobotNotesMode | null | undefined
): mode is NonNullable<RobotNotesMode> {
  if (!mode) return false
  return getNotesModeOptions(kind).some((option) => option.value === mode)
}

export function getRobotNotesMode(robot: Pick<RobotRecord, "is_fiscal_notes_robot" | "fiscal_notes_kind" | "notes_mode">): RobotNotesMode | null {
  if (!robot.is_fiscal_notes_robot || !robot.fiscal_notes_kind) return null
  if (!isNotesModeCompatible(robot.fiscal_notes_kind, robot.notes_mode)) {
    return getDefaultNotesMode(robot.fiscal_notes_kind)
  }
  return robot.notes_mode
}

export function getCommonRobotNotesMode(
  robots: Array<Pick<RobotRecord, "is_fiscal_notes_robot" | "fiscal_notes_kind" | "notes_mode">>
): RobotNotesMode | null {
  const modes = robots
    .map(getRobotNotesMode)
    .filter((mode): mode is NonNullable<RobotNotesMode> => !!mode)

  if (modes.length === 0 || modes.length !== robots.filter((robot) => robot.is_fiscal_notes_robot).length) {
    return null
  }

  const first = modes[0]
  return modes.every((mode) => mode === first) ? first : null
}
