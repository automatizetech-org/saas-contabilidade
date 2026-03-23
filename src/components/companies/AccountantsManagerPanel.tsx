import { useMemo, useRef, useState } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { ArrowRightLeft, Loader2, Pencil, Search, Trash2 } from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { cn } from "@/utils"
import {
  createAccountant,
  deleteAccountant,
  formatCpf,
  getAccountants,
  updateAccountant,
} from "@/services/accountantsService"
import { getCompaniesForUser, updateCompaniesContadorBatch } from "@/services/companiesService"
import { formatCpfInput, isValidCpf } from "@/lib/brazilDocuments"
import type { Company } from "@/services/profilesService"

type CompanyWithCert = Company & {
  contador_nome?: string | null
  contador_cpf?: string | null
}

function onlyDigits(s: string) {
  return s.replace(/\D/g, "")
}

export function AccountantsManagerPanel({
  enabled = true,
  showCloseButton = false,
  onClose,
  onAccountantCreated,
}: {
  enabled?: boolean
  showCloseButton?: boolean
  onClose?: () => void
  onAccountantCreated?: (cpf: string) => void
}) {
  const queryClient = useQueryClient()
  const [contadorNewName, setContadorNewName] = useState("")
  const [contadorNewCpf, setContadorNewCpf] = useState("")
  const [contadorSaving, setContadorSaving] = useState(false)
  const [contadorError, setContadorError] = useState("")
  const [contadorEditingId, setContadorEditingId] = useState<string | null>(null)
  const [contadorEditName, setContadorEditName] = useState("")
  const [contadorEditCpf, setContadorEditCpf] = useState("")
  const [contadorEditActive, setContadorEditActive] = useState(true)
  const [contadorExcludeId, setContadorExcludeId] = useState<string | null>(null)
  const [moveStep, setMoveStep] = useState<null | "source" | "companies" | "dest">(null)
  const [moveSourceCpf, setMoveSourceCpf] = useState<string | null>(null)
  const [moveSelectedIds, setMoveSelectedIds] = useState<Set<string>>(new Set())
  const [moveCompaniesSearch, setMoveCompaniesSearch] = useState("")
  const [moveLastClickedIndex, setMoveLastClickedIndex] = useState<number | null>(null)
  const [moveDestCpf, setMoveDestCpf] = useState<string | null>(null)
  const [moveSaving, setMoveSaving] = useState(false)
  const moveCompaniesListRef = useRef<HTMLDivElement>(null)

  const { data: accountantsAll = [], isLoading: accountantsAllLoading } = useQuery({
    queryKey: ["accountants", "all"],
    queryFn: () => getAccountants(false),
    enabled,
  })
  const { data: companiesForMove = [] } = useQuery({
    queryKey: ["companies-list", "all"],
    queryFn: () => getCompaniesForUser("all"),
    enabled: enabled && moveStep !== null,
  })

  const sourceAcc = useMemo(
    () => accountantsAll.find((a) => onlyDigits(a.cpf) === onlyDigits(moveSourceCpf ?? "")),
    [accountantsAll, moveSourceCpf]
  )

  const companiesFromSource = useMemo(
    () =>
      companiesForMove.filter(
        (c) => onlyDigits((c as CompanyWithCert).contador_cpf ?? "") === onlyDigits(moveSourceCpf ?? "")
      ),
    [companiesForMove, moveSourceCpf]
  )

  const filteredCompanies = useMemo(() => {
    const searchTerm = moveCompaniesSearch.trim()
    const searchLower = searchTerm.toLowerCase()
    const searchDigits = searchTerm.replace(/\D/g, "")
    return searchTerm
      ? companiesFromSource.filter((c) => {
          const nameMatch = searchLower ? c.name.toLowerCase().includes(searchLower) : false
          const docMatch = searchDigits.length > 0 && (c.document ?? "").replace(/\D/g, "").includes(searchDigits)
          return nameMatch || docMatch
        })
      : companiesFromSource
  }, [companiesFromSource, moveCompaniesSearch])

  const visibleIds = useMemo(() => new Set(filteredCompanies.map((c) => c.id)), [filteredCompanies])
  const allFilteredSelected =
    filteredCompanies.length > 0 && filteredCompanies.every((c) => moveSelectedIds.has(c.id))

  const syncAccountantCompanies = async (currentCpf: string, nextCpf: string, nextName: string) => {
    const previousCpfDigits = onlyDigits(currentCpf)
    if (!previousCpfDigits) return

    const companies = await getCompaniesForUser("all")
    const linkedCompanies = companies.filter(
      (company) => onlyDigits((company as CompanyWithCert).contador_cpf ?? "") === previousCpfDigits
    )

    const ids = linkedCompanies.map((c) => c.id)
    await updateCompaniesContadorBatch(ids, nextCpf, nextName)
  }

  return (
    <>
      <p className="text-sm text-muted-foreground">
        Contadores cadastrados aparecem ao escolher o responsavel por uma empresa. Voce pode editar, inativar e excluir qualquer um.
      </p>
      {moveStep !== null ? (
        <>
          {moveStep === "source" && (
            <>
              <p className="text-sm font-medium">Selecione o contador de origem</p>
              <Select value={moveSourceCpf ?? ""} onValueChange={setMoveSourceCpf}>
                <SelectTrigger className="w-full"><SelectValue placeholder="Contador de origem" /></SelectTrigger>
                <SelectContent>
                  {accountantsAll.map((a) => (
                    <SelectItem key={a.id} value={a.cpf}>{a.name} - {formatCpf(a.cpf)}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <div className="flex flex-col gap-2 sm:flex-row">
                <Button variant="outline" size="sm" onClick={() => setMoveStep(null)}>Voltar</Button>
                <Button
                  size="sm"
                  disabled={!moveSourceCpf}
                  onClick={() => {
                    setMoveStep("companies")
                    setMoveSelectedIds(new Set())
                    setMoveCompaniesSearch("")
                    setMoveLastClickedIndex(null)
                  }}
                >
                  Avancar
                </Button>
              </div>
            </>
          )}
          {moveStep === "companies" && moveSourceCpf && (
            <>
              <p className="text-sm font-medium">Empresas sob responsabilidade de {sourceAcc?.name ?? "-"}</p>
              <p className="text-xs text-muted-foreground">Selecione as empresas que deseja mover. Use Ctrl+A para selecionar todas as visiveis e Shift+clique para intervalo.</p>
              <div className="relative">
                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground pointer-events-none" />
                <Input
                  placeholder="Buscar por nome ou CNPJ..."
                  value={moveCompaniesSearch}
                  onChange={(e) => setMoveCompaniesSearch(e.target.value)}
                  className="pl-8 h-9 mb-2"
                  autoComplete="off"
                  aria-label="Buscar empresas por nome ou CNPJ"
                />
              </div>
              {moveCompaniesSearch.trim() ? (
                <p className="text-xs text-muted-foreground mb-1">
                  {filteredCompanies.length} de {companiesFromSource.length} empresa(s)
                </p>
              ) : null}
              <div
                ref={moveCompaniesListRef}
                className="border rounded-lg divide-y divide-border max-h-48 overflow-y-auto focus-within:ring-2 focus-within:ring-ring focus-within:ring-offset-2 outline-none"
                tabIndex={0}
                onKeyDown={(e) => {
                  const target = e.target as HTMLElement
                  if (target.closest("input") || target.closest("textarea")) return
                  if (e.ctrlKey && e.key.toLowerCase() === "a") {
                    e.preventDefault()
                    setMoveSelectedIds((prev) => {
                      const next = new Set(prev)
                      if (allFilteredSelected) visibleIds.forEach((id) => next.delete(id))
                      else visibleIds.forEach((id) => next.add(id))
                      return next
                    })
                  }
                }}
              >
                {filteredCompanies.length === 0 ? (
                  <div className="px-4 py-3 text-sm text-muted-foreground">
                    {companiesFromSource.length === 0 ? "Nenhuma empresa vinculada a este contador." : "Nenhuma empresa encontrada na busca."}
                  </div>
                ) : (
                  filteredCompanies.map((emp, index) => (
                    <label
                      key={emp.id}
                      className="flex items-center gap-2 px-4 py-2 hover:bg-muted/30 cursor-pointer select-none"
                      onClick={(e) => {
                        e.preventDefault()
                        const target = e.target as HTMLElement
                        if (target.closest('input[type="checkbox"]')) return
                        const shift = (e as React.MouseEvent).shiftKey
                        if (shift && moveLastClickedIndex !== null) {
                          const from = Math.min(moveLastClickedIndex, index)
                          const to = Math.max(moveLastClickedIndex, index)
                          const toSelect = !moveSelectedIds.has(emp.id)
                          setMoveSelectedIds((prev) => {
                            const next = new Set(prev)
                            for (let i = from; i <= to; i += 1) {
                              const id = filteredCompanies[i].id
                              if (toSelect) next.add(id)
                              else next.delete(id)
                            }
                            return next
                          })
                        } else {
                          setMoveLastClickedIndex(index)
                          setMoveSelectedIds((prev) => {
                            const next = new Set(prev)
                            if (next.has(emp.id)) next.delete(emp.id)
                            else next.add(emp.id)
                            return next
                          })
                        }
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={moveSelectedIds.has(emp.id)}
                        onChange={(e) => {
                          e.stopPropagation()
                          setMoveLastClickedIndex(index)
                          setMoveSelectedIds((prev) => {
                            const next = new Set(prev)
                            if (e.target.checked) next.add(emp.id)
                            else next.delete(emp.id)
                            return next
                          })
                        }}
                        className="rounded border-input"
                      />
                      <span className="text-sm truncate">{emp.name}</span>
                      {emp.document && <span className="text-xs text-muted-foreground truncate ml-1">({emp.document.replace(/\D/g, "").slice(0, 8)}...)</span>}
                    </label>
                  ))
                )}
              </div>
              <div className="mt-2 flex flex-col gap-2 sm:flex-row sm:flex-wrap">
                <Button variant="outline" size="sm" onClick={() => setMoveStep("source")}>Voltar</Button>
                <Button size="sm" disabled={moveSelectedIds.size === 0} onClick={() => { setMoveStep("dest"); setMoveDestCpf(null) }}>
                  Mover {moveSelectedIds.size} selecionada(s)
                </Button>
              </div>
            </>
          )}
          {moveStep === "dest" && moveSourceCpf && (
            <>
              <p className="text-sm font-medium">Mover para qual contador?</p>
              <Select value={moveDestCpf ?? ""} onValueChange={setMoveDestCpf}>
                <SelectTrigger className="w-full"><SelectValue placeholder="Selecione o contador de destino" /></SelectTrigger>
                <SelectContent>
                  {accountantsAll.filter((a) => onlyDigits(a.cpf) !== onlyDigits(moveSourceCpf)).map((a) => (
                    <SelectItem key={a.id} value={a.cpf}>{a.name} - {formatCpf(a.cpf)}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <div className="flex flex-col gap-2 sm:flex-row">
                <Button variant="outline" size="sm" onClick={() => setMoveStep("companies")}>Voltar</Button>
                <Button
                  size="sm"
                  disabled={!moveDestCpf || moveSaving}
                  onClick={async () => {
                    const destAcc = accountantsAll.find((a) => onlyDigits(a.cpf) === onlyDigits(moveDestCpf ?? ""))
                    const n = moveSelectedIds.size
                    if (!destAcc || n === 0) return
                    const ids = [...moveSelectedIds]
                    setMoveSaving(true)
                    try {
                      await updateCompaniesContadorBatch(ids, destAcc.cpf, destAcc.name)
                      queryClient.invalidateQueries({ queryKey: ["companies-list"] })
                      setMoveStep(null)
                      setMoveSourceCpf(null)
                      setMoveSelectedIds(new Set())
                      setMoveDestCpf(null)
                      toast.success(`${n} empresa(s) movida(s) para ${destAcc.name}.`)
                    } catch (error) {
                      toast.error(error instanceof Error ? error.message : "Erro ao mover")
                    } finally {
                      setMoveSaving(false)
                    }
                  }}
                >
                  {moveSaving ? "Movendo..." : "Confirmar"}
                </Button>
              </div>
            </>
          )}
        </>
      ) : accountantsAllLoading ? (
        <div className="flex items-center justify-center py-8">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : (
        <>
          <ul className="border rounded-lg divide-y divide-border max-h-60 overflow-y-auto">
            {accountantsAll.length === 0 ? (
              <li className="px-4 py-3 text-sm text-muted-foreground">Nenhum contador cadastrado.</li>
            ) : (
              accountantsAll.map((acc) => (
                <li key={acc.id} className="px-4 py-3">
                  {contadorEditingId === acc.id ? (
                    <div className="flex flex-col gap-2 flex-1 min-w-0">
                      <Input
                        value={contadorEditName}
                        onChange={(e) => setContadorEditName(e.target.value)}
                        placeholder="Nome"
                        className="h-8 text-sm"
                        disabled={contadorSaving}
                      />
                      <Input
                        value={contadorEditCpf}
                        onChange={(e) => setContadorEditCpf(formatCpfInput(e.target.value))}
                        placeholder="000.000.000-00"
                        className="h-8 text-sm"
                        disabled={contadorSaving}
                      />
                      <div className="flex items-center gap-2">
                        <label className="flex items-center gap-1.5 text-xs cursor-pointer">
                          <input
                            type="checkbox"
                            checked={contadorEditActive}
                            onChange={(e) => setContadorEditActive(e.target.checked)}
                            disabled={contadorSaving}
                            className="rounded border-input"
                          />
                          Ativo
                        </label>
                        <Button
                          size="sm"
                          className="h-7"
                          disabled={contadorSaving}
                          onClick={async () => {
                            setContadorError("")
                            setContadorSaving(true)
                            try {
                              await updateAccountant(acc.id, {
                                name: contadorEditName,
                                cpf: contadorEditCpf,
                                active: contadorEditActive,
                              })
                              await syncAccountantCompanies(acc.cpf, contadorEditCpf, contadorEditName.trim())
                              queryClient.invalidateQueries({ queryKey: ["accountants"] })
                              queryClient.invalidateQueries({ queryKey: ["accountants", "all"] })
                              queryClient.invalidateQueries({ queryKey: ["companies-list"] })
                              setContadorEditingId(null)
                              toast.success("Contador atualizado.")
                            } catch (error) {
                              setContadorError(error instanceof Error ? error.message : "Erro ao salvar")
                            } finally {
                              setContadorSaving(false)
                            }
                          }}
                        >
                          Salvar
                        </Button>
                        <Button size="sm" variant="ghost" className="h-7" disabled={contadorSaving} onClick={() => setContadorEditingId(null)}>
                          Cancelar
                        </Button>
                      </div>
                    </div>
                  ) : (
                    <div className="space-y-3">
                      <div className="min-w-0 flex-1">
                        <p className="text-sm font-medium truncate">{acc.name}</p>
                        <p className="text-xs text-muted-foreground">{formatCpf(acc.cpf)}</p>
                      </div>
                      <div className="flex flex-wrap items-center gap-2">
                        <span className={cn("text-xs font-medium px-2 py-0.5 rounded-full", acc.active ? "bg-success/15 text-success" : "bg-muted text-muted-foreground")}>
                          {acc.active ? "Ativo" : "Inativo"}
                        </span>
                        <Button
                          size="sm"
                          variant="outline"
                          className="h-7 gap-1"
                          onClick={() => {
                            setContadorEditingId(acc.id)
                            setContadorEditName(acc.name)
                            setContadorEditCpf(formatCpfInput(acc.cpf))
                            setContadorEditActive(acc.active)
                            setContadorError("")
                          }}
                        >
                          <Pencil className="h-3 w-3" />
                          Editar
                        </Button>
                        {acc.active && (
                          <Button
                            size="sm"
                            variant="ghost"
                            className="h-7 text-destructive hover:text-destructive"
                            disabled={contadorSaving}
                            onClick={async () => {
                              setContadorError("")
                              setContadorSaving(true)
                              try {
                                await updateAccountant(acc.id, { active: false })
                                queryClient.invalidateQueries({ queryKey: ["accountants"] })
                                queryClient.invalidateQueries({ queryKey: ["accountants", "all"] })
                                toast.success("Contador inativado.")
                              } catch (error) {
                                setContadorError(error instanceof Error ? error.message : "Erro ao inativar")
                              } finally {
                                setContadorSaving(false)
                              }
                            }}
                          >
                            Inativar
                          </Button>
                        )}
                        <Button
                          size="sm"
                          variant="ghost"
                          className="h-7 text-destructive hover:text-destructive"
                          disabled={contadorSaving}
                          onClick={() => setContadorExcludeId(acc.id)}
                          title="Excluir contador"
                        >
                          <Trash2 className="h-3 w-3" />
                        </Button>
                      </div>
                    </div>
                  )}
                </li>
              ))
            )}
          </ul>
          {contadorError && <p className="text-sm text-destructive">{contadorError}</p>}
          <div className="border-t pt-4">
            <Button type="button" variant="outline" size="sm" className="mb-4 w-full gap-2" onClick={() => setMoveStep("source")}>
              <ArrowRightLeft className="h-4 w-4" />
              Mover responsabilidade de empresas
            </Button>
          </div>
          <div className="border-t pt-4 space-y-2">
            <p className="text-sm font-medium">Adicionar contador</p>
            <div className="grid gap-3">
              <div className="space-y-1">
                <Label className="text-xs">Nome</Label>
                <Input
                  value={contadorNewName}
                  onChange={(e) => setContadorNewName(e.target.value)}
                  placeholder="Nome do contador"
                  className="h-8"
                  disabled={contadorSaving}
                />
              </div>
              <div className="space-y-1">
                <Label className="text-xs">CPF</Label>
                <Input
                  value={contadorNewCpf}
                  onChange={(e) => setContadorNewCpf(formatCpfInput(e.target.value))}
                  placeholder="000.000.000-00"
                  className="h-8"
                  disabled={contadorSaving}
                />
              </div>
            </div>
            <div className="flex justify-start">
              <Button
                className="w-full sm:w-auto"
                size="sm"
                disabled={contadorSaving || !contadorNewName.trim() || !isValidCpf(contadorNewCpf)}
                onClick={async () => {
                  setContadorError("")
                  setContadorSaving(true)
                  try {
                    const accountant = await createAccountant({ name: contadorNewName.trim(), cpf: contadorNewCpf })
                    queryClient.invalidateQueries({ queryKey: ["accountants"] })
                    queryClient.invalidateQueries({ queryKey: ["accountants", "all"] })
                    onAccountantCreated?.(accountant.cpf)
                    setContadorNewName("")
                    setContadorNewCpf("")
                    toast.success("Contador adicionado.")
                  } catch (error) {
                    setContadorError(error instanceof Error ? error.message : "Erro ao adicionar")
                  } finally {
                    setContadorSaving(false)
                  }
                }}
              >
                Adicionar
              </Button>
            </div>
          </div>
        </>
      )}
      {showCloseButton && onClose ? (
        <div className="flex justify-end">
          <Button type="button" onClick={onClose}>Fechar</Button>
        </div>
      ) : null}

      <AlertDialog open={!!contadorExcludeId} onOpenChange={(open) => !open && setContadorExcludeId(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Excluir contador?</AlertDialogTitle>
            <AlertDialogDescription>
              {contadorExcludeId && accountantsAll.find((a) => a.id === contadorExcludeId)
                ? `O contador "${accountantsAll.find((a) => a.id === contadorExcludeId)!.name}" sera excluido. As empresas vinculadas a ele nao serao removidas, apenas ficarao sem contador responsavel.`
                : "Este contador sera excluido."}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={contadorSaving}>Cancelar</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={contadorSaving}
              onClick={async () => {
                if (!contadorExcludeId) return
                setContadorSaving(true)
                try {
                  await deleteAccountant(contadorExcludeId)
                  queryClient.invalidateQueries({ queryKey: ["accountants"] })
                  queryClient.invalidateQueries({ queryKey: ["accountants", "all"] })
                  setContadorExcludeId(null)
                  toast.success("Contador excluido.")
                } catch (error) {
                  toast.error(error instanceof Error ? error.message : "Erro ao excluir")
                } finally {
                  setContadorSaving(false)
                }
              }}
            >
              {contadorSaving ? "Excluindo..." : "Excluir"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}
