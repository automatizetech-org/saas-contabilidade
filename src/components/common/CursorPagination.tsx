import { Button } from "@/components/ui/button";
import {
  Pagination,
  PaginationContent,
  PaginationItem,
  PaginationNext,
  PaginationPrevious,
} from "@/components/ui/pagination";

type CursorPaginationProps = {
  currentPage: number;
  pageSize: number;
  shownItems: number;
  hasMore: boolean;
  onFirst: () => void;
  onPrevious: () => void;
  onNext: () => void;
  onPageSizeChange: (pageSize: number) => void;
  pageSizeOptions?: number[];
  label?: string;
  /** Total conhecido (ex.: do resumo RPC) para exibir "Itens X–Y de Z" em listas fiscais. */
  totalApprox?: number | null;
};

export function CursorPagination({
  currentPage,
  pageSize,
  shownItems,
  hasMore,
  onFirst,
  onPrevious,
  onNext,
  onPageSizeChange,
  pageSizeOptions = [10, 20, 50],
  label = "por página",
  totalApprox = null,
}: CursorPaginationProps) {
  if (shownItems <= 0) return null;

  const maxPageFromTotal =
    typeof totalApprox === "number" && totalApprox > 0
      ? Math.max(1, Math.ceil(totalApprox / pageSize))
      : null;

  let from: number;
  let to: number;
  let pageLabel: number;

  if (maxPageFromTotal != null) {
    pageLabel = Math.min(currentPage, maxPageFromTotal);
    if (currentPage > maxPageFromTotal) {
      // Estado defasado (ex.: total caiu ou RPC corrigiu); exibe última faixa válida até o pai resetar
      from = (maxPageFromTotal - 1) * pageSize + 1;
      to = totalApprox;
    } else {
      from = (currentPage - 1) * pageSize + 1;
      to = Math.min((currentPage - 1) * pageSize + shownItems, totalApprox);
    }
  } else {
    pageLabel = currentPage;
    from = (currentPage - 1) * pageSize + 1;
    to = (currentPage - 1) * pageSize + shownItems;
  }

  const totalSuffix =
    maxPageFromTotal != null && typeof totalApprox === "number"
      ? ` de ${totalApprox.toLocaleString("pt-BR")}`
      : "";

  const nextDisabled = !hasMore || (maxPageFromTotal != null && currentPage >= maxPageFromTotal);

  return (
    <div className="flex flex-col gap-3 border-t border-border bg-muted/20 px-3 py-3 sm:flex-row sm:items-center sm:justify-between">
      <div className="flex items-center gap-3">
        <span className="text-xs text-muted-foreground">Mostrar</span>
        <select
          value={pageSize}
          onChange={(e) => onPageSizeChange(Number(e.target.value))}
          className="rounded-md border border-border bg-background px-2 py-1 text-xs"
        >
          {pageSizeOptions.map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
        <span className="text-xs text-muted-foreground">{label}</span>
      </div>

      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
        <span className="text-xs text-muted-foreground">
          Itens {from.toLocaleString("pt-BR")}–{to.toLocaleString("pt-BR")}
          {totalSuffix}
        </span>
        <Pagination className="mx-0 w-auto justify-start">
          <PaginationContent>
            <PaginationItem>
              <PaginationPrevious
                href="#"
                onClick={(e) => {
                  e.preventDefault();
                  onPrevious();
                }}
                className={currentPage <= 1 ? "pointer-events-none opacity-50" : ""}
              />
            </PaginationItem>
            <PaginationItem>
              <Button variant="outline" size="sm" className="h-8 px-3 text-xs" disabled>
                Página {pageLabel}
                {maxPageFromTotal != null ? ` / ${maxPageFromTotal}` : ""}
              </Button>
            </PaginationItem>
            <PaginationItem>
              <PaginationNext
                href="#"
                onClick={(e) => {
                  e.preventDefault();
                  onNext();
                }}
                className={nextDisabled ? "pointer-events-none opacity-50" : ""}
              />
            </PaginationItem>
          </PaginationContent>
        </Pagination>
      </div>
    </div>
  );
}
