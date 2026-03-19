import { ChevronsLeft, ChevronsRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Pagination,
  PaginationContent,
  PaginationItem,
  PaginationNext,
  PaginationPrevious,
} from "@/components/ui/pagination";

type DataPaginationProps = {
  currentPage: number;
  totalPages: number;
  totalItems: number;
  from: number;
  to: number;
  pageSize: number;
  onPageChange: (page: number) => void;
  onPageSizeChange: (pageSize: number) => void;
  pageSizeOptions?: number[];
  label?: string;
};

export function DataPagination({
  currentPage,
  totalPages,
  totalItems,
  from,
  to,
  pageSize,
  onPageChange,
  onPageSizeChange,
  pageSizeOptions = [10, 20, 50],
  label = "por página",
}: DataPaginationProps) {
  if (totalItems <= 0) return null;

  const safeTotalPages = Math.max(1, Math.ceil(totalItems / pageSize));
  const isLastPage = currentPage >= safeTotalPages;
  const isFirstPage = currentPage <= 1;
  const canGoToLast = safeTotalPages > 1 && !isLastPage;

  return (
    <div className="flex flex-col gap-3 border-t border-border bg-muted/20 px-3 py-3 sm:flex-row sm:items-center sm:justify-between min-w-0">
      <div className="flex items-center gap-3 shrink-0">
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

      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3 min-w-0 flex-wrap">
        <span className="text-xs text-muted-foreground shrink-0">
          Itens {from}-{to} de {totalItems}
        </span>
        <Pagination className="mx-0 w-auto justify-start min-w-0">
          <PaginationContent className="flex-wrap gap-1">
            <PaginationItem>
              <Button
                variant="ghost"
                size="sm"
                type="button"
                className="h-8 gap-0.5 px-2 text-xs"
                onClick={() => onPageChange(1)}
                disabled={isFirstPage}
                aria-label="Primeira página"
              >
                <ChevronsLeft className="h-4 w-4" />
                <span className="hidden sm:inline">Primeira</span>
              </Button>
            </PaginationItem>
            <PaginationItem>
              <PaginationPrevious
                href="#"
                onClick={(e) => {
                  e.preventDefault();
                  onPageChange(Math.max(1, currentPage - 1));
                }}
                className={isFirstPage ? "pointer-events-none opacity-50" : ""}
              />
            </PaginationItem>
            <PaginationItem>
              <Button variant="outline" size="sm" className="h-8 px-3 text-xs" disabled>
                Página {currentPage} de {safeTotalPages}
              </Button>
            </PaginationItem>
            <PaginationItem>
              <PaginationNext
                href="#"
                onClick={(e) => {
                  e.preventDefault();
                  onPageChange(Math.min(safeTotalPages, currentPage + 1));
                }}
                className={isLastPage ? "pointer-events-none opacity-50" : ""}
              />
            </PaginationItem>
            <PaginationItem>
              <Button
                variant="ghost"
                size="sm"
                type="button"
                className="h-8 gap-0.5 px-2 text-xs shrink-0"
                onClick={() => canGoToLast && onPageChange(safeTotalPages)}
                disabled={!canGoToLast}
                aria-label="Última página"
              >
                <span className="hidden sm:inline">Última</span>
                <ChevronsRight className="h-4 w-4" />
              </Button>
            </PaginationItem>
          </PaginationContent>
        </Pagination>
      </div>
    </div>
  );
}
