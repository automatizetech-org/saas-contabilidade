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
  onPrevious: () => void;
  onNext: () => void;
  onPageSizeChange: (pageSize: number) => void;
  pageSizeOptions?: number[];
  label?: string;
};

export function CursorPagination({
  currentPage,
  pageSize,
  shownItems,
  hasMore,
  onPrevious,
  onNext,
  onPageSizeChange,
  pageSizeOptions = [10, 20, 50],
  label = "por página",
}: CursorPaginationProps) {
  if (shownItems <= 0) return null;

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

      <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
        <span className="text-xs text-muted-foreground">
          Página {currentPage} • {shownItems} item(ns)
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
                {hasMore ? "Mais resultados disponíveis" : "Fim da lista"}
              </Button>
            </PaginationItem>
            <PaginationItem>
              <PaginationNext
                href="#"
                onClick={(e) => {
                  e.preventDefault();
                  onNext();
                }}
                className={!hasMore ? "pointer-events-none opacity-50" : ""}
              />
            </PaginationItem>
          </PaginationContent>
        </Pagination>
      </div>
    </div>
  );
}
