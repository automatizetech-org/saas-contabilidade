from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

try:
    from PyPDF2 import PdfReader
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "PyPDF2 nao esta instalado. Instale com: pip install PyPDF2"
    ) from exc


DEFAULT_ROOT = Path(r"Z:\METODOLOGIA\PESSOAL\Victor\Downloads")
DEFAULT_COMPETENCIA = "02/2026"
COMPETENCIA_RE = re.compile(r"\b(?:0[1-9]|1[0-2])/\d{4}\b")
FGTS_MARKERS = (
    "guia do fgts",
    "fgts digital",
    "gfd - guia do fgts digital",
)


@dataclass
class PdfAnalysis:
    company: str
    pdf_path: Path
    competencias: list[str]
    is_fgts: bool
    error: str | None = None


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip().lower()


def extract_pdf_text(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    chunks: list[str] = []

    for page in reader.pages:
        text = page.extract_text() or ""
        if text:
            chunks.append(text)

    return "\n".join(chunks)


def looks_like_fgts(pdf_path: Path, text: str) -> bool:
    normalized_text = normalize_text(text)
    normalized_name = normalize_text(pdf_path.name)

    if any(marker in normalized_text for marker in FGTS_MARKERS):
        return True

    if "fgts" in normalized_name and "guia" in normalized_name:
        return True

    return False


def extract_competencias(text: str) -> list[str]:
    targeted_patterns = [
        re.compile(
            r"tag.{0,120}?\b((?:0[1-9]|1[0-2])/\d{4})\b\s+mensal",
            flags=re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r"compet[êe]ncia.{0,300}?\b((?:0[1-9]|1[0-2])/\d{4})\b",
            flags=re.IGNORECASE | re.DOTALL,
        ),
    ]

    for pattern in targeted_patterns:
        matches = [match.group(1) for match in pattern.finditer(text)]
        ordered_unique = list(dict.fromkeys(matches))
        if ordered_unique:
            return ordered_unique

    competencias = [match.group(0) for match in COMPETENCIA_RE.finditer(text)]
    ordered_unique = list(dict.fromkeys(competencias))

    return ordered_unique


def analyze_pdf(company: str, pdf_path: Path) -> PdfAnalysis:
    try:
        text = extract_pdf_text(pdf_path)
    except Exception as exc:
        return PdfAnalysis(
            company=company,
            pdf_path=pdf_path,
            competencias=[],
            is_fgts=False,
            error=f"falha ao ler PDF: {exc}",
        )

    is_fgts = looks_like_fgts(pdf_path, text)
    competencias = extract_competencias(text) if is_fgts else []

    return PdfAnalysis(
        company=company,
        pdf_path=pdf_path,
        competencias=competencias,
        is_fgts=is_fgts,
    )


def iter_company_folders(root_path: Path) -> list[Path]:
    return sorted(path for path in root_path.iterdir() if path.is_dir())


def iter_pdf_files(company_path: Path) -> list[Path]:
    return sorted(path for path in company_path.rglob("*.pdf") if path.is_file())


def print_report(
    divergencias: list[PdfAnalysis],
    sem_competencia: list[PdfAnalysis],
    total_empresas: int,
    total_fgts: int,
    expected_competencia: str,
) -> int:
    print(f"Empresas analisadas: {total_empresas}")
    print(f"Guias FGTS encontradas: {total_fgts}")
    print(f"Competencia esperada: {expected_competencia}")
    print()

    if not divergencias and not sem_competencia:
        print("Nenhuma divergencia encontrada.")
        return 0

    if divergencias:
        print("PDFs com competencia diferente da esperada:")
        for item in divergencias:
            competencias = ", ".join(item.competencias) if item.competencias else "nao identificada"
            print(f"- Empresa: {item.company}")
            print(f"  Arquivo: {item.pdf_path}")
            print(f"  Competencias encontradas: {competencias}")
        print()

    if sem_competencia:
        print("PDFs FGTS sem competencia identificada ou com erro de leitura:")
        for item in sem_competencia:
            detalhe = item.error or "competencia nao localizada no texto"
            print(f"- Empresa: {item.company}")
            print(f"  Arquivo: {item.pdf_path}")
            print(f"  Detalhe: {detalhe}")

    return 1


def run(root_path: Path, expected_competencia: str) -> int:
    if not root_path.exists():
        print(f"Pasta raiz nao encontrada: {root_path}", file=sys.stderr)
        return 2

    company_folders = iter_company_folders(root_path)
    divergencias: list[PdfAnalysis] = []
    sem_competencia: list[PdfAnalysis] = []
    total_fgts = 0

    for company_path in company_folders:
        pdf_files = iter_pdf_files(company_path)
        for pdf_path in pdf_files:
            analysis = analyze_pdf(company_path.name, pdf_path)

            if analysis.error:
                if "fgts" in pdf_path.name.lower():
                    sem_competencia.append(analysis)
                continue

            if not analysis.is_fgts:
                continue

            total_fgts += 1

            if not analysis.competencias:
                sem_competencia.append(analysis)
                continue

            if set(analysis.competencias) != {expected_competencia}:
                divergencias.append(analysis)

    return print_report(
        divergencias=divergencias,
        sem_competencia=sem_competencia,
        total_empresas=len(company_folders),
        total_fgts=total_fgts,
        expected_competencia=expected_competencia,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Varre as pastas das empresas e identifica guias FGTS com competencia divergente."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help=f"Pasta raiz com as empresas. Padrao: {DEFAULT_ROOT}",
    )
    parser.add_argument(
        "--competencia",
        default=DEFAULT_COMPETENCIA,
        help=f"Competencia esperada. Padrao: {DEFAULT_COMPETENCIA}",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    raise SystemExit(run(root_path=args.root, expected_competencia=args.competencia))
