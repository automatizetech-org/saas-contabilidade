from __future__ import annotations

import difflib
import filecmp
import json
import shutil
import subprocess
import unicodedata
from dataclasses import dataclass
from pathlib import Path
import re


BASE_DIR = Path("C:\\Users\\Victor\\Downloads\\TESTE IMPORTA\u00c7AO NFE-NFC")
SPREADSHEET_PATH = Path(__file__).with_name("RELAÇÃO DE EMPRESAS.xls")
DOMAIN_COMPANIES_PATH = Path(__file__).with_name("empresas dominio")
DESTINATION_ROOTS = {
    "NFC": BASE_DIR / "NFC",
    "NFE": BASE_DIR / "NFE",
    "NFS": BASE_DIR / "NFS",
}
SOURCE_FOLDER_MAP = {
    "NFC-e": "NFC",
    "NF-e": "NFE",
    "NFS-e": "NFS",
    "Entrada": "NFE",
    "Eventos": "NFE",
}
PERIOD_PATTERN = re.compile(r"^\d{6}$")
XML_SUFFIX = ".xml"
MIN_MATCH_SCORE = 0.70
IGNORED_COMPANY_WORDS = {
    "LTDA",
    "EIRELI",
    "ME",
    "MEI",
    "EPP",
    "EI",
    "S A",
    "SA",
    "SOCIEDADE",
    "INDIVIDUAL",
    "EMPRESARIO",
}
TOKEN_ALIASES = {
    "COM": "COMERCIO",
    "COML": "COMERCIO",
    "IND": "INDUSTRIA",
    "INDUS": "INDUSTRIA",
    "INDUT": "INDUSTRIA",
    "SERV": "SERVICOS",
    "SERVICO": "SERVICOS",
    "SERVICOS": "SERVICOS",
    "REP": "REPRESENTACOES",
    "REPRES": "REPRESENTACOES",
    "IMP": "IMPORTACAO",
    "IMPORT": "IMPORTACAO",
    "EXP": "EXPORTACAO",
    "EXPORT": "EXPORTACAO",
    "MATZ": "MATRIZ",
    "MTZ": "MATRIZ",
    "FILAL": "FILIAL",
}


@dataclass
class CopyStats:
    copied: int = 0
    skipped_equal: int = 0
    renamed: int = 0


@dataclass
class CompanyAlias:
    code: str
    alias: str
    normalized_alias: str


@dataclass
class MatchCandidate:
    original: str
    normalized: str


def log(message: str) -> None:
    print(message, flush=True)


def ensure_destination_roots() -> None:
    for destination in DESTINATION_ROOTS.values():
        destination.mkdir(parents=True, exist_ok=True)


def choose_period_separation() -> bool:
    log("Separar copia por competencia?")
    log("1 - Sim, manter pasta MMYYYY dentro da empresa")
    log("2 - Nao, juntar tudo direto na pasta da empresa")
    while True:
        choice = input("Escolha 1 ou 2: ").strip()
        if choice == "1":
            return True
        if choice == "2":
            return False
        log("Opcao invalida. Digite 1 ou 2.")


def choose_txt_filter() -> bool:
    log("Filtrar empresas pelo arquivo 'empresas dominio'?")
    log("1 - Sim, copiar apenas empresas que tiverem similaridade com o TXT")
    log("2 - Nao, continuar com o comportamento atual")
    while True:
        choice = input("Escolha 1 ou 2: ").strip()
        if choice == "1":
            return True
        if choice == "2":
            return False
        log("Opcao invalida. Digite 1 ou 2.")


def sanitize_folder_name(value: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', "_", value).strip()


def normalize_company_name(value: str) -> str:
    ascii_value = (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode("ascii")
        .upper()
    )
    cleaned = re.sub(r"[^A-Z0-9]+", " ", ascii_value)
    tokens = []
    for token in cleaned.split():
        if not token:
            continue
        token = TOKEN_ALIASES.get(token, token)
        if token in IGNORED_COMPANY_WORDS:
            continue
        tokens.append(token)
    return " ".join(tokens)


def tokenize(value: str) -> set[str]:
    return {token for token in value.split() if token}


def load_domain_companies(txt_path: Path) -> list[MatchCandidate]:
    if not txt_path.exists():
        return []

    companies: list[MatchCandidate] = []
    for line in txt_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        company = line.strip()
        if not company:
            continue
        companies.append(
            MatchCandidate(
                original=company,
                normalized=normalize_company_name(company),
            )
        )
    return companies


def load_company_aliases(spreadsheet_path: Path) -> list[CompanyAlias]:
    powershell_script = f"""
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$excel.DisplayAlerts = $false
$wb = $excel.Workbooks.Open('{str(spreadsheet_path)}')
$ws = $wb.Worksheets.Item(1)
$range = $ws.UsedRange
$rows = $range.Rows.Count
$items = @()
for ($r = 8; $r -le $rows; $r++) {{
    $code = [string]$ws.Cells.Item($r, 1).Text
    $alias = [string]$ws.Cells.Item($r, 2).Text
    if (-not [string]::IsNullOrWhiteSpace($code) -and -not [string]::IsNullOrWhiteSpace($alias)) {{
        $items += [PSCustomObject]@{{ code = $code.Trim(); alias = $alias.Trim() }}
    }}
}}
$wb.Close($false)
$excel.Quit()
[System.Runtime.Interopservices.Marshal]::ReleaseComObject($ws) | Out-Null
[System.Runtime.Interopservices.Marshal]::ReleaseComObject($wb) | Out-Null
[System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null
[GC]::Collect()
[GC]::WaitForPendingFinalizers()
$items | ConvertTo-Json -Compress
"""
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-Command", powershell_script],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    output = completed.stdout.strip()
    if not output:
        return []

    raw_items = json.loads(output)
    if isinstance(raw_items, dict):
        raw_items = [raw_items]

    aliases: list[CompanyAlias] = []
    for item in raw_items:
        alias = str(item["alias"]).strip()
        code = str(item["code"]).strip()
        aliases.append(
            CompanyAlias(
                code=code,
                alias=alias,
                normalized_alias=normalize_company_name(alias),
            )
        )
    return aliases


def company_match_score(company_name: str, alias: CompanyAlias) -> float:
    normalized_company = normalize_company_name(company_name)
    normalized_alias = alias.normalized_alias
    if not normalized_company or not normalized_alias:
        return 0.0

    if normalized_company == normalized_alias:
        return 1.0
    if normalized_alias in normalized_company or normalized_company in normalized_alias:
        return 0.96

    company_tokens = tokenize(normalized_company)
    alias_tokens = tokenize(normalized_alias)
    if not company_tokens or not alias_tokens:
        return 0.0

    overlap = len(company_tokens & alias_tokens) / max(len(alias_tokens), 1)
    sequence = difflib.SequenceMatcher(None, normalized_company, normalized_alias).ratio()
    return max(sequence, overlap)


def candidate_match_score(company_name: str, candidate: MatchCandidate) -> float:
    normalized_company = normalize_company_name(company_name)
    normalized_candidate = candidate.normalized
    if not normalized_company or not normalized_candidate:
        return 0.0

    if normalized_company == normalized_candidate:
        return 1.0
    if normalized_candidate in normalized_company or normalized_company in normalized_candidate:
        return 0.96

    company_tokens = tokenize(normalized_company)
    candidate_tokens = tokenize(normalized_candidate)
    if not company_tokens or not candidate_tokens:
        return 0.0

    overlap = len(company_tokens & candidate_tokens) / max(len(candidate_tokens), 1)
    sequence = difflib.SequenceMatcher(None, normalized_company, normalized_candidate).ratio()
    return max(sequence, overlap)


def filter_companies_by_txt(
    company_dirs: list[Path],
    domain_companies: list[MatchCandidate],
) -> tuple[list[Path], list[str]]:
    if not domain_companies:
        return company_dirs, []

    matched_txt_names: set[str] = set()
    filtered_dirs: list[Path] = []

    for company_dir in company_dirs:
        best_match: MatchCandidate | None = None
        best_score = 0.0
        for candidate in domain_companies:
            score = candidate_match_score(company_dir.name, candidate)
            if score > best_score:
                best_score = score
                best_match = candidate
        if best_match and best_score >= MIN_MATCH_SCORE:
            filtered_dirs.append(company_dir)
            matched_txt_names.add(best_match.original)

    unmatched_txt = sorted(
        candidate.original
        for candidate in domain_companies
        if candidate.original not in matched_txt_names
    )
    return filtered_dirs, unmatched_txt


def resolve_company_copy_name(company_name: str, aliases: list[CompanyAlias]) -> str:
    best_alias: CompanyAlias | None = None
    best_score = 0.0

    for alias in aliases:
        score = company_match_score(company_name, alias)
        if score > best_score:
            best_score = score
            best_alias = alias

    if best_alias and best_score >= MIN_MATCH_SCORE:
        resolved = sanitize_folder_name(f"{best_alias.code}-{best_alias.alias}")
        return resolved

    return sanitize_folder_name(company_name)


def company_directories(base_dir: Path) -> list[Path]:
    excluded = {path.name for path in DESTINATION_ROOTS.values()}
    excluded.add("_tmp_sittax")
    return sorted(
        path
        for path in base_dir.iterdir()
        if path.is_dir() and path.name not in excluded
    )


def period_directories(company_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in company_dir.iterdir()
        if path.is_dir() and PERIOD_PATTERN.match(path.name)
    )


def destination_for(source_dir: Path) -> str | None:
    mapped = SOURCE_FOLDER_MAP.get(source_dir.name)
    if mapped:
        return mapped
    return None


def unique_destination_path(destination_dir: Path, filename: str) -> Path:
    candidate = destination_dir / filename
    if not candidate.exists():
        return candidate

    stem = Path(filename).stem
    suffix = Path(filename).suffix
    counter = 1
    while True:
        candidate = destination_dir / f"{stem}_dup{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def copy_xml_file(source_file: Path, destination_dir: Path, stats: CopyStats) -> None:
    destination_dir.mkdir(parents=True, exist_ok=True)
    target = destination_dir / source_file.name

    if target.exists():
        if filecmp.cmp(source_file, target, shallow=False):
            stats.skipped_equal += 1
            return
        target = unique_destination_path(destination_dir, source_file.name)
        stats.renamed += 1

    shutil.copy2(source_file, target)
    stats.copied += 1


def collect_source_directories(period_dir: Path) -> list[tuple[Path, str]]:
    collected: list[tuple[Path, str]] = []
    for child in sorted(period_dir.iterdir()):
        if not child.is_dir():
            continue

        mapped = destination_for(child)
        if mapped:
            collected.append((child, mapped))
            continue

        if child.name == "Notas canceladas":
            for nested in sorted(child.rglob("*")):
                if nested.is_dir():
                    nested_mapped = destination_for(nested)
                    if nested_mapped:
                        collected.append((nested, nested_mapped))
    return collected


def organize_period(
    company_dir: Path,
    period_dir: Path,
    destination_company_name: str,
    separate_by_period: bool,
    stats_by_type: dict[str, CopyStats],
) -> None:
    company_name = company_dir.name
    period_name = period_dir.name
    source_directories = collect_source_directories(period_dir)

    if not source_directories:
        return

    log(f"Processando {company_name} | {period_name}")
    for source_dir, destination_key in source_directories:
        destination_dir = DESTINATION_ROOTS[destination_key] / destination_company_name
        if separate_by_period:
            destination_dir = destination_dir / period_name
        xml_files = sorted(path for path in source_dir.rglob(f"*{XML_SUFFIX}") if path.is_file())
        if not xml_files:
            continue

        log(f"  Copiando {len(xml_files)} XML(s) de {source_dir.name} para {destination_key}")
        for xml_file in xml_files:
            copy_xml_file(xml_file, destination_dir, stats_by_type[destination_key])


def summarize(stats_by_type: dict[str, CopyStats]) -> None:
    log("")
    log("Resumo")
    for key in ("NFC", "NFE", "NFS"):
        stats = stats_by_type[key]
        log(
            f"{key}: copiados={stats.copied} | iguais_ignorados={stats.skipped_equal} | "
            f"renomeados={stats.renamed}"
        )


def summarize_empty_companies(empty_companies: list[str]) -> None:
    log("")
    log("Empresas sem copia de XML")
    if not empty_companies:
        log("Nenhuma")
        return

    for company in empty_companies:
        log(company)


def summarize_unmatched_companies(unmatched_companies: list[str]) -> None:
    log("")
    log("Empresas sem similaridade na planilha")
    if not unmatched_companies:
        log("Nenhuma")
        return

    for company in unmatched_companies:
        log(company)


def summarize_unmatched_txt_companies(unmatched_txt_companies: list[str]) -> None:
    log("")
    log("Empresas do TXT sem correspondencia nas pastas originais")
    if not unmatched_txt_companies:
        log("Nenhuma")
        return

    for company in unmatched_txt_companies:
        log(company)


def run() -> None:
    ensure_destination_roots()
    separate_by_period = choose_period_separation()
    log(
        "Modo de copia: "
        + ("separado por competencia" if separate_by_period else "conteudo consolidado por empresa")
    )
    use_txt_filter = choose_txt_filter()
    aliases = load_company_aliases(SPREADSHEET_PATH)
    log(f"Registros carregados da planilha: {len(aliases)}")
    stats_by_type = {key: CopyStats() for key in DESTINATION_ROOTS}
    empty_companies: list[str] = []
    unmatched_companies: list[str] = []
    unmatched_txt_companies: list[str] = []
    selected_company_dirs = company_directories(BASE_DIR)

    if use_txt_filter:
        domain_companies = load_domain_companies(DOMAIN_COMPANIES_PATH)
        log(f"Registros carregados do TXT: {len(domain_companies)}")
        selected_company_dirs, unmatched_txt_companies = filter_companies_by_txt(selected_company_dirs, domain_companies)
        log(f"Empresas originais selecionadas pelo TXT: {len(selected_company_dirs)}")
    else:
        log("Filtro por TXT desativado")

    for company_dir in selected_company_dirs:
        destination_company_name = resolve_company_copy_name(company_dir.name, aliases)
        if destination_company_name == sanitize_folder_name(company_dir.name):
            unmatched_companies.append(company_dir.name)
        copied_before_company = sum(stats.copied for stats in stats_by_type.values())
        for period_dir in period_directories(company_dir):
            organize_period(company_dir, period_dir, destination_company_name, separate_by_period, stats_by_type)
        copied_after_company = sum(stats.copied for stats in stats_by_type.values())
        if copied_after_company == copied_before_company:
            empty_companies.append(company_dir.name)

    summarize(stats_by_type)
    summarize_empty_companies(empty_companies)
    summarize_unmatched_companies(sorted(set(unmatched_companies)))
    summarize_unmatched_txt_companies(unmatched_txt_companies)


if __name__ == "__main__":
    run()
