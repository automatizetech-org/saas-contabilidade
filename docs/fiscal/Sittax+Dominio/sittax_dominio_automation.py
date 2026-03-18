from __future__ import annotations

import csv
import re
import shutil
import sys
import traceback
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from playwright.sync_api import Error, Locator, Page, TimeoutError, expect, sync_playwright


LOGIN_URL = "https://app.sittax.com.br/login"
GERAR_ARQUIVO_URL = "https://app.sittax.com.br/integracao/gerar-arquivo"
EMAIL = "elianderson@fleury.cnt.br"
PASSWORD = "sittax123"
BASE_OUTPUT_DIR = Path("C:\\Users\\Victor\\Downloads\\TESTE IMPORTA\u00c7AO NFE-NFC")
TEMP_DOWNLOAD_DIR = BASE_OUTPUT_DIR / "_tmp_sittax"
REPORT_PATH = BASE_OUTPUT_DIR / "sittax_dominio_resultados.csv"
HEADLESS = False
PAGE_SIZE = 500
DEFAULT_TIMEOUT_MS = 30_000
TASK_REFRESH_ATTEMPTS = 12
TASK_REFRESH_INTERVAL_SECONDS = 2
TARGET_YEAR = 2025
MAX_RETRIES_PER_ENTRY = 10

GENERATE_HEADING_PATTERN = re.compile(r"Gerar Arquivos de Integra", re.IGNORECASE)
DOMAIN_XML_PATTERN = re.compile(r"Arquivo XML .*Dom", re.IGNORECASE)
BATCH_OPERATION_PATTERN = re.compile(r"Realizar opera", re.IGNORECASE)
TASKS_PATTERN = re.compile(r"Tarefas", re.IGNORECASE)
TASK_TITLE_PATTERN = re.compile(r"INTEGRACAO-(Dom.nio|XML)", re.IGNORECASE)
TASK_DATETIME_PATTERN = re.compile(r"\d{2}/\d{2}/\d{4} - \d{2}:\d{2}:\d{2}")
TASK_FINAL_MESSAGE_PATTERNS = (
    re.compile(r"n.o foram encontrados documentos", re.IGNORECASE),
    re.compile(r"arquivo gerado com sucesso", re.IGNORECASE),
    re.compile(r"falha|erro|n.o foi poss.vel", re.IGNORECASE),
)
NO_DOCUMENTS_PATTERN = re.compile(r"n.o foram encontrados documentos", re.IGNORECASE)
PERIOD_DISPLAY_PATTERN = re.compile(r"^[a-z]{3}/\d{4}$", re.IGNORECASE)
MONTH_LABELS = {
    1: "Janeiro",
    2: "Fevereiro",
    3: "Março",
    4: "Abril",
    5: "Maio",
    6: "Junho",
    7: "Julho",
    8: "Agosto",
    9: "Setembro",
    10: "Outubro",
    11: "Novembro",
    12: "Dezembro",
}
MONTH_SHORT_LABELS = {
    1: "jan",
    2: "fev",
    3: "mar",
    4: "abr",
    5: "mai",
    6: "jun",
    7: "jul",
    8: "ago",
    9: "set",
    10: "out",
    11: "nov",
    12: "dez",
}


@dataclass
class CompanyRow:
    cnpj: str
    company: str
    period: str


@dataclass
class TaskResult:
    title: str
    company: str
    message: str
    has_download: bool
    download_path: str | None
    started_at: datetime | None
    finished_at: datetime | None
    finished: bool


REPORT_FIELDNAMES = [
    "cnpj",
    "company",
    "period",
    "status",
    "task_title",
    "message",
    "downloaded",
    "download_path",
    "attempts",
]

STATUS_DOWNLOADED = "BAIXADO"
STATUS_NO_DOCUMENTS = "SEM_DOCUMENTO"
STATUS_ERROR = "ERRO"


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def sanitize_name(value: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*]', "_", value).strip()
    return cleaned or "empresa_sem_nome"


def ensure_directories() -> None:
    BASE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    log(f"Pasta base: {BASE_OUTPUT_DIR}")
    log(f"Pasta temporaria: {TEMP_DOWNLOAD_DIR}")


def wait_for_page_idle(page: Page) -> None:
    page.wait_for_load_state("domcontentloaded")
    try:
        page.wait_for_load_state("networkidle", timeout=5_000)
    except TimeoutError:
        pass


def ensure_expected_route(page: Page) -> None:
    allowed_fragments = (
        "/integracao/gerar-arquivo",
        "/painel-contador",
    )
    if any(fragment in page.url for fragment in allowed_fragments):
        return
    log(f"URL inesperada detectada: {page.url}. Retornando para gerar arquivo")
    open_generate_page(page)


def wait_for_company_table(page: Page, minimum_rows: int = 1) -> None:
    wait_for_page_idle(page)
    page.wait_for_function(
        """(minimum) => {
            const tables = Array.from(document.querySelectorAll('table'));
            const target = tables.find((table) =>
                Array.from(table.querySelectorAll('th')).some((th) => th.textContent && th.textContent.includes('CNPJ'))
            );
            if (!target) return false;
            return target.querySelectorAll('tbody tr').length >= minimum;
        }""",
        arg=minimum_rows,
        timeout=DEFAULT_TIMEOUT_MS,
    )
    table_rows(page).first.wait_for(timeout=DEFAULT_TIMEOUT_MS)


def total_records(page: Page) -> int:
    text = page.locator("body").inner_text()
    match = re.search(r"Total de registros:\s*(\d+)", text)
    return int(match.group(1)) if match else 0


def wait_for_visible_row_count(page: Page, expected_rows: int) -> None:
    page.wait_for_function(
        """(expected) => {
            const tables = Array.from(document.querySelectorAll('table'));
            const target = tables.find((table) =>
                Array.from(table.querySelectorAll('th')).some((th) => th.textContent && th.textContent.includes('CNPJ'))
            );
            if (!target) return false;
            return target.querySelectorAll('tbody tr').length === expected;
        }""",
        arg=expected_rows,
        timeout=DEFAULT_TIMEOUT_MS,
    )


def login(page: Page) -> None:
    log("Abrindo tela de login")
    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    page.get_by_role("textbox", name="Digite o e-mail").fill(EMAIL)
    page.get_by_role("textbox", name="Senha").fill(PASSWORD)
    log("Enviando login")
    page.get_by_role("button", name="Acessar").click()
    page.wait_for_url("**/painel-contador", timeout=DEFAULT_TIMEOUT_MS)
    page.get_by_role("button", name=TASKS_PATTERN).wait_for(timeout=DEFAULT_TIMEOUT_MS)
    close_welcome_modal_if_present(page)
    log("Login concluido")


def close_welcome_modal_if_present(page: Page) -> None:
    try:
        page.get_by_role("button", name="Close Tour").click(timeout=3_000)
        log("Modal de boas-vindas fechado")
    except TimeoutError:
        return


def open_generate_page(page: Page) -> None:
    log("Abrindo pagina de gerar arquivo")
    page.goto(GERAR_ARQUIVO_URL, wait_until="domcontentloaded")
    page.get_by_role("heading", name=GENERATE_HEADING_PATTERN).wait_for(timeout=DEFAULT_TIMEOUT_MS)
    page.locator('[data-test-id="tabelaPadraoPaginadaBotaoQtdItens"]').wait_for(timeout=DEFAULT_TIMEOUT_MS)
    wait_for_company_table(page)
    log("Pagina de gerar arquivo pronta")


def period_button(page: Page) -> Locator:
    return page.locator(".st-periodo-fiscal").first


def format_period(month: int, year: int) -> str:
    return f"{month:02d}/{year}"


def format_period_folder(month: int, year: int) -> str:
    return f"{month:02d}{year}"


def format_period_chip(month: int, year: int) -> str:
    return f"{MONTH_SHORT_LABELS[month]}/{year}"


def set_fiscal_period(page: Page, month: int, year: int) -> str:
    expected_period = format_period(month, year)
    expected_chip = format_period_chip(month, year)
    current_chip = period_button(page).inner_text().strip().lower()
    if current_chip == expected_chip:
        log(f"Competencia ja selecionada: {expected_period}")
        return expected_period

    log(f"Alterando competencia para {expected_period}")
    period_button(page).click()
    page.locator("button").filter(has_text=re.compile(r"^\d{4}$")).first.click()
    page.get_by_text(str(year), exact=True).click()
    page.get_by_text(MONTH_LABELS[month], exact=True).click()
    expect(period_button(page)).to_have_text(re.compile(re.escape(expected_chip), re.IGNORECASE))
    page.wait_for_function(
        """(expectedPeriod) => {
            const tables = Array.from(document.querySelectorAll('table'));
            const target = tables.find((table) =>
                Array.from(table.querySelectorAll('th')).some((th) => th.textContent && th.textContent.includes('CNPJ'))
            );
            if (!target) return false;
            const rows = target.querySelectorAll('tbody tr');
            if (!rows.length) return false;
            const firstPeriodCell = rows[0].querySelectorAll('td')[3];
            return !!firstPeriodCell && firstPeriodCell.textContent.trim() === expectedPeriod;
        }""",
        arg=expected_period,
        timeout=DEFAULT_TIMEOUT_MS,
    )
    wait_for_company_table(page)
    log(f"Competencia aplicada: {expected_period}")
    return expected_period


def set_page_size(page: Page, page_size: int) -> None:
    log(f"Definindo quantidade da grade para {page_size}")
    field = page.locator('[data-test-id="tabelaPadraoPaginadaBotaoQtdItens"]')
    field.click()
    field.press("Control+A")
    field.press("Delete")
    field.type(str(page_size), delay=80)
    field.press("Enter")
    expect(field).to_have_value(str(page_size))
    wait_for_company_table(page)
    expected_rows = total_records(page)
    if expected_rows and expected_rows <= page_size:
        wait_for_visible_row_count(page, expected_rows)
    log("Quantidade da grade aplicada")


def select_domain_xml(page: Page) -> None:
    log("Selecionando Arquivo XML (Dominio)")
    page.get_by_label(DOMAIN_XML_PATTERN).check()
    wait_for_company_table(page)
    log("Tipo de arquivo selecionado")


def table_rows(page: Page) -> Locator:
    company_table = page.locator("table").filter(has=page.get_by_role("columnheader", name="CNPJ")).first
    return company_table.locator("tbody tr")


def search_input(page: Page) -> Locator:
    return page.get_by_role("textbox", name="Pesquisar em filtrados")


def clear_search(page: Page) -> None:
    field = search_input(page)
    field.fill("")
    page.keyboard.press("Enter")
    wait_for_company_table(page)


def collect_rows(page: Page) -> list[CompanyRow]:
    rows = table_rows(page)
    total = rows.count()
    log(f"Total de empresas visiveis na grade: {total}")
    items: list[CompanyRow] = []
    for index in range(total):
        row = rows.nth(index)
        cells = row.locator("td")
        items.append(
            CompanyRow(
                cnpj=cells.nth(1).inner_text().strip(),
                company=cells.nth(2).inner_text().strip(),
                period=cells.nth(3).inner_text().strip(),
            )
        )
    return items


def clear_selected_rows(page: Page) -> None:
    checked_rows = table_rows(page).filter(has=page.locator('input[type="checkbox"]:checked'))
    total = checked_rows.count()
    if total:
        log(f"Limpando {total} selecoes anteriores")
    for index in range(total):
        checked_rows.nth(index).locator("label").first.click()


def company_row_locator(page: Page, company_row: CompanyRow) -> Locator:
    return table_rows(page).filter(has_text=company_row.cnpj).filter(has_text=company_row.company).first


def select_company_row(page: Page, company_row: CompanyRow) -> CompanyRow:
    row = company_row_locator(page, company_row)
    row.wait_for(timeout=DEFAULT_TIMEOUT_MS)
    cells = row.locator("td")
    details = CompanyRow(
        cnpj=cells.nth(1).inner_text().strip(),
        company=cells.nth(2).inner_text().strip(),
        period=cells.nth(3).inner_text().strip(),
    )
    if details.company != company_row.company or details.cnpj != company_row.cnpj:
        raise TimeoutError(
            f"Linha localizada inesperada: {(details.cnpj, details.company)!r} != {(company_row.cnpj, company_row.company)!r}"
        )
    log(f"Selecionando empresa: {details.company}")
    checkbox = row.locator('input[type="checkbox"]').first
    row.locator("label").first.click()
    expect(checkbox).to_be_checked()
    return details


def configure_generate_grid(page: Page, month: int, year: int) -> str:
    period = set_fiscal_period(page, month, year)
    select_domain_xml(page)
    set_page_size(page, PAGE_SIZE)
    visible = table_rows(page).count()
    total = total_records(page)
    log(f"Grade configurada para {period}: visiveis={visible}, total_registros={total}")
    return period


def submit_generate_operation(page: Page) -> None:
    log("Selecionando operacao Gerar Arquivo")
    page.locator("select").last.select_option(label="Gerar Arquivo")
    log("Enviando operacao em lote")
    page.get_by_role("button", name=BATCH_OPERATION_PATTERN).click()
    page.get_by_role("button", name="Ok").wait_for(timeout=DEFAULT_TIMEOUT_MS)
    page.get_by_role("button", name="Ok").click()
    log("Modal de operacao concluida confirmado")


def open_tasks_panel(page: Page) -> None:
    log("Abrindo painel de tarefas")
    button = page.get_by_role("button", name=TASKS_PATTERN)
    button.click()
    page.locator(".tarefa").first.wait_for(timeout=DEFAULT_TIMEOUT_MS)
    log("Painel de tarefas aberto")


def close_tasks_panel(page: Page) -> None:
    if page.locator(".tarefa").count() == 0:
        return
    page.get_by_role("button", name=TASKS_PATTERN).click()
    log("Painel de tarefas fechado")


def refresh_tasks(page: Page) -> None:
    log("Atualizando tarefas")
    page.locator('[data-test-id="optionsHeaderButtonRefresh"]').click()
    page.wait_for_function("() => document.querySelectorAll('.tarefa').length > 0", timeout=DEFAULT_TIMEOUT_MS)
    page.wait_for_timeout(500)


def task_cards(page: Page, company: str) -> Locator:
    return page.locator(".tarefa").filter(has_text=company).filter(has_text=TASK_TITLE_PATTERN)


def parse_task_datetime(value: str) -> datetime | None:
    try:
        return datetime.strptime(value.strip(), "%d/%m/%Y - %H:%M:%S")
    except ValueError:
        return None


def parse_task_card(card: Locator, company: str) -> TaskResult:
    title = card.locator("h4").first.inner_text().strip()
    message_parts = [text.strip() for text in card.locator("p").all_inner_texts() if text.strip()]
    message = " ".join(message_parts) if message_parts else card.inner_text().strip()
    card_text = card.inner_text()
    timestamps = [parse_task_datetime(value) for value in TASK_DATETIME_PATTERN.findall(card_text)]
    timestamps = [value for value in timestamps if value is not None]
    started_at = timestamps[0] if timestamps else None
    finished_at = timestamps[1] if len(timestamps) > 1 else None

    download_button = card.locator("button").last
    has_download = False
    if download_button.count() > 0:
        try:
            has_download = download_button.is_visible() and not download_button.is_disabled()
        except Error:
            has_download = download_button.is_visible()

    finished = has_download or finished_at is not None
    if not finished:
        finished = any(pattern.search(message) for pattern in TASK_FINAL_MESSAGE_PATTERNS)

    return TaskResult(
        title=title,
        company=company,
        message=message,
        has_download=has_download,
        download_path=None,
        started_at=started_at,
        finished_at=finished_at,
        finished=finished,
    )


def find_task(page: Page, company: str, requested_at: datetime) -> TaskResult | None:
    cards = task_cards(page, company)
    total = cards.count()
    if total == 0:
        return None

    requested_floor = requested_at - timedelta(seconds=10)
    candidates: list[TaskResult] = []
    for index in range(total):
        result = parse_task_card(cards.nth(index), company)
        if result.started_at is None or result.started_at >= requested_floor:
            candidates.append(result)

    if not candidates:
        return None

    candidates.sort(key=lambda item: item.started_at or datetime.min, reverse=True)
    return candidates[0]


def wait_for_task(page: Page, company: str, requested_at: datetime) -> TaskResult:
    for attempt in range(1, TASK_REFRESH_ATTEMPTS + 1):
        log(f"Buscando tarefa da empresa '{company}' - tentativa {attempt}/{TASK_REFRESH_ATTEMPTS}")
        refresh_tasks(page)
        result = find_task(page, company, requested_at)
        if result:
            log(
                "Tarefa localizada para "
                f"{company}: {result.title} | inicio={result.started_at or 'n/d'} | "
                f"fim={result.finished_at or 'pendente'} | download={'yes' if result.has_download else 'no'}"
            )
            if result.finished:
                return result
            log(f"Tarefa de {company} ainda em processamento; aguardando novo refresh")
        page.wait_for_timeout(TASK_REFRESH_INTERVAL_SECONDS * 1000)

    raise TimeoutError(f"Nao encontrei tarefa finalizada para {company!r} apos varias atualizacoes.")


def locate_task_action(page: Page, task: TaskResult) -> Locator | None:
    cards = task_cards(page, task.company)
    total = cards.count()
    for index in range(total):
        card = cards.nth(index)
        parsed = parse_task_card(card, task.company)
        if parsed.started_at != task.started_at:
            continue
        action = card.locator("button").last
        if action.count() == 0:
            continue
        if not action.is_visible():
            continue
        try:
            disabled = action.is_disabled()
        except Error:
            disabled = False
        if not disabled:
            return action
    return None


def save_download_from_task(page: Page, task: TaskResult, period: str) -> str | None:
    button = locate_task_action(page, task)
    if button is None:
        return None

    period_folder = period.replace("/", "")
    company_dir = BASE_OUTPUT_DIR / sanitize_name(task.company) / period_folder
    company_dir.mkdir(parents=True, exist_ok=True)
    log(f"Iniciando download para {task.company} em {period}")

    with page.expect_download(timeout=20_000) as download_info:
        button.click()

    download = download_info.value
    target_file = TEMP_DOWNLOAD_DIR / download.suggested_filename
    download.save_as(str(target_file))

    if zipfile.is_zipfile(target_file):
        extract_zip(target_file, company_dir)
    else:
        shutil.copy2(target_file, company_dir / target_file.name)

    log(f"Download salvo em: {target_file}")
    return str(target_file)


def extract_zip(zip_path: Path, destination: Path) -> None:
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(destination)


def infer_status_from_row(row: dict[str, str]) -> str:
    downloaded = (row.get("downloaded") or "").strip().lower()
    message = row.get("message") or ""
    task_title = row.get("task_title") or ""

    if downloaded == "yes":
        return STATUS_DOWNLOADED
    if NO_DOCUMENTS_PATTERN.search(message):
        return STATUS_NO_DOCUMENTS
    if task_title == "ERRO_AUTOMACAO":
        return STATUS_ERROR
    if downloaded == "no":
        return STATUS_ERROR
    return STATUS_ERROR


def load_existing_rows() -> list[dict[str, str]]:
    if not REPORT_PATH.exists():
        return []
    with REPORT_PATH.open("r", newline="", encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))
    migrated = False
    for row in rows:
        if not row.get("status"):
            row["status"] = infer_status_from_row(row)
            migrated = True
        if not row.get("attempts"):
            row["attempts"] = "1"
            migrated = True
        row.setdefault("downloaded", "no")
        row.setdefault("download_path", "")
        row.setdefault("message", "")
        row.setdefault("task_title", "")
        row.setdefault("cnpj", "")
        row.setdefault("company", "")
        row.setdefault("period", "")
    if migrated:
        log("CSV antigo detectado; migrando resultados para o modelo novo")
    return rows


def row_key(company: str, period: str) -> tuple[str, str]:
    return (company, period)


def build_results_index(rows: Iterable[dict[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    index: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows:
        company = row.get("company", "")
        period = row.get("period", "")
        if company and period:
            index[row_key(company, period)] = row
    return index


def is_completed_row(row: dict[str, str]) -> bool:
    return row.get("status") in {STATUS_DOWNLOADED, STATUS_NO_DOCUMENTS}


def upsert_result(results: list[dict[str, str]], result_row: dict[str, str]) -> None:
    key = row_key(result_row["company"], result_row["period"])
    for index, existing in enumerate(results):
        if row_key(existing.get("company", ""), existing.get("period", "")) == key:
            results[index] = result_row
            return
    results.append(result_row)


def write_report(rows: Iterable[dict[str, str]]) -> None:
    with REPORT_PATH.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    log(f"Relatorio atualizado: {REPORT_PATH}")


def log_report_summary(rows: Iterable[dict[str, str]]) -> None:
    counts = {
        STATUS_DOWNLOADED: 0,
        STATUS_NO_DOCUMENTS: 0,
        STATUS_ERROR: 0,
    }
    for row in rows:
        status = row.get("status", "")
        if status in counts:
            counts[status] += 1

    log("Resumo final do relatorio:")
    log(f"  {STATUS_DOWNLOADED}: {counts[STATUS_DOWNLOADED]}")
    log(f"  {STATUS_NO_DOCUMENTS}: {counts[STATUS_NO_DOCUMENTS]}")
    log(f"  {STATUS_ERROR}: {counts[STATUS_ERROR]}")


def log_existing_progress(rows: list[dict[str, str]]) -> None:
    if not rows:
        log("Nenhum registro anterior encontrado no CSV")
        return

    last_row = rows[-1]
    error_rows = [row for row in rows if row.get("status") == STATUS_ERROR]
    log(
        f"Ultimo registro no CSV: {last_row.get('company', '')} | "
        f"{last_row.get('period', '')} | status={last_row.get('status', '')}"
    )
    log(f"Registros com erro pendente no CSV: {len(error_rows)}")


def run() -> None:
    ensure_directories()
    results = load_existing_rows()
    if results:
        write_report(results)
    results_index = build_results_index(results)
    completed_count = sum(1 for row in results if is_completed_row(row))
    log(f"Registros concluidos no relatorio: {completed_count}")
    log_existing_progress(results)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=HEADLESS)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.set_default_timeout(DEFAULT_TIMEOUT_MS)
        page.set_default_navigation_timeout(DEFAULT_TIMEOUT_MS)

        try:
            login(page)
            open_generate_page(page)
            for month in range(1, 13):
                log(f"Iniciando competencia {format_period(month, TARGET_YEAR)}")
                open_generate_page(page)
                configured_period = configure_generate_grid(page, month, TARGET_YEAR)
                companies = collect_rows(page)
                log(f"Total de empresas carregadas para {configured_period}: {len(companies)}")
                pending = list(companies)
                pass_number = 1

                while pending:
                    log(
                        f"Competencia {configured_period}: iniciando passagem {pass_number} "
                        f"com {len(pending)} empresa(s) pendente(s)"
                    )
                    next_pending: list[CompanyRow] = []

                    for row_meta in pending:
                        current_key = row_key(row_meta.company, configured_period)
                        existing = results_index.get(current_key)
                        if existing and is_completed_row(existing):
                            log(f"Pulando empresa ja concluida: {row_meta.company} | {configured_period}")
                            continue

                        attempts = int(existing.get("attempts", "0")) if existing else 0
                        if attempts >= MAX_RETRIES_PER_ENTRY:
                            log(
                                f"Limite de tentativas atingido: {row_meta.company} | "
                                f"{configured_period} | tentativas={attempts}"
                            )
                            continue

                        result_row = {
                            "cnpj": row_meta.cnpj,
                            "company": row_meta.company,
                            "period": configured_period,
                            "status": STATUS_ERROR,
                            "task_title": "",
                            "message": "",
                            "downloaded": "no",
                            "download_path": "",
                            "attempts": str(attempts + 1),
                        }

                        try:
                            log(
                                f"Iniciando processamento da empresa: {row_meta.company} | "
                                f"{configured_period} | tentativa={attempts + 1}"
                            )
                            ensure_expected_route(page)
                            open_generate_page(page)
                            configured_period = configure_generate_grid(page, month, TARGET_YEAR)
                            clear_selected_rows(page)
                            current = select_company_row(page, row_meta)
                            result_row["cnpj"] = current.cnpj
                            result_row["company"] = current.company
                            result_row["period"] = configured_period

                            submit_generate_operation(page)
                            requested_at = datetime.now()
                            open_tasks_panel(page)
                            task = wait_for_task(page, current.company, requested_at)

                            download_path = None
                            if task.has_download:
                                try:
                                    download_path = save_download_from_task(page, task, configured_period)
                                except TimeoutError:
                                    download_path = None

                            result_row["task_title"] = task.title
                            result_row["message"] = task.message
                            result_row["downloaded"] = "yes" if download_path else "no"
                            result_row["download_path"] = download_path or ""

                            if download_path:
                                result_row["status"] = STATUS_DOWNLOADED
                            elif NO_DOCUMENTS_PATTERN.search(task.message):
                                result_row["status"] = STATUS_NO_DOCUMENTS
                            else:
                                result_row["status"] = STATUS_ERROR

                            log(
                                f"Empresa concluida: {current.company} | periodo={configured_period} | "
                                f"status={result_row['status']} | download={result_row['downloaded']} | "
                                f"tarefa={task.title}"
                            )
                        except Exception as exc:
                            result_row["status"] = STATUS_ERROR
                            result_row["task_title"] = "ERRO_AUTOMACAO"
                            result_row["message"] = f"{type(exc).__name__}: {exc}"
                            log(
                                f"Erro ao processar {result_row['company']} | {result_row['period']}: "
                                f"{type(exc).__name__}: {exc}"
                            )
                            traceback.print_exc()
                        finally:
                            try:
                                ensure_expected_route(page)
                            except Exception:
                                pass
                            try:
                                clear_search(page)
                            except Exception:
                                pass
                            try:
                                close_tasks_panel(page)
                            except Exception:
                                pass

                        upsert_result(results, result_row)
                        results_index[current_key] = result_row
                        write_report(results)

                        if result_row["status"] == STATUS_ERROR:
                            next_pending.append(row_meta)

                    if not next_pending:
                        break

                    if len(next_pending) == len(pending) and all(
                        int(results_index[row_key(item.company, configured_period)]["attempts"]) >= MAX_RETRIES_PER_ENTRY
                        for item in next_pending
                    ):
                        break

                    pending = next_pending
                    pass_number += 1
        finally:
            write_report(results)
            log_report_summary(results)
            log("Encerrando browser")
            context.close()
            browser.close()


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        log(f"Falha fatal na automacao Sittax Dominio: {type(exc).__name__}: {exc}")
        traceback.print_exc()
        raise SystemExit(1) from exc
