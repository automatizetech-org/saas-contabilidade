from __future__ import annotations

import asyncio
import atexit
import json
import hashlib
import io
import os
import re
import shutil
import socket
import signal
import time
import subprocess
import sys
import uuid
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import quote, urlparse
import urllib.request

from dotenv import load_dotenv
from playwright.async_api import BrowserContext, Frame, Page, async_playwright
from postgrest.exceptions import APIError
from PySide6.QtCore import QThread, QTimer, Qt, Signal
from PySide6.QtGui import QAction, QColor, QFont, QIcon
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QStyle,
    QSystemTrayIcon,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)
import requests
from supabase import Client, create_client


# Diretório base: quando iniciado pelo agendador, defina ROBOT_SCRIPT_DIR com a pasta do robô
# (a que contém este .py e a pasta data/). Assim o perfil Chrome (data/chrome_profile) será sempre essa pasta.
RUNTIME_FOLDER_NAME = "goiania_taxas_impostos"


def _resolve_runtime_base_dir() -> Path:
    explicit = os.environ.get("ROBOT_SCRIPT_DIR", "").strip().rstrip(os.sep)
    if explicit:
        return Path(explicit).resolve()

    current_dir = Path(__file__).resolve().parent
    candidates: list[Path] = []
    robots_root = (os.getenv("ROBOTS_ROOT_PATH") or "").strip()

    if robots_root:
        candidates.append(Path(robots_root) / RUNTIME_FOLDER_NAME)
    candidates.append(Path.home() / "Documents" / "ROBOS" / RUNTIME_FOLDER_NAME)
    candidates.append(current_dir)

    seen: set[str] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except Exception:
            resolved = candidate
        key = str(resolved).lower()
        if key in seen:
            continue
        seen.add(key)
        if (resolved / "data" / "json").exists():
            return resolved

    return current_dir


BASE_DIR = _resolve_runtime_base_dir()
os.environ.setdefault("ROBOT_SCRIPT_DIR", str(BASE_DIR))
os.chdir(BASE_DIR)

# Perfil Chrome: data/chrome_profile relativo a BASE_DIR.
# Com agendador, defina ROBOT_SCRIPT_DIR para a pasta do robô; o perfil será BASE_DIR/data/chrome_profile.
CHROME_PROFILE_DIRNAME = "chrome_profile"
CHROME_PROFILE_BACKUP_DIRNAME = "chrome_profile_backup"
LEGACY_CHROME_PROFILE_DIRNAME = "chrome_cdp_profile"
LEGACY_CHROME_PROFILE_BACKUP_DIRNAME = "chrome_cdp_profile_backup"


def _get_chrome_profile_dir() -> Path:
    return (BASE_DIR / "data" / CHROME_PROFILE_DIRNAME).resolve()


def _get_chrome_profile_backup_dir() -> Path:
    return (BASE_DIR / "data" / CHROME_PROFILE_BACKUP_DIRNAME).resolve()


def _migrate_legacy_profile_dir(preferred_dir: Path, legacy_dir: Path) -> None:
    if preferred_dir.exists() or not legacy_dir.exists():
        return
    try:
        legacy_dir.replace(preferred_dir)
        return
    except Exception:
        pass
    try:
        shutil.copytree(legacy_dir, preferred_dir)
    except Exception:
        return
    try:
        shutil.rmtree(legacy_dir, ignore_errors=True)
    except Exception:
        pass

def _resolve_robots_base_env_dir() -> Path:
    candidates: list[Path] = []
    env_root = (os.getenv("ROBOTS_ROOT_PATH") or "").strip()
    robot_script_dir = (os.getenv("ROBOT_SCRIPT_DIR") or "").strip()

    if env_root:
        candidates.append(Path(env_root))
    if robot_script_dir:
        candidates.append(Path(robot_script_dir).resolve().parent)
    candidates.append(BASE_DIR.parent)
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        candidates.append(exe_dir.parent)
        candidates.append(exe_dir)

    seen: set[str] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except Exception:
            resolved = candidate
        key = str(resolved).lower()
        if key in seen:
            continue
        seen.add(key)
        if (resolved / ".env").exists() or (resolved / ".env.example").exists():
            return resolved

    return Path(env_root).resolve() if env_root else BASE_DIR.parent.resolve()


def _json_runtime_utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _json_runtime_ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _json_runtime_atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    _json_runtime_ensure_parent(path)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(path)


class JsonRobotRuntime:
    def __init__(self, technical_id: str, display_name: str, base_dir: Path) -> None:
        self.technical_id = technical_id
        self.display_name = display_name
        self.base_dir = base_dir
        self.last_status = "inactive"
        self.json_dir = self.base_dir / "data" / "json"
        self.job_path = self.json_dir / "job.json"
        self.result_path = self.json_dir / "result.json"
        self.heartbeat_path = self.json_dir / "heartbeat.json"

    def register_robot(self) -> str:
        self.write_heartbeat(status="active")
        return self.technical_id

    def load_job(self) -> Optional[dict[str, Any]]:
        if not self.job_path.exists():
            return None
        try:
            payload = json.loads(self.job_path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        if not payload.get("id"):
            payload["id"] = payload.get("execution_request_id") or payload.get("job_id")
        if not payload.get("job_id"):
            payload["job_id"] = payload.get("id")
        if not payload.get("execution_request_id"):
            payload["execution_request_id"] = payload.get("id")
        job_execution_id = str(
            payload.get("execution_request_id")
            or payload.get("job_id")
            or payload.get("id")
            or ""
        ).strip()
        if not job_execution_id:
            return None
        if self.result_path.exists():
            try:
                result_payload = json.loads(self.result_path.read_text(encoding="utf-8"))
            except Exception:
                result_payload = None
            if isinstance(result_payload, dict):
                result_execution_id = str(
                    result_payload.get("execution_request_id")
                    or result_payload.get("job_id")
                    or result_payload.get("event_id")
                    or ""
                ).strip()
                if result_execution_id and result_execution_id == job_execution_id:
                    return None
        return payload

    def load_job_companies(
        self,
        job: Optional[dict[str, Any]],
        company_ids: Optional[list[str]] = None,
    ) -> list[dict[str, Any]]:
        if not isinstance(job, dict):
            return []
        companies = job.get("companies")
        if not isinstance(companies, list):
            return []
        wanted = {str(company_id) for company_id in (company_ids or []) if str(company_id).strip()}
        rows: list[dict[str, Any]] = []
        for row in companies:
            if not isinstance(row, dict):
                continue
            company_id = str(row.get("company_id") or row.get("id") or "").strip()
            if wanted and company_id not in wanted:
                continue
            rows.append(row)
        return rows

    def write_heartbeat(
        self,
        *,
        status: str,
        current_job_id: Optional[str] = None,
        current_execution_request_id: Optional[str] = None,
        message: Optional[str] = None,
        progress: Optional[dict[str, Any]] = None,
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        self.last_status = status
        payload: dict[str, Any] = {
            "robot_technical_id": self.technical_id,
            "display_name": self.display_name,
            "status": status,
            "updated_at": _json_runtime_utc_now_iso(),
            "current_job_id": current_job_id,
            "current_execution_request_id": current_execution_request_id,
            "message": message,
            "progress": progress or {},
        }
        if extra:
            payload.update(extra)
        _json_runtime_atomic_write_json(self.heartbeat_path, payload)

    def write_result(
        self,
        *,
        job: Optional[dict[str, Any]],
        success: bool,
        error_message: Optional[str] = None,
        summary: Optional[dict[str, Any]] = None,
        payload: Optional[dict[str, Any]] = None,
        company_results: Optional[list[dict[str, Any]]] = None,
    ) -> None:
        execution_request_id = None
        job_id = None
        if isinstance(job, dict):
            execution_request_id = str(job.get("execution_request_id") or job.get("id") or "").strip() or None
            job_id = str(job.get("job_id") or job.get("id") or "").strip() or execution_request_id

        event_id = execution_request_id or str(uuid.uuid4())
        result_payload: dict[str, Any] = {
            "event_id": event_id,
            "job_id": job_id or event_id,
            "execution_request_id": execution_request_id,
            "robot_technical_id": self.technical_id,
            "status": "completed" if success else "failed",
            "started_at": _json_runtime_utc_now_iso(),
            "finished_at": _json_runtime_utc_now_iso(),
            "error_message": error_message,
            "summary": summary or {},
            "company_results": company_results or [],
            "payload": payload or {},
        }
        _json_runtime_atomic_write_json(self.result_path, result_payload)
        self.write_heartbeat(
            status="active",
            current_job_id=None,
            current_execution_request_id=None,
            message="result_ready",
        )


ROBOTS_BASE_ENV_DIR = _resolve_robots_base_env_dir()
ENV_CANDIDATES = [
    ROBOTS_BASE_ENV_DIR / ".env",
    ROBOTS_BASE_ENV_DIR / ".env.example",
    BASE_DIR / ".env",
    BASE_DIR / ".env.example",
]
for env_path in ENV_CANDIDATES:
    if env_path.exists():
        load_dotenv(env_path, override=False)

DATA_DIR = BASE_DIR / "data"
ROBOT_CONFIG_FILE = DATA_DIR / "goiania_robot_config.json"
PNG_DIR = DATA_DIR / "image"
ICO_DIR = DATA_DIR / "ico"
PLAYWRIGHT_DIR = DATA_DIR / "ms-playwright"
EXTENSIONS_DIR = DATA_DIR / "extensions"
LOGS_DIR = DATA_DIR / "logs"
RUNTIME_LOG_PATH = LOGS_DIR / "runtime.log"
INSTANCE_LOCK_PATH = DATA_DIR / "runtime.lock"
CDP_PORT = int(os.getenv("GOIANIA_CDP_PORT", "9223"))


def _is_cdp_port_in_use() -> bool:
    """Retorna True se algo estiver escutando na porta CDP (navegador ainda aberto)."""
    try:
        with socket.create_connection(("127.0.0.1", CDP_PORT), timeout=1.0):
            return True
    except OSError:
        return False


PROXY_DIR = DATA_DIR / "proxy"
PROXIES_FILE = PROXY_DIR / "proxies.txt"
CHROME_EXE = (DATA_DIR / "Chrome" / "chrome.exe").resolve()
# Perfil obrigatório: só data/chrome_profile do robô (nunca AppData/Playwright).
CHROME_PROFILE_DIR = _get_chrome_profile_dir()
CHROME_PROFILE_BACKUP_DIR = _get_chrome_profile_backup_dir()
_migrate_legacy_profile_dir(CHROME_PROFILE_DIR, (DATA_DIR / LEGACY_CHROME_PROFILE_DIRNAME).resolve())
_migrate_legacy_profile_dir(CHROME_PROFILE_BACKUP_DIR, (DATA_DIR / LEGACY_CHROME_PROFILE_BACKUP_DIRNAME).resolve())
CHROME_LOG_PATH = (DATA_DIR / "chrome_start.log").resolve()
SYSTEM_CHROME_USER_DATA_DIR = (
    (Path(os.getenv("LOCALAPPDATA")) / "Google" / "Chrome" / "User Data").resolve()
    if os.getenv("LOCALAPPDATA")
    else Path()
)
SYSTEM_CHROME_DEFAULT_PROFILE_DIR = (
    (SYSTEM_CHROME_USER_DATA_DIR / "Default").resolve()
    if str(SYSTEM_CHROME_USER_DATA_DIR)
    else Path()
)
SYSTEM_CHROME_PROFILE_1_DIR = (
    (SYSTEM_CHROME_USER_DATA_DIR / "Profile 1").resolve()
    if str(SYSTEM_CHROME_USER_DATA_DIR)
    else Path()
)
SKILL_UP_EXTENSION_ID = "eihghbeaaeedpcojhbghbocnkcponaeo"
HELPER_CAPTCHA_EXTENSION_ID = "hlifkpholllijblknnmbfagnkjneagid"
SKILL_UP_EXTENSION_UPDATE_URL = (
    "https://clients2.google.com/service/update2/crx"
    "?response=redirect"
    "&prodversion=144.0.7559.110"
    "&acceptformat=crx2,crx3"
    f"&x=id%3D{SKILL_UP_EXTENSION_ID}%26uc"
)


def _load_proxy_list() -> list[str]:
    """Carrega lista de proxies: env GOIANIA_PROXY_LIST (vírgula) ou data/proxy/proxies.txt (uma URL por linha)."""
    from_env = os.getenv("GOIANIA_PROXY_LIST", "").strip()
    if from_env:
        return [p.strip() for p in from_env.split(",") if p.strip()]
    if PROXIES_FILE.exists():
        try:
            lines = [ln.strip() for ln in PROXIES_FILE.read_text(encoding="utf-8", errors="replace").splitlines() if ln.strip()]
            return lines
        except Exception:
            pass
    return []


PROXY_LIST_URL = "https://api.proxyscrape.com/?request=displayproxies&proxytype=http&country=BR"


def _is_proxy_reachable(proxy_url: str, timeout: float = 3.0) -> bool:
    """Verifica se o proxy está acessível (conexão TCP)."""
    if not proxy_url or not proxy_url.strip():
        return False
    try:
        parsed = urlparse(proxy_url.strip())
        host = parsed.hostname or parsed.path.split(":")[0] if parsed.path else None
        port = parsed.port
        if not host:
            return False
        if port is None:
            port = 80 if (parsed.scheme or "").lower() != "https" else 443
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except Exception:
        return False


def _update_proxy_list() -> int:
    """Baixa lista de proxies HTTP (Brasil), testa cada um e grava em proxies.txt só os que respondem."""
    try:
        req = urllib.request.Request(
            PROXY_LIST_URL,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read().decode("utf-8", errors="replace")
    except Exception:
        return 0
    lines = [ln.strip() for ln in data.splitlines() if ln.strip()]
    urls = []
    for ln in lines:
        if ":" in ln and not ln.startswith("#"):
            parts = ln.rsplit(":", 1)
            if len(parts) == 2 and parts[1].isdigit():
                urls.append(f"http://{ln.strip()}")
    # Só gravar proxies que respondem ao teste de conectividade
    working = [u for u in urls if _is_proxy_reachable(u, timeout=2.0)]
    PROXY_DIR.mkdir(parents=True, exist_ok=True)
    PROXIES_FILE.write_text("\n".join(working) + ("\n" if working else ""), encoding="utf-8")
    return len(working)


SUPABASE_URL = os.getenv("SUPABASE_URL", "")
def _get_supabase_service_role_key() -> str:
    for env_name in (
        "SUPABASE_SERVICE_ROLE_KEY",
        "SERVICE_ROLE_KEY",
        "SUPABASE_KEY",
        "SUPABASE_SECRET_KEY",
        "NEXT_PUBLIC_SUPABASE_SERVICE_ROLE_KEY",
    ):
        value = (os.getenv(env_name) or "").strip()
        if value:
            return value
    return ""

SUPABASE_SERVICE_ROLE_KEY = _get_supabase_service_role_key()
GOIANIA_PORTAL_CPF = os.getenv("GOIANIA_PORTAL_CPF", "")
GOIANIA_PORTAL_PASSWORD = os.getenv("GOIANIA_PORTAL_PASSWORD", "")
# URL do server-api (dashboard); base_path e estrutura de pastas (segment_path) vêm daqui.
SERVER_API_URL = (os.getenv("SERVER_API_URL") or "").strip()
CONNECTOR_SECRET = (os.getenv("CONNECTOR_SECRET") or "").strip()
# Não usado para o Chrome do robô; o robô usa apenas CHROME_PROFILE_DIR (data/chrome_profile).
PLAYWRIGHT_USER_DATA_DIR = os.getenv("PLAYWRIGHT_USER_DATA_DIR", str(BASE_DIR / ".playwright-profile"))

ROBOT_TECHNICAL_ID = "goiania_taxas_impostos"
ROBOT_DISPLAY_NAME_DEFAULT = "Taxas e Impostos Goiania"
# Fallback só para registro do robô na tabela; o caminho para salvar PDFs vem do dashboard (api/robot-config).
ROBOT_SEGMENT_PATH_DEFAULT = "PARALEGAL/TAXAS-IMPOSTOS"
LOGIN_URL = "https://www10.goiania.go.gov.br/Internet/Login.aspx?OriginalURL="
INTERNET_HOME_URL = "https://www10.goiania.go.gov.br/Internet/"
HOME_URL = "https://servicos.goiania.go.gov.br/SicaePortal/"
INTRANET_LOGIN_URL = "https://servicos.goiania.go.gov.br/Intranet/Login.aspx"
LOGIN_NAVIGATION_TIMEOUT_MS = 25000
PORTAL_TRIBUTOS_URL_PART = "PortalTributos/ConsultaTributos"
DEBITOS_URL_PART = "MostraDebitos"
URLEMISSAO_DUAM = "http://www11.goiania.go.gov.br/sistemas/scarr/asp/scarr32010s2.asp"
HEARTBEAT_INTERVAL_MS = 30000
JOB_POLL_INTERVAL_MS = 10000
DISPLAY_CONFIG_INTERVAL_MS = 10000
JSON_RUNTIME = JsonRobotRuntime(
    ROBOT_TECHNICAL_ID,
    ROBOT_DISPLAY_NAME_DEFAULT,
    BASE_DIR,
)

_PROCESS_SHUTDOWN_MARKED_INACTIVE = False
_WINDOWS_CONSOLE_HANDLER = None


def _mark_process_inactive(window: Any | None = None, reason: str = "process_exit") -> None:
    global _PROCESS_SHUTDOWN_MARKED_INACTIVE
    if _PROCESS_SHUTDOWN_MARKED_INACTIVE:
        return
    _PROCESS_SHUTDOWN_MARKED_INACTIVE = True
    try:
        JSON_RUNTIME.write_heartbeat(
            status="inactive",
            current_job_id=None,
            current_execution_request_id=None,
            message=reason,
        )
    except Exception:
        pass
    try:
        if window is not None and getattr(window, "backend", None) is not None:
            window.backend.ensure_robot_registration()
            window.backend.update_robot_status("inactive")
    except Exception:
        pass


def _install_process_shutdown_handlers(window: Any | None = None) -> None:
    global _WINDOWS_CONSOLE_HANDLER

    def _handle_signal(signum, _frame) -> None:
        _mark_process_inactive(window, f"signal_{signum}")
        raise SystemExit(0)

    atexit.register(lambda: _mark_process_inactive(window, "atexit"))
    for signal_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, signal_name, None)
        if sig is None:
            continue
        try:
            signal.signal(sig, _handle_signal)
        except Exception:
            pass

    if os.name == "nt":
        try:
            import ctypes

            handler_type = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_uint)

            @handler_type
            def _console_handler(ctrl_type: int) -> bool:
                _mark_process_inactive(window, f"console_ctrl_{ctrl_type}")
                return False

            ctypes.windll.kernel32.SetConsoleCtrlHandler(_console_handler, True)
            _WINDOWS_CONSOLE_HANDLER = _console_handler
        except Exception:
            pass


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def is_scheduler_mode_enabled() -> bool:
    return str(os.getenv("AUTOMATIZE_SCHEDULER_MODE") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "sim",
        "on",
    }


def append_runtime_log(message: str) -> None:
    try:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        with RUNTIME_LOG_PATH.open("a", encoding="utf-8") as handle:
            handle.write(message.rstrip() + "\n")
    except Exception:
        pass


def emit_terminal_log(message: str) -> str:
    text = str(message or "").rstrip()
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{timestamp}] [GOIANIA] {text}" if text else f"[{timestamp}] [GOIANIA]"
    try:
        print(line, flush=True)
    except Exception:
        pass
    return line


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def ensure_single_instance() -> None:
    INSTANCE_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    current_pid = os.getpid()
    if INSTANCE_LOCK_PATH.exists():
        try:
            existing_pid = int((INSTANCE_LOCK_PATH.read_text(encoding="utf-8") or "").strip() or "0")
        except Exception:
            existing_pid = 0
        if existing_pid and existing_pid != current_pid and _pid_is_alive(existing_pid):
            append_runtime_log(f"[Robo] Instância já em execução (PID {existing_pid}). Encerrando novo processo.")
            raise SystemExit(0)
    INSTANCE_LOCK_PATH.write_text(str(current_pid), encoding="utf-8")

    def _cleanup() -> None:
        try:
            if not INSTANCE_LOCK_PATH.exists():
                return
            saved_pid = int((INSTANCE_LOCK_PATH.read_text(encoding="utf-8") or "").strip() or "0")
            if saved_pid == current_pid:
                INSTANCE_LOCK_PATH.unlink()
        except Exception:
            pass

    atexit.register(_cleanup)


def _extract_crx_payload(crx_bytes: bytes) -> bytes:
    if crx_bytes[:4] != b"Cr24":
        raise ValueError("Arquivo CRX invÃ¡lido.")
    version = int.from_bytes(crx_bytes[4:8], "little")
    if version == 2:
        pubkey_len = int.from_bytes(crx_bytes[8:12], "little")
        sig_len = int.from_bytes(crx_bytes[12:16], "little")
        offset = 16 + pubkey_len + sig_len
    elif version == 3:
        header_len = int.from_bytes(crx_bytes[8:12], "little")
        offset = 12 + header_len
    else:
        raise ValueError(f"VersÃ£o de CRX nÃ£o suportada: {version}")
    return crx_bytes[offset:]


def ensure_skill_up_extension_dir() -> Path | None:
    extension_dir = EXTENSIONS_DIR / SKILL_UP_EXTENSION_ID
    if (extension_dir / "manifest.json").exists():
        return extension_dir

    EXTENSIONS_DIR.mkdir(parents=True, exist_ok=True)
    tmp_dir = EXTENSIONS_DIR / f".{SKILL_UP_EXTENSION_ID}.tmp"
    shutil.rmtree(tmp_dir, ignore_errors=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    try:
        req = urllib.request.Request(
            SKILL_UP_EXTENSION_UPDATE_URL,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            zip_payload = _extract_crx_payload(resp.read())
        with zipfile.ZipFile(io.BytesIO(zip_payload)) as zip_ref:
            zip_ref.extractall(tmp_dir)
        if not (tmp_dir / "manifest.json").exists():
            raise RuntimeError("Manifest da extensÃ£o nÃ£o encontrado apÃ³s extraÃ§Ã£o.")
        shutil.rmtree(extension_dir, ignore_errors=True)
        tmp_dir.replace(extension_dir)
        return extension_dir
    except Exception as exc:
        try:
            print(f"[Goiania] Falha ao preparar extensÃ£o captcha: {exc}", file=sys.stderr)
        except Exception:
            pass
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return None


def digits(value: str | None) -> str:
    return re.sub(r"\D", "", value or "")


def normalize_name(value: str | None) -> str:
    base = re.sub(r"[^A-Z0-9 ]", " ", (value or "").upper())
    for token in [" LTDA", " EIRELI", " ME", " EPP", " S A", " SA", " SERVICOS", " SERVIÇOS"]:
        base = base.replace(token, " ")
    return " ".join(base.split())


def sanitize_company_folder(name: str | None) -> str:
    """Nome da pasta da empresa no disco (BASE_PATH/{isso})."""
    s = (name or "").strip()
    s = re.sub(r'[<>:"/\\|?*]', "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.rstrip(" .")
    return s or "Empresa"


def normalize_location_name(value: str | None) -> str:
    text = (value or "").upper()
    replacements = {
        "Á": "A",
        "À": "A",
        "Â": "A",
        "Ã": "A",
        "É": "E",
        "Ê": "E",
        "Í": "I",
        "Ó": "O",
        "Ô": "O",
        "Õ": "O",
        "Ú": "U",
        "Ç": "C",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return " ".join(re.sub(r"[^A-Z0-9 ]", " ", text).split())


def parse_money(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return float(value.replace("R$", "").replace(".", "").replace(",", ".").strip())
    except ValueError:
        return 0.0


def parse_date(value: str | None) -> str | None:
    if not value:
        return None
    match = re.search(r"(\d{2})/(\d{2})/(\d{4})", value)
    if not match:
        return None
    day, month, year = match.groups()
    return f"{year}-{month}-{day}"


_robot_api_config: dict[str, Any] | None = None


def get_robot_supabase() -> tuple[str | None, str | None]:
    url = SUPABASE_URL.strip()
    key = SUPABASE_SERVICE_ROLE_KEY.strip()
    if url and key:
        return (url, key)
    return (None, None)


def fetch_robot_config_from_api() -> dict[str, Any] | None:
    url_base = SERVER_API_URL.rstrip("/")
    if not url_base:
        return None
    try:
        headers: dict[str, str] = {}
        if "ngrok" in url_base.lower():
            headers["ngrok-skip-browser-warning"] = "true"
        if CONNECTOR_SECRET:
            headers["Authorization"] = f"Bearer {hashlib.sha256(CONNECTOR_SECRET.encode('utf-8')).hexdigest()}"
        response = requests.get(
            f"{url_base}/api/robot-config",
            params={"technical_id": ROBOT_TECHNICAL_ID},
            headers=headers,
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def get_robot_api_config() -> dict[str, Any] | None:
    global _robot_api_config
    if _robot_api_config is None:
        _robot_api_config = fetch_robot_config_from_api()
    return _robot_api_config


def normalize_dashboard_logins(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        cpf = digits(item.get("cpf") or item.get("login") or item.get("username") or "")
        password = str(item.get("password") or item.get("senha") or "").strip()
        city = str(item.get("city") or item.get("municipio") or item.get("provider") or "").strip().lower()
        technical_id = str(item.get("robot_technical_id") or "").strip().lower()
        if len(cpf) != 11 or not password:
            continue
        normalized.append(
            {
                "cpf": cpf,
                "password": password,
                "is_default": bool(item.get("is_default")),
                "city": city,
                "robot_technical_id": technical_id,
            }
        )
    if normalized and not any(item.get("is_default") for item in normalized):
        normalized[0]["is_default"] = True
    return normalized


def find_icon_path(*names: str) -> Path | None:
    candidates: list[Path] = []
    for name in names:
        raw = name.strip()
        if not raw:
            continue
        stem = Path(raw).stem
        normalized = stem.lower().replace(" ", "_")
        for folder in (ICO_DIR, PNG_DIR):
            candidates.append(folder / raw)
            candidates.append(folder / stem)
            candidates.append(folder / normalized)
            candidates.append(folder / f"{stem}.png")
            candidates.append(folder / f"{stem}.ico")
            candidates.append(folder / f"{raw}.png")
            candidates.append(folder / f"{normalized}.png")
            candidates.append(folder / f"{raw}.ico")
            candidates.append(folder / f"{normalized}.ico")
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def qicon(*names: str) -> QIcon | None:
    path = find_icon_path(*names)
    if not path:
        return None
    return QIcon(str(path))


def button_style(base: str, hover: str, pressed: str) -> str:
    return f"""
    QPushButton {{
        font: 9pt 'Verdana';
        font-weight: bold;
        color: #E8F4FF;
        padding: 10px 14px;
        border-radius: 10px;
        border: 1px solid rgba(255,255,255,0.12);
        background: qlineargradient(x1:0,y1:0, x2:1,y2:1,
            stop:0 {base}, stop:1 #0F1E35);
    }}
    QPushButton:hover {{
        background: qlineargradient(x1:0,y1:0, x2:1,y2:1,
            stop:0 {hover}, stop:1 #12304E);
    }}
    QPushButton:pressed {{
        background: {pressed};
    }}
    QPushButton:disabled {{
        color: #9CA3AF;
        background: #243041;
    }}
    """


@dataclass
class CompanyItem:
    id: str
    name: str
    document: str | None
    active: bool
    enabled_for_robot: bool
    selected_login_cpf: str | None = None
    state_registration: str | None = None
    cae: str | None = None
    selected: bool = False
    status: str = "AGUARDANDO"
    message: str = "-"


@dataclass
class DebtRow:
    ano: int | None
    tributo: str
    numero_documento: str
    data_vencimento: str | None
    valor: float
    situacao: str | None
    portal_inscricao: str | None = None
    portal_cai: str | None = None
    detalhes: dict[str, Any] = field(default_factory=dict)


def _is_iss_tributo(tributo: str) -> bool:
    """True se o tributo for ISS (ex.: ISS, 1759 - ISS RETIDO NA FONTE). Usado quando robot_goiania_skip_iss está ativo."""
    if not tributo or not str(tributo).strip():
        return False
    n = tributo.strip().upper()
    if n == "ISS" or n.startswith("ISS ") or n.startswith("ISS-") or n.endswith(" ISS"):
        return True
    if " ISS " in f" {n} ":
        return True
    return bool(re.search(r"\bISS\b", n))


def _debt_is_iss(debt: DebtRow) -> bool:
    """True se o débito for de ISS (tributo ou qualquer campo em linha_portal)."""
    if _is_iss_tributo(debt.tributo):
        return True
    linha = (debt.detalhes or {}).get("linha_portal") or {}
    if isinstance(linha, dict):
        for v in linha.values():
            if v and isinstance(v, str) and re.search(r"\bISS\b", v.upper()):
                return True
    return False


class StopRequested(RuntimeError):
    pass


class RecaptchaTimeoutError(RuntimeError):
    """reCAPTCHA não foi resolvido em 1 minuto; permite fechar navegador, trocar porta e tentar de novo."""


class RobotBackend:
    def __init__(self) -> None:
        if False and (not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY):
            raise RuntimeError("SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY precisam estar definidos no .env do robô.")
        self.supabase: Client | None = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY) if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY else None
        self.playwright = None
        self.browser = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None
        self.portal_home_page: Page | None = None
        self.chrome_proc: subprocess.Popen | None = None
        self.robot_row: dict[str, Any] | None = None
        self.display_config_updated_at: str | None = None
        self._log_cb: Callable[[str], None] | None = None
        self.robot_id = self.register_robot()
        self._proxy_list: list[str] = _load_proxy_list()
        self._proxy_index: int = 0
        self._mitmdump_proc: subprocess.Popen | None = None
        self._mitmdump_port: int | None = None
        self._use_proxy_rotation: bool = True
        self.current_json_job: dict[str, Any] | None = None
        self.pending_result_operations: list[dict[str, Any]] = []
        self.pending_run_rows: dict[str, dict[str, Any]] = {}
        self.pending_company_debts: dict[str, list[dict[str, Any]]] = {}
        self._skill_up_extension_ready = False
        self._extension_profile_seeded = False
        self._working_profile_prepared = False

    def _reset_pending_result_payload(self) -> None:
        self.pending_result_operations.clear()
        self.pending_run_rows.clear()
        self.pending_company_debts.clear()

    def _is_json_runtime_job(self) -> bool:
        return isinstance(self.current_json_job, dict)

    def _build_result_payload(self) -> dict[str, Any]:
        operations = list(self.pending_result_operations)
        for company_id, rows in self.pending_company_debts.items():
            operations.append(
                {
                    "kind": "replace_company_rows",
                    "table": "municipal_tax_debts",
                    "company_id": company_id,
                    "rows": rows,
                }
            )
        if self.pending_run_rows:
            operations.append(
                {
                    "kind": "upsert_rows",
                    "table": "municipal_tax_collection_runs",
                    "on_conflict": "id",
                    "rows": list(self.pending_run_rows.values()),
                }
            )
        return {"operations": operations}

    def _build_result_summary(self) -> dict[str, Any]:
        company_ids = set(self.pending_company_debts.keys()) | set(self.pending_run_rows.keys())
        debts_total = sum(len(rows) for rows in self.pending_company_debts.values())
        successful_runs = sum(1 for row in self.pending_run_rows.values() if str(row.get("status") or "").lower() == "completed")
        failed_runs = sum(1 for row in self.pending_run_rows.values() if str(row.get("status") or "").lower() == "failed")
        return {
            "companies_total": len(company_ids),
            "debts_total": debts_total,
            "successful_runs": successful_runs,
            "failed_runs": failed_runs,
        }

    def _build_company_results(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        company_ids = set(self.pending_company_debts.keys()) | set(self.pending_run_rows.keys())
        for company_id in sorted(company_ids):
            run_row = self.pending_run_rows.get(company_id) or {}
            debt_rows = self.pending_company_debts.get(company_id) or []
            status = str(run_row.get("status") or "completed").lower()
            results.append(
                {
                    "company_id": company_id,
                    "company_name": run_row.get("company_name"),
                    "status": "success" if status == "completed" else status,
                    "debts_found": len(debt_rows),
                }
            )
        return results

    def set_use_proxy_rotation(self, value: bool) -> None:
        """Ativa ou desativa o uso de rotação de proxy (lista + mitmdump)."""
        self._use_proxy_rotation = value

    def get_current_proxy(self) -> str | None:
        """Retorna a URL do proxy atual (lista ou mitmdump local), ou None para não usar proxy."""
        if not self._use_proxy_rotation:
            return None
        if self._proxy_list:
            return self._proxy_list[self._proxy_index % len(self._proxy_list)]
        if self._mitmdump_port is not None:
            return f"http://127.0.0.1:{self._mitmdump_port}"
        return None

    def use_next_proxy(self) -> str | None:
        """Avança para o próximo proxy (para retry após RecaptchaTimeoutError). Retorna o próximo em uso."""
        if self._proxy_list:
            self._proxy_index = (self._proxy_index + 1) % len(self._proxy_list)
            return self.get_current_proxy()
        if self._mitmdump_proc is not None:
            self._stop_mitmdump()
            self._start_mitmdump_if_needed()
            return self.get_current_proxy()
        return None

    def reload_proxy_list(self) -> None:
        """Recarrega a lista de proxies do arquivo (útil após atualizar proxies.txt)."""
        self._proxy_list = _load_proxy_list()
        self._proxy_index = 0

    async def _ensure_skill_up_extension_ready(self) -> None:
        if self._skill_up_extension_ready or self.context is None:
            return

        if self._profile_has_skill_up_installed(_get_chrome_profile_dir()):
            self._skill_up_extension_ready = True
            return

        extension_url_prefix = f"chrome-extension://{SKILL_UP_EXTENSION_ID}/"
        setup_url = f"{extension_url_prefix}setup.html"
        extension_confirmed = False

        for _ in range(30):
            try:
                workers = list(getattr(self.context, "service_workers", []) or [])
            except Exception:
                workers = []
            if any(SKILL_UP_EXTENSION_ID in (getattr(worker, "url", "") or "") for worker in workers):
                extension_confirmed = True
                break
            await asyncio.sleep(0.5)

        if not extension_confirmed:
            try:
                response = requests.get(f"http://127.0.0.1:{CDP_PORT}/json/list", timeout=3)
                response.raise_for_status()
                targets = response.json() or []
                extension_confirmed = any(
                    SKILL_UP_EXTENSION_ID in str((target or {}).get("url") or "")
                    for target in targets
                    if isinstance(target, dict)
                )
            except Exception as exc:
                self._log(f"[WARN] Não foi possível consultar os alvos CDP da extensão Skill Up: {exc}")

        setup_page: Page | None = None
        try:
            setup_page = await self.context.new_page()
            await setup_page.goto(setup_url, wait_until="domcontentloaded", timeout=20_000)
            await asyncio.sleep(2)
            extension_confirmed = True
            self._log("Extensão Skill Up carregada e página de setup validada.")
        except Exception as exc:
            self._log(f"[WARN] Não foi possível validar a página da extensão Skill Up: {exc}")
        finally:
            if setup_page is not None:
                try:
                    await setup_page.close()
                except Exception:
                    pass

        if not extension_confirmed:
            raise RuntimeError(
                "Extensão Skill Up não foi confirmada no Chrome. O robô não pode prosseguir sem a extensão."
            )
        self._skill_up_extension_ready = True

    def set_log_callback(self, cb: Callable[[str], None] | None) -> None:
        self._log_cb = cb

    def _log(self, message: str) -> None:
        if self._log_cb:
            self._log_cb(message)
            return
        line = emit_terminal_log(message)
        append_runtime_log(line)

    def _client(self) -> Client:
        if self.supabase is None:
            raise RuntimeError("SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY precisam estar definidos no .env do robô.")
        return self.supabase

    def fetch_robot_row(self) -> dict[str, Any] | None:
        if self.current_json_job:
            robot_job = self.current_json_job.get("robot")
            if isinstance(robot_job, dict):
                self.robot_row = robot_job
                return robot_job
        try:
            response = (
                self._client()
                .table("robots")
                .select("id,technical_id,display_name,status,segment_path,global_logins")
                .eq("technical_id", ROBOT_TECHNICAL_ID)
                .limit(1)
                .execute()
            )
            rows = response.data or []
            self.robot_row = rows[0] if rows else None
            return self.robot_row
        except Exception:
            return None

    def fetch_robot_display_config(self) -> dict[str, Any] | None:
        try:
            response = (
                self._client()
                .table("robot_display_config")
                .select("*")
                .eq("robot_technical_id", ROBOT_TECHNICAL_ID)
                .limit(1)
                .execute()
            )
            rows = response.data or []
            return rows[0] if rows else None
        except Exception:
            return None

    def get_dashboard_portal_credentials(self, selected_login_cpf: str | None = None) -> tuple[str, str]:
        robot_row = self.fetch_robot_row() or self.robot_row or {}
        candidates = normalize_dashboard_logins(robot_row.get("global_logins"))
        goiania_only = [
            item
            for item in candidates
            if item.get("robot_technical_id") in ("", ROBOT_TECHNICAL_ID)
            and item.get("city") in ("", "goiania", "goiânia", "prefeitura_goiania", "prefeitura-goiania")
        ]
        if not goiania_only:
            goiania_only = candidates

        selected_cpf = digits(selected_login_cpf)
        if selected_cpf:
            for item in goiania_only:
                if item.get("cpf") == selected_cpf:
                    return (item["cpf"], item["password"])

        for item in goiania_only:
            if item.get("is_default"):
                return (item["cpf"], item["password"])
        if goiania_only:
            return (goiania_only[0]["cpf"], goiania_only[0]["password"])

        if GOIANIA_PORTAL_CPF and GOIANIA_PORTAL_PASSWORD:
            return (digits(GOIANIA_PORTAL_CPF), GOIANIA_PORTAL_PASSWORD)
        raise RuntimeError(
            "Nenhum login da Prefeitura de Goiania foi encontrado em robots.global_logins nem no .env."
        )

    def _fetch_company_config_rows(self, company_ids: list[str] | None = None) -> list[dict[str, Any]]:
        if self.supabase is None:
            return []
        query = (
            self._client()
            .table("company_robot_config")
            .select("company_id,enabled,selected_login_cpf")
            .eq("robot_technical_id", ROBOT_TECHNICAL_ID)
            .eq("enabled", True)
        )
        if company_ids:
            query = query.in_("company_id", company_ids)
        response = query.execute()
        return response.data or []

    def _load_companies_by_ids(self, company_ids: list[str] | None = None) -> list[CompanyItem]:
        if self.supabase is None:
            return []
        config_rows = self._fetch_company_config_rows(company_ids)
        config_by_company = {
            row.get("company_id"): row
            for row in config_rows
            if row.get("company_id")
        }
        enabled_company_ids = list(config_by_company.keys())
        if company_ids is not None and not enabled_company_ids:
            return []

        query = (
            self._client()
            .table("companies")
            .select("id,name,document,active,state_registration,state_code,city_name,cae")
            .eq("active", True)
            .order("name")
        )
        if company_ids is not None:
            query = query.in_("id", company_ids)
        elif enabled_company_ids:
            query = query.in_("id", enabled_company_ids)

        response = query.execute()
        rows = response.data or []
        companies: list[CompanyItem] = []
        for row in rows:
            company_id = row.get("id")
            if not company_id:
                continue
            state_code = str(row.get("state_code") or "").strip().upper()
            city_name = normalize_location_name(row.get("city_name"))
            if state_code != "GO" or city_name != "GOIANIA":
                continue
            config = config_by_company.get(company_id)
            if company_ids is None and not config:
                continue
            companies.append(
                CompanyItem(
                    id=company_id,
                    name=(row.get("name") or "").strip(),
                    document=row.get("document"),
                    active=bool(row.get("active", True)),
                    enabled_for_robot=bool(config),
                    selected_login_cpf=digits((config or {}).get("selected_login_cpf") or "") or None,
                    state_registration=(row.get("state_registration") or "").strip() or None,
                    cae=(row.get("cae") or "").strip() or None,
                    status="ATIVO" if row.get("active", True) else "INATIVO",
                )
            )

        if company_ids is not None:
            order_map = {company_id: index for index, company_id in enumerate(company_ids)}
            companies.sort(key=lambda item: order_map.get(item.id, 10**9))
        return companies

    def fetch_companies(self) -> list[CompanyItem]:
        return self._load_companies_by_ids()

    def fetch_companies_by_ids(self, company_ids: list[str]) -> list[CompanyItem]:
        snapshot_rows = JSON_RUNTIME.load_job_companies(self.current_json_job, company_ids)
        if snapshot_rows:
            items: list[CompanyItem] = []
            for row in snapshot_rows:
                company_id = str(row.get("company_id") or row.get("id") or "").strip()
                if not company_id:
                    continue
                items.append(
                    CompanyItem(
                        id=company_id,
                        name=str(row.get("name") or "").strip(),
                        document=digits(str(row.get("document") or "")),
                        active=bool(row.get("active", True)),
                        enabled_for_robot=bool(row.get("enabled_for_robot", True)),
                        selected_login_cpf=digits(str(row.get("selected_login_cpf") or (row.get("settings") or {}).get("selected_login_cpf") or "")) or None,
                        state_registration=str(row.get("state_registration") or "").strip() or None,
                        cae=str(row.get("cae") or "").strip() or None,
                        selected=False,
                        status="ATIVO" if row.get("active", True) else "INATIVO",
                    )
                )
            return items
        return self._load_companies_by_ids(company_ids)

    def create_run(self, company: CompanyItem) -> str:
        if self._is_json_runtime_job():
            run_id = str(uuid.uuid4())
            self.pending_run_rows[run_id] = {
                "id": run_id,
                "robot_technical_id": ROBOT_TECHNICAL_ID,
                "company_id": company.id,
                "company_name": company.name,
                "status": "running",
                "started_at": utc_now_iso(),
            }
            return run_id
        try:
            response = self.supabase.table("municipal_tax_collection_runs").insert(
                {
                    "robot_technical_id": ROBOT_TECHNICAL_ID,
                    "company_id": company.id,
                    "company_name": company.name,
                    "status": "running",
                    "started_at": utc_now_iso(),
                }
            ).execute()
            return response.data[0]["id"]
        except APIError as exc:
            if exc.code == "PGRST205":
                return ""
            raise

    def register_robot(self) -> str | None:
        try:
            return JSON_RUNTIME.register_robot()
        except Exception as exc:
            self._log(f"[Robo] Falha ao iniciar runtime JSON: {exc}")
            return None

    def ensure_robot_registration(self) -> str | None:
        if self.robot_id:
            return self.robot_id
        self.robot_id = self.register_robot()
        return self.robot_id

    def update_robot_status(self, status: str) -> None:
        if not self.ensure_robot_registration():
            return
        try:
            job = self.current_json_job
            JSON_RUNTIME.write_heartbeat(
                status=status,
                current_job_id=(job or {}).get("job_id"),
                current_execution_request_id=(job or {}).get("execution_request_id"),
            )
        except Exception as exc:
            self._log(f"[Robo] Falha ao atualizar status '{status}' em robots: {exc}")

    def update_robot_heartbeat(self) -> None:
        if not self.ensure_robot_registration():
            return
        try:
            job = self.current_json_job
            JSON_RUNTIME.write_heartbeat(
                status="processing" if job else "active",
                current_job_id=(job or {}).get("job_id"),
                current_execution_request_id=(job or {}).get("execution_request_id"),
            )
        except Exception as exc:
            self._log(f"[Robo] Falha ao atualizar heartbeat em robots: {exc}")

    def finish_run(self, run_id: str, status: str, debts_found: int = 0, error_message: str | None = None) -> None:
        if not run_id:
            return
        if self._is_json_runtime_job():
            row = self.pending_run_rows.get(run_id)
            if row is not None:
                row.update(
                    {
                        "status": status,
                        "debts_found": debts_found,
                        "error_message": error_message,
                        "finished_at": utc_now_iso(),
                    }
                )
            return
        try:
            self.supabase.table("municipal_tax_collection_runs").update(
                {
                    "status": status,
                    "debts_found": debts_found,
                    "error_message": error_message,
                    "finished_at": utc_now_iso(),
                }
            ).eq("id", run_id).execute()
        except APIError as exc:
            if exc.code != "PGRST205":
                raise

    def _get_skip_iss_config(self) -> bool:
        """Lê admin_settings.robot_goiania_skip_iss (dashboard: Não capturar débitos de ISS)."""
        try:
            if self.current_json_job:
                admin_settings = ((self.current_json_job.get("robot") or {}).get("admin_settings") or {})
                if isinstance(admin_settings, dict):
                    return bool(admin_settings.get("skip_iss"))
            if self.supabase is None:
                return False
            r = self.supabase.table("admin_settings").select("value").eq("key", "robot_goiania_skip_iss").limit(1).execute()
            row = (r.data or [None])[0]
            val = str((row or {}).get("value") or "").strip().lower() == "true"
            if not val and (not r.data or len(r.data) == 0):
                self._log("[Config] Chave robot_goiania_skip_iss não encontrada em admin_settings; débitos de ISS serão capturados. Crie a chave no dashboard (Editar robô Taxas e Impostos) ou aplique a migration 20260316120000.")
            return val
        except Exception as e:
            self._log(f"[Config] Erro ao ler robot_goiania_skip_iss: {e}. Débitos de ISS serão capturados. Verifique se anon pode SELECT em admin_settings.")
            return False

    def _clear_company_debts(self, company_id: str) -> None:
        if self._is_json_runtime_job():
            self.pending_company_debts[company_id] = []
            return
        try:
            self.supabase.table("municipal_tax_debts").delete().eq("company_id", company_id).execute()
        except Exception:
            pass

    def _debt_to_json_item(self, debt: DebtRow) -> dict[str, Any]:
        return {
            "ano": debt.ano,
            "tributo": debt.tributo,
            "numero_documento": debt.numero_documento,
            "data_vencimento": debt.data_vencimento,
            "valor": debt.valor,
            "situacao": debt.situacao,
            "portal_inscricao": debt.portal_inscricao,
            "portal_cai": debt.portal_cai,
            "detalhes": debt.detalhes,
        }

    def replace_company_debts_rpc(self, company_id: str, debts: list[DebtRow]) -> int:
        """Substitui todos os débitos da empresa pelos novos (uma operação atômica no Supabase).
        Deduplica por (tributo, numero_documento, data_vencimento) para evitar violar municipal_tax_debts_dedupe_unique."""
        def _key(d: DebtRow) -> tuple[str, str, str]:
            return (d.tributo or "", d.numero_documento or "", d.data_vencimento or "")
        seen: dict[tuple[str, str, str], DebtRow] = {}
        for d in debts:
            seen[_key(d)] = d
        deduped = list(seen.values())
        payload = [self._debt_to_json_item(d) for d in deduped]
        if self._is_json_runtime_job():
            self.pending_company_debts[company_id] = payload
            return len(payload)
        result = self.supabase.rpc(
            "replace_company_municipal_tax_debts",
            {"p_company_id": company_id, "p_debts": payload},
        ).execute()
        return int(result.data) if result.data is not None else 0

    def insert_one_debt(self, company_id: str, debt: DebtRow) -> None:
        """Envia um débito ao Supabase assim que é capturado (fallback se a RPC não existir)."""
        payload = {
            "company_id": company_id,
            "ano": debt.ano,
            "tributo": debt.tributo,
            "numero_documento": debt.numero_documento,
            "data_vencimento": debt.data_vencimento,
            "valor": debt.valor,
            "situacao": debt.situacao,
            "portal_inscricao": debt.portal_inscricao,
            "portal_cai": debt.portal_cai,
            "detalhes": debt.detalhes,
            "fetched_at": utc_now_iso(),
        }
        if self._is_json_runtime_job():
            self.pending_company_debts.setdefault(company_id, []).append(payload)
            return
        self.supabase.table("municipal_tax_debts").insert(payload).execute()

    def sync_company_debts(self, company: CompanyItem, debts: list[DebtRow]) -> int:
        if self._is_json_runtime_job():
            payload = [
                {
                    "company_id": company.id,
                    "ano": debt.ano,
                    "tributo": debt.tributo,
                    "numero_documento": debt.numero_documento,
                    "data_vencimento": debt.data_vencimento,
                    "valor": debt.valor,
                    "situacao": debt.situacao,
                    "portal_inscricao": debt.portal_inscricao,
                    "portal_cai": debt.portal_cai,
                    "detalhes": debt.detalhes,
                    "fetched_at": utc_now_iso(),
                }
                for debt in debts
            ]
            self.pending_company_debts[company.id] = payload
            return len(payload)
        try:
            self.supabase.table("municipal_tax_debts").delete().eq("company_id", company.id).execute()
        except Exception:
            pass

        payload = [
            {
                "company_id": company.id,
                "ano": debt.ano,
                "tributo": debt.tributo,
                "numero_documento": debt.numero_documento,
                "data_vencimento": debt.data_vencimento,
                "valor": debt.valor,
                "situacao": debt.situacao,
                "portal_inscricao": debt.portal_inscricao,
                "portal_cai": debt.portal_cai,
                "detalhes": debt.detalhes,
                "fetched_at": utc_now_iso(),
            }
            for debt in debts
        ]
        if not payload:
            return 0

        self.supabase.table("municipal_tax_debts").insert(payload).execute()
        verify = self.supabase.table("municipal_tax_debts").select("id").eq("company_id", company.id).execute()
        stored_rows = verify.data or []
        if len(stored_rows) != len(payload):
            raise RuntimeError(
                f"Supabase nao confirmou a gravacao dos debitos. "
                f"Esperado: {len(payload)}, confirmado: {len(stored_rows)}"
            )
        return len(stored_rows)

    def update_debt_guia_pdf_path(
        self,
        company_id: str,
        tributo: str,
        numero_documento: str | None,
        data_vencimento: str | None,
        guia_pdf_path: str,
        parcela: str | None = None,
        ano: int | None = None,
    ) -> bool:
        """Atualiza o caminho do PDF da guia; se o match exato falhar, tenta por ano/parcela/vencimento."""
        if self._is_json_runtime_job():
            candidate_rows = self.pending_company_debts.get(company_id, [])
            parcela_norm = str(parcela or "").strip()
            ano_norm = str(ano or "").strip()
            venc_norm = str(data_vencimento or "").strip()
            numero_norm = " ".join((numero_documento or "").upper().split())
            for row in candidate_rows:
                row_parcela = str(((row.get("detalhes") or {}).get("parcela")) or "").strip()
                row_ano = str(row.get("ano") or "").strip()
                row_venc = str(row.get("data_vencimento") or "").strip()
                row_numero = " ".join((str(row.get("numero_documento") or "")).upper().split())
                row_tributo = str(row.get("tributo") or "")
                if tributo and row_tributo != tributo:
                    continue
                if parcela_norm and row_parcela != parcela_norm:
                    continue
                if ano_norm and row_ano != ano_norm:
                    continue
                if venc_norm and row_venc != venc_norm:
                    continue
                if numero_norm and row_numero and row_numero != numero_norm and not row_numero.endswith(parcela_norm):
                    continue
                row["guia_pdf_path"] = guia_pdf_path
                row["updated_at"] = utc_now_iso()
                return True
            return False
        q = (
            self.supabase.table("municipal_tax_debts")
            .update({"guia_pdf_path": guia_pdf_path, "updated_at": utc_now_iso()})
            .eq("company_id", company_id)
            .eq("tributo", tributo or "")
        )
        if numero_documento is not None:
            q = q.eq("numero_documento", numero_documento)
        if data_vencimento is not None:
            q = q.eq("data_vencimento", data_vencimento)
        q.execute()
        exact_rows = (
            self.supabase.table("municipal_tax_debts")
            .select("id")
            .eq("company_id", company_id)
            .eq("tributo", tributo or "")
            .eq("guia_pdf_path", guia_pdf_path)
            .execute()
        ).data or []
        if exact_rows:
            return True

        candidate_rows = (
            self.supabase.table("municipal_tax_debts")
            .select("id,detalhes,ano,numero_documento,data_vencimento,tributo")
            .eq("company_id", company_id)
            .eq("tributo", tributo or "")
            .execute()
        ).data or []

        parcela_norm = str(parcela or "").strip()
        ano_norm = str(ano or "").strip()
        venc_norm = str(data_vencimento or "").strip()
        numero_norm = " ".join((numero_documento or "").upper().split())

        for row in candidate_rows:
            row_parcela = str(((row.get("detalhes") or {}).get("parcela")) or "").strip()
            row_ano = str(row.get("ano") or "").strip()
            row_venc = str(row.get("data_vencimento") or "").strip()
            row_numero = " ".join((str(row.get("numero_documento") or "")).upper().split())
            if parcela_norm and row_parcela != parcela_norm:
                continue
            if ano_norm and row_ano != ano_norm:
                continue
            if venc_norm and row_venc != venc_norm:
                continue
            if numero_norm and row_numero and row_numero != numero_norm and not row_numero.endswith(parcela_norm):
                continue
            self.supabase.table("municipal_tax_debts").update(
                {"guia_pdf_path": guia_pdf_path, "updated_at": utc_now_iso()}
            ).eq("id", row["id"]).execute()
            return True

        return False

    def claim_execution_request(self, log_callback: Callable[[str], None] | None = None) -> dict[str, Any] | None:
        if not self.ensure_robot_registration():
            return None
        try:
            job = JSON_RUNTIME.load_job()
            if not job:
                return None
            self.current_json_job = job
            robot_row = job.get("robot")
            if isinstance(robot_row, dict):
                self.robot_row = robot_row
            self._reset_pending_result_payload()
            JSON_RUNTIME.write_heartbeat(
                status="processing",
                current_job_id=job.get("job_id"),
                current_execution_request_id=job.get("execution_request_id"),
                message="job_loaded",
            )
            return job
        except Exception as exc:
            if log_callback:
                log_callback(f"[Robô] Erro ao buscar job da fila: {exc}")
            return None

    def complete_execution_request(
        self,
        request_id: str,
        success: bool,
        error_message: str | None = None,
    ) -> None:
        try:
            job = self.current_json_job
            JSON_RUNTIME.write_result(
                job=job if isinstance(job, dict) and str(job.get("execution_request_id") or job.get("id") or "") == str(request_id) else {"execution_request_id": request_id, "job_id": request_id},
                success=success,
                error_message=error_message,
                summary=self._build_result_summary(),
                company_results=self._build_company_results(),
                payload=self._build_result_payload(),
            )
            self.current_json_job = None
            self._reset_pending_result_payload()
        except Exception:
            pass

    async def ensure_browser(self) -> None:
        await self._ensure_browser_resilient()
        return
        chrome_exe = self._resolve_chrome_exe()
        if not chrome_exe.exists():
            raise FileNotFoundError(f"Chrome nao encontrado em: {chrome_exe}")

        self._kill_automation_chrome()
        for _ in range(25):
            try:
                with socket.create_connection(("127.0.0.1", CDP_PORT), timeout=0.3):
                    pass
            except OSError:
                break
            try:
                await asyncio.sleep(0.4)
            except Exception:
                pass
        profile_dir = _get_chrome_profile_dir()
        profile_dir.mkdir(parents=True, exist_ok=True)
        try:
            devtools_port = profile_dir / "DevToolsActivePort"
            if devtools_port.exists():
                devtools_port.unlink()
        except Exception:
            pass

        proxy_url = self.get_current_proxy()
        if proxy_url is None and not self._proxy_list and self._use_proxy_rotation:
            self._start_mitmdump_if_needed()
            proxy_url = self.get_current_proxy()
        # Se temos lista de proxies e rotação ativa, testar antes de usar; trocar até achar um que responda
        if self._proxy_list and self._use_proxy_rotation:
            max_attempts = len(self._proxy_list)
            for _ in range(max_attempts):
                if not proxy_url:
                    break
                if _is_proxy_reachable(proxy_url):
                    break
                self._log("Proxy inacessível, tentando próximo...")
                self.use_next_proxy()
                proxy_url = self.get_current_proxy()
            if proxy_url and not _is_proxy_reachable(proxy_url):
                proxy_url = None
                self._log("Nenhum proxy respondeu; iniciando sem proxy.")
        extension_dir = ensure_skill_up_extension_dir()
        chrome_cmd = [
            str(chrome_exe),
            f"--remote-debugging-port={CDP_PORT}",
            f"--user-data-dir={profile_dir}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-gpu",
            "--disable-popup-blocking",
            "--start-maximized",
            "--ignore-certificate-errors",
        ]
        if extension_dir:
            chrome_cmd.extend(
                [
                    f"--disable-extensions-except={extension_dir}",
                    f"--load-extension={extension_dir}",
                ]
            )
        if proxy_url:
            chrome_cmd.append(f"--proxy-server={proxy_url}")
        CHROME_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(CHROME_LOG_PATH, "w", encoding="utf-8", errors="replace") as chrome_log:
            chrome_log.write(
                "=== Chrome bootstrap (perfil exclusivo do robô) ===\n"
                f"EXE: {chrome_exe}\n"
                f"PROFILE_DIR: {profile_dir}\n"
                f"CDP_PORT: {CDP_PORT}\n"
                f"PROXY: {proxy_url or 'nenhum'}\n"
                f"CMD: {' '.join(chrome_cmd)}\n\n"
            )
            self.chrome_proc = subprocess.Popen(
                chrome_cmd,
                stdout=chrome_log,
                stderr=subprocess.STDOUT,
                cwd=str(chrome_exe.parent),
                shell=False,
            )

        await self._wait_for_cdp()
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.connect_over_cdp(f"http://127.0.0.1:{CDP_PORT}")
        self.context = self.browser.contexts[0] if self.browser.contexts else await self.browser.new_context(
            ignore_https_errors=True,
            viewport={"width": 1440, "height": 960},
        )
        self.page = self.context.pages[0] if self.context.pages else await self.context.new_page()
        await self._ensure_skill_up_extension_ready()
        self.portal_home_page = self.page
        self.page.set_default_timeout(90000)
        self.context.set_default_navigation_timeout(90000)

    async def _ensure_browser_resilient(self) -> None:
        if self.page:
            return
        chrome_exe = self._resolve_chrome_exe()
        if not chrome_exe.exists():
            raise FileNotFoundError(f"Chrome nao encontrado em: {chrome_exe}")

        profile_dir = _get_chrome_profile_dir()
        backup_dir = CHROME_PROFILE_BACKUP_DIR
        self._prepare_working_profile(profile_dir, backup_dir)

        last_error: Exception | None = None
        for attempt in range(2):
            self._kill_automation_chrome()
            for _ in range(25):
                try:
                    with socket.create_connection(("127.0.0.1", CDP_PORT), timeout=0.3):
                        pass
                except OSError:
                    break
                try:
                    await asyncio.sleep(0.4)
                except Exception:
                    pass
            self._cleanup_profile_runtime_files(profile_dir)

            proxy_url = self.get_current_proxy()
            if proxy_url is None and not self._proxy_list and self._use_proxy_rotation:
                self._start_mitmdump_if_needed()
                proxy_url = self.get_current_proxy()
            if self._proxy_list and self._use_proxy_rotation:
                max_attempts = len(self._proxy_list)
                for _ in range(max_attempts):
                    if not proxy_url:
                        break
                    if _is_proxy_reachable(proxy_url):
                        break
                    self._log("Proxy inacessivel, tentando proximo...")
                    self.use_next_proxy()
                    proxy_url = self.get_current_proxy()
                if proxy_url and not _is_proxy_reachable(proxy_url):
                    proxy_url = None
                    self._log("Nenhum proxy respondeu; iniciando sem proxy.")

            chrome_cmd = [
                str(chrome_exe),
                f"--remote-debugging-port={CDP_PORT}",
                f"--user-data-dir={profile_dir}",
                "--profile-directory=Default",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-gpu",
                "--disable-popup-blocking",
                "--start-maximized",
                "--ignore-certificate-errors",
            ]
            if proxy_url:
                chrome_cmd.append(f"--proxy-server={proxy_url}")
            CHROME_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(CHROME_LOG_PATH, "w", encoding="utf-8", errors="replace") as chrome_log:
                chrome_log.write(
                    "=== Chrome bootstrap (perfil exclusivo do robo / modo resiliente) ===\n"
                    f"EXE: {chrome_exe}\n"
                    f"PROFILE_DIR: {profile_dir}\n"
                    f"PROFILE_BACKUP_DIR: {backup_dir}\n"
                    f"CDP_PORT: {CDP_PORT}\n"
                    f"PROXY: {proxy_url or 'nenhum'}\n"
                    "EXTENSION_MODE: backup-profile-only\n"
                    f"TENTATIVA: {attempt + 1}\n"
                    f"CMD: {' '.join(chrome_cmd)}\n\n"
                )
                self.chrome_proc = subprocess.Popen(
                    chrome_cmd,
                    stdout=chrome_log,
                    stderr=subprocess.STDOUT,
                    cwd=str(chrome_exe.parent),
                    shell=False,
                )

            try:
                await self._wait_for_cdp()
                self.playwright = await async_playwright().start()
                self.browser = await self.playwright.chromium.connect_over_cdp(f"http://127.0.0.1:{CDP_PORT}")
                self.context = self.browser.contexts[0] if self.browser.contexts else await self.browser.new_context(
                    ignore_https_errors=True,
                    viewport={"width": 1440, "height": 960},
                )
                self.page = self.context.pages[0] if self.context.pages else await self.context.new_page()
                await self._ensure_skill_up_extension_ready()
                self.portal_home_page = self.page
                self.page.set_default_timeout(90000)
                self.context.set_default_navigation_timeout(90000)
                return
            except Exception as exc:
                last_error = exc
                self._log(f"Aviso: falha ao abrir Chrome com o perfil principal: {exc}")
                try:
                    if self.browser:
                        await self.browser.close()
                except Exception:
                    pass
                self.browser = None
                self.context = None
                self.page = None
                if self.playwright:
                    try:
                        await self.playwright.stop()
                    except Exception:
                        pass
                self.playwright = None
                self._kill_automation_chrome()
                self.chrome_proc = None
                if attempt == 0:
                    self._restore_working_profile(profile_dir, backup_dir)
                    continue
                break
        if last_error:
            raise last_error

    async def close(self) -> None:
        """Desconecta Playwright e fecha o navegador Chrome por completo (incluindo processo)."""
        try:
            if self.browser:
                try:
                    await self.browser.close()
                except Exception:
                    pass
                self.browser = None
            self.context = None
            self.page = None
            if self.playwright:
                try:
                    await self.playwright.stop()
                except Exception:
                    pass
                self.playwright = None
        finally:
            self._kill_automation_chrome()
            self.chrome_proc = None
            self.browser = None
            self.context = None
            self.page = None
            self.playwright = None
            self._stop_mitmdump()
            try:
                await asyncio.sleep(2.5)
            except Exception:
                pass
            self._restore_working_profile(_get_chrome_profile_dir(), CHROME_PROFILE_BACKUP_DIR)

    def _stop_mitmdump(self) -> None:
        """Encerra o processo mitmdump iniciado por este backend."""
        proc = self._mitmdump_proc
        self._mitmdump_proc = None
        self._mitmdump_port = None
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    def _start_mitmdump_if_needed(self) -> None:
        """Se não há lista de proxies e existe mitmdump em data/proxy, inicia mitmdump local e usa como proxy."""
        if self._proxy_list or self._mitmdump_proc is not None:
            return
        mitmdump_exe = PROXY_DIR / "mitmdump.exe"
        if not mitmdump_exe.exists():
            return
        for port in (8889, 8890, 8891, 8892, 8893):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.3):
                    continue
            except OSError:
                pass
            break
        else:
            return
        self._mitmdump_port = port
        log_path = PROXY_DIR / "mitmdump.log"
        try:
            with open(log_path, "a", encoding="utf-8", errors="replace") as log_file:
                log_file.write(f"\n--- mitmdump start {datetime.now(UTC).isoformat()} porta={port}\n")
            with open(log_path, "a", encoding="utf-8", errors="replace") as log_file:
                self._mitmdump_proc = subprocess.Popen(
                    [str(mitmdump_exe), "-p", str(port), "--set", "block_global=false"],
                    cwd=str(PROXY_DIR),
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                )
        except Exception as e:
            self._log(f"Aviso: não foi possível iniciar mitmdump: {e}")
            self._mitmdump_proc = None
            self._mitmdump_port = None
            return
        for _ in range(40):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                    self._log(f"Proxy mitmdump ativo em http://127.0.0.1:{port}")
                    return
            except OSError:
                if self._mitmdump_proc and self._mitmdump_proc.poll() is not None:
                    self._mitmdump_proc = None
                    self._mitmdump_port = None
                    return
                import time
                time.sleep(0.25)
        self._stop_mitmdump()
        self._log("Aviso: mitmdump não respondeu a tempo.")

    def _resolve_chrome_exe(self) -> Path:
        candidates = [
            CHROME_EXE,
            PLAYWRIGHT_DIR / "chromium-1208" / "chrome-win64" / "chrome.exe",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        try:
            for candidate in PLAYWRIGHT_DIR.glob("chromium-*/chrome-win64/chrome.exe"):
                if candidate.exists():
                    return candidate
        except Exception:
            pass
        return CHROME_EXE

    def _resolve_extension_source_profile(self) -> Path | None:
        explicit = (os.getenv("GOIANIA_EXTENSION_SOURCE_PROFILE") or "").strip()
        candidates: list[Path] = []
        if explicit:
            candidates.append(Path(explicit))
        if str(SYSTEM_CHROME_PROFILE_1_DIR):
            candidates.append(SYSTEM_CHROME_PROFILE_1_DIR)
        if str(SYSTEM_CHROME_DEFAULT_PROFILE_DIR):
            candidates.append(SYSTEM_CHROME_DEFAULT_PROFILE_DIR)
        seen: set[str] = set()
        for candidate in candidates:
            try:
                resolved = candidate.resolve()
            except Exception:
                resolved = candidate
            key = str(resolved).lower()
            if not key or key in seen:
                continue
            seen.add(key)
            if self._source_profile_has_captcha_stack(resolved):
                return resolved
        return None

    def _source_profile_has_captcha_stack(self, source_profile_dir: Path) -> bool:
        secure_prefs = source_profile_dir / "Secure Preferences"
        helper_root = source_profile_dir / "Extensions" / HELPER_CAPTCHA_EXTENSION_ID
        if not secure_prefs.exists() or not helper_root.exists():
            return False
        try:
            payload = json.loads(secure_prefs.read_text(encoding="utf-8", errors="ignore"))
            settings = (payload.get("extensions") or {}).get("settings") or {}
            entry = settings.get(HELPER_CAPTCHA_EXTENSION_ID)
            if not isinstance(entry, dict):
                return False
            manifest = entry.get("manifest") or {}
            return "Captcha Solver" in str(manifest.get("name") or "")
        except Exception:
            return False

    def _profile_has_skill_up_installed(self, profile_dir: Path) -> bool:
        secure_prefs = profile_dir / "Default" / "Secure Preferences"
        extension_root = profile_dir / "Default" / "Extensions" / SKILL_UP_EXTENSION_ID
        if not secure_prefs.exists() or not extension_root.exists():
            return False
        try:
            payload = json.loads(secure_prefs.read_text(encoding="utf-8", errors="ignore"))
            settings = (payload.get("extensions") or {}).get("settings") or {}
            entry = settings.get(SKILL_UP_EXTENSION_ID)
            if not isinstance(entry, dict):
                return False
            manifest = entry.get("manifest") or {}
            return "Skill Up" in str(manifest.get("name") or "")
        except Exception:
            return False

    def _inject_skill_up_into_profile(self, target_profile_dir: Path) -> bool:
        target_default = target_profile_dir / "Default"
        secure_prefs_path = target_default / "Secure Preferences"
        preferences_path = target_default / "Preferences"
        default_secure_prefs_path = SYSTEM_CHROME_DEFAULT_PROFILE_DIR / "Secure Preferences"
        skill_up_bundle = ensure_skill_up_extension_dir()
        if skill_up_bundle is None:
            return False
        if not secure_prefs_path.exists() or not default_secure_prefs_path.exists():
            return False
        try:
            skill_up_target = target_default / "Extensions" / SKILL_UP_EXTENSION_ID / "1.4.0_0"
            skill_up_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(skill_up_bundle, skill_up_target, dirs_exist_ok=True)

            manifest_path = skill_up_target / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8", errors="ignore"))
            if not manifest.get("key"):
                manifest["key"] = (
                    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqdo3sqelSaciAVW56eUtqgv0zs7qA+bj//DczBD1Pc9VR2UbtY0iDFJXJKH/4nxQ1rNVmUYZzjmdsAmvdh3UbH3TuVPWV486cHB80u8ON1fwEURRHdQizEkLj39i8B6WUzI48KTXN49ItzN5wGu9ScXH4S78HwgjQ8qFIoqLC1HF+t7OFrU2YilHD7AcfC3d3TDPVq4IMwvdRHjy1tIirSbtQdTAYUlG134R6UhIlEJLvPQ/1eQDDYVJL8r6PP4LtNzU5lnaLm9Ip044xG+/wwV7C1rfIDDXTQtCwwSaetBgiCtqyZdmESJ8dqa+Ps8e/ZY8lK4Gk+rrbcSclCCs2wIDAQAB"
                )
                manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

            default_secure = json.loads(default_secure_prefs_path.read_text(encoding="utf-8", errors="ignore"))
            skill_entry = (((default_secure.get("extensions") or {}).get("settings")) or {}).get(SKILL_UP_EXTENSION_ID)
            if not isinstance(skill_entry, dict):
                return False

            secure_payload = json.loads(secure_prefs_path.read_text(encoding="utf-8", errors="ignore"))
            secure_payload.setdefault("extensions", {}).setdefault("settings", {})[SKILL_UP_EXTENSION_ID] = skill_entry
            secure_prefs_path.write_text(json.dumps(secure_payload, ensure_ascii=False), encoding="utf-8")

            if preferences_path.exists():
                prefs_payload = json.loads(preferences_path.read_text(encoding="utf-8", errors="ignore"))
                prefs_payload.setdefault("extensions", {}).setdefault("ui", {}).setdefault("developer_mode", True)
                preferences_path.write_text(json.dumps(prefs_payload, ensure_ascii=False), encoding="utf-8")

            for folder_name in ["Local Extension Settings", "Managed Extension Settings", "Sync Extension Settings"]:
                source_dir = SYSTEM_CHROME_DEFAULT_PROFILE_DIR / folder_name / SKILL_UP_EXTENSION_ID
                if not source_dir.exists():
                    continue
                target_dir = target_default / folder_name / SKILL_UP_EXTENSION_ID
                target_dir.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(source_dir, target_dir, dirs_exist_ok=True)
            return True
        except Exception as exc:
            self._log(f"Aviso: falha ao injetar Skill Up no perfil Chrome do robô: {exc}")
            return False

    def _sync_extension_profile_from_source(self, target_profile_dir: Path) -> bool:
        source_profile_dir = self._resolve_extension_source_profile()
        if source_profile_dir is None:
            return False
        source_user_data_dir = source_profile_dir.parent
        temp_dir = target_profile_dir.with_name(f"{target_profile_dir.name}_seed_tmp")
        shutil.rmtree(temp_dir, ignore_errors=True)
        (temp_dir / "Default").mkdir(parents=True, exist_ok=True)
        copy_items = [
            ("Local State", False),
            ("Preferences", True),
            ("Secure Preferences", True),
            ("Extensions", True),
            ("Local Extension Settings", True),
            ("Managed Extension Settings", True),
            ("Sync Extension Settings", True),
            ("Extension Rules", True),
            ("Extension Scripts", True),
            ("Extension State", True),
            ("Service Worker", True),
        ]
        try:
            for relative_name, in_default in copy_items:
                source_path = (source_profile_dir / relative_name) if in_default else (source_user_data_dir / relative_name)
                target_path = (temp_dir / "Default" / relative_name) if in_default else (temp_dir / relative_name)
                if not source_path.exists():
                    continue
                if source_path.is_dir():
                    shutil.copytree(source_path, target_path, dirs_exist_ok=True)
                else:
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source_path, target_path)

            if not self._inject_skill_up_into_profile(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
                return False

            if not self._profile_has_skill_up_installed(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
                return False

            if target_profile_dir.exists():
                try:
                    old_dir = target_profile_dir.with_name(
                        f"{target_profile_dir.name}_seed_old_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    )
                    target_profile_dir.replace(old_dir)
                except Exception:
                    shutil.rmtree(target_profile_dir, ignore_errors=True)
            temp_dir.replace(target_profile_dir)
            self._cleanup_profile_runtime_files(target_profile_dir)
            self._log("Perfil Chrome do robô atualizado a partir das extensões do Chrome local.")
            return True
        except Exception as exc:
            self._log(f"Aviso: falha ao sincronizar perfil Chrome com extensões locais: {exc}")
            shutil.rmtree(temp_dir, ignore_errors=True)
            return False

    def _prepare_extension_enabled_profile(self, profile_dir: Path, backup_dir: Path) -> bool:
        if self._profile_has_skill_up_installed(profile_dir):
            self._extension_profile_seeded = True
            return True
        if self._profile_has_skill_up_installed(backup_dir) and self._restore_profile_from_backup(profile_dir, backup_dir):
            self._extension_profile_seeded = True
            return True
        if self._sync_extension_profile_from_source(profile_dir):
            self._extension_profile_seeded = True
            try:
                self._copy_profile_tree(profile_dir, backup_dir)
            except Exception:
                pass
            return True
        self._extension_profile_seeded = False
        return False

    def _remove_tree_with_retries(self, target: Path, *, max_attempts: int = 6) -> None:
        if not target.exists():
            return
        for attempt in range(max_attempts):
            try:
                shutil.rmtree(target, ignore_errors=False)
            except FileNotFoundError:
                return
            except Exception:
                time.sleep(1.0 + attempt * 0.5)
            if not target.exists():
                return
        shutil.rmtree(target, ignore_errors=True)
        if target.exists():
            raise RuntimeError(f"Nao foi possivel remover completamente o perfil Chrome em {target}")

    def _ensure_profile_backup(self, profile_dir: Path, backup_dir: Path) -> None:
        profile_dir.parent.mkdir(parents=True, exist_ok=True)
        backup_dir.parent.mkdir(parents=True, exist_ok=True)
        if not profile_dir.exists():
            profile_dir.mkdir(parents=True, exist_ok=True)
            self._log(f"Pasta do perfil Chrome criada em {profile_dir}.")
        if self._is_profile_healthy(backup_dir) and self._profile_has_skill_up_installed(backup_dir):
            return
        if self._is_profile_healthy(profile_dir) and self._profile_has_skill_up_installed(profile_dir):
            self._log("Criando backup estavel do perfil Chrome preparado do robo.")
            self._copy_profile_tree(profile_dir, backup_dir)
            self._cleanup_profile_runtime_files(backup_dir)
            return
        if self._sync_extension_profile_from_source(backup_dir):
            self._cleanup_profile_runtime_files(backup_dir)
            self._log("Backup estavel do perfil Chrome criado a partir do perfil preparado do Chrome local.")
            return
        raise RuntimeError(
            "Perfil Chrome preparado do robo nao encontrado. "
            "Prepare data/chrome_profile com a extensao/configuracao necessaria antes de executar."
        )

    def _prepare_working_profile(self, profile_dir: Path, backup_dir: Path) -> None:
        self._ensure_profile_backup(profile_dir, backup_dir)
        self._kill_automation_chrome()
        self._remove_tree_with_retries(profile_dir)
        profile_dir.parent.mkdir(parents=True, exist_ok=True)
        self._copy_profile_tree(backup_dir, profile_dir)
        self._cleanup_profile_runtime_files(profile_dir)
        if not self._profile_has_skill_up_installed(profile_dir):
            raise RuntimeError("O perfil de trabalho nao contem a extensao Skill Up preparada.")
        self._extension_profile_seeded = True
        self._working_profile_prepared = True
        self._log("Perfil Chrome de trabalho recriado a partir do backup estavel.")

    def _restore_working_profile(self, profile_dir: Path, backup_dir: Path) -> None:
        try:
            self._ensure_profile_backup(profile_dir, backup_dir)
        except Exception as exc:
            self._log(f"Aviso: backup do perfil Chrome indisponivel para restauracao: {exc}")
            return
        try:
            self._remove_tree_with_retries(profile_dir)
            profile_dir.parent.mkdir(parents=True, exist_ok=True)
            self._copy_profile_tree(backup_dir, profile_dir)
            self._cleanup_profile_runtime_files(profile_dir)
            self._working_profile_prepared = False
            self._log("Perfil Chrome de trabalho descartado e restaurado a partir do backup estavel.")
        except Exception as exc:
            self._log(f"Aviso: falha ao restaurar o perfil Chrome de trabalho: {exc}")

    def _cleanup_profile_runtime_files(self, profile_dir: Path) -> None:
        runtime_entries = [
            "DevToolsActivePort",
            "SingletonCookie",
            "SingletonLock",
            "SingletonSocket",
            "lockfile",
            "chrome_debug.log",
        ]
        for name in runtime_entries:
            try:
                target = profile_dir / name
                if target.exists():
                    target.unlink()
            except Exception:
                pass

    def _is_profile_healthy(self, profile_dir: Path) -> bool:
        required = [
            profile_dir / "Local State",
            profile_dir / "Default" / "Preferences",
            profile_dir / "Default" / "Secure Preferences",
        ]
        return all(path.exists() for path in required)

    def _copy_profile_tree(self, source: Path, target: Path) -> None:
        ignore_names = {
            "Cache",
            "Code Cache",
            "GPUCache",
            "GrShaderCache",
            "GraphiteDawnCache",
            "DawnGraphiteCache",
            "DawnWebGPUCache",
            "ShaderCache",
            "Crashpad",
            "BrowserMetrics",
            "BrowserMetrics-spare.pma",
            "DevToolsActivePort",
            "SingletonCookie",
            "SingletonLock",
            "SingletonSocket",
            "lockfile",
            "chrome_debug.log",
            "LOCK",
            "Cookies",
            "Cookies-journal",
            "Network Persistent State",
            "Session Storage",
            "Sessions",
            "Local Storage",
            "shared_proto_db",
            "Site Characteristics Database",
            "Sync Data",
            "Safe Browsing Network",
        }

        def _ignore(_dir: str, names: list[str]) -> set[str]:
            return {name for name in names if name in ignore_names}

        if target.exists():
            shutil.rmtree(target, ignore_errors=True)
        shutil.copytree(source, target, ignore=_ignore)

    def _restore_profile_from_backup(self, profile_dir: Path, backup_dir: Path) -> bool:
        if not self._is_profile_healthy(backup_dir):
            return False
        try:
            broken_dir = profile_dir.with_name(f"{profile_dir.name}_broken_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            if profile_dir.exists():
                try:
                    profile_dir.replace(broken_dir)
                except Exception:
                    shutil.rmtree(profile_dir, ignore_errors=True)
            self._copy_profile_tree(backup_dir, profile_dir)
            self._cleanup_profile_runtime_files(profile_dir)
            self._log("Perfil Chrome restaurado a partir do backup estavel.")
            return True
        except Exception as exc:
            self._log(f"Aviso: falha ao restaurar backup do perfil Chrome: {exc}")
            return False

    def _snapshot_profile_backup(self, profile_dir: Path, backup_dir: Path) -> None:
        """Atualiza o backup do perfil apenas quando seguro (Chrome já fechado). Falhas são silenciosas."""
        if not self._is_profile_healthy(profile_dir):
            return
        for attempt in range(3):
            try:
                self._copy_profile_tree(profile_dir, backup_dir)
                return
            except Exception:
                if attempt < 2:
                    time.sleep(1.5)
        return

    async def _wait_for_cdp(self, timeout_seconds: int = 60) -> None:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while asyncio.get_running_loop().time() < deadline:
            if self.chrome_proc and self.chrome_proc.poll() not in (None, 0):
                raise RuntimeError(
                    f"Chrome finalizou com codigo {self.chrome_proc.returncode}. Consulte o log em {CHROME_LOG_PATH}"
                )
            try:
                with socket.create_connection(("127.0.0.1", CDP_PORT), timeout=1):
                    return
            except OSError:
                await asyncio.sleep(0.5)
        raise RuntimeError(
            f"Nao foi possivel conectar ao Chrome via CDP na porta {CDP_PORT} em {timeout_seconds}s. "
            f"Verifique se outro Chrome usa a porta ou consulte {CHROME_LOG_PATH}"
        )

    def _kill_automation_chrome(self) -> None:
        proc = self.chrome_proc
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        if os.name != "nt":
            return
        try:
            chrome_exe = os.path.normcase(str(self._resolve_chrome_exe().resolve()))
            profile_dir = os.path.normcase(str(CHROME_PROFILE_DIR.resolve()))
            result = subprocess.run(
                ["wmic", "process", "where", "name='chrome.exe'", "get", "ProcessId,ExecutablePath,CommandLine", "/format:csv"],
                capture_output=True,
                text=True,
                check=False,
            )
            pids: list[str] = []
            for line in (result.stdout or "").splitlines():
                parts = [part.strip() for part in line.split(",")]
                if len(parts) < 4:
                    continue
                pid = parts[-1]
                command_line = ",".join(parts[:-1]).lower()
                if chrome_exe.lower() in command_line or profile_dir.lower() in command_line:
                    if pid.isdigit():
                        pids.append(pid)
            for pid in pids:
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", pid],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=8,
                )
        except Exception:
            pass

    async def ensure_login(self, log: Callable[[str], None] | None = None, selected_login_cpf: str | None = None) -> None:
        await self._ensure_browser_resilient()
        assert self.page is not None
        login_cpf, login_password = self.get_dashboard_portal_credentials(selected_login_cpf)

        if self.page.url.startswith(HOME_URL):
            self.portal_home_page = self.page
            return

        max_attempts = 2
        for attempt in range(1, max_attempts + 1):
            if attempt > 1 and log:
                log("Segunda tentativa de login no portal...")
            await self.page.goto(LOGIN_URL, wait_until="domcontentloaded")
            if log and attempt == 1:
                log("Abrindo login do portal da Prefeitura de Goiânia.")

            if self.page.url.startswith(INTERNET_HOME_URL):
                await self._open_portal_do_contribuinte()

            if await self._is_login_form_visible():
                await self._submit_login_form(login_cpf, login_password)
                await self._wait_after_login()
                if self.page.url.startswith(INTERNET_HOME_URL):
                    await self._open_portal_do_contribuinte()

            if "Intranet/Login.aspx" in self.page.url:
                if log:
                    log("Fazendo login na Intranet (portal servicos)...")
                if await self._is_login_form_visible():
                    await self._submit_login_form(login_cpf, login_password)
                    await self._wait_after_login()
                else:
                    await self.page.goto(
                        f"{INTRANET_LOGIN_URL}?OriginalURL={quote(HOME_URL)}",
                        wait_until="domcontentloaded",
                    )
                    await self.page.wait_for_load_state("networkidle")
                    if await self._is_login_form_visible():
                        await self._submit_login_form(login_cpf, login_password)
                        await self._wait_after_login()

            if self.page.url.startswith(INTERNET_HOME_URL):
                await self._open_portal_do_contribuinte()

            if self.page.url.startswith(HOME_URL):
                self.portal_home_page = self.page
                return
            await asyncio.sleep(2)

        raise RuntimeError(f"Login não concluído no portal após {max_attempts} tentativa(s). URL atual: {self.page.url}")

    async def ensure_company_selection_screen(self) -> None:
        assert self.page is not None
        nav_timeout = 20000
        if self.portal_home_page:
            try:
                if not self.portal_home_page.is_closed():
                    self.page = self.portal_home_page
            except Exception:
                pass
        if self.page.url.startswith(HOME_URL):
            try:
                await self.page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            return
        await self.page.goto(HOME_URL, wait_until="domcontentloaded", timeout=nav_timeout)
        try:
            await self.page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        self.portal_home_page = self.page

    async def _open_portal_do_contribuinte(self) -> None:
        assert self.page is not None
        link = self.page.get_by_role("link", name=re.compile("Portal do Contribuinte", re.I))
        if await link.count():
            await link.first.click()
            await self.page.wait_for_load_state("networkidle")

    async def _is_login_form_visible(self) -> bool:
        assert self.page is not None
        for locator in [
            self.page.get_by_placeholder("Informe o CPF"),
            self.page.locator("input[placeholder='Informe o CPF']"),
            self.page.locator("input[id*='wtLoginInput']").first,
            self.page.locator("input[id*='txt'], input[name*='cpf'], input[name*='usuario']").first,
            self.page.locator("input[type='password']").first,
        ]:
            try:
                target = locator.first
                if await target.count() and await target.is_visible():
                    return True
            except Exception:
                continue
        return False

    async def _submit_login_form(self, login_cpf: str, login_password: str) -> None:
        assert self.page is not None
        cpf_selectors = (
            "input[placeholder='Informe o CPF'], input[id*='wtLoginInput'], "
            "input[id*='txtCPF'], input[id*='txtUsuario'], input[name*='cpf'], input[name*='usuario']"
        )
        password_selectors = (
            "input[type='password'], input[id*='wtPasswordInput'], "
            "input[id*='txtSenha'], input[id*='txtPassword'], input[name*='senha'], input[name*='password']"
        )
        cpf_input = self.page.locator(cpf_selectors).first
        password_input = self.page.locator(password_selectors).first
        if await cpf_input.count() == 0 or await password_input.count() == 0:
            raise RuntimeError("Campos de CPF ou senha não encontrados na tela de login.")
        await cpf_input.fill(login_cpf)
        await password_input.fill(login_password)
        keep_connected = self.page.get_by_label("Mantenha-me Conectado")
        try:
            if await keep_connected.count():
                await keep_connected.check()
        except Exception:
            pass
        for locator in [
            self.page.get_by_role("button", name=re.compile("Entrar", re.I)),
            self.page.locator("button:has-text('ENTRAR')").first,
            self.page.locator("input[type='submit'][value*='ntrar'], input[type='submit']").first,
            self.page.locator("a:has-text('Entrar'), input[id*='btnEntrar']").first,
        ]:
            try:
                target = locator.first
                if await target.count() and await target.is_visible():
                    await target.click()
                    return
            except Exception:
                continue
        raise RuntimeError("Botão Entrar não encontrado na tela de login.")

    async def _wait_after_login(self) -> None:
        """Aguarda redirecionamento após submit do login (especialmente na Intranet)."""
        assert self.page is not None
        try:
            await self.page.wait_for_url(
                lambda url: url.startswith(HOME_URL) or url.startswith(INTERNET_HOME_URL),
                timeout=LOGIN_NAVIGATION_TIMEOUT_MS,
            )
        except Exception:
            await self.page.wait_for_load_state("networkidle")
            await asyncio.sleep(2)

    async def collect_company_debts(
        self,
        company: CompanyItem,
        stop_cb: callable,
        log: callable,
        status_cb: callable,
    ) -> list[DebtRow]:
        self._skip_iss = self._get_skip_iss_config()
        await self.ensure_login(log, company.selected_login_cpf)
        await self.ensure_company_selection_screen()
        stop_cb()
        status_cb("EXECUTANDO", "Selecionando empresa no PERFIL")
        await self.select_company(company)
        stop_cb()
        status_cb("EXECUTANDO", "Abrindo Pessoa Jurídica - Atividade Econômica > Taxas e Impostos")
        frame = await self.open_duam(company)
        stop_cb()
        status_cb("AGUARDANDO RECAPTCHA", "Tentando clicar no reCAPTCHA; conclua manualmente se o site exigir")
        try:
            await self.try_click_recaptcha()
            await self.wait_recaptcha_manual(stop_cb)
        except RecaptchaTimeoutError:
            await self._close_debts_modal_and_return_home(frame, log)
            raise
        stop_cb()
        status_cb("EXECUTANDO", "Consultando débitos municipais")
        debts = await self.extract_debts(frame, company, stop_cb, log=log)
        log(f"{company.name}: {len(debts)} débito(s) capturado(s) e enviados ao Supabase.")
        if debts:
            status_cb("EXECUTANDO", "Baixando PDFs das guias (um por débito)")
            await self._download_guia_pdfs_batch(frame, company, debts, log, status_cb, stop_cb)
        await self._close_debts_modal_and_return_home(frame, log)
        return debts

    async def select_company(self, company: CompanyItem) -> None:
        assert self.page is not None
        for attempt in range(2):
            select = self.page.locator("select").first
            try:
                await select.wait_for(state="visible", timeout=20000)
            except Exception:
                if attempt == 0 and self.page.url.startswith(HOME_URL):
                    await self.page.reload(wait_until="domcontentloaded", timeout=15000)
                    await asyncio.sleep(2)
                    continue
                raise
            options = await select.locator("option").evaluate_all(
                "(nodes) => nodes.map((node) => ({ value: node.value, text: (node.textContent || '').trim() }))"
            )
            target_value = None
            cae_digits = digits(company.cae) if company.cae else ""
            if cae_digits:
                for option in options:
                    opt_text = option["text"] or ""
                    if f"CAE : {cae_digits}" in opt_text or (company.cae and company.cae in opt_text):
                        target_value = option["value"]
                        break
            if not target_value:
                company_name = normalize_name(company.name)
                company_document = digits(company.document)
                company_name_short = company_name.replace(" LTDA", "").replace(" ME", "").replace(" EPP", "").strip() if company_name else ""
                for option in options:
                    opt_text = option["text"]
                    option_text = normalize_name(opt_text)
                    opt_doc = digits(opt_text)
                    if (
                        (company_name and (company_name in option_text or option_text in company_name))
                        or (company_name_short and company_name_short in option_text)
                        or (company_document and (company_document in opt_doc or opt_doc in company_document))
                    ):
                        target_value = option["value"]
                        break
            if not target_value:
                raise RuntimeError(f"Empresa não localizada no seletor PERFIL: {company.name}")
            break
        await select.select_option(target_value)
        try:
            await self.page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            pass
        await self.wait_notification_modal()

    async def wait_notification_modal(self) -> None:
        assert self.page is not None
        modal = self.page.locator("text=/Notificação Fiscalização/i, text=/Notificacao Fiscalizacao/i").first
        try:
            await modal.wait_for(timeout=3000)
            await modal.wait_for(state="hidden", timeout=12000)
        except Exception:
            return

    async def _close_debts_modal_and_return_home(
        self, frame: Frame | None, log: Callable[[str], None] | None = None
    ) -> None:
        """Fecha o modal de débitos (X do diálogo OutSystems/RichWidgets) e volta à tela inicial.
        O modal fica na página principal (não no iframe); clicar no X fecha o diálogo de fato.
        """
        assert self.page is not None
        click_timeout = 5000
        nav_timeout = 8000
        try:
            page = self.page
            # Priorizar a PÁGINA para o botão X: o diálogo os-internal-ui-dialog está no DOM da página,
            # não dentro do iframe. Se tentarmos no frame primeiro, podemos acionar "Voltar" dentro do iframe.
            targets: list[tuple[Frame | Page, str]] = [(page, "page")]
            if frame and frame != page.main_frame:
                targets.append((frame, "frame"))

            close_candidates = [
                # Botão X do diálogo OutSystems/RichWidgets (Portal do Contribuinte)
                "a.os-internal-ui-dialog-titlebar-close-no-title",
                ".os-internal-ui-dialog-titlebar-close-no-title",
                "[class*='os-internal-ui-dialog'] a[role='button']",
                ".os-internal-ui-dialog .os-internal-ui-dialog-titlebar a[href='#']",
                ("xpath=/html/body/div[3]/div[1]/a", "xpath"),
                "button[class*='close'], a[class*='close']",
                "[class*='modal'] button[class*='close'], [class*='modal'] a[class*='close']",
                "[class*='Modal'] button, [class*='Modal'] a[href='#']",
                ".modal-header button, .modal-header a[href='#']",
                "button[title*='Fechar'], [aria-label*='echar']",
                # Evitar "Voltar" dentro do iframe (só fecha de fato o X do diálogo)
                "button:has-text('Voltar'), a:has-text('Voltar')",
                "a:has-text('Consultar Tributos')",
            ]
            clicked = False
            for ctx, _ in targets:
                locator_ctx = frame if ctx == frame else page
                for sel in close_candidates:
                    selector = sel[0] if isinstance(sel, tuple) else sel
                    try:
                        loc = locator_ctx.locator(selector).first
                        if await loc.count() == 0:
                            continue
                        if await loc.is_visible():
                            await loc.click(timeout=click_timeout)
                            if log:
                                log("Modal fechado; voltando à tela inicial.")
                            clicked = True
                            await asyncio.sleep(0.25)
                            break
                    except Exception:
                        continue
                if clicked:
                    break

            if self.portal_home_page and not self.portal_home_page.is_closed():
                if self.page != self.portal_home_page:
                    try:
                        await self.page.close()
                    except Exception:
                        pass
                self.page = self.portal_home_page
            await self.page.wait_for_load_state("domcontentloaded", timeout=nav_timeout)
            try:
                await self.page.wait_for_load_state("networkidle", timeout=3000)
            except Exception:
                pass
            if not self.page.url.startswith(HOME_URL):
                await self.page.goto(HOME_URL, wait_until="domcontentloaded", timeout=nav_timeout)
                await asyncio.sleep(0.25)
                try:
                    await self.page.wait_for_load_state("networkidle", timeout=3000)
                except Exception:
                    pass
        except Exception as e:
            if log:
                log(f"Aviso ao fechar modal: {e}")
            if self.portal_home_page and not self.portal_home_page.is_closed():
                self.page = self.portal_home_page

    async def open_duam(self, company: CompanyItem) -> Frame:
        assert self.page is not None
        origin_page = self.page
        await self._ensure_activity_context()

        popup = await self._click_tax_tile()
        if popup:
            self.portal_home_page = origin_page
            self.page = popup
            await self.page.wait_for_load_state("networkidle")

        for _ in range(30):
            frame = self._find_tributos_frame(self.page)
            if frame:
                body = await frame.locator("body").inner_text()
                if "DUAM" in body or "Consulta e Emissão de Guia" in body or "Consulta e Emissao de Guia" in body:
                    return frame
            if DEBITOS_URL_PART.lower() in self.page.url.lower():
                return self.page.main_frame
            await asyncio.sleep(1)

        details = await self._debug_taxas_state(company)
        raise RuntimeError(
            "Não foi possível abrir a tela de Taxas e Impostos. "
            f"URL atual: {self.page.url}. Diagnóstico: {details}"
        )

    async def _ensure_activity_context(self) -> None:
        assert self.page is not None
        context_candidates = [
            self.page.get_by_text(re.compile(r"Pessoa Jur.*Atividade Econ", re.I)).first,
            self.page.locator("a[href*='MostraDebitos.aspx']").filter(
                has_text=re.compile("Taxas e Impostos", re.I)
            ).first,
            self.page.locator(".Tabs__content.active a[href*='MostraDebitos.aspx']").filter(
                has_text=re.compile("Taxas e Impostos", re.I)
            ).first,
            self.page.locator(".PH.Tabs__content.active a[href*='MostraDebitos.aspx']").filter(
                has_text=re.compile("Taxas e Impostos", re.I)
            ).first,
        ]
        for _ in range(10):
            for locator in context_candidates:
                try:
                    if await locator.count() and await locator.is_visible():
                        return
                except Exception:
                    continue
            await asyncio.sleep(1)

        visible_groups = await self.page.evaluate(
            """() =>
                Array.from(document.querySelectorAll('div, span, a'))
                    .map((el) => ({
                        text: (el.textContent || '').replace(/\\s+/g, ' ').trim(),
                        id: el.id || '',
                        visible: !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length),
                    }))
                    .filter((item) => item.visible && (
                        /Pessoa Jur/i.test(item.text) ||
                        /Atividade Econ/i.test(item.text) ||
                        /Financeiro/i.test(item.text) ||
                        /Taxas e Impostos/i.test(item.text)
                    ))
                    .slice(0, 20)
            """
        )
        raise RuntimeError(
            "O contexto de Pessoa Juridica - Atividade Economica nao ficou disponivel apos selecionar a empresa. "
            f"Elementos visiveis: {visible_groups}"
        )

    async def _click_tax_tile(self) -> Page | None:
        assert self.page is not None
        popup_task = asyncio.create_task(self.page.wait_for_event("popup", timeout=5000))
        candidates = [
            self.page.locator("a[href*='MostraDebitos.aspx']").filter(
                has_text=re.compile("Taxas e Impostos", re.I)
            ),
            self.page.locator(".Tabs__content.active a[href*='MostraDebitos.aspx']").filter(
                has_text=re.compile("Taxas e Impostos", re.I)
            ),
            self.page.locator(".PH.Tabs__content.active a[href*='MostraDebitos.aspx']").filter(
                has_text=re.compile("Taxas e Impostos", re.I)
            ),
        ]
        clicked = False
        for locator in candidates:
            try:
                target = locator.first
                if await locator.count() == 0:
                    continue
                await target.wait_for(state="visible", timeout=3000)
                await target.click(force=True, timeout=3000)
                clicked = True
                break
            except Exception:
                continue

        if not clicked:
            popup_task.cancel()
            active_links = await self.page.evaluate(
                """() =>
                    Array.from(
                        document.querySelectorAll(
                            '.Tabs__content.active a, .PH.Tabs__content.active a'
                        )
                    )
                        .map((el) => ({
                            id: el.id || '',
                            text: (el.textContent || '').replace(/\\s+/g, ' ').trim(),
                            href: el.getAttribute('href') || '',
                            visible: !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length),
                        }))
                        .filter((item) => item.visible)
                        .slice(0, 20)
                """
            )
            raise RuntimeError(
                "Tile de Taxas e Impostos do bloco Financeiro nÃ£o encontrado na aba ativa. "
                f"Links visÃ­veis: {active_links}"
            )
            raise RuntimeError("Tile de Taxas e Impostos não encontrado.")

        await asyncio.sleep(2)
        if popup_task.done():
            try:
                popup = popup_task.result()
            except Exception:
                popup = None
            if popup:
                return popup
        popup_task.cancel()
        return None

    def _find_tributos_frame(self, current_page: Page) -> Frame | None:
        for frame in current_page.frames:
            if PORTAL_TRIBUTOS_URL_PART.lower() in frame.url.lower():
                return frame
        return None

    async def _debug_taxas_state(self, company: CompanyItem) -> dict[str, Any]:
        assert self.page is not None
        return await self.page.evaluate(
            """(companyName) => ({
                companyName,
                url: window.location.href,
                frames: window.frames.length,
                visibleTaxItems: Array.from(document.querySelectorAll('span,div,a'))
                    .map((el) => ({
                        id: el.id,
                        text: (el.textContent || '').replace(/\\s+/g, ' ').trim(),
                        width: el.getBoundingClientRect().width,
                        height: el.getBoundingClientRect().height
                    }))
                    .filter((item) => /Taxas e Impostos/i.test(item.text) && (item.width || item.height))
                    .slice(-5)
            })""",
            company.name,
        )

    async def _wait_recaptcha_frame(self, timeout_seconds: float = 15.0) -> bool:
        """Aguarda o iframe do reCAPTCHA aparecer na página. Retorna True se encontrado."""
        assert self.page is not None
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while asyncio.get_running_loop().time() < deadline:
            for frame in self.page.frames:
                if "recaptcha/api2/anchor" in frame.url:
                    try:
                        anchor = frame.locator("#recaptcha-anchor")
                        if await anchor.count() > 0:
                            return True
                    except Exception:
                        pass
            await asyncio.sleep(0.5)
        return False

    async def try_click_recaptcha(self) -> None:
        """Aguarda o reCAPTCHA aparecer, clica no anchor uma vez; o usuário resolve e wait_recaptcha_manual verifica a cada segundo."""
        assert self.page is not None
        if not await self._wait_recaptcha_frame(timeout_seconds=15.0):
            return
        for _ in range(5):
            for frame in self.page.frames:
                if "recaptcha/api2/anchor" not in frame.url:
                    continue
                try:
                    anchor = frame.locator("#recaptcha-anchor")
                    if await anchor.count() == 0:
                        continue
                    checked = await anchor.get_attribute("aria-checked")
                    if checked == "true":
                        return
                    await anchor.click(force=True, timeout=5000)
                    return
                except Exception:
                    continue
            await asyncio.sleep(1.0)

    async def wait_recaptcha_manual(self, stop_cb: callable) -> None:
        """Aguarda até 2 minutos pelo reCAPTCHA ser resolvido; verifica a cada segundo. Se não resolver, lança RecaptchaTimeoutError."""
        assert self.page is not None
        for _ in range(120):
            stop_cb()
            for frame in self.page.frames:
                if "recaptcha/api2/anchor" in frame.url:
                    checked = await frame.locator("#recaptcha-anchor").get_attribute("aria-checked")
                    if checked == "true":
                        return
            await asyncio.sleep(1)
        raise RecaptchaTimeoutError("reCAPTCHA não foi concluído em 2 minutos.")

    async def extract_debts(
        self,
        frame: Frame,
        company: CompanyItem,
        stop_cb: callable,
        log: Callable[[str], None] | None = None,
    ) -> list[DebtRow]:
        if frame != self.page.main_frame:
            await frame.get_by_role("button", name=re.compile("Consultar", re.I)).click()

        for _ in range(90):
            stop_cb()
            body = await frame.locator("body").inner_text()
            if "Nenhum débito" in body or "Nenhum debito" in body:
                return []
            if "Não foram encontradas guias" in body or "nao foram encontradas guias" in body.lower():
                if log:
                    log(f"{company.name}: nenhuma guia encontrada para os dados informados; indo para a próxima empresa.")
                return []
            if await frame.locator("table").count():
                break
            await asyncio.sleep(1)

        table = frame.locator("table").filter(
            has_text=re.compile("Vencimento|Documento|Tributo|Situação|Situacao|Rubrica|Processo", re.I)
        ).first
        await table.wait_for(timeout=20000)

        summary_info = {"inscricao": None, "nome": None, "validade": None}
        try:
            summary_info = await frame.evaluate(
                """() => {
                    const root = document.querySelector("main") || document.body;
                    const nodes = Array.from(root.querySelectorAll("div, span, label, p"));
                    const readValue = (label) => {
                        const labelNode = nodes.find((node) => (node.textContent || "").trim() === label);
                        if (!labelNode) return null;
                        let valueNode = labelNode.nextElementSibling;
                        while (valueNode && !(valueNode.textContent || "").trim()) {
                            valueNode = valueNode.nextElementSibling;
                        }
                        return valueNode ? (valueNode.textContent || "").trim() : null;
                    };
                    return {
                        inscricao: readValue("Inscrição"),
                        nome: readValue("Nome"),
                        validade: readValue("Validade"),
                    };
                }"""
            )
        except Exception:
            pass

        headers = [text.strip().lower() for text in await table.locator("thead th, tr th").all_text_contents()]
        rows = await table.locator("tbody tr, tr").all()
        debts: list[DebtRow] = []
        for row in rows:
            cells = [text.strip() for text in await row.locator("td").all_text_contents()]
            if not cells:
                continue
            mapped: dict[str, Any] = {
                "ano": None,
                "tributo": None,
                "numero_documento": None,
                "data_vencimento": None,
                "valor": None,
                "valor_original": None,
                "parcela": None,
                "situacao": None,
                "inscricao": None,
                "cai": None,
            }
            for index, cell in enumerate(cells):
                header = headers[index] if index < len(headers) else ""
                if "ano" in header:
                    mapped["ano"] = cell
                elif "rubrica" in header or "tribut" in header:
                    mapped["tributo"] = cell
                elif "document" in header or "processo" in header or "número" in header or "numero" in header:
                    mapped["numero_documento"] = cell
                elif "parcela" in header:
                    mapped["parcela"] = cell
                elif "venc" in header:
                    mapped["data_vencimento"] = cell
                elif "débito original" in header or "debito original" in header:
                    mapped["valor_original"] = cell
                elif "total" in header or ("valor" in header and not mapped["valor"]):
                    mapped["valor"] = cell
                elif "situ" in header:
                    mapped["situacao"] = cell
                elif "inscri" in header:
                    mapped["inscricao"] = cell
                elif "cai" in header:
                    mapped["cai"] = cell
            if not mapped["tributo"] and len(cells) >= 2:
                mapped["tributo"] = cells[1]
            if not mapped["parcela"] and len(cells) >= 4:
                mapped["parcela"] = cells[3]
            if not mapped["valor_original"] and len(cells) >= 7:
                mapped["valor_original"] = cells[-2]
            if not mapped["valor"] and len(cells) >= 8:
                mapped["valor"] = cells[-1]
            if not mapped["numero_documento"] and mapped["parcela"]:
                mapped["numero_documento"] = f"Parcela {mapped['parcela']}"
            if not mapped["numero_documento"] and len(cells) >= 3:
                mapped["numero_documento"] = cells[2]
            if not mapped["tributo"] and not mapped["numero_documento"]:
                continue
            parsed_total = parse_money(mapped["valor"])
            parsed_original = parse_money(mapped["valor_original"])
            debt = DebtRow(
                ano=int(mapped["ano"]) if str(mapped["ano"]).isdigit() else None,
                tributo=mapped["tributo"] or "Tributo não identificado",
                numero_documento=mapped["numero_documento"] or f"{company.id}-{len(debts) + 1}",
                data_vencimento=parse_date(mapped["data_vencimento"]),
                valor=parsed_total or parsed_original,
                situacao=mapped["situacao"],
                portal_inscricao=mapped["inscricao"] or summary_info.get("inscricao"),
                portal_cai=mapped["cai"] or digits(company.document),
                detalhes={
                    "linha_portal": mapped,
                    "parcela": mapped["parcela"],
                    "valor_original": parsed_original,
                    "valor_total": parsed_total,
                    "nome_portal": summary_info.get("nome"),
                    "validade_guia": parse_date(summary_info.get("validade")),
                },
            )
            debts.append(debt)
        if getattr(self, "_skip_iss", False):
            before = len(debts)
            debts = [d for d in debts if not _debt_is_iss(d)]
            if log and before != len(debts):
                log(f"{company.name}: excluídos {before - len(debts)} débito(s) de ISS (opção ativa no dashboard).")
        try:
            self.replace_company_debts_rpc(company.id, debts)
        except Exception as e:
            if log:
                log(f"Erro ao substituir débitos no Supabase: {e}")
            raise
        return debts

    async def _download_guia_pdfs(
        self,
        frame: Frame,
        company: CompanyItem,
        debts: list[DebtRow],
        log: Callable[[str], None] | None,
        status_cb: Callable[[str, str], None],
        stop_cb: callable,
    ) -> None:
        """Seleciona cada débito na tabela, captura a Chave do payload da requisição (request) e salva PDF em base_path/segment_path/empresa/."""
        assert self.page is not None
        # base_path e segment_path (estrutura de pastas) vêm do dashboard via API; sem "EMPRESAS" fixo.
        api_cfg = get_robot_api_config() or {}
        if not api_cfg and not SERVER_API_URL and log:
            log("SERVER_API_URL não definido; não foi possível obter caminho base e estrutura de pastas do dashboard.")
        base_path = (api_cfg.get("base_path") or "").strip() if api_cfg else ""
        if not base_path:
            base_path = (os.getenv("BASE_PATH") or "").strip()
        if not base_path:
            if log:
                log("Caminho base não definido no dashboard (Admin) nem em BASE_PATH; PDFs das guias não serão salvos.")
            return
        segment_path = (api_cfg.get("segment_path") or "").strip() if api_cfg else ""
        if not segment_path:
            if log:
                log("Estrutura de pastas (Departamento) do robô não definida no dashboard; defina em Admin > Robôs. PDFs das guias não serão salvos.")
            return
        company_folder = sanitize_company_folder(company.name)
        # Base na VM = EMPRESAS; estrutura do robô = segment_path (ex.: PARALEGAL/TAXAS-IMPOSTOS). Caminho = base_path/empresa/segment_path.
        rel_dir = f"{company_folder}/{segment_path.strip().replace(chr(92), '/')}".replace("\\", "/")
        full_dir = Path(base_path) / rel_dir.replace("/", os.sep)
        full_dir.mkdir(parents=True, exist_ok=True)

        chave_queue: asyncio.Queue[str] = asyncio.Queue()
        current_target_identity: dict[str, str] | None = None

        def _item_matches_target(item: dict[str, Any], target: dict[str, str] | None) -> bool:
            if not target:
                return False
            dados = item.get("DadosDebito") or {}
            rubrica = str(dados.get("Rubrica") or "").strip()
            ano = str(dados.get("Ano") or "").strip()
            parcela = str(dados.get("Parcela") or "").strip()
            vencimento = str(dados.get("Vencimento") or "").strip()
            return (
                rubrica == target["rubrica"]
                and ano == target["ano"]
                and parcela == target["parcela"]
                and vencimento == target["vencimento"]
            )

        def _build_target_identity(debt: DebtRow) -> dict[str, str]:
            rubrica_match = re.match(r"\s*(\d+)", debt.tributo or "")
            parcela = str((debt.detalhes or {}).get("parcela") or "").strip()
            return {
                "rubrica": (rubrica_match.group(1) if rubrica_match else "").strip(),
                "ano": str(debt.ano or "").strip(),
                "parcela": parcela,
                "vencimento": str(debt.data_vencimento or "").strip(),
            }

        def _parse_chave_from_payload(data: dict) -> None:
            """Extrai a Chave apenas da nova requisicao do debito atualmente selecionado."""
            variables = (data.get("screenData") or {}).get("variables") or data.get("variables") or data.get("data") or {}
            if not isinstance(variables, dict):
                return
            lista = variables.get("ListaDebitosPaginada")
            if not isinstance(lista, dict):
                lista = data.get("ListaDebitosPaginada")
            items = (lista.get("List") or []) if isinstance(lista, dict) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                chave_item = str(item.get("Chave") or "").strip()
                is_selected = item.get("IsSelecionado")
                if not chave_item or not (is_selected is True or is_selected == "true"):
                    continue
                if not _item_matches_target(item, current_target_identity):
                    continue
                chave_queue.put_nowait(chave_item)
                return

        def on_request(request):
            """Ao marcar o débito, o front envia POST com o estado; a Chave vem no payload da REQUEST."""
            try:
                if "DataActionGetParametrosPortal" not in (request.url or ""):
                    return
                if request.method != "POST":
                    return
                post_data = request.post_data
                if not post_data:
                    return
                data = json.loads(post_data if isinstance(post_data, str) else post_data.decode("utf-8", errors="replace"))
                _parse_chave_from_payload(data)
            except Exception:
                pass

        def on_response(response):
            """Alguns fluxos devolvem a Chave na resposta; captura também."""
            try:
                if "screenservices" not in (response.url or ""):
                    return
                body = response.body()
                if not body:
                    return
                data = json.loads(body.decode("utf-8", errors="replace"))
                _parse_chave_from_payload(data)
            except Exception:
                pass

        try:
            context = self.page.context
            context.on("request", on_request)
        except Exception:
            if log:
                log("Não foi possível registrar listener para capturar Chave.")
            return

        checkboxes = await frame.locator("table input[type=checkbox]").all()
        n = min(len(debts), len(checkboxes))
        if n == 0:
            if log:
                log("Nenhuma linha com checkbox encontrada para gerar PDFs das guias.")
            return

        # Só processar débitos com valor diferente de zero (débito com valor zero não tem guia para pegar).
        indices_com_valor = [
            i for i in range(n)
            if (debts[i].valor is not None and float(debts[i].valor) != 0)
        ]
        total_guias = len(indices_com_valor)
        if not indices_com_valor:
            if log:
                log("Nenhum débito com valor > 0 para gerar guia PDF.")
            return

        selected_checkbox_index: int | None = None

        for idx, i in enumerate(indices_com_valor):
            stop_cb()
            status_cb("EXECUTANDO", f"Guia PDF {idx + 1}/{total_guias}: selecionando e baixando")
            debt = debts[i]
            chave: str | None = None
            current_target_identity = _build_target_identity(debt)
            try:
                if selected_checkbox_index is not None and selected_checkbox_index != i:
                    previous_checkbox = checkboxes[selected_checkbox_index]
                    try:
                        if await previous_checkbox.is_checked():
                            await previous_checkbox.click(timeout=8000)
                            for _ in range(10):
                                if not await previous_checkbox.is_checked():
                                    break
                                await asyncio.sleep(0.2)
                    except Exception as deselect_error:
                        if log:
                            log(
                                f"Debito {selected_checkbox_index + 1}: erro ao desmarcar antes de selecionar o proximo: {deselect_error}"
                            )
                # Esvaziar a fila antes do clique para pegar so a Chave da nova requisicao deste debito.
                while not chave_queue.empty():
                    try:
                        chave_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                await checkboxes[i].click(timeout=8000)
                await asyncio.sleep(0.6)
                chave = await asyncio.wait_for(chave_queue.get(), timeout=20.0)
                if await checkboxes[i].is_checked():
                    selected_checkbox_index = i
                else:
                    selected_checkbox_index = None
            except asyncio.TimeoutError:
                if log:
                    log(f"Débito {i + 1}: Chave não capturada (nova requisição); pulando PDF.")
                continue
            except Exception as e:
                if log:
                    log(f"Débito {i + 1}: erro ao selecionar/capturar: {e}")
                continue

            if not chave:
                if log:
                    log(f"Debito {i + 1}: Chave nao encontrada na nova requisicao; pulando PDF.")
                continue
            ano = debt.ano or 0
            parcela = (debt.detalhes or {}).get("parcela") or ""
            safe_parcela = re.sub(r"[^\w\-]", "_", str(parcela))[:20]
            filename = f"guia_{ano}_{safe_parcela}_{idx}.pdf"
            pdf_path = full_dir / filename
            rel_path = f"{rel_dir}/{filename}"

            try:
                guia_page = await context.new_page()
                await guia_page.goto(f"{URLEMISSAO_DUAM}?chave={chave}", wait_until="domcontentloaded", timeout=15000)
                await guia_page.wait_for_load_state("networkidle", timeout=10000)
                await guia_page.pdf(path=str(pdf_path), format="A4")
                await guia_page.close()
                self.update_debt_guia_pdf_path(
                    company.id,
                    debt.tributo,
                    debt.numero_documento,
                    debt.data_vencimento,
                    rel_path,
                )
                if log:
                    log(f"Guia PDF salva: {rel_path}")
            except Exception as e:
                if log:
                    log(f"Erro ao gerar PDF da guia {i + 1}: {e}")
                try:
                    await guia_page.close()
                except Exception:
                    pass

        if log:
            log(f"{company.name}: PDFs das guias salvos em {rel_dir}")

    async def _download_guia_pdfs_batch(
        self,
        frame: Frame,
        company: CompanyItem,
        debts: list[DebtRow],
        log: Callable[[str], None] | None,
        status_cb: Callable[[str, str], None],
        stop_cb: callable,
    ) -> None:
        """Seleciona os debitos com valor > 0, emite as guias e salva cada popup como PDF."""
        assert self.page is not None
        api_cfg = get_robot_api_config() or {}
        if not api_cfg and not SERVER_API_URL and log:
            log("SERVER_API_URL nÃ£o definido; nÃ£o foi possÃ­vel obter caminho base e estrutura de pastas do dashboard.")
        base_path = (api_cfg.get("base_path") or "").strip() if api_cfg else ""
        if not base_path:
            base_path = (os.getenv("BASE_PATH") or "").strip()
        if not base_path:
            if log:
                log("Caminho base nÃ£o definido no dashboard (Admin) nem em BASE_PATH; PDFs das guias nÃ£o serÃ£o salvos.")
            return
        segment_path = (api_cfg.get("segment_path") or "").strip() if api_cfg else ""
        if not segment_path:
            if log:
                log("Estrutura de pastas (Departamento) do robÃ´ nÃ£o definida no dashboard; defina em Admin > RobÃ´s. PDFs das guias nÃ£o serÃ£o salvos.")
            return

        company_folder = sanitize_company_folder(company.name)
        rel_dir = f"{company_folder}/{segment_path.strip().replace(chr(92), '/')}".replace("\\", "/")
        full_dir = Path(base_path) / rel_dir.replace("/", os.sep)
        full_dir.mkdir(parents=True, exist_ok=True)

        context = self.page.context
        current_debts = list(debts)
        checkboxes = await frame.locator("table input[type=checkbox]").all()
        n = min(len(current_debts), len(checkboxes))
        if n == 0:
            if log:
                log("Nenhuma linha com checkbox encontrada para gerar PDFs das guias.")
            return

        indices_com_valor = [
            i for i in range(n)
            if current_debts[i].valor is not None and float(current_debts[i].valor) != 0
        ]
        if not indices_com_valor:
            if log:
                log("Nenhum dÃ©bito com valor > 0 para gerar guia PDF.")
            return

        selected_debts = [current_debts[i] for i in indices_com_valor]

        def _normalize_text(value: str | None) -> str:
            return " ".join((value or "").upper().split())

        def _strip_rubrica_code(value: str | None) -> str:
            return " ".join(re.sub(r"^\s*\d+\s*-\s*", "", value or "").split())

        def _safe_filename_part(value: str | None, fallback: str) -> str:
            text = re.sub(r'[<>:"/\\|?*]', "_", (value or "").strip())
            text = re.sub(r"\s+", " ", text).strip(" .")
            return text or fallback

        async def _wait_loading_to_finish(timeout_seconds: float = 60.0) -> None:
            deadline = asyncio.get_running_loop().time() + timeout_seconds
            next_log_at = asyncio.get_running_loop().time() + 10.0
            while asyncio.get_running_loop().time() < deadline:
                stop_cb()
                body = await frame.locator("body").inner_text()
                if "Obtendo detalhes do débito" not in body and "Obtendo detalhes dos débitos" not in body:
                    return
                if asyncio.get_running_loop().time() >= next_log_at:
                    if log:
                        log("Ainda carregando os detalhes dos débitos; aguardando mais 10 segundos.")
                    next_log_at += 10.0
                await asyncio.sleep(0.3)
            raise TimeoutError("A mensagem de carregamento dos débitos não desapareceu a tempo.")

        def _same_debt(left: DebtRow, right: DebtRow) -> bool:
            return (
                _normalize_text(left.tributo) == _normalize_text(right.tributo)
                and str(left.ano or "").strip() == str(right.ano or "").strip()
                and str((left.detalhes or {}).get("parcela") or "").strip()
                == str((right.detalhes or {}).get("parcela") or "").strip()
                and str(left.data_vencimento or "").strip() == str(right.data_vencimento or "").strip()
            )

        def _find_debt_index(target: DebtRow, pool: list[DebtRow]) -> int | None:
            for idx, candidate in enumerate(pool):
                if _same_debt(target, candidate):
                    return idx
            return None

        async def _recover_from_loading_stall() -> None:
            nonlocal frame, checkboxes, current_debts, n
            if log:
                log("Carregamento passou de 1 minuto. Fechando o modal e reabrindo Taxas e Impostos para continuar.")
            await self._close_debts_modal_and_return_home(frame, log)
            frame = await self.open_duam(company)
            try:
                await self.try_click_recaptcha()
                await self.wait_recaptcha_manual(stop_cb)
            except RecaptchaTimeoutError:
                await self._close_debts_modal_and_return_home(frame, log)
                raise
            current_debts = await self.extract_debts(frame, company, stop_cb, log=log)
            checkboxes = await frame.locator("table input[type=checkbox]").all()
            n = min(len(current_debts), len(checkboxes))
            if n == 0:
                raise RuntimeError("A tabela de débitos não voltou após reabrir Taxas e Impostos.")

        async def _wait_checkbox_state(
            checkbox: Any,
            expected_checked: bool,
            timeout_seconds: float = 10.0,
        ) -> bool:
            deadline = asyncio.get_running_loop().time() + timeout_seconds
            while asyncio.get_running_loop().time() < deadline:
                stop_cb()
                try:
                    if await checkbox.is_checked() == expected_checked:
                        return True
                except Exception:
                    pass
                await asyncio.sleep(0.2)
            return False

        async def _ensure_checkbox_checked(checkbox: Any) -> None:
            for attempt in range(3):
                stop_cb()
                try:
                    if await checkbox.is_checked():
                        await _wait_loading_to_finish()
                        if await _wait_checkbox_state(checkbox, True, timeout_seconds=2.0):
                            return
                    await checkbox.click(timeout=8000, force=True)
                    await _wait_loading_to_finish()
                    if await _wait_checkbox_state(checkbox, True, timeout_seconds=3.0):
                        return
                except TimeoutError:
                    await _recover_from_loading_stall()
                    raise RuntimeError("recover-and-retry")
                except Exception:
                    if attempt == 2:
                        raise
                await asyncio.sleep(0.3)
            raise RuntimeError("O débito não permaneceu marcado após as tentativas de seleção.")

        async def _is_confirmation_modal_visible() -> bool:
            try:
                return await frame.locator("text=/Confirme as guias a serem emitidas/i").count() > 0
            except Exception:
                return False

        async def _close_confirmation_modal_if_open() -> None:
            try:
                close_button = frame.get_by_role("button", name="X").first
                if await close_button.count() and await close_button.is_visible():
                    await close_button.click(timeout=5000)
                    await asyncio.sleep(0.5)
            except Exception:
                pass

        async def _collect_new_popup_pages(existing_pages: list[Page], expected_min: int) -> list[Page]:
            deadline = asyncio.get_running_loop().time() + 20.0
            known_ids = {id(pg) for pg in existing_pages}
            stable_since: float | None = None
            latest: list[Page] = []
            while asyncio.get_running_loop().time() < deadline:
                stop_cb()
                latest = [pg for pg in context.pages if id(pg) not in known_ids and not pg.is_closed()]
                if len(latest) >= expected_min:
                    if stable_since is None:
                        stable_since = asyncio.get_running_loop().time()
                    elif asyncio.get_running_loop().time() - stable_since >= 2.0:
                        return latest
                else:
                    stable_since = None
                await asyncio.sleep(0.5)
            return latest

        async def _extract_guia_metadata(guia_page: Page) -> dict[str, str | None]:
            await guia_page.wait_for_load_state("domcontentloaded", timeout=15000)
            try:
                await guia_page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass
            body = await guia_page.locator("body").inner_text()
            especificacao_match = re.search(r"ESPECIFICACAO:\s*(.+)", body)
            tributo_match = re.search(r"TRIBUTO\s+(.+?)\s+REFERENCIA", body, re.S)
            ano_match = re.search(r"\bANO\s*:\s*(\d{4})\b", body)
            parcela_match = re.search(r"\bPARCELA\s*:\s*(\d+)\b", body)
            vencimento_match = re.search(r"VENCIMENTO\s+(\d{2}/\d{2}/\d{4})", body)
            return {
                "descricao": _strip_rubrica_code(
                    (especificacao_match.group(1) if especificacao_match else "")
                    or (tributo_match.group(1) if tributo_match else "")
                ),
                "ano": ano_match.group(1) if ano_match else None,
                "parcela": parcela_match.group(1) if parcela_match else None,
                "vencimento": parse_date(vencimento_match.group(1)) if vencimento_match else None,
            }

        def _match_debt_to_guia(metadata: dict[str, str | None], remaining: list[DebtRow]) -> DebtRow | None:
            descricao = _normalize_text(metadata.get("descricao"))
            ano = str(metadata.get("ano") or "").strip()
            parcela = str(metadata.get("parcela") or "").strip()
            vencimento = str(metadata.get("vencimento") or "").strip()
            for candidate in remaining:
                candidate_parcela = str((candidate.detalhes or {}).get("parcela") or "").strip()
                if (
                    _normalize_text(_strip_rubrica_code(candidate.tributo)) == descricao
                    and str(candidate.ano or "").strip() == ano
                    and candidate_parcela == parcela
                    and str(candidate.data_vencimento or "").strip() == vencimento
                ):
                    return candidate
            for candidate in remaining:
                if (
                    _normalize_text(_strip_rubrica_code(candidate.tributo)) == descricao
                    and str(candidate.data_vencimento or "").strip() == vencimento
                ):
                    return candidate
            return remaining[0] if remaining else None

        for checkbox in checkboxes:
            try:
                if await checkbox.is_checked():
                    await checkbox.click(timeout=8000, force=True)
                    await asyncio.sleep(0.2)
            except Exception:
                continue

        idx = 0
        while idx < len(selected_debts):
            target_debt = selected_debts[idx]
            stop_cb()
            status_cb("EXECUTANDO", f"Marcando debito {idx + 1}/{len(selected_debts)}")
            attempts_for_debt = 0
            while True:
                attempts_for_debt += 1
                debt_index = _find_debt_index(target_debt, current_debts[:n])
                if debt_index is None:
                    await _recover_from_loading_stall()
                    debt_index = _find_debt_index(target_debt, current_debts[:n])
                if debt_index is None:
                    raise RuntimeError(
                        f"Nao foi possivel reencontrar o debito {target_debt.tributo} apos reabrir Taxas e Impostos."
                    )
                try:
                    await _ensure_checkbox_checked(checkboxes[debt_index])
                    idx += 1
                    break
                except RuntimeError as exc:
                    if str(exc) == "recover-and-retry" and attempts_for_debt < 5:
                        idx = 0
                        continue
                    if log:
                        log(f"Debito {debt_index + 1}: erro ao marcar antes da emissao das guias: {exc}")
                    raise

        try:
            await _wait_loading_to_finish()
        except TimeoutError:
            await _recover_from_loading_stall()

        existing_pages = [pg for pg in context.pages if not pg.is_closed()]
        gerar_guia_button = frame.get_by_role("button", name=re.compile("Gerar Guia", re.I)).first
        await gerar_guia_button.click(timeout=8000)
        await asyncio.sleep(1.0)

        if await _is_confirmation_modal_visible():
            emitir_guias_button = frame.get_by_role("button", name=re.compile("Emitir Guias?", re.I)).first
            await emitir_guias_button.click(timeout=8000)

        expected_popups = 1 if len(selected_debts) == 1 else len(selected_debts)
        popup_pages = await _collect_new_popup_pages(existing_pages, expected_min=expected_popups)
        if not popup_pages:
            if log:
                log("Nenhuma guia foi aberta em nova aba. Verifique se o navegador bloqueou pop-ups.")
            await _close_confirmation_modal_if_open()
            return

        remaining_debts = list(selected_debts)
        for popup_index, guia_page in enumerate(popup_pages, start=1):
            try:
                metadata = await _extract_guia_metadata(guia_page)
                matched_debt = _match_debt_to_guia(metadata, remaining_debts)
                if matched_debt and matched_debt in remaining_debts:
                    remaining_debts.remove(matched_debt)
                debt_for_file = matched_debt or selected_debts[min(popup_index - 1, len(selected_debts) - 1)]
                rubrica_nome = _strip_rubrica_code(debt_for_file.tributo)
                data_nome = str(debt_for_file.data_vencimento or metadata.get("vencimento") or f"guia_{popup_index}")
                filename = f"{_safe_filename_part(rubrica_nome, 'guia')}_{_safe_filename_part(data_nome, 'sem-data')}.pdf"
                pdf_path = full_dir / filename
                rel_path = f"{rel_dir}/{filename}"
                await guia_page.pdf(path=str(pdf_path), format="A4")
                updated = self.update_debt_guia_pdf_path(
                    company.id,
                    debt_for_file.tributo,
                    debt_for_file.numero_documento,
                    debt_for_file.data_vencimento,
                    rel_path,
                    parcela=str((debt_for_file.detalhes or {}).get("parcela") or ""),
                    ano=debt_for_file.ano,
                )
                if log:
                    if updated:
                        log(f"Guia PDF salva: {rel_path}")
                    else:
                        log(f"Guia PDF salva, mas o caminho nao encontrou match no Supabase: {rel_path}")
            except Exception as exc:
                if log:
                    log(f"Erro ao gerar PDF da guia {popup_index}: {exc}")
            finally:
                try:
                    await guia_page.close()
                except Exception:
                    pass

        await _close_confirmation_modal_if_open()

        if log:
            log(f"{company.name}: PDFs das guias salvos em {rel_dir}")


class RobotWorker(QThread):
    status_changed = Signal(str)
    log_message = Signal(str)
    company_changed = Signal(str, str, str)

    def __init__(
        self,
        backend: RobotBackend,
        companies: list[CompanyItem],
        job: dict[str, Any] | None = None,
        use_proxy_rotation: bool = True,
    ) -> None:
        super().__init__()
        self.backend = backend
        self.companies = companies
        self.job = job
        self.use_proxy_rotation = use_proxy_rotation
        self._stop_requested = False
        self.error_messages: list[str] = []
        self.was_stopped = False

    def request_stop(self) -> None:
        self._stop_requested = True

    def assert_not_stopped(self) -> None:
        if self._stop_requested:
            raise StopRequested("Execução interrompida manualmente.")

    def run(self) -> None:
        asyncio.run(self._run())

    async def _run(self) -> None:
        self.status_changed.emit("EXECUTANDO")
        self.backend.update_robot_status("processing")
        self.backend.set_use_proxy_rotation(self.use_proxy_rotation)
        try:
            if self.use_proxy_rotation:
                self.log_message.emit("Atualizando lista de proxies...")
                loop = asyncio.get_running_loop()
                n = await loop.run_in_executor(None, _update_proxy_list)
                self.backend.reload_proxy_list()
                if n > 0:
                    self.log_message.emit(f"Lista de proxies atualizada ({n} proxy(s)).")
            await self.backend.ensure_login(self.log_message.emit)
            self.backend.update_robot_heartbeat()
            self.log_message.emit("Login validado no portal da Prefeitura de Goiânia.")
            for company in self.companies:
                self.assert_not_stopped()
                self.backend.update_robot_heartbeat()
                run_id = self.backend.create_run(company)
                try:
                    debts = await self.backend.collect_company_debts(
                        company,
                        self.assert_not_stopped,
                        self.log_message.emit,
                        lambda status, message, company_id=company.id: self.company_changed.emit(company_id, status, message),
                    )
                    synced_count = len(debts)
                    self.backend.finish_run(run_id, "completed", debts_found=synced_count)
                    self.company_changed.emit(company.id, "CONCLUIDO", f"{synced_count} débito(s) sincronizado(s)")
                except RecaptchaTimeoutError as exc:
                    self.backend.finish_run(run_id, "failed", error_message=str(exc))
                    self.company_changed.emit(company.id, "ERRO", str(exc))
                    self.log_message.emit(f"{company.name}: reCAPTCHA não concluído em 2 min; modal fechado, próxima empresa.")
                    self.error_messages.append(f"{company.name}: {exc}")
                except StopRequested as exc:
                    self.backend.finish_run(run_id, "failed", error_message=str(exc))
                    self.company_changed.emit(company.id, "PARADO", str(exc))
                    self.log_message.emit(str(exc))
                    self.was_stopped = True
                    self.error_messages.append(str(exc))
                    break
                except Exception as exc:
                    self.backend.finish_run(run_id, "failed", error_message=str(exc))
                    self.company_changed.emit(company.id, "ERRO", str(exc))
                    self.log_message.emit(f"{company.name}: erro - {exc}")
                    self.error_messages.append(f"{company.name}: {exc}")
                if self.was_stopped:
                    break
        except Exception as exc:
            self.log_message.emit(f"Erro: {exc}")
            self.error_messages.append(str(exc))
        finally:
            self.status_changed.emit("AGUARDANDO")
            self.backend.update_robot_status("active")
            if self.backend.browser is not None or self.backend.chrome_proc is not None:
                self.log_message.emit("Fechando navegador...")
                try:
                    await self.backend.close()
                    await asyncio.sleep(1.0)
                    loop = asyncio.get_running_loop()
                    still_open = await loop.run_in_executor(None, _is_cdp_port_in_use)
                    if still_open:
                        self.log_message.emit("O navegador pode ainda estar aberto.")
                    else:
                        self.log_message.emit("Navegador fechado.")
                except Exception as e:
                    self.log_message.emit(f"Aviso ao fechar navegador: {e}")
                    self.log_message.emit("O navegador pode ainda estar aberto.")


class LogFrame(QFrame):
    def __init__(self, height: int = 230):
        super().__init__()
        self.setMinimumHeight(height)
        self.setStyleSheet(
            "QFrame { border:1px solid rgba(52,73,94,0.65); border-radius:12px; background:rgba(12,24,40,0.85); }"
        )
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self.text = QTextEdit(self)
        self.text.setReadOnly(True)
        self.text.setAcceptRichText(True)
        self.text.setPlaceholderText("Logs da execucao...")
        self.text.setStyleSheet(
            "QTextEdit{background:transparent;color:#E8F4FF;font:10pt Consolas,'Courier New';padding:10px;}"
        )
        layout.addWidget(self.text)

    def append(self, msg: str) -> None:
        bar = self.text.verticalScrollBar()
        at_bottom = bar.value() >= (bar.maximum() - 2)
        old_value = bar.value()
        self.text.append(msg)
        if at_bottom:
            bar.setValue(bar.maximum())
        else:
            bar.setValue(old_value)


class CompanyListItem(QWidget):
    def __init__(self, company: CompanyItem, toggle_cb: callable):
        super().__init__()
        self.company = company
        layout = QHBoxLayout(self)
        layout.setContentsMargins(6, 4, 6, 4)
        layout.setSpacing(8)

        self.checkbox = QCheckBox()
        self.checkbox.setChecked(company.selected)
        self.checkbox.setEnabled(company.active)
        self.checkbox.stateChanged.connect(lambda state: toggle_cb(company.id, state))
        layout.addWidget(self.checkbox)

        suffix = f" - {company.document}" if company.document else ""
        self.label = QLabel(f"{company.name}{suffix}")
        self.label.setStyleSheet("QLabel { color:#ECF0F1; font:9pt Verdana; font-weight:bold; }")
        layout.addWidget(self.label, 1)

        self.status = QLabel(company.status)
        self.status.setStyleSheet("QLabel { color:#94A3B8; font:9pt Verdana; font-weight:bold; }")
        layout.addWidget(self.status)

        self.message = QLabel(company.message or "-")
        self.message.setStyleSheet("QLabel { color:#CBD5E1; font:9pt Verdana; }")
        layout.addWidget(self.message)


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.backend = RobotBackend()
        self.all_companies: list[CompanyItem] = []
        self.filtered_companies: list[CompanyItem] = []
        self.items: list[CompanyListItem] = []
        self.worker: RobotWorker | None = None
        self.active_job: dict[str, Any] | None = None
        self._tray_icon: QSystemTrayIcon | None = None
        self.heartbeat_timer = QTimer(self)
        self.heartbeat_timer.timeout.connect(self._on_robot_heartbeat)
        self.display_config_timer = QTimer(self)
        self.display_config_timer.timeout.connect(self._on_display_config_poll)
        self.job_poll_timer = QTimer(self)
        self.job_poll_timer.timeout.connect(self._on_robot_poll_job)
        self.setWindowTitle("Robô Goiânia - Taxas e Impostos")
        self.resize(980, 760)
        icon = qicon("app", "logo")
        if icon:
            self.setWindowIcon(icon)
        self._build_ui()
        self.backend.set_log_callback(self.append_log)
        QApplication.instance().aboutToQuit.connect(self._on_about_to_quit)
        self._setup_tray_icon()
        self._register_robot_panel()
        self.sync_companies()

    def _build_ui(self) -> None:
        self.setStyleSheet(
            """
            QMainWindow { background:qlineargradient(x1:0,y1:0, x2:1,y2:1, stop:0 #0f1722, stop:1 #111827); }
            QWidget { color:#ECF0F1; }
            QLabel { font-weight:bold; font:9pt 'Verdana'; }
            QLineEdit, QTextEdit {
                background:#34495E;
                color:#ECF0F1;
                border-radius:6px;
                padding:6px;
                font:9pt 'Verdana';
                border: 1px solid #22344a;
            }
            QScrollArea { border:none; }
            QCheckBox::indicator {
                width: 18px;
                height: 18px;
                border-radius: 6px;
                border: 1px solid #334155;
                background: #0b1220;
            }
            QCheckBox::indicator:checked {
                background: #2563eb;
                border: 1px solid #2563eb;
            }
            """
        )

        central = QWidget()
        layout = QVBoxLayout(central)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        top = QFrame()
        top.setStyleSheet(
            "background:qlineargradient(x1:0,y1:0, x2:1,y2:0, stop:0 #1f2f46, stop:1 #2f3f5b);"
            "border-radius:10px; border:1px solid #22344a;"
        )
        top_layout = QHBoxLayout(top)
        self.title_label = QLabel("Robô Goiânia - Taxas e Impostos")
        self.title_label.setStyleSheet("color:#ECF0F1; font:12pt 'Verdana'; font-weight:bold;")
        top_layout.addWidget(self.title_label, alignment=Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(top)

        controls = QVBoxLayout()
        first_row = QHBoxLayout()
        self.search = QLineEdit()
        self.search.setPlaceholderText("Pesquisar empresa...")
        self.search.textChanged.connect(self.apply_filter)
        first_row.addWidget(self.search, 1)

        self.btn_select_all = QPushButton("Marcar todas")
        self.btn_select_all.setStyleSheet(button_style("#2980B9", "#3498DB", "#2471A3"))
        select_icon = qicon("selecionar")
        if select_icon:
            self.btn_select_all.setIcon(select_icon)
        self.btn_select_all.clicked.connect(self.select_all_visible)
        first_row.addWidget(self.btn_select_all)

        self.btn_deselect_all = QPushButton("Desmarcar")
        self.btn_deselect_all.setStyleSheet(button_style("#7F8C8D", "#95A5A6", "#626E70"))
        deselect_icon = qicon("desmarcar-cancelar", "excluir")
        if deselect_icon:
            self.btn_deselect_all.setIcon(deselect_icon)
        self.btn_deselect_all.clicked.connect(self.deselect_all_visible)
        first_row.addWidget(self.btn_deselect_all)
        controls.addLayout(first_row)
        layout.addLayout(controls)

        wrap = QFrame()
        wrap.setStyleSheet("background:rgba(17,23,39,0.85);border:1px solid #22344a;border-radius:10px;")
        wrap_layout = QVBoxLayout(wrap)
        wrap_layout.setContentsMargins(5, 5, 5, 5)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("border:none;")

        self.list_container = QWidget()
        self.list_layout = QVBoxLayout(self.list_container)
        self.list_layout.setContentsMargins(6, 6, 6, 6)
        self.list_layout.setSpacing(4)

        scroll.setWidget(self.list_container)
        wrap_layout.addWidget(scroll)
        layout.addWidget(wrap, 3)

        self.log_frame = LogFrame(height=230)
        self.log_frame.setMinimumHeight(120)
        self.log_frame.setMaximumHeight(16777215)
        layout.addWidget(self.log_frame, 2)

        bottom = QHBoxLayout()
        self.check_use_proxy = QCheckBox("Usar rotação de proxy")
        self.check_use_proxy.setToolTip("Quando ativo, atualiza e usa lista de proxies ao iniciar; desmarque para rodar sem proxy.")
        self._load_robot_config()
        self.check_use_proxy.stateChanged.connect(self._save_robot_config)
        bottom.addWidget(self.check_use_proxy)

        self.btn_start = QPushButton("Iniciar downloads")
        self.btn_start.setStyleSheet(button_style("#27AE60", "#2ECC71", "#1E8449"))
        start_icon = qicon("iniciar")
        if start_icon:
            self.btn_start.setIcon(start_icon)
        self.btn_start.clicked.connect(self.start_execution)

        self.btn_stop = QPushButton("Parar")
        self.btn_stop.setStyleSheet(button_style("#C0392B", "#E74C3C", "#922B21"))
        stop_icon = qicon("parar")
        if stop_icon:
            self.btn_stop.setIcon(stop_icon)
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self.stop_execution)

        self.btn_clear = QPushButton("Limpar log")
        self.btn_clear.setStyleSheet(button_style("#1ABC9C", "#16A085", "#117A65"))
        clear_icon = qicon("limpar")
        if clear_icon:
            self.btn_clear.setIcon(clear_icon)
        self.btn_clear.clicked.connect(lambda: self.log_frame.text.clear())

        bottom.addWidget(self.btn_start)
        bottom.addWidget(self.btn_stop)
        bottom.addWidget(self.btn_clear)
        layout.addLayout(bottom)

        self.setCentralWidget(central)

    def _load_robot_config(self) -> None:
        """Carrega opção 'Usar rotação de proxy' do arquivo de config."""
        try:
            if ROBOT_CONFIG_FILE.exists():
                data = json.loads(ROBOT_CONFIG_FILE.read_text(encoding="utf-8"))
                self.check_use_proxy.setChecked(bool(data.get("use_proxy_rotation", True)))
            else:
                self.check_use_proxy.setChecked(True)
        except Exception:
            self.check_use_proxy.setChecked(True)

    def _save_robot_config(self) -> None:
        """Persiste opção 'Usar rotação de proxy' no arquivo de config."""
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            data = {"use_proxy_rotation": self.check_use_proxy.isChecked()}
            ROBOT_CONFIG_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _setup_tray_icon(self) -> None:
        self._tray_icon = QSystemTrayIcon(self)
        app_icon = qicon("app", "logo")
        if app_icon and not app_icon.isNull():
            self._tray_icon.setIcon(app_icon)
        else:
            self._tray_icon.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_ComputerIcon))
        menu = QMenu()
        show_act = QAction("Abrir janela", self)
        show_act.triggered.connect(self._show_from_tray)
        menu.addAction(show_act)
        quit_act = QAction("Fechar robô", self)
        quit_act.triggered.connect(self._quit_from_tray)
        menu.addAction(quit_act)
        self._tray_icon.setContextMenu(menu)
        self._tray_icon.activated.connect(self._on_tray_activated)
        self._tray_icon.setToolTip("Taxas e Impostos Goiânia")

    def _show_from_tray(self) -> None:
        self.showNormal()
        self.raise_()
        self.activateWindow()
        if not self.heartbeat_timer.isActive():
            self.heartbeat_timer.start(HEARTBEAT_INTERVAL_MS)
        if not self.display_config_timer.isActive():
            self.display_config_timer.start(DISPLAY_CONFIG_INTERVAL_MS)
        if not self.job_poll_timer.isActive():
            self.job_poll_timer.start(JOB_POLL_INTERVAL_MS)

    def _on_tray_activated(self, reason: int) -> None:
        if reason == QSystemTrayIcon.ActivationReason.DoubleClick:
            self._show_from_tray()

    def _quit_from_tray(self) -> None:
        """Fechar de verdade: para timers, marca inativo e encerra o app."""
        if self.worker and self.worker.isRunning():
            self.worker.request_stop()
            if not self.worker.wait(5000):
                self.worker.terminate()
                self.worker.wait(1000)
        self.heartbeat_timer.stop()
        self.display_config_timer.stop()
        self.job_poll_timer.stop()
        self.backend.ensure_robot_registration()
        self.backend.update_robot_status("inactive")
        if self._tray_icon is not None:
            self._tray_icon.hide()
        QApplication.quit()

    def _stat_card(self, title: str, value: str) -> tuple[QFrame, QLabel]:
        frame = QFrame()
        frame.setStyleSheet("QFrame { background: #111827; border: 1px solid #1f2937; border-radius: 16px; }")
        frame.setMinimumHeight(88)
        grid = QGridLayout(frame)
        title_label = QLabel(title)
        title_label.setStyleSheet("color: #94a3b8; font-size: 12px;")
        value_label = QLabel(value)
        value_label.setFont(QFont("Verdana", 14, QFont.Weight.Bold))
        grid.addWidget(title_label, 0, 0)
        grid.addWidget(value_label, 1, 0)
        return frame, value_label

    def sync_companies(self) -> None:
        try:
            selection = {company.id: company.selected for company in self.all_companies}
            self.all_companies = self.backend.fetch_companies()
            for company in self.all_companies:
                company.selected = selection.get(company.id, False)
            self.apply_filter()
            self.append_log("Empresas sincronizadas do Supabase.")
        except Exception as exc:
            self.append_log(f"Erro ao sincronizar empresas: {exc}")
            QMessageBox.critical(self, "Erro", str(exc))

    def apply_filter(self) -> None:
        query = self.search.text().strip().lower()
        if not query:
            self.filtered_companies = list(self.all_companies)
        else:
            self.filtered_companies = [
                company
                for company in self.all_companies
                if query in company.name.lower() or query in (company.document or "").lower()
            ]
        self.populate_table()

    def populate_table(self) -> None:
        while self.list_layout.count():
            item = self.list_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()
        self.items = []
        for company in self.filtered_companies:
            item = CompanyListItem(company, self.toggle_company)
            self.items.append(item)
            self._apply_item_status(item, company.status, company.message)
            self.list_layout.addWidget(item)
        self.list_layout.addStretch()

    def toggle_company(self, company_id: str, state: int) -> None:
        for company in self.all_companies:
            if company.id == company_id:
                company.selected = state == Qt.CheckState.Checked.value
                break

    def select_all_visible(self) -> None:
        for company in self.filtered_companies:
            if company.active:
                company.selected = True
        self.populate_table()

    def deselect_all_visible(self) -> None:
        for company in self.filtered_companies:
            company.selected = False
        self.populate_table()

    def _status_color(self, text: str) -> str:
        upper = text.upper()
        if "ERRO" in upper:
            return "#fb7185"
        if "CONCLUIDO" in upper:
            return "#34d399"
        if "EXECUTANDO" in upper or "AGUARDANDO" in upper:
            return "#60a5fa"
        if "PARADO" in upper:
            return "#fbbf24"
        return "#cbd5e1"

    def _apply_item_status(self, item: CompanyListItem, status: str, message: str) -> None:
        item.status.setText(status)
        item.status.setStyleSheet(
            f"QLabel {{ color:{self._status_color(status)}; font:9pt Verdana; font-weight:bold; }}"
        )
        item.message.setText(message or "-")

    def update_company_state(self, company_id: str, status: str, message: str) -> None:
        for company in self.all_companies:
            if company.id == company_id:
                company.status = status
                company.message = message or "-"
                break
        self.populate_table()

    def set_global_status(self, status: str) -> None:
        self.title_label.setText(f"Robô Goiânia - Taxas e Impostos [{status}]")

    def _register_robot_panel(self) -> None:
        robot_id = self.backend.ensure_robot_registration()
        if robot_id:
            self.backend.update_robot_status("active")
            self.heartbeat_timer.start(HEARTBEAT_INTERVAL_MS)
            self.display_config_timer.start(DISPLAY_CONFIG_INTERVAL_MS)
            self.job_poll_timer.start(JOB_POLL_INTERVAL_MS)
            QTimer.singleShot(500, self._on_display_config_poll)
            QTimer.singleShot(1500, self._on_robot_poll_job)
            self.append_log("[Robo] Conectado ao painel. Status: active.")
        else:
            self.append_log("[Robo] Nao foi possivel registrar na tabela robots.")

    def _on_robot_heartbeat(self) -> None:
        self.backend.update_robot_heartbeat()

    def _on_display_config_poll(self) -> None:
        if self.worker and self.worker.isRunning():
            return
        cfg = self.backend.fetch_robot_display_config()
        if not cfg:
            return
        updated = (cfg.get("updated_at") or "").strip()
        if updated and updated == self.backend.display_config_updated_at:
            return
        self.backend.display_config_updated_at = updated or None
        company_ids = cfg.get("company_ids")
        selection = {company.id: company.selected for company in self.all_companies}
        if isinstance(company_ids, list):
            self.all_companies = self.backend.fetch_companies_by_ids([str(company_id) for company_id in company_ids])
            for company in self.all_companies:
                company.selected = True
        else:
            self.all_companies = self.backend.fetch_companies()
            for company in self.all_companies:
                company.selected = selection.get(company.id, False)
        self.apply_filter()

    def _on_robot_poll_job(self) -> None:
        if self.active_job:
            return
        if self.worker and self.worker.isRunning():
            return
        job = self.backend.claim_execution_request(self.append_log)
        if job:
            self.append_log("[Robô] Job do dashboard iniciado.")
            try:
                self._run_job(job)
            except Exception as exc:
                self.append_log(f"[Robô] Falha ao preparar job do dashboard: {exc}")
                append_runtime_log(f"[{datetime.now().strftime('%H:%M:%S')}] TRACE preparing job failed: {exc!r}")
                try:
                    self.backend.complete_execution_request(job["id"], False, f"Falha ao preparar job: {exc}")
                except Exception:
                    pass
                self.active_job = None

    def _run_job(self, job: dict[str, Any]) -> None:
        company_ids = [str(company_id) for company_id in (job.get("company_ids") or []) if str(company_id).strip()]
        if not company_ids:
            self.backend.complete_execution_request(job["id"], False, "Nenhuma empresa no job")
            return
        companies = self.backend.fetch_companies_by_ids(company_ids)
        if not companies:
            self.backend.complete_execution_request(job["id"], False, "Nenhuma empresa habilitada de Goiânia encontrada")
            self.append_log("[Robô] Nenhuma empresa habilitada retornada pelo dashboard para este job.")
            return
        for company in companies:
            company.selected = True
        self.active_job = job
        self._start_worker(companies, job=job, origin_message=f"Job do dashboard com {len(companies)} empresa(s).")

    def append_log(self, message: str) -> None:
        line = emit_terminal_log(message)
        self.log_frame.append(line)
        append_runtime_log(line)

    def _start_worker(
        self,
        companies: list[CompanyItem],
        job: dict[str, Any] | None = None,
        origin_message: str | None = None,
    ) -> None:
        self.backend.ensure_robot_registration()
        self.backend.update_robot_status("processing")
        use_proxy = self.check_use_proxy.isChecked()
        worker_use_proxy = use_proxy and not is_scheduler_mode_enabled()
        if use_proxy and not worker_use_proxy:
            self.append_log("[Robô] Rotação de proxy desativada no modo agendador.")
        self.worker = RobotWorker(self.backend, companies, job=job, use_proxy_rotation=worker_use_proxy)
        self.worker.status_changed.connect(self.set_global_status)
        self.worker.log_message.connect(self.append_log)
        self.worker.company_changed.connect(self.update_company_state)
        self.worker.finished.connect(self.execution_finished)
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.worker.start()
        if origin_message:
            self.append_log(origin_message)

    def start_execution(self) -> None:
        selected = [company for company in self.all_companies if company.selected and company.active]
        if not selected:
            QMessageBox.information(self, "Seleção", "Selecione ao menos uma empresa ativa.")
            return
        self.active_job = None
        self._start_worker(selected, origin_message=f"Iniciando coleta para {len(selected)} empresa(s).")

    def stop_execution(self) -> None:
        if self.worker and self.worker.isRunning():
            self.worker.request_stop()
            self.append_log("Parada solicitada. O robô vai interromper na próxima etapa segura.")
            self.btn_stop.setEnabled(False)

    def execution_finished(self) -> None:
        worker = self.worker
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.append_log("Execução finalizada.")
        self.backend.ensure_robot_registration()
        self.backend.update_robot_status("active")
        if self.active_job:
            error_message = "\n".join(worker.error_messages) if worker and worker.error_messages else None
            success = not bool(error_message) and not (worker.was_stopped if worker else False)
            self.backend.complete_execution_request(self.active_job["id"], success, error_message)
            self.active_job = None
        self.worker = None

    def closeEvent(self, event) -> None:  # type: ignore[override]
        event.ignore()
        self.hide()
        if self._tray_icon is not None and not self._tray_icon.icon().isNull():
            self._tray_icon.show()

    def _on_about_to_quit(self) -> None:
        self.heartbeat_timer.stop()
        self.display_config_timer.stop()
        self.job_poll_timer.stop()
        self.backend.ensure_robot_registration()
        self.backend.update_robot_status("inactive")


def sync_local_resources() -> None:
    source = BASE_DIR.parent.parent / "nfs" / "NFs Padrao" / "data"
    EXTENSIONS_DIR.mkdir(parents=True, exist_ok=True)
    if not source.exists():
        DATA_DIR.mkdir(parents=True, exist_ok=True)
    elif not DATA_DIR.exists():
        shutil.copytree(source, DATA_DIR)
    if PLAYWRIGHT_DIR.exists():
        shutil.rmtree(PLAYWRIGHT_DIR, ignore_errors=True)
    chrome_target = DATA_DIR / "Chrome"
    chrome_sources = [
        BASE_DIR.parent.parent.parent / "fiscal" / "nfe-nfc" / "Sefaz Xml" / "data" / "Chrome",
        BASE_DIR.parent.parent.parent / "fiscal" / "Certidoes" / "Certidao Estadual, Federal e FGTS" / "data" / "Chrome",
    ]
    if not chrome_target.exists():
        for chrome_source in chrome_sources:
            if chrome_source.exists():
                shutil.copytree(chrome_source, chrome_target)
                break


def main() -> int:
    sync_local_resources()
    ensure_single_instance()
    app = QApplication(sys.argv)
    window = MainWindow()
    app.aboutToQuit.connect(lambda: _mark_process_inactive(window, "qt_about_to_quit"))
    _install_process_shutdown_handlers(window)
    scheduler_mode = str(os.getenv("AUTOMATIZE_SCHEDULER_MODE") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "sim",
        "on",
    }
    if scheduler_mode:
        window.hide()
    else:
        window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
