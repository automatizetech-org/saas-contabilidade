from __future__ import annotations

"""e-CAC - Caixa Postal

Implementa??o consolidada em arquivo ?nico.
"""

# === models ===
import base64
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional


@dataclass(slots=True)
class RuntimePaths:
    runtime_dir: Path
    base_dir: Path
    robots_root_dir: Path
    shared_env_dir: Path
    data_dir: Path
    json_dir: Path
    logs_dir: Path
    output_dir: Path
    chrome_profile_dir: Path
    certificates_registry_path: Path
    runtime_log_path: Path


@dataclass(slots=True)
class OfficeContext:
    office_id: str = ""
    office_server_id: str = ""
    base_path: str = ""
    segment_path: str = ""
    notes_mode: str = ""
    source: str = ""


@dataclass(slots=True)
class CompanyRecord:
    company_id: str
    name: str
    document: str
    active: bool = True
    eligible: bool = True
    block_reason: str = ""
    source: str = "dashboard"
    auth_mode: str = "password"
    cert_password: str = ""
    cert_blob_b64: str = ""
    cert_path: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def search_text(self) -> str:
        return f"{self.name} {self.document}".strip().lower()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["raw"] = dict(self.raw)
        return payload


@dataclass(slots=True)
class CertificateMetadata:
    alias: str
    subject: str
    issuer: str = ""
    thumbprint: str = ""
    pfx_path: str = ""
    source: str = "registry"

    def display_label(self) -> str:
        return self.alias or self.subject or self.thumbprint or "Sem nome"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class JobPayload:
    job_id: str = ""
    execution_request_id: str = ""
    office_id: str = ""
    company_ids: list[str] = field(default_factory=list)
    companies: list[dict[str, Any]] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MessageRecord:
    company_id: str
    company_name: str
    company_document: str
    run_id: str
    extracted_at: str
    source_system: str
    row_index: int
    company_profile_context: str = ""
    message_id: str = ""
    subject: str = ""
    sender: str = ""
    category: str = ""
    received_at: str = ""
    sent_at: str = ""
    posted_at: str = ""
    read_status: str = ""
    unread: Optional[bool] = None
    priority: str = ""
    snippet: str = ""
    body: str = ""
    attachments: list[dict[str, Any]] = field(default_factory=list)
    raw_visible_text: str = ""
    detail_visible_text: str = ""

    def dedupe_key(self) -> str:
        return "|".join(
            [
                self.message_id.strip(),
                self.subject.strip(),
                self.received_at.strip(),
                self.posted_at.strip(),
                self.raw_visible_text.strip(),
            ]
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CompanyRunResult:
    company_id: str
    company_name: str
    company_document: str
    status: str = "pending"
    eligible: bool = True
    block_reason: str = ""
    profile_switched: bool = False
    mailbox_opened: bool = False
    company_profile_context: str = ""
    messages: list[MessageRecord] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    started_at: str = ""
    finished_at: str = ""
    output_dir: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["messages"] = [message.to_dict() for message in self.messages]
        return payload


@dataclass(slots=True)
class RunSummary:
    run_id: str
    started_at: str
    finished_at: str = ""
    status: str = "pending"
    total_companies: int = 0
    total_success: int = 0
    total_failed: int = 0
    total_messages: int = 0
    interrupted_at_company: str = ""
    company_results: list[CompanyRunResult] = field(default_factory=list)
    output_dir: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["company_results"] = [item.to_dict() for item in self.company_results]
        return payload

# === runtime ===
import json
import logging
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional


ROBOT_TECHNICAL_ID = "ecac_caixa_postal"
ROBOT_DISPLAY_NAME = "e-CAC - Caixa Postal"
HEARTBEAT_INTERVAL_SECONDS = 30


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def only_digits(text: str) -> str:
    return "".join(ch for ch in str(text or "") if ch.isdigit())


def format_cpf(cpf: str) -> str:
    digits = only_digits(cpf)
    if len(digits) != 11:
        return digits
    return f"{digits[0:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:11]}"


def format_cnpj(cnpj: str) -> str:
    digits = only_digits(cnpj)
    if len(digits) != 14:
        return digits
    return f"{digits[0:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:14]}"


def format_document(text: str) -> str:
    digits = only_digits(text)
    if len(digits) <= 11:
        return format_cpf(digits)
    return format_cnpj(digits)


def slugify(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower())
    return text.strip("_") or "item"


def read_json(path: Path, default: Any = None) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(path)


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        from dotenv import load_dotenv

        load_dotenv(path, override=False)
        return
    except Exception:
        pass

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def resolve_runtime_dir() -> Path:
    explicit = (os.environ.get("ROBOT_SCRIPT_DIR") or "").strip().rstrip("\\/")
    if explicit:
        return Path(explicit).resolve()
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def resolve_base_dir(runtime_dir: Path) -> Path:
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        internal_dir = exe_dir / "_internal"
        if internal_dir.is_dir():
            return internal_dir
        meipass_dir = getattr(sys, "_MEIPASS", None)
        if meipass_dir:
            return Path(meipass_dir).resolve()
        return exe_dir
    return runtime_dir


def resolve_robots_base_env_dir(base_dir: Path, runtime_dir: Path) -> Path:
    candidates: list[Path] = []
    robots_root = (os.environ.get("ROBOTS_ROOT_PATH") or "").strip()
    robot_script_dir = (os.environ.get("ROBOT_SCRIPT_DIR") or "").strip()

    if robots_root:
        candidates.append(Path(robots_root))
    if robot_script_dir:
        candidates.append(Path(robot_script_dir).resolve().parent)
    candidates.append(runtime_dir.parent)
    candidates.append(base_dir.parent)

    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        candidates.append(exe_dir)
        candidates.append(exe_dir.parent)

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

    if robots_root:
        return Path(robots_root).resolve()
    return runtime_dir.parent.resolve()


def bootstrap_environment() -> RuntimePaths:
    runtime_dir = resolve_runtime_dir()
    base_dir = resolve_base_dir(runtime_dir)
    shared_env_dir = resolve_robots_base_env_dir(base_dir, runtime_dir)

    shared_env = shared_env_dir / ".env"
    shared_env_example = shared_env_dir / ".env.example"
    local_env = runtime_dir / ".env"
    local_env_example = runtime_dir / ".env.example"

    _load_env_file(shared_env if shared_env.exists() else shared_env_example)
    _load_env_file(local_env if local_env.exists() else local_env_example)

    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        if exe_dir != shared_env_dir:
            _load_env_file(exe_dir / ".env")
            _load_env_file(exe_dir / ".env.example")

    data_dir = runtime_dir / "data"
    json_dir = data_dir / "json"
    logs_dir = data_dir / "logs"
    output_dir = data_dir / "output"
    chrome_profile_dir = runtime_dir / "chrome_profile"

    for folder in (data_dir, json_dir, logs_dir, output_dir, chrome_profile_dir):
        folder.mkdir(parents=True, exist_ok=True)

    playwright_dir = data_dir / "ms-playwright"
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(playwright_dir))

    return RuntimePaths(
        runtime_dir=runtime_dir,
        base_dir=base_dir,
        robots_root_dir=shared_env_dir.parent if shared_env_dir.parent != shared_env_dir else shared_env_dir,
        shared_env_dir=shared_env_dir,
        data_dir=data_dir,
        json_dir=json_dir,
        logs_dir=logs_dir,
        output_dir=output_dir,
        chrome_profile_dir=chrome_profile_dir,
        certificates_registry_path=json_dir / "certificates.json",
        runtime_log_path=logs_dir / "runtime.log",
    )


class RuntimeLogger:
    def __init__(self, log_path: Path, sink: Optional[Callable[[str], None]] = None) -> None:
        self.log_path = log_path
        self.sink = sink
        self._logger = logging.getLogger(f"ecac_caixa_postal::{log_path}")
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False
        if not self._logger.handlers:
            handler = logging.FileHandler(log_path, encoding="utf-8")
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            self._logger.addHandler(handler)

    def bind_sink(self, sink: Optional[Callable[[str], None]]) -> None:
        self.sink = sink

    def _emit(self, level: str, message: str) -> None:
        text = str(message).rstrip()
        getattr(self._logger, level.lower())(text)
        line = f"[{datetime.now().strftime('%H:%M:%S')}] [{level}] {text}"
        if self.sink:
            self.sink(line)

    def info(self, message: str) -> None:
        self._emit("INFO", message)

    def warning(self, message: str) -> None:
        self._emit("WARNING", message)

    def error(self, message: str) -> None:
        self._emit("ERROR", message)

    def exception(self, message: str) -> None:
        self._logger.exception(message)
        line = f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] {message}"
        if self.sink:
            self.sink(line)


class JsonRobotRuntime:
    def __init__(self, technical_id: str, display_name: str, json_dir: Path) -> None:
        self.technical_id = technical_id
        self.display_name = display_name
        self.json_dir = json_dir
        self.job_path = json_dir / "job.json"
        self.result_path = json_dir / "result.json"
        self.heartbeat_path = json_dir / "heartbeat.json"

    def register_robot(self, extra: Optional[dict[str, Any]] = None) -> str:
        self.write_heartbeat(status="active", message="robot_registered", extra=extra)
        return self.technical_id

    def load_job(self) -> Optional[JobPayload]:
        payload = read_json(self.job_path, default=None)
        if not isinstance(payload, dict):
            return None
        execution_request_id = str(
            payload.get("execution_request_id") or payload.get("job_id") or payload.get("id") or ""
        ).strip()
        if not execution_request_id:
            return None
        result_payload = read_json(self.result_path, default=None)
        if isinstance(result_payload, dict):
            existing = str(
                result_payload.get("execution_request_id")
                or result_payload.get("job_id")
                or result_payload.get("event_id")
                or ""
            ).strip()
            if existing and existing == execution_request_id:
                return None
        company_ids = [str(item) for item in payload.get("company_ids") or [] if str(item).strip()]
        companies = payload.get("companies") if isinstance(payload.get("companies"), list) else []
        return JobPayload(
            job_id=str(payload.get("job_id") or payload.get("id") or execution_request_id),
            execution_request_id=execution_request_id,
            office_id=str(payload.get("office_id") or "").strip(),
            company_ids=company_ids,
            companies=[item for item in companies if isinstance(item, dict)],
            raw=payload,
        )

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
        payload: dict[str, Any] = {
            "robot_technical_id": self.technical_id,
            "display_name": self.display_name,
            "status": status,
            "updated_at": utc_now_iso(),
            "current_job_id": current_job_id,
            "current_execution_request_id": current_execution_request_id,
            "message": message,
            "progress": progress or {},
        }
        if extra:
            payload.update(extra)
        write_json_atomic(self.heartbeat_path, payload)

    def write_result(
        self,
        *,
        job: Optional[JobPayload],
        success: bool,
        summary: dict[str, Any],
        payload: Optional[dict[str, Any]] = None,
        error_message: Optional[str] = None,
    ) -> None:
        execution_request_id = job.execution_request_id if job else None
        job_id = job.job_id if job else None
        event_id = execution_request_id or job_id or str(uuid.uuid4())
        result_payload: dict[str, Any] = {
            "event_id": event_id,
            "job_id": job_id or event_id,
            "execution_request_id": execution_request_id,
            "robot_technical_id": self.technical_id,
            "display_name": self.display_name,
            "status": "completed" if success else "failed",
            "started_at": summary.get("started_at"),
            "finished_at": summary.get("finished_at") or utc_now_iso(),
            "error_message": error_message,
            "summary": summary,
            "payload": payload or {},
        }
        write_json_atomic(self.result_path, result_payload)
        self.write_heartbeat(
            status="active",
            message="result_ready",
            current_job_id=None,
            current_execution_request_id=None,
        )


@dataclass(slots=True)
class RuntimeEnvironment:
    paths: RuntimePaths
    logger: RuntimeLogger
    json_runtime: JsonRobotRuntime

    def resolve_supabase_service_role_key(self) -> str:
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

    def generate_run_id(self) -> str:
        return datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]

    def create_run_output_dir(self, run_id: str) -> Path:
        path = self.paths.output_dir / run_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def mark_inactive(self, reason: str = "application_exit") -> None:
        try:
            self.json_runtime.write_heartbeat(status="inactive", message=reason)
        except Exception:
            pass


def build_runtime(sink: Optional[Callable[[str], None]] = None) -> RuntimeEnvironment:
    paths = bootstrap_environment()
    logger = RuntimeLogger(paths.runtime_log_path, sink=sink)
    if not paths.certificates_registry_path.exists():
        paths.certificates_registry_path.write_text("[]\n", encoding="utf-8")
    json_runtime = JsonRobotRuntime(ROBOT_TECHNICAL_ID, ROBOT_DISPLAY_NAME, paths.json_dir)
    return RuntimeEnvironment(paths=paths, logger=logger, json_runtime=json_runtime)

# === dashboard_client ===
import hashlib
import os
from typing import Any, Iterable, Optional

import requests



class DashboardClient:
    def __init__(self, runtime_env: RuntimeEnvironment) -> None:
        self.runtime = runtime_env
        self.supabase_url = (os.getenv("SUPABASE_URL") or "").strip()
        self.supabase_key = runtime_env.resolve_supabase_service_role_key()
        self.server_api_url = (
            os.getenv("FOLDER_STRUCTURE_API_URL") or os.getenv("SERVER_API_URL") or ""
        ).strip().rstrip("/")
        self.connector_secret = (os.getenv("CONNECTOR_SECRET") or "").strip()
        self._office_context: Optional[OfficeContext] = None
        self._robot_config_cache: Optional[dict[str, Any]] = None
        self._registered_robot_id: Optional[str] = None

    def is_configured(self) -> bool:
        return bool(self.supabase_url and self.supabase_key)

    def _supabase(self):
        if not self.supabase_url or not self.supabase_key:
            raise RuntimeError("Supabase não configurado. Defina SUPABASE_URL e uma service role key.")
        try:
            from supabase import create_client
        except Exception as exc:
            raise RuntimeError("Biblioteca supabase não disponível no ambiente do robô.") from exc
        return create_client(self.supabase_url, self.supabase_key)

    def build_server_api_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.server_api_url and "ngrok" in self.server_api_url.lower():
            headers["ngrok-skip-browser-warning"] = "true"
        if self.connector_secret:
            hashed = hashlib.sha256(self.connector_secret.encode("utf-8")).hexdigest()
            headers["Authorization"] = f"Bearer {hashed}"
        return headers

    def fetch_robot_config_from_api(self) -> Optional[dict[str, Any]]:
        if not self.server_api_url:
            return None
        url = f"{self.server_api_url}/api/robot-config"
        response = requests.get(
            url,
            params={"technical_id": ROBOT_TECHNICAL_ID},
            headers=self.build_server_api_headers(),
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("Resposta inválida da API /api/robot-config.")
        self._robot_config_cache = payload
        return payload

    def register_robot_presence(self, status: str = "active") -> Optional[str]:
        try:
            client = self._supabase()
            api_cfg = self._robot_config_cache
            if api_cfg is None:
                try:
                    api_cfg = self.fetch_robot_config_from_api()
                except Exception:
                    api_cfg = None
            payload = {
                "display_name": ROBOT_DISPLAY_NAME,
                "status": status,
                "last_heartbeat_at": utc_now_iso(),
                "segment_path": str((api_cfg or {}).get("segment_path") or "").strip() or None,
                "notes_mode": str((api_cfg or {}).get("notes_mode") or "").strip() or None,
            }
            response = client.table("robots").select("id").eq("technical_id", ROBOT_TECHNICAL_ID).limit(1).execute()
            rows = getattr(response, "data", None) or []
            if rows:
                robot_id = str(rows[0].get("id") or "").strip()
                if robot_id:
                    client.table("robots").update(payload).eq("id", robot_id).execute()
                    self._registered_robot_id = robot_id
                    return robot_id
            insert_payload = {"technical_id": ROBOT_TECHNICAL_ID, **payload}
            inserted = client.table("robots").insert(insert_payload).execute()
            inserted_rows = getattr(inserted, "data", None) or []
            if inserted_rows:
                robot_id = str(inserted_rows[0].get("id") or "").strip()
                if robot_id:
                    self._registered_robot_id = robot_id
                    return robot_id
            reread = client.table("robots").select("id").eq("technical_id", ROBOT_TECHNICAL_ID).limit(1).execute()
            reread_rows = getattr(reread, "data", None) or []
            if reread_rows:
                robot_id = str(reread_rows[0].get("id") or "").strip()
                self._registered_robot_id = robot_id or None
                return robot_id or None
            self.runtime.logger.warning("Falha ao registrar o robô na tabela robots: insert sem retorno.")
            return None
        except Exception as exc:
            self.runtime.logger.warning(f"Falha ao registrar o robô na tabela robots: {exc}")
            return None

    def update_robot_presence(self, status: str = "active", robot_id: str = "") -> None:
        try:
            client = self._supabase()
            update_payload = {
                "status": status,
                "last_heartbeat_at": utc_now_iso(),
            }
            target_id = str(robot_id or self._registered_robot_id or "").strip()
            if target_id:
                client.table("robots").update(update_payload).eq("id", target_id).execute()
                return
            client.table("robots").update(update_payload).eq("technical_id", ROBOT_TECHNICAL_ID).execute()
        except Exception as exc:
            self.runtime.logger.warning(f"Falha ao atualizar heartbeat do robô na tabela robots: {exc}")

    def resolve_office_context(self, job: Optional[JobPayload] = None, force_refresh: bool = False) -> OfficeContext:
        if self._office_context and not force_refresh:
            return self._office_context

        payload: Optional[dict[str, Any]] = None
        if force_refresh or self._robot_config_cache is None:
            try:
                payload = self.fetch_robot_config_from_api()
            except Exception as exc:
                self.runtime.logger.warning(f"Falha ao consultar /api/robot-config: {exc}")
        else:
            payload = self._robot_config_cache

        office_id = ""
        office_server_id = ""
        base_path = ""
        segment_path = ""
        notes_mode = ""
        source = ""

        if isinstance(payload, dict):
            office_id = str(payload.get("office_id") or "").strip()
            office_server_id = str(payload.get("office_server_id") or "").strip()
            base_path = str(payload.get("base_path") or "").strip()
            segment_path = str(payload.get("segment_path") or "").strip()
            notes_mode = str(payload.get("notes_mode") or "").strip()
            if office_id:
                source = "server_api"

        if not office_id and job:
            office_id = job.office_id
            if office_id:
                source = "job_json"

        if not office_id:
            office_id = str(os.getenv("OFFICE_ID") or "").strip()
            office_server_id = office_server_id or str(os.getenv("OFFICE_SERVER_ID") or "").strip()
            if office_id:
                source = "env"

        context = OfficeContext(
            office_id=office_id,
            office_server_id=office_server_id,
            base_path=base_path,
            segment_path=segment_path,
            notes_mode=notes_mode,
            source=source,
        )
        self._office_context = context
        return context

    def _normalize_job_companies(
        self,
        job: Optional[JobPayload],
        company_ids: Optional[Iterable[str]] = None,
    ) -> list[CompanyRecord]:
        if not job or not job.companies:
            return []
        wanted = {str(item).strip() for item in (company_ids or []) if str(item).strip()}
        rows: list[CompanyRecord] = []
        for row in job.companies:
            company_id = str(row.get("company_id") or row.get("id") or "").strip()
            if not company_id or (wanted and company_id not in wanted):
                continue
            name = str(row.get("name") or "").strip()
            document = only_digits(row.get("document") or row.get("doc") or "")
            active = bool(row.get("active", True))
            eligible = bool(row.get("eligible", active))
            block_reason = str(row.get("block_reason") or "").strip()
            auth_mode = str(
                row.get("auth_mode")
                or ("certificate" if row.get("cert_blob_b64") or row.get("cert_path") else "password")
            ).strip().lower()
            if auth_mode not in {"password", "certificate"}:
                auth_mode = "password"
            if not active and not block_reason:
                block_reason = "Empresa inativa no dashboard."
            rows.append(
                CompanyRecord(
                    company_id=company_id,
                    name=name,
                    document=document,
                    active=active,
                    eligible=eligible and active,
                    block_reason=block_reason,
                    source="job_json",
                    auth_mode=auth_mode,
                    cert_password=str(row.get("cert_password") or "").strip(),
                    cert_blob_b64=str(row.get("cert_blob_b64") or "").strip(),
                    cert_path=str(row.get("cert_path") or "").strip(),
                    raw=dict(row),
                )
            )
        return sorted(rows, key=lambda item: item.name.lower())

    def load_companies(
        self,
        office_context: OfficeContext,
        *,
        job: Optional[JobPayload] = None,
        company_ids: Optional[Iterable[str]] = None,
    ) -> list[CompanyRecord]:
        snapshot_rows = self._normalize_job_companies(job, company_ids)
        if snapshot_rows:
            return snapshot_rows

        if not office_context.office_id:
            raise RuntimeError("office_id não resolvido. Verifique o conector da VM e /api/robot-config.")

        client = self._supabase()
        wanted_ids = [str(item).strip() for item in (company_ids or []) if str(item).strip()]
        config_query = (
            client.table("company_robot_config")
            .select("company_id,enabled,settings,auth_mode")
            .eq("office_id", office_context.office_id)
            .eq("robot_technical_id", ROBOT_TECHNICAL_ID)
            .eq("enabled", True)
        )
        if wanted_ids:
            config_query = config_query.in_("company_id", wanted_ids)
        config_rows = getattr(config_query.execute(), "data", None) or []

        config_by_company: dict[str, dict[str, Any]] = {}
        enabled_company_ids: list[str] = []
        for row in config_rows:
            company_id = str(row.get("company_id") or "").strip()
            if not company_id:
                continue
            normalized_row = dict(row)
            settings = normalized_row.get("settings")
            if isinstance(settings, str):
                try:
                    settings = json.loads(settings)
                except Exception:
                    settings = {}
            if not isinstance(settings, dict):
                settings = {}
            normalized_row["settings"] = settings
            config_by_company[company_id] = normalized_row
            enabled_company_ids.append(company_id)

        target_company_ids = enabled_company_ids or wanted_ids
        companies_query = (
            client.table("companies")
            .select("id,name,document,active,office_id,auth_mode,cert_blob_b64,cert_password")
            .eq("office_id", office_context.office_id)
            .order("name")
        )
        if target_company_ids:
            companies_query = companies_query.in_("id", target_company_ids)
        elif not enabled_company_ids:
            companies_query = companies_query.eq("active", True)
        company_rows = getattr(companies_query.execute(), "data", None) or []

        normalized: list[CompanyRecord] = []
        for row in company_rows:
            company_id = str(row.get("id") or "").strip()
            name = str(row.get("name") or "").strip()
            document = only_digits(row.get("document") or "")
            active = bool(row.get("active", False))
            company_auth_mode = str(row.get("auth_mode") or ("certificate" if row.get("cert_blob_b64") else "password")).strip().lower()
            if company_auth_mode not in {"password", "certificate"}:
                company_auth_mode = "password"
            cfg = config_by_company.get(company_id)
            cfg_settings = dict((cfg or {}).get("settings") or {})
            cfg_auth_mode = str((cfg or {}).get("auth_mode") or cfg_settings.get("auth_mode") or "").strip().lower()
            if cfg_auth_mode not in {"", "password", "certificate"}:
                cfg_auth_mode = ""
            auth_mode = cfg_auth_mode or company_auth_mode
            cert_password = str(cfg_settings.get("cert_password") or row.get("cert_password") or "").strip()
            cert_blob_b64 = str(cfg_settings.get("cert_blob_b64") or row.get("cert_blob_b64") or "").strip()
            cert_path = str(cfg_settings.get("cert_path") or "").strip()
            if auth_mode != "certificate":
                cert_password = ""
                cert_blob_b64 = ""
                cert_path = ""
            if enabled_company_ids:
                eligible = active and bool(cfg)
                block_reason = ""
                if not active:
                    block_reason = "Empresa inativa no dashboard."
                elif not cfg:
                    block_reason = "Empresa sem company_robot_config habilitado para este robô."
            else:
                eligible = active
                block_reason = "" if active else "Empresa inativa no dashboard."
            normalized.append(
                CompanyRecord(
                    company_id=company_id,
                    name=name,
                    document=document,
                    active=active,
                    eligible=eligible,
                    block_reason=block_reason,
                    source="company_robot_config" if enabled_company_ids else "companies_fallback",
                    auth_mode=auth_mode,
                    cert_password=cert_password,
                    cert_blob_b64=cert_blob_b64,
                    cert_path=cert_path,
                    raw={"company": dict(row), "config": dict(cfg or {})},
                )
            )

        if wanted_ids:
            missing = {item for item in wanted_ids if item not in {company.company_id for company in normalized}}
            for company_id in sorted(missing):
                normalized.append(
                    CompanyRecord(
                        company_id=company_id,
                        name=f"Empresa {company_id}",
                        document="",
                        active=False,
                        eligible=False,
                        block_reason="Empresa não encontrada no escopo do escritório.",
                        source="missing",
                        auth_mode="password",
                        raw={},
                    )
                )

        normalized.sort(key=lambda item: item.name.lower())
        return normalized

# === certificate_auth ===
import json
import os
import platform
import re
import shutil
import subprocess
import textwrap
from pathlib import Path
from typing import Optional



def _ps_quote(value: str) -> str:
    return str(value or "").replace("'", "''")


def ensure_windows_environment() -> None:
    if platform.system().lower() != "windows":
        raise RuntimeError("Este robô é Windows-only para o fluxo de certificado digital via PowerShell/UI Automation.")
    if shutil.which("powershell") is None:
        raise RuntimeError("PowerShell não encontrado no PATH. O fluxo de certificado do e-CAC depende dele.")


def run_powershell(script: str, timeout_ms: int = 60000) -> str:
    ensure_windows_environment()
    creationflags = 0
    try:
        creationflags = subprocess.CREATE_NO_WINDOW  # type: ignore[attr-defined]
    except Exception:
        creationflags = 0

    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-WindowStyle",
            "Hidden",
            "-Command",
            script,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
        timeout=max(1, timeout_ms // 1000),
        check=False,
        creationflags=creationflags,
    )
    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    if result.returncode != 0:
        message = f"PowerShell retornou código {result.returncode}."
        if stderr:
            message += f"\nErro:\n{stderr[-1800:]}"
        elif stdout:
            message += f"\nSaída:\n{stdout[-1800:]}"
        raise RuntimeError(message)
    return stdout


def load_registry(path: Path) -> list[CertificateMetadata]:
    payload = read_json(path, default=[])
    if not isinstance(payload, list):
        return []
    certificates: list[CertificateMetadata] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        certificates.append(
            CertificateMetadata(
                alias=str(item.get("alias") or item.get("name") or "").strip(),
                subject=str(item.get("subject") or "").strip(),
                issuer=str(item.get("issuer") or "").strip(),
                thumbprint=str(item.get("thumbprint") or "").strip(),
                pfx_path=str(item.get("pfx_path") or "").strip(),
                source=str(item.get("source") or "registry").strip(),
            )
        )
    return certificates


def save_registry(path: Path, certificates: list[CertificateMetadata]) -> None:
    rows = [item.to_dict() for item in certificates]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build_selector_candidates(subject: str, alias: str = "") -> list[str]:
    candidates: list[str] = []
    raw_values = [alias.strip(), subject.strip()]
    for value in raw_values:
        if value and value not in candidates:
            candidates.append(value)

    match = re.search(r"CN=([^,]+)", subject or "", flags=re.IGNORECASE)
    if match:
        cn = match.group(1).strip()
        if cn and cn not in candidates:
            candidates.append(cn)

    for current in list(candidates):
        normalized = current.replace("CN=", "").strip()
        if normalized and normalized not in candidates:
            candidates.append(normalized)
        name_only = normalized.split(":")[0].strip()
        if name_only and name_only not in candidates:
            candidates.append(name_only)
    return candidates


def import_pfx_and_get_metadata(pfx_path: Path, pfx_password: str) -> CertificateMetadata:
    ensure_windows_environment()
    if not pfx_path.exists():
        raise FileNotFoundError(f"Arquivo PFX não encontrado: {pfx_path}")
    script = textwrap.dedent(
        f"""
        $path = '{_ps_quote(str(pfx_path))}'
        $password = ConvertTo-SecureString '{_ps_quote(pfx_password)}' -AsPlainText -Force
        $cert = Import-PfxCertificate -FilePath $path -CertStoreLocation Cert:\\CurrentUser\\My -Password $password
        if (-not $cert) {{
            throw 'Falha ao importar o certificado para Cert:\\CurrentUser\\My.'
        }}
        ($cert | Select-Object -First 1 Subject, Thumbprint, Issuer) | ConvertTo-Json -Compress
        """
    )
    payload = json.loads(run_powershell(script))
    return CertificateMetadata(
        alias=pfx_path.stem,
        subject=str(payload.get("Subject") or "").strip(),
        issuer=str(payload.get("Issuer") or "").strip(),
        thumbprint=str(payload.get("Thumbprint") or "").strip(),
        pfx_path=str(pfx_path),
        source="imported_pfx",
    )


def find_certificate_in_store(subject: str, issuer: str = "", thumbprint: str = "") -> CertificateMetadata:
    ensure_windows_environment()
    filters: list[str] = []
    if thumbprint:
        filters.append(f"$_.Thumbprint -eq '{_ps_quote(thumbprint)}'")
    if subject:
        filters.append(f"$_.Subject -like '*{_ps_quote(subject)}*'")
    if issuer:
        filters.append(f"$_.Issuer -like '*{_ps_quote(issuer)}*'")
    where_clause = " -and ".join(filters) if filters else "$true"
    script = textwrap.dedent(
        f"""
        $cert = Get-ChildItem Cert:\\CurrentUser\\My |
            Where-Object {{ {where_clause} }} |
            Select-Object -First 1 Subject, Thumbprint, Issuer
        if (-not $cert) {{
            throw 'Certificado alvo não encontrado em Cert:\\CurrentUser\\My.'
        }}
        $cert | ConvertTo-Json -Compress
        """
    )
    payload = json.loads(run_powershell(script))
    return CertificateMetadata(
        alias="",
        subject=str(payload.get("Subject") or "").strip(),
        issuer=str(payload.get("Issuer") or "").strip(),
        thumbprint=str(payload.get("Thumbprint") or "").strip(),
        source="store",
    )


def register_certificate(
    runtime_env: RuntimeEnvironment,
    *,
    alias: str,
    pfx_path: Path,
    password: str,
) -> CertificateMetadata:
    metadata = import_pfx_and_get_metadata(pfx_path, password)
    metadata.alias = alias.strip() or metadata.alias or pfx_path.stem
    registry = load_registry(runtime_env.paths.certificates_registry_path)
    registry = [item for item in registry if item.thumbprint != metadata.thumbprint]
    registry.append(metadata)
    save_registry(runtime_env.paths.certificates_registry_path, registry)
    return metadata


def resolve_certificate_from_env() -> Optional[CertificateMetadata]:
    pfx_path = (os.getenv("ECAC_CERT_PFX_PATH") or "").strip()
    pfx_password = os.getenv("ECAC_CERT_PFX_PASSWORD") or ""
    subject = (os.getenv("ECAC_CERT_SUBJECT") or "").strip()
    issuer = (os.getenv("ECAC_CERT_ISSUER") or "").strip()

    if pfx_path:
        metadata = import_pfx_and_get_metadata(Path(pfx_path), pfx_password)
        if subject:
            metadata.subject = subject
        if issuer:
            metadata.issuer = issuer
        metadata.alias = metadata.alias or Path(pfx_path).stem
        metadata.source = "env_pfx"
        return metadata

    if subject:
        metadata = find_certificate_in_store(subject=subject, issuer=issuer)
        metadata.alias = metadata.alias or subject
        metadata.source = "env_store"
        return metadata
    return None


def peek_certificate_configuration_from_env() -> Optional[CertificateMetadata]:
    pfx_path = (os.getenv("ECAC_CERT_PFX_PATH") or "").strip()
    subject = (os.getenv("ECAC_CERT_SUBJECT") or "").strip()
    issuer = (os.getenv("ECAC_CERT_ISSUER") or "").strip()
    if not any([pfx_path, subject, issuer]):
        return None
    return CertificateMetadata(
        alias=Path(pfx_path).stem if pfx_path else (subject or "Certificado via .env"),
        subject=subject,
        issuer=issuer,
        pfx_path=pfx_path,
        source="env_hint",
    )


def _decode_certificate_blob(cert_blob_b64: str) -> bytes:
    raw = str(cert_blob_b64 or "").strip()
    if not raw:
        raise ValueError("cert_blob_b64 vazio.")
    if "," in raw and raw.lower().startswith("data:"):
        raw = raw.split(",", 1)[1]
    raw = "".join(raw.split())
    return base64.b64decode(raw)


def peek_certificate_configuration_from_dashboard(companies: list[CompanyRecord]) -> Optional[CertificateMetadata]:
    for company in companies:
        auth_mode = str(company.auth_mode or "").strip().lower()
        if auth_mode != "certificate" and not company.cert_blob_b64 and not company.cert_path:
            continue
        if not company.cert_blob_b64 and not company.cert_path:
            continue
        return CertificateMetadata(
            alias=f"{company.name} (dashboard)",
            subject="",
            issuer="",
            thumbprint="",
            pfx_path="",
            source="dashboard_hint",
        )
    return None


def resolve_certificate_from_dashboard(
    runtime_env: RuntimeEnvironment,
    companies: list[CompanyRecord],
) -> Optional[CertificateMetadata]:
    for company in companies:
        auth_mode = str(company.auth_mode or "").strip().lower()
        if auth_mode != "certificate" and not company.cert_blob_b64 and not company.cert_path:
            continue
        if not company.cert_blob_b64 and not company.cert_path:
            continue
        cert_password = str(company.cert_password or "").strip()
        if not cert_password:
            raise RuntimeError(f"Senha do certificado não configurada no dashboard para {company.name}.")
        if company.cert_blob_b64:
            try:
                cert_data = _decode_certificate_blob(company.cert_blob_b64)
            except Exception as exc:
                raise RuntimeError(f"Falha ao decodificar cert_blob_b64 do dashboard para {company.name}: {exc}") from exc
        else:
            cert_path = Path(str(company.cert_path or "")).expanduser()
            if not cert_path.is_file():
                raise RuntimeError(f"Caminho de certificado do dashboard invÃ¡lido para {company.name}: {cert_path}")
            cert_data = cert_path.read_bytes()

        cert_dir = runtime_env.paths.data_dir / "temp_certificates"
        cert_dir.mkdir(parents=True, exist_ok=True)
        temp_pfx = cert_dir / f"{company.company_id}_{slugify(company.name or company.document)}.pfx"
        temp_pfx.write_bytes(cert_data)

        metadata = import_pfx_and_get_metadata(temp_pfx, cert_password)
        metadata.alias = f"{company.name} (dashboard)"
        metadata.pfx_path = str(temp_pfx)
        metadata.source = "dashboard_company"
        return metadata
    return None


def resolve_certificate(
    runtime_env: RuntimeEnvironment,
    companies: Optional[list[CompanyRecord]] = None,
    preferred_thumbprint: str = "",
) -> Optional[CertificateMetadata]:
    env_certificate = resolve_certificate_from_env()
    if env_certificate:
        return env_certificate

    if companies:
        dashboard_certificate = resolve_certificate_from_dashboard(runtime_env, companies)
        if dashboard_certificate:
            return dashboard_certificate

    registry = load_registry(runtime_env.paths.certificates_registry_path)
    if not registry:
        return None

    selected: Optional[CertificateMetadata] = None
    if preferred_thumbprint:
        selected = next((item for item in registry if item.thumbprint == preferred_thumbprint), None)
    if selected is None:
        selected = registry[0]
    if selected is None:
        return None

    store_certificate = find_certificate_in_store(
        subject=selected.subject,
        issuer=selected.issuer,
        thumbprint=selected.thumbprint,
    )
    store_certificate.alias = selected.alias or store_certificate.subject
    store_certificate.pfx_path = selected.pfx_path
    store_certificate.source = selected.source
    return store_certificate


def wait_for_certificate_dialog(timeout_seconds: int = 12) -> bool:
    script = textwrap.dedent(
        f"""
        Add-Type -AssemblyName UIAutomationClient
        Add-Type -AssemblyName UIAutomationTypes
        $deadline = (Get-Date).AddSeconds({timeout_seconds})
        $root = [System.Windows.Automation.AutomationElement]::RootElement
        $dialogTitle = 'Selecione um certificado'
        while ((Get-Date) -lt $deadline) {{
            $condition = New-Object System.Windows.Automation.PropertyCondition(
                [System.Windows.Automation.AutomationElement]::NameProperty,
                $dialogTitle
            )
            $dialog = $root.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $condition)
            if ($dialog) {{
                Write-Output 'FOUND'
                exit 0
            }}
            Start-Sleep -Milliseconds 250
        }}
        Write-Output 'NOT_FOUND'
        """
    )
    return run_powershell(script, timeout_ms=(timeout_seconds + 5) * 1000).strip() == "FOUND"


def select_certificate_dialog(certificate: CertificateMetadata, timeout_ms: int = 45000) -> None:
    targets = build_selector_candidates(certificate.subject, certificate.alias)
    if not targets:
        raise RuntimeError("Nenhum seletor de certificado disponível para automação da janela nativa.")

    powershell_targets = "@(" + ", ".join("'" + _ps_quote(item) + "'" for item in targets) + ")"
    script = textwrap.dedent(
        f"""
        Add-Type -AssemblyName UIAutomationClient
        Add-Type -AssemblyName UIAutomationTypes
        Add-Type @"
        using System;
        using System.Runtime.InteropServices;
        public struct POINT {{
          public int X;
          public int Y;
        }}
        public class NativeCertUi {{
          [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr hWnd);
          [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);
          [DllImport("user32.dll")] public static extern IntPtr GetForegroundWindow();
          [DllImport("user32.dll")] public static extern bool GetCursorPos(out POINT lpPoint);
          [DllImport("user32.dll")] public static extern bool SetCursorPos(int X, int Y);
          [DllImport("user32.dll")] public static extern void mouse_event(uint dwFlags, uint dx, uint dy, uint dwData, UIntPtr dwExtraInfo);
        }}
        "@

        $targets = {powershell_targets}
        $dialogTitle = 'Selecione um certificado'
        $root = [System.Windows.Automation.AutomationElement]::RootElement
        $deadline = (Get-Date).AddSeconds(35)
        $dialog = $null

        while ((Get-Date) -lt $deadline -and -not $dialog) {{
            $condition = New-Object System.Windows.Automation.PropertyCondition(
                [System.Windows.Automation.AutomationElement]::NameProperty,
                $dialogTitle
            )
            $dialog = $root.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $condition)
            if (-not $dialog) {{ Start-Sleep -Milliseconds 250 }}
        }}

        if (-not $dialog) {{
            throw 'Janela "Selecione um certificado" não apareceu dentro do timeout.'
        }}

        function Matches-Target([string]$name, $targets) {{
            if (-not $name) {{ return $false }}
            foreach ($target in $targets) {{
                if (-not $target) {{ continue }}
                if ($name -eq $target -or $target.Contains($name) -or $name.Contains($target)) {{
                    return $true
                }}
            }}
            return $false
        }}

        function Find-DialogRows($dialog) {{
            $all = $dialog.FindAll([System.Windows.Automation.TreeScope]::Descendants, [System.Windows.Automation.Condition]::TrueCondition)
            $rows = @()
            for ($i = 0; $i -lt $all.Count; $i++) {{
                $item = $all.Item($i)
                $type = [string]$item.Current.ControlType.ProgrammaticName
                if ($type -in @('ControlType.DataItem', 'ControlType.ListItem')) {{
                    $rows += $item
                }}
            }}
            return $rows
        }}

        function Try-Select($element) {{
            if (-not $element) {{ return $false }}
            foreach ($pattern in $element.GetSupportedPatterns()) {{
                try {{
                    $programmatic = [string]$pattern.ProgrammaticName
                    if ($programmatic -eq 'ScrollItemPatternIdentifiers.Pattern') {{
                        $scrollItem = $element.GetCurrentPattern([System.Windows.Automation.ScrollItemPattern]::Pattern)
                        $scrollItem.ScrollIntoView()
                    }}
                    if ($programmatic -eq 'SelectionItemPatternIdentifiers.Pattern') {{
                        $selectionItem = $element.GetCurrentPattern([System.Windows.Automation.SelectionItemPattern]::Pattern)
                        $selectionItem.Select()
                        return $true
                    }}
                    if ($programmatic -eq 'LegacyIAccessiblePatternIdentifiers.Pattern') {{
                        $legacy = $element.GetCurrentPattern([System.Windows.Automation.LegacyIAccessiblePattern]::Pattern)
                        $legacy.DoDefaultAction()
                        return $true
                    }}
                }} catch {{}}
            }}
            try {{
                $element.SetFocus()
                return $true
            }} catch {{
                return $false
            }}
        }}

        function Get-OkButton($dialog) {{
            $condition = New-Object System.Windows.Automation.PropertyCondition(
                [System.Windows.Automation.AutomationElement]::NameProperty,
                'OK'
            )
            return $dialog.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $condition)
        }}

        function Try-ClickOk($button) {{
            if (-not $button) {{ return $false }}
            foreach ($pattern in $button.GetSupportedPatterns()) {{
                try {{
                    $programmatic = [string]$pattern.ProgrammaticName
                    if ($programmatic -eq 'InvokePatternIdentifiers.Pattern') {{
                        $invoke = $button.GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern)
                        $invoke.Invoke()
                        return $true
                    }}
                    if ($programmatic -eq 'LegacyIAccessiblePatternIdentifiers.Pattern') {{
                        $legacy = $button.GetCurrentPattern([System.Windows.Automation.LegacyIAccessiblePattern]::Pattern)
                        $legacy.DoDefaultAction()
                        return $true
                    }}
                }} catch {{}}
            }}
            return $false
        }}

        function Wait-Closed() {{
            $closeDeadline = (Get-Date).AddSeconds(8)
            while ((Get-Date) -lt $closeDeadline) {{
                $condition = New-Object System.Windows.Automation.PropertyCondition(
                    [System.Windows.Automation.AutomationElement]::NameProperty,
                    $dialogTitle
                )
                $stillOpen = $root.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $condition)
                if (-not $stillOpen) {{
                    return $true
                }}
                Start-Sleep -Milliseconds 180
            }}
            return $false
        }}

        $rows = Find-DialogRows $dialog
        $matched = $null
        foreach ($row in $rows) {{
            $text = [string]$row.Current.Name
            if (Matches-Target $text $targets) {{
                $matched = $row
                break
            }}
        }}

        $okButton = Get-OkButton $dialog
        if ($matched -and (Try-Select $matched) -and $okButton -and (Try-ClickOk $okButton) -and (Wait-Closed)) {{
            exit 0
        }}

        $chromeWindow = $null
        $children = $root.FindAll([System.Windows.Automation.TreeScope]::Children, [System.Windows.Automation.Condition]::TrueCondition)
        for ($i = 0; $i -lt $children.Count; $i++) {{
            $candidate = $children.Item($i)
            $title = ([string]$candidate.Current.Name).ToLower()
            if ($title.Contains('chrome') -or $title.Contains('edge') -or $title.Contains('gov.br')) {{
                $chromeWindow = $candidate
                break
            }}
        }}
        if (-not $chromeWindow) {{
            throw 'Falha na UI Automation direta e nenhuma janela Chrome/Edge foi localizada para fallback.'
        }}
        $handle = [IntPtr]$chromeWindow.Current.NativeWindowHandle
        if ($handle -eq [IntPtr]::Zero) {{
            throw 'A janela principal do navegador não possui handle válido para fallback.'
        }}

        $previous = [NativeCertUi]::GetForegroundWindow()
        $cursor = New-Object POINT
        [NativeCertUi]::GetCursorPos([ref]$cursor) | Out-Null

        try {{
            [NativeCertUi]::ShowWindow($handle, 5) | Out-Null
            [NativeCertUi]::SetForegroundWindow($handle) | Out-Null
            Start-Sleep -Milliseconds 200
            $rect = $chromeWindow.Current.BoundingRectangle
            $clickX = [int]($rect.Left + ($rect.Width * 0.48))
            $clickY = [int]($rect.Top + ($rect.Height * 0.31))
            [NativeCertUi]::SetCursorPos($clickX, $clickY) | Out-Null
            Start-Sleep -Milliseconds 80
            [NativeCertUi]::mouse_event(0x0002, 0, 0, 0, [UIntPtr]::Zero)
            Start-Sleep -Milliseconds 30
            [NativeCertUi]::mouse_event(0x0004, 0, 0, 0, [UIntPtr]::Zero)
            Start-Sleep -Milliseconds 120
            $wsh = New-Object -ComObject WScript.Shell
            $wsh.SendKeys('{{HOME}}')
            Start-Sleep -Milliseconds 150
            for ($step = 0; $step -lt 50; $step++) {{
                $focused = [System.Windows.Automation.AutomationElement]::FocusedElement
                $focusedName = ''
                if ($focused) {{
                    $focusedName = [string]$focused.Current.Name
                }}
                if (Matches-Target $focusedName $targets) {{
                    $wsh.SendKeys('~')
                    if (Wait-Closed) {{
                        exit 0
                    }}
                    throw 'O seletor do certificado permaneceu aberto após confirmar o item focado.'
                }}
                if ($step -lt 49) {{
                    $wsh.SendKeys('{{DOWN}}')
                    Start-Sleep -Milliseconds 140
                }}
            }}
        }} finally {{
            [NativeCertUi]::SetCursorPos($cursor.X, $cursor.Y) | Out-Null
            if ($previous -ne [IntPtr]::Zero) {{
                [NativeCertUi]::SetForegroundWindow($previous) | Out-Null
            }}
        }}

        throw 'Não foi possível selecionar o certificado alvo na janela nativa.'
        """
    )
    run_powershell(script, timeout_ms=timeout_ms)



import types as _types_singlefile
certificate_auth = _types_singlefile.SimpleNamespace(
    ensure_windows_environment=ensure_windows_environment,
    run_powershell=run_powershell,
    load_registry=load_registry,
    save_registry=save_registry,
    build_selector_candidates=build_selector_candidates,
    import_pfx_and_get_metadata=import_pfx_and_get_metadata,
    find_certificate_in_store=find_certificate_in_store,
    register_certificate=register_certificate,
    resolve_certificate_from_env=resolve_certificate_from_env,
    peek_certificate_configuration_from_env=peek_certificate_configuration_from_env,
    peek_certificate_configuration_from_dashboard=peek_certificate_configuration_from_dashboard,
    resolve_certificate_from_dashboard=resolve_certificate_from_dashboard,
    resolve_certificate=resolve_certificate,
    wait_for_certificate_dialog=wait_for_certificate_dialog,
    select_certificate_dialog=select_certificate_dialog,
)

# === ecac_browser ===
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional



@dataclass(slots=True)
class BrowserSession:
    playwright: object
    context: object
    page: object
    executable_path: str


def _playwright_browser_candidates() -> list[Path]:
    root = Path(os.environ.get("PLAYWRIGHT_BROWSERS_PATH") or "").expanduser()
    if not root.exists():
        return []
    candidates: list[Path] = []
    for folder in sorted(root.glob("chromium-*"), reverse=True):
        exe = folder / "chrome-win64" / "chrome.exe"
        if exe.exists():
            candidates.append(exe)
            continue
        exe = folder / "chrome-win" / "chrome.exe"
        if exe.exists():
            candidates.append(exe)
    return candidates


def resolve_browser_executable(runtime_env: RuntimeEnvironment) -> Path:
    base_dir = runtime_env.paths.base_dir
    runtime_dir = runtime_env.paths.runtime_dir
    candidates = [
        base_dir / "Chrome" / "chrome.exe",
        base_dir / "_internal" / "Chrome" / "chrome.exe",
        runtime_dir / "_internal" / "Chrome" / "chrome.exe",
        runtime_dir / "Chrome" / "chrome.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",
        Path(os.environ.get("PROGRAMFILES", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",
        Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
        Path(os.environ.get("PROGRAMFILES", "")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
        Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
    ]
    candidates.extend(_playwright_browser_candidates())

    seen: set[str] = set()
    normalized: list[Path] = []
    for candidate in candidates:
        key = str(candidate.resolve(strict=False)).lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(candidate)

    for candidate in normalized:
        if candidate.exists():
            return candidate

    checked = " | ".join(str(item) for item in normalized)
    raise FileNotFoundError(f"Chrome/Edge não encontrado. Caminhos verificados: {checked}")


def launch_browser(runtime_env: RuntimeEnvironment, *, headless: bool = False) -> BrowserSession:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise RuntimeError("Playwright não está disponível no ambiente do robô.") from exc

    executable = resolve_browser_executable(runtime_env)
    runtime_env.paths.chrome_profile_dir.mkdir(parents=True, exist_ok=True)
    playwright = sync_playwright().start()
    try:
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(runtime_env.paths.chrome_profile_dir),
            executable_path=str(executable),
            headless=headless,
            ignore_https_errors=True,
            no_viewport=True,
            args=[
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
            ],
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.set_default_timeout(30000)
        context.set_default_navigation_timeout(60000)
        return BrowserSession(
            playwright=playwright,
            context=context,
            page=page,
            executable_path=str(executable),
        )
    except Exception:
        playwright.stop()
        raise


def close_browser(session: Optional[BrowserSession]) -> None:
    if not session:
        return
    try:
        session.context.close()
    except Exception:
        pass
    try:
        session.playwright.stop()
    except Exception:
        pass

# === ecac_service ===
import csv
import json
import re
import traceback
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional


LOGIN_URL = "https://cav.receita.fazenda.gov.br/autenticacao/login"
SOURCE_SYSTEM = ROBOT_TECHNICAL_ID

# Ajuste fino de layout: centralize mudanças do portal aqui.
PROFILE_MENU_LABELS = [
    "Alterar perfil de acesso",
    "Perfil de acesso",
    "Trocar perfil",
    "Selecionar perfil",
]
MAILBOX_ENTRY_LABELS = [
    "Você tem novas mensagens",
    "Novas mensagens",
    "Caixa Postal",
    "Mensagens",
    "Caixa postal",
]
MODAL_CLOSE_LABELS = ["Fechar", "OK", "Entendi", "Entendido", "Continuar", "Não quero ver novamente"]
MESSAGE_ROW_SELECTORS = [
    "table tbody tr",
    "[role='row']",
    ".mat-row",
    ".datatable-row-wrapper",
    ".ui-datatable-data > tr",
    ".table-responsive tbody tr",
    ".lista-mensagens tr",
]
NEXT_PAGE_LABELS = ["Próximo", "Próxima", ">", "Avançar"]
DETAIL_CLOSE_LABELS = ["Fechar", "Voltar", "X", "Cancelar"]


def _first_line(text: str) -> str:
    for line in str(text or "").splitlines():
        line = line.strip()
        if line:
            return line
    return ""


def _stack_tail(exc: BaseException) -> str:
    return "".join(traceback.format_exception_only(type(exc), exc)).strip()


class EcacMailboxAutomation:
    def __init__(
        self,
        runtime_env: RuntimeEnvironment,
        session: BrowserSession,
        certificate: CertificateMetadata,
        *,
        stop_requested: Callable[[], bool],
    ) -> None:
        self.runtime = runtime_env
        self.session = session
        self.page = session.page
        self.certificate = certificate
        self.stop_requested = stop_requested

    def _iter_scopes(self) -> list[Any]:
        scopes = [self.page]
        try:
            scopes.extend(frame for frame in self.page.frames if frame != self.page.main_frame)
        except Exception:
            pass
        unique: list[Any] = []
        seen: set[int] = set()
        for scope in scopes:
            if id(scope) in seen:
                continue
            seen.add(id(scope))
            unique.append(scope)
        return unique

    def _wait_until(
        self,
        predicate: Callable[[], bool],
        *,
        timeout_ms: int,
        interval_ms: int = 250,
    ) -> bool:
        deadline = datetime.now().timestamp() + (timeout_ms / 1000)
        last_error: Optional[Exception] = None
        while datetime.now().timestamp() < deadline:
            if self.stop_requested():
                return False
            try:
                if predicate():
                    return True
            except Exception as exc:
                last_error = exc
            try:
                self.page.wait_for_timeout(interval_ms)
            except Exception:
                pass
        if last_error:
            self.runtime.logger.warning(f"Espera expirou: {last_error}")
        return False

    def _body_text(self, scope: Optional[Any] = None) -> str:
        target = scope or self.page
        try:
            return target.locator("body").inner_text(timeout=3000)
        except Exception:
            return ""

    def _page_contains(self, text: str) -> bool:
        lowered = text.lower()
        for scope in self._iter_scopes():
            try:
                if lowered in self._body_text(scope).lower():
                    return True
            except Exception:
                continue
        return False

    def _click_by_label(self, labels: list[str], *, timeout_ms: int = 7000) -> bool:
        for label in labels:
            pattern = re.compile(re.escape(label), re.IGNORECASE)
            for scope in self._iter_scopes():
                locators = [
                    scope.get_by_role("button", name=pattern),
                    scope.get_by_role("link", name=pattern),
                    scope.get_by_text(pattern),
                    scope.locator(f"[aria-label*='{label}']"),
                    scope.locator(f"text={label}"),
                ]
                for locator in locators:
                    try:
                        if locator.count() < 1:
                            continue
                        candidate = locator.first
                        candidate.wait_for(state="visible", timeout=1000)
                        candidate.click(timeout=timeout_ms, force=True)
                        return True
                    except Exception:
                        continue
        return False

    def _fill_company_search(self, company: CompanyRecord) -> bool:
        search_terms = [format_document(company.document), only_digits(company.document), company.name]
        selector_candidates = [
            "input[placeholder*='CNPJ']",
            "input[placeholder*='CPF']",
            "input[placeholder*='Nome']",
            "input[placeholder*='Pesquisar']",
            "input[placeholder*='empresa']",
            "input[type='search']",
            "input",
        ]
        for scope in self._iter_scopes():
            for selector in selector_candidates:
                locator = scope.locator(selector)
                try:
                    count = locator.count()
                except Exception:
                    continue
                for index in range(min(count, 8)):
                    field = locator.nth(index)
                    try:
                        if not field.is_visible():
                            continue
                        field.click()
                        field.fill("")
                        for term in search_terms:
                            if not term:
                                continue
                            field.fill(term)
                            self.page.wait_for_timeout(200)
                            return True
                    except Exception:
                        continue
        return False

    def _select_company_in_profile(self, company: CompanyRecord) -> bool:
        targets = [
            company.name,
            format_document(company.document),
            only_digits(company.document),
        ]
        for scope in self._iter_scopes():
            body = self._body_text(scope).lower()
            for target in targets:
                if target and target.lower() not in body:
                    continue
                pattern = re.compile(re.escape(target), re.IGNORECASE)
                candidates = [
                    scope.get_by_role("row", name=pattern),
                    scope.get_by_role("option", name=pattern),
                    scope.get_by_text(pattern),
                    scope.locator(f"text={target}"),
                ]
                for locator in candidates:
                    try:
                        if locator.count() < 1:
                            continue
                        locator.first.click(timeout=5000, force=True)
                        return True
                    except Exception:
                        continue
        return False

    def _confirm_profile_switch(self) -> bool:
        labels = ["Confirmar", "Selecionar", "Aplicar", "Alterar perfil", "OK"]
        return self._click_by_label(labels, timeout_ms=7000)

    def _current_profile_context(self) -> str:
        snippets: list[str] = []
        for scope in self._iter_scopes():
            text = self._body_text(scope)
            if not text:
                continue
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                if "perfil" in line.lower() or "cnpj" in line.lower() or "titular" in line.lower():
                    snippets.append(line)
                if len(snippets) >= 6:
                    break
            if len(snippets) >= 6:
                break
        return " | ".join(snippets[:6])

    def close_blocking_modals(self) -> None:
        for _ in range(6):
            clicked = False
            for label in MODAL_CLOSE_LABELS:
                clicked = self._click_by_label([label], timeout_ms=2000) or clicked
            for scope in self._iter_scopes():
                try:
                    locator = scope.locator("[aria-label*='Fechar'], [aria-label*='close'], .close, .btn-close")
                    if locator.count() > 0:
                        locator.first.click(timeout=1500, force=True)
                        clicked = True
                except Exception:
                    continue
            try:
                self.page.keyboard.press("Escape")
            except Exception:
                pass
            if not clicked:
                break
            self.page.wait_for_timeout(250)

    def is_authenticated(self) -> bool:
        try:
            if "/ecac/" in self.page.url and not self._page_contains("Acesso Gov BR"):
                return True
        except Exception:
            pass
        indicators = [
            "Alterar perfil de acesso",
            "Titular (Acesso GOV.BR por Certificado)",
            "Caixa Postal",
            "Mensagens",
        ]
        return any(self._page_contains(indicator) for indicator in indicators)

    def _click_access_gov_br(self) -> None:
        previous_url = self.page.url
        for _ in range(5):
            if self._click_by_label(["Acesso Gov BR"], timeout_ms=8000):
                moved = self._wait_until(
                    lambda: self.page.url != previous_url or "acesso.gov.br" in self.page.url or self._page_contains("Seu certificado digital"),
                    timeout_ms=12000,
                )
                if moved:
                    return
            self.page.reload(wait_until="domcontentloaded")
        raise RuntimeError("Falha ao acionar o botão 'Acesso Gov BR'.")

    def _click_cert_login(self) -> None:
        for _ in range(4):
            if self._click_by_label(["Seu certificado digital"], timeout_ms=10000):
                if certificate_auth.wait_for_certificate_dialog(timeout_seconds=10):
                    return
            self.page.reload(wait_until="domcontentloaded")
            self._wait_until(lambda: self._page_contains("Seu certificado digital"), timeout_ms=10000)
        raise RuntimeError("A janela nativa de certificado não apareceu após clicar em 'Seu certificado digital'.")

    def login(self) -> None:
        self.page.goto(LOGIN_URL, wait_until="domcontentloaded")
        if self.is_authenticated():
            self.close_blocking_modals()
            return

        self._click_access_gov_br()
        self._wait_until(
            lambda: "acesso.gov.br" in self.page.url or self._page_contains("Seu certificado digital"),
            timeout_ms=20000,
        )
        self._click_cert_login()
        certificate_auth.select_certificate_dialog(self.certificate)
        logged = self._wait_until(self.is_authenticated, timeout_ms=90000, interval_ms=500)
        if not logged:
            raise RuntimeError("O e-CAC não confirmou a sessão autenticada dentro do timeout.")
        self.close_blocking_modals()

    def ensure_logged_in(self) -> None:
        if self.is_authenticated():
            return
        self.runtime.logger.warning("Sessão do e-CAC não parece autenticada. Tentando reautenticação controlada.")
        self.login()

    def switch_company_profile(self, company: CompanyRecord) -> str:
        self.ensure_logged_in()
        self.close_blocking_modals()
        if not self._click_by_label(PROFILE_MENU_LABELS, timeout_ms=7000):
            raise RuntimeError("Fluxo de 'Alterar perfil de acesso' não foi localizado.")
        self._wait_until(
            lambda: self._page_contains("perfil") or self._page_contains("representado") or self._page_contains("CNPJ"),
            timeout_ms=10000,
        )
        if not self._fill_company_search(company):
            raise RuntimeError("Campo de busca do perfil não foi localizado.")
        self.page.wait_for_timeout(500)
        if not self._select_company_in_profile(company):
            raise RuntimeError(f"Empresa alvo não apareceu no seletor de perfil: {company.name}.")
        self._confirm_profile_switch()

        targets = [company.name.lower(), only_digits(company.document), format_document(company.document).lower()]
        changed = self._wait_until(
            lambda: any(target and target in self._current_profile_context().lower() for target in targets),
            timeout_ms=15000,
            interval_ms=400,
        )
        context = self._current_profile_context()
        if not changed and not any(target and target in self._body_text().lower() for target in targets):
            raise RuntimeError("A troca de perfil não foi validada visualmente no portal.")
        return context

    def open_mailbox(self) -> None:
        self.close_blocking_modals()
        if self._click_by_label(MAILBOX_ENTRY_LABELS, timeout_ms=8000):
            ready = self._wait_until(
                lambda: self._message_rows_locator()[0] is not None or self._page_contains("mensagem"),
                timeout_ms=15000,
            )
            if ready:
                return
        raise RuntimeError("A entrada da Caixa Postal / Mensagens não foi localizada.")

    def _message_rows_locator(self) -> tuple[Optional[Any], Optional[str], Optional[Any]]:
        for scope in self._iter_scopes():
            for selector in MESSAGE_ROW_SELECTORS:
                locator = scope.locator(selector)
                try:
                    count = locator.count()
                except Exception:
                    continue
                if count <= 0:
                    continue
                return locator, selector, scope
        return None, None, None

    def _parse_message(self, company: CompanyRecord, run_id: str, index: int, text: str, detail_text: str = "") -> MessageRecord:
        clean_text = " ".join(str(text or "").split())
        lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
        all_text = detail_text or clean_text
        date_match = re.findall(r"\b\d{2}/\d{2}/\d{4}(?:\s+\d{2}:\d{2})?\b", all_text)
        protocol_match = re.search(r"\b\d{8,}\b", all_text)
        sender_match = re.search(r"(?:Origem|Remetente|Enviado por)\s*[:\-]\s*(.+)", all_text, flags=re.IGNORECASE)
        category_match = re.search(r"(?:Categoria|Tipo)\s*[:\-]\s*(.+)", all_text, flags=re.IGNORECASE)
        status_match = re.search(r"(?:Lida|Não lida|Nao lida|Lido|Não Visualizada)", all_text, flags=re.IGNORECASE)
        priority_match = re.search(r"(?:Prioridade|Urgência|Urgencia)\s*[:\-]\s*(.+)", all_text, flags=re.IGNORECASE)

        subject = ""
        if lines:
            non_header = [line for line in lines if line.lower() not in {"assunto", "data", "tipo", "origem"}]
            subject = non_header[0] if non_header else lines[0]
        attachments: list[dict[str, Any]] = []
        for match in re.findall(r"([A-Za-z0-9_\- ]+\.(?:pdf|xml|zip|docx?|xlsx?))", all_text, flags=re.IGNORECASE):
            attachments.append({"name": match.strip()})

        return MessageRecord(
            company_id=company.company_id,
            company_name=company.name,
            company_document=format_document(company.document),
            run_id=run_id,
            extracted_at=utc_now_iso(),
            source_system=SOURCE_SYSTEM,
            row_index=index,
            company_profile_context=self._current_profile_context(),
            message_id=protocol_match.group(0) if protocol_match else "",
            subject=subject,
            sender=sender_match.group(1).strip() if sender_match else "",
            category=category_match.group(1).strip() if category_match else "",
            received_at=date_match[0] if date_match else "",
            sent_at=date_match[1] if len(date_match) > 1 else "",
            posted_at=date_match[0] if date_match else "",
            read_status=status_match.group(0) if status_match else "",
            unread=("não" in status_match.group(0).lower() or "nao" in status_match.group(0).lower()) if status_match else None,
            priority=priority_match.group(1).strip() if priority_match else "",
            snippet=_first_line(clean_text),
            body=detail_text.strip(),
            attachments=attachments,
            raw_visible_text=clean_text,
            detail_visible_text=detail_text.strip(),
        )

    def _open_row_detail(self, row: Any) -> str:
        original_snapshot = self._body_text()
        try:
            row.click(timeout=4000, force=True)
        except Exception:
            return ""
        self._wait_until(lambda: self._body_text() != original_snapshot or self._page_contains("Fechar"), timeout_ms=4000)
        detail_text = self._body_text()
        for label in DETAIL_CLOSE_LABELS:
            self._click_by_label([label], timeout_ms=1500)
        try:
            self.page.keyboard.press("Escape")
        except Exception:
            pass
        self.page.wait_for_timeout(200)
        return detail_text if detail_text != original_snapshot else ""

    def _click_next_page(self) -> bool:
        return self._click_by_label(NEXT_PAGE_LABELS, timeout_ms=4000)

    def extract_first_messages(self, company: CompanyRecord, run_id: str, limit: int = 20) -> list[MessageRecord]:
        locator, _, _ = self._message_rows_locator()
        if locator is None:
            raise RuntimeError("Nenhuma tabela/lista de mensagens visível foi encontrada na Caixa Postal.")

        messages: list[MessageRecord] = []
        seen: set[str] = set()
        page_cycles = 0
        while len(messages) < limit and page_cycles < 4:
            try:
                row_count = locator.count()
            except Exception:
                row_count = 0
            if row_count <= 0:
                break
            for index in range(row_count):
                if len(messages) >= limit or self.stop_requested():
                    break
                row = locator.nth(index)
                try:
                    if not row.is_visible():
                        continue
                    cells = row.locator("td, th, [role='cell'], [role='gridcell']").all_inner_texts()
                    raw_text = "\n".join(item.strip() for item in cells if item.strip()) or row.inner_text(timeout=2000)
                    if not raw_text.strip():
                        continue
                    lowered = raw_text.lower()
                    if "assunto" in lowered and "data" in lowered and len(raw_text) < 120:
                        continue
                    detail_text = self._open_row_detail(row)
                    message = self._parse_message(company, run_id, len(messages) + 1, raw_text, detail_text)
                    key = message.dedupe_key()
                    if key in seen:
                        continue
                    seen.add(key)
                    messages.append(message)
                except Exception as exc:
                    self.runtime.logger.warning(f"Falha ao extrair linha de mensagem: {exc}")
                    continue
            if len(messages) >= limit:
                break
            if not self._click_next_page():
                break
            page_cycles += 1
            self._wait_until(lambda: True, timeout_ms=800, interval_ms=250)
            locator, _, _ = self._message_rows_locator()
            if locator is None:
                break
        return messages[:limit]

    def process_company(self, company: CompanyRecord, run_id: str) -> CompanyRunResult:
        result = CompanyRunResult(
            company_id=company.company_id,
            company_name=company.name,
            company_document=format_document(company.document),
            eligible=company.eligible,
            block_reason=company.block_reason,
            started_at=utc_now_iso(),
        )
        if not company.eligible:
            result.status = "blocked"
            result.errors.append(company.block_reason or "Empresa não elegível.")
            result.finished_at = utc_now_iso()
            return result

        try:
            profile_context = self.switch_company_profile(company)
            result.profile_switched = True
            result.company_profile_context = profile_context
            self.open_mailbox()
            result.mailbox_opened = True
            result.messages = self.extract_first_messages(company, run_id, limit=20)
            result.company_profile_context = self._current_profile_context()
            result.status = "success" if result.messages else "empty"
        except Exception as exc:
            result.status = "error"
            result.errors.append(_stack_tail(exc))
            self.runtime.logger.warning(f"Empresa {company.name}: {exc}")
        finally:
            result.finished_at = utc_now_iso()
        return result


def _persist_company_result(run_dir: Path, result: CompanyRunResult) -> None:
    company_folder = run_dir / "companies" / f"{result.company_id}_{slugify(result.company_name or result.company_document)}"
    company_folder.mkdir(parents=True, exist_ok=True)
    result.output_dir = str(company_folder)

    messages_json = company_folder / "messages.json"
    messages_csv = company_folder / "messages.csv"
    result_json = company_folder / "result.json"

    write_json_atomic(messages_json, {"messages": [message.to_dict() for message in result.messages]})
    write_json_atomic(result_json, result.to_dict())

    fieldnames = list(MessageRecord.__dataclass_fields__.keys())
    with messages_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for message in result.messages:
            row = message.to_dict()
            row["attachments"] = json.dumps(row["attachments"], ensure_ascii=False)
            writer.writerow(row)


def _finalize_summary(summary: dict[str, Any], company_results: list[CompanyRunResult]) -> dict[str, Any]:
    total_messages = sum(len(result.messages) for result in company_results)
    total_success = sum(1 for result in company_results if result.status in {"success", "empty"})
    total_failed = sum(1 for result in company_results if result.status not in {"success", "empty"})
    status = summary.get("status") or "completed"
    if total_failed and total_success:
        status = "partial"
    elif total_failed and not total_success:
        status = "failed"
    elif status == "stopped":
        status = "partial"
    else:
        status = "completed"
    summary.update(
        {
            "status": status,
            "total_companies": len(company_results),
            "total_success": total_success,
            "total_failed": total_failed,
            "total_messages": total_messages,
            "companies": [result.to_dict() for result in company_results],
        }
    )
    return summary


def execute_mailbox_run(
    runtime_env: RuntimeEnvironment,
    *,
    companies: list[CompanyRecord],
    certificate: CertificateMetadata,
    job: Optional[JobPayload] = None,
    stop_requested: Callable[[], bool],
    progress_callback: Optional[Callable[[dict[str, Any]], None]] = None,
    headless: bool = False,
) -> dict[str, Any]:
    if not companies:
        raise RuntimeError("Nenhuma empresa selecionada/elegível para execução.")

    run_id = runtime_env.generate_run_id()
    run_dir = runtime_env.create_run_output_dir(run_id)
    summary: dict[str, Any] = {
        "run_id": run_id,
        "started_at": utc_now_iso(),
        "finished_at": "",
        "status": "processing",
        "output_dir": str(run_dir),
        "robot_technical_id": ROBOT_TECHNICAL_ID,
        "interrupted_at_company": "",
    }
    runtime_env.json_runtime.write_heartbeat(
        status="processing",
        current_job_id=job.job_id if job else None,
        current_execution_request_id=job.execution_request_id if job else None,
        message="run_started",
        progress={"current": 0, "total": len(companies)},
        extra={"run_id": run_id},
    )

    run_log_path = run_dir / "run.log"
    company_results: list[CompanyRunResult] = []
    run_log_path.write_text("", encoding="utf-8")

    def append_run_log(line: str) -> None:
        with run_log_path.open("a", encoding="utf-8") as handle:
            handle.write(line.rstrip() + "\n")

    session: Optional[BrowserSession] = None
    try:
        session = launch_browser(runtime_env, headless=headless)
        runtime_env.logger.info(f"Navegador iniciado em {session.executable_path}")
        append_run_log(f"[browser] {session.executable_path}")
        automation = EcacMailboxAutomation(
            runtime_env,
            session,
            certificate,
            stop_requested=stop_requested,
        )
        automation.login()

        for index, company in enumerate(companies, start=1):
            if stop_requested():
                summary["status"] = "stopped"
                summary["interrupted_at_company"] = company.name
                break
            runtime_env.logger.info(f"Processando empresa {index}/{len(companies)}: {company.name}")
            runtime_env.json_runtime.write_heartbeat(
                status="processing",
                current_job_id=job.job_id if job else None,
                current_execution_request_id=job.execution_request_id if job else None,
                message=f"processing_company:{company.company_id}",
                progress={"current": index, "total": len(companies), "company_id": company.company_id},
                extra={"run_id": run_id, "company_name": company.name},
            )
            if progress_callback:
                progress_callback({"current": index, "total": len(companies), "company_name": company.name})
            append_run_log(f"[company] {company.company_id} {company.name}")
            result = automation.process_company(company, run_id)
            company_results.append(result)
            _persist_company_result(run_dir, result)
            append_run_log(
                f"[result] {company.company_id} status={result.status} mensagens={len(result.messages)} erros={'; '.join(result.errors)}"
            )
    finally:
        close_browser(session)

    summary["finished_at"] = utc_now_iso()
    summary = _finalize_summary(summary, company_results)
    write_json_atomic(run_dir / "summary.json", summary)
    runtime_env.json_runtime.write_result(
        job=job,
        success=summary["status"] in {"completed", "partial"},
        summary=summary,
        error_message=None if summary["status"] != "failed" else "Falha geral na execução do robô.",
    )
    return summary

# === worker ===
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Optional


try:
    from PySide6.QtCore import QThread, Signal
except Exception:  # pragma: no cover - CLI mode sem PySide6
    QThread = object  # type: ignore[misc,assignment]

    class Signal:  # type: ignore[override]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def emit(self, *args: Any, **kwargs: Any) -> None:
            return


def run_mailbox_job(
    runtime_env: RuntimeEnvironment,
    *,
    companies: list[CompanyRecord],
    certificate_thumbprint: str = "",
    job: Optional[JobPayload] = None,
    headless: bool = False,
    stop_requested: Optional[Callable[[], bool]] = None,
    log_callback: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[dict[str, Any]], None]] = None,
) -> dict[str, Any]:
    stop_requested = stop_requested or (lambda: False)
    if log_callback:
        runtime_env.logger.bind_sink(log_callback)
    certificate_auth.ensure_windows_environment()

    dashboard_client = DashboardClient(runtime_env)
    office_context = dashboard_client.resolve_office_context(job)
    dashboard_robot_id = dashboard_client.register_robot_presence(status="processing")
    runtime_env.json_runtime.register_robot(
        extra={
            "office_id": office_context.office_id,
            "office_server_id": office_context.office_server_id,
            "office_source": office_context.source,
        }
    )
    if not office_context.office_id:
        raise RuntimeError("office_id não resolvido. Verifique /api/robot-config, CONNECTOR_SECRET e a VM do escritório.")

    selected_companies = [company for company in companies if company.eligible]
    if not selected_companies:
        raise RuntimeError("Nenhuma empresa elegível/selecionada para execução.")

    certificate = certificate_auth.resolve_certificate(
        runtime_env,
        companies=selected_companies,
        preferred_thumbprint=certificate_thumbprint,
    )
    if not certificate:
        raise RuntimeError(
            "Certificado não configurado. O robô procura nesta ordem: ECAC_CERT_* do .env, certificado do dashboard (cert_blob_b64/cert_password) das empresas selecionadas e registro local em data/json/certificates.json."
        )

    heartbeat_state: dict[str, Any] = {
        "current": 0,
        "total": len(selected_companies),
        "company_name": "",
    }
    stop_flag = threading.Event()

    def heartbeat_loop() -> None:
        while not stop_flag.wait(HEARTBEAT_INTERVAL_SECONDS):
            runtime_env.json_runtime.write_heartbeat(
                status="processing",
                current_job_id=job.job_id if job else None,
                current_execution_request_id=job.execution_request_id if job else None,
                message="heartbeat",
                progress=dict(heartbeat_state),
                extra={"office_id": office_context.office_id, "office_server_id": office_context.office_server_id},
            )
            dashboard_client.update_robot_presence(status="processing", robot_id=dashboard_robot_id or "")

    def on_progress(payload: dict[str, Any]) -> None:
        heartbeat_state.update(payload)
        if progress_callback:
            progress_callback(payload)

    heartbeat_thread = threading.Thread(target=heartbeat_loop, name="ecac-mailbox-heartbeat", daemon=True)
    heartbeat_thread.start()
    try:
        summary = execute_mailbox_run(
            runtime_env,
            companies=selected_companies,
            certificate=certificate,
            job=job,
            stop_requested=stop_requested,
            progress_callback=on_progress,
            headless=headless,
        )
        return summary
    except Exception as exc:
        failed_summary = {
            "run_id": runtime_env.generate_run_id(),
            "started_at": utc_now_iso(),
            "finished_at": utc_now_iso(),
            "status": "failed",
            "output_dir": str(runtime_env.create_run_output_dir(datetime.now().strftime("%Y%m%d_%H%M%S_fail"))),
            "robot_technical_id": "ecac_caixa_postal",
            "total_companies": len(selected_companies),
            "total_success": 0,
            "total_failed": len(selected_companies),
            "total_messages": 0,
            "error": str(exc),
            "companies": [],
        }
        runtime_env.json_runtime.write_result(
            job=job,
            success=False,
            summary=failed_summary,
            error_message=str(exc),
        )
        runtime_env.json_runtime.write_heartbeat(
            status="active",
            message="result_ready",
            extra={"office_id": office_context.office_id, "office_server_id": office_context.office_server_id},
        )
        dashboard_client.update_robot_presence(status="active", robot_id=dashboard_robot_id or "")
        raise
    finally:
        stop_flag.set()
        heartbeat_thread.join(timeout=2)
        if not stop_requested():
            dashboard_client.update_robot_presence(status="active", robot_id=dashboard_robot_id or "")


class EcacMailboxWorker(QThread):  # type: ignore[misc]
    log = Signal(str)
    status = Signal(str)
    progress = Signal(object)
    summary_ready = Signal(object)
    error = Signal(str)

    def __init__(
        self,
        runtime_env: RuntimeEnvironment,
        *,
        companies: list[CompanyRecord],
        certificate_thumbprint: str = "",
        job: Optional[JobPayload] = None,
        headless: bool = False,
    ) -> None:
        super().__init__()
        self.runtime = runtime_env
        self.companies = companies
        self.certificate_thumbprint = certificate_thumbprint
        self.job = job
        self.headless = headless
        self._stop_requested = False

    def request_stop(self) -> None:
        self._stop_requested = True
        try:
            self.requestInterruption()
        except Exception:
            pass

    def run(self) -> None:
        try:
            self.status.emit("Executando")
            summary = run_mailbox_job(
                self.runtime,
                companies=self.companies,
                certificate_thumbprint=self.certificate_thumbprint,
                job=self.job,
                headless=self.headless,
                stop_requested=lambda: self._stop_requested,
                log_callback=self.log.emit,
                progress_callback=self.progress.emit,
            )
            self.summary_ready.emit(summary)
            self.status.emit("Concluído")
        except Exception as exc:
            self.error.emit(str(exc))
            self.status.emit("Falha")

# === ui_main ===
import os
from pathlib import Path
from typing import Optional


from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QCheckBox,
)


class MainWindow(QMainWindow):
    def __init__(self, runtime_env: RuntimeEnvironment) -> None:
        super().__init__()
        self.runtime = runtime_env
        self.dashboard = DashboardClient(runtime_env)
        self.robot_dashboard_id: Optional[str] = None
        self.job: Optional[JobPayload] = self.runtime.json_runtime.load_job()
        self.office_context = self.dashboard.resolve_office_context(self.job)
        self.worker: Optional[EcacMailboxWorker] = None
        self.companies: list[CompanyRecord] = []
        self.filtered_companies: list[CompanyRecord] = []
        self.robot_heartbeat_timer = QTimer(self)
        self.robot_heartbeat_timer.setInterval(30000)
        self.robot_heartbeat_timer.timeout.connect(self._on_robot_heartbeat)

        self.setWindowTitle("e-CAC - Caixa Postal")
        self.resize(1180, 760)
        self._build_ui()
        self.runtime.logger.bind_sink(self.append_log)
        self._load_initial_state()
        self.robot_dashboard_id = self.dashboard.register_robot_presence(status="active")
        self.robot_heartbeat_timer.start()

    def _build_ui(self) -> None:
        central = QWidget()
        central.setObjectName("centralRoot")
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(14)

        title = QLabel("e-CAC - Caixa Postal")
        title.setStyleSheet("font-size: 24px; font-weight: 700; color: #14213d;")
        layout.addWidget(title)

        subtitle = QLabel("Autenticação via certificado do operador, troca de perfil por empresa e extração da Caixa Postal.")
        subtitle.setStyleSheet("color: #4b5563; font-size: 13px;")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        top_row = QHBoxLayout()
        top_row.setSpacing(14)
        layout.addLayout(top_row)

        companies_box = QGroupBox("Empresas")
        companies_layout = QVBoxLayout(companies_box)
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Filtrar por nome ou CNPJ")
        self.search_input.textChanged.connect(self._apply_company_filter)
        companies_layout.addWidget(self.search_input)

        self.company_list = QListWidget()
        self.company_list.setSelectionMode(QAbstractItemView.MultiSelection)
        self.company_list.setAlternatingRowColors(True)
        self.company_list.setUniformItemSizes(False)
        self.company_list.itemSelectionChanged.connect(self._refresh_certificate_status)
        companies_layout.addWidget(self.company_list, 1)

        company_buttons = QHBoxLayout()
        self.refresh_button = QPushButton("Atualizar empresas")
        self.refresh_button.clicked.connect(self.reload_companies)
        self.select_job_button = QPushButton("Selecionar job.json")
        self.select_job_button.clicked.connect(self._select_job_companies)
        company_buttons.addWidget(self.refresh_button)
        company_buttons.addWidget(self.select_job_button)
        companies_layout.addLayout(company_buttons)
        top_row.addWidget(companies_box, 2)

        side_column = QVBoxLayout()
        side_column.setSpacing(12)
        top_row.addLayout(side_column, 1)

        certificate_box = QGroupBox("Certificado do Operador")
        certificate_layout = QVBoxLayout(certificate_box)
        self.certificate_status = QLabel("Nenhum certificado configurado.")
        self.certificate_status.setWordWrap(True)
        self.certificate_registry_input = QLineEdit()
        self.certificate_registry_input.setPlaceholderText("Thumbprint do certificado salvo (opcional)")
        self.import_certificate_button = QPushButton("Importar certificado PFX")
        self.import_certificate_button.clicked.connect(self.import_certificate)
        certificate_layout.addWidget(self.certificate_status)
        certificate_layout.addWidget(self.certificate_registry_input)
        certificate_layout.addWidget(self.import_certificate_button)
        side_column.addWidget(certificate_box)

        runtime_box = QGroupBox("Runtime")
        runtime_layout = QGridLayout(runtime_box)
        self.job_mode_checkbox = QCheckBox("Usar job.json / modo painel")
        self.job_mode_checkbox.setChecked(self.job is not None)
        self.status_label = QLabel("Pronto")
        self.progress_label = QLabel("0 / 0")
        self.office_label = QLabel(self.office_context.office_id or "office_id não resolvido")
        runtime_layout.addWidget(QLabel("Status"), 0, 0)
        runtime_layout.addWidget(self.status_label, 0, 1)
        runtime_layout.addWidget(QLabel("Progresso"), 1, 0)
        runtime_layout.addWidget(self.progress_label, 1, 1)
        runtime_layout.addWidget(QLabel("office_id"), 2, 0)
        runtime_layout.addWidget(self.office_label, 2, 1)
        runtime_layout.addWidget(self.job_mode_checkbox, 3, 0, 1, 2)
        side_column.addWidget(runtime_box)

        controls = QHBoxLayout()
        self.start_button = QPushButton("Iniciar")
        self.start_button.clicked.connect(self.start_worker)
        self.stop_button = QPushButton("Parar")
        self.stop_button.clicked.connect(self.stop_worker)
        self.stop_button.setEnabled(False)
        self.clear_log_button = QPushButton("Limpar log")
        controls.addWidget(self.start_button)
        controls.addWidget(self.stop_button)
        controls.addWidget(self.clear_log_button)
        side_column.addLayout(controls)
        side_column.addStretch(1)

        log_box = QGroupBox("Logs")
        log_layout = QVBoxLayout(log_box)
        self.log_panel = QTextEdit()
        self.log_panel.setReadOnly(True)
        self.log_panel.setLineWrapMode(QTextEdit.NoWrap)
        self.log_panel.setPlaceholderText("Logs da execuÃ§Ã£o aparecerÃ£o aqui.")
        self.clear_log_button.clicked.connect(self.log_panel.clear)
        log_layout.addWidget(self.log_panel)
        layout.addWidget(log_box, 1)

        self.setStyleSheet(
            """
            QMainWindow, QWidget#centralRoot {
                background: #eef2f6;
                color: #18212f;
            }
            QLabel {
                color: #18212f;
            }
            QGroupBox {
                font-weight: 700;
                color: #14213d;
                border: 1px solid #cfd8e3;
                border-radius: 12px;
                margin-top: 12px;
                padding-top: 10px;
                background: #ffffff;
            }
            QGroupBox::title {
                left: 12px;
                padding: 0 6px;
                color: #0f172a;
                background: #ffffff;
            }
            QPushButton {
                background: #0b5cab;
                color: #ffffff;
                border: 1px solid #0a4f95;
                border-radius: 8px;
                padding: 9px 14px;
                font-weight: 600;
            }
            QPushButton:hover {
                background: #0e6dcb;
            }
            QPushButton:pressed {
                background: #094a8c;
            }
            QPushButton:disabled {
                background: #b8c2cc;
                color: #eef2f6;
                border-color: #b8c2cc;
            }
            QLineEdit, QListWidget, QTextEdit {
                background: #ffffff;
                color: #111827;
                border: 1px solid #c7d2df;
                border-radius: 8px;
                padding: 8px;
                selection-background-color: #0b5cab;
                selection-color: #ffffff;
            }
            QLineEdit:focus, QListWidget:focus, QTextEdit:focus {
                border: 1px solid #0b5cab;
            }
            QCheckBox {
                color: #18212f;
                spacing: 8px;
            }
            QListWidget {
                alternate-background-color: #f7fafc;
                outline: 0;
            }
            QListWidget::item {
                color: #111827;
                background: transparent;
                border-radius: 6px;
                padding: 8px 6px;
                margin: 2px;
            }
            QListWidget::item:selected {
                background: #dbeafe;
                color: #0f172a;
                border: 1px solid #93c5fd;
            }
            QListWidget::item:hover {
                background: #eff6ff;
                color: #0f172a;
            }
            QTextEdit {
                background: #0f172a;
                color: #e5eef8;
                border: 1px solid #1e293b;
                font-family: Consolas, 'Courier New', monospace;
            }
            """
        )

    def append_log(self, message: str) -> None:
        self.log_panel.append(message)

    def _load_initial_state(self) -> None:
        self.append_log("Carregando estado inicial do robô.")
        self.reload_companies()
        if self.job and self.job.company_ids:
            self._select_companies_by_ids(self.job.company_ids)
        self._refresh_certificate_status()

    def _refresh_certificate_status(self) -> None:
        env_cert = certificate_auth.peek_certificate_configuration_from_env()
        if env_cert:
            text = f"Via .env: {env_cert.alias or env_cert.subject or env_cert.pfx_path}"
            if env_cert.subject:
                text += f"\nSubject: {env_cert.subject}"
            self.certificate_status.setText(text)
            return

        companies = self._selected_companies() or self.companies
        dashboard_cert = certificate_auth.peek_certificate_configuration_from_dashboard(companies)
        if dashboard_cert:
            self.certificate_status.setText(
                f"Via dashboard: {dashboard_cert.display_label()}\nOrigem: cert_blob_b64/cert_password da empresa selecionada."
            )
            return

        registry = certificate_auth.load_registry(self.runtime.paths.certificates_registry_path)
        if registry:
            first = registry[0]
            self.certificate_status.setText(
                f"Cadastrado localmente: {first.display_label()}\nThumbprint: {first.thumbprint or '(não informado)'}"
            )
            if not self.certificate_registry_input.text().strip() and first.thumbprint:
                self.certificate_registry_input.setText(first.thumbprint)
            return

        self.certificate_status.setText(
            "Nenhum certificado configurado. O robÃ´ procura em ECAC_CERT_* no .env, no dashboard (cert_blob_b64/cert_password) e no registro local."
        )

    def reload_companies(self) -> None:
        try:
            self.office_context = self.dashboard.resolve_office_context(self.job, force_refresh=True)
            company_ids = self.job.company_ids if self.job_mode_checkbox.isChecked() and self.job else None
            self.companies = self.dashboard.load_companies(self.office_context, job=self.job, company_ids=company_ids)
            self.office_label.setText(self.office_context.office_id or "office_id não resolvido")
            self._refresh_certificate_status()
            self._apply_company_filter()
            self.append_log(f"{len(self.companies)} empresas carregadas do dashboard.")
        except Exception as exc:
            self.append_log(f"Falha ao carregar empresas: {exc}")
            self.companies = []
            self._refresh_certificate_status()
            self._apply_company_filter()

    def _apply_company_filter(self) -> None:
        search = self.search_input.text().strip().lower()
        self.filtered_companies = []
        self.company_list.clear()
        for company in self.companies:
            if search and search not in company.search_text:
                continue
            self.filtered_companies.append(company)
            item = QListWidgetItem(
                f"{company.name}\n{format_document(company.document)} | {'Elegível' if company.eligible else 'Bloqueada'}"
            )
            item.setData(Qt.UserRole, company.company_id)
            if company.block_reason:
                item.setToolTip(company.block_reason)
            if not company.eligible:
                item.setForeground(QColor("#8b1e1e"))
                item.setFlags(item.flags() & ~Qt.ItemIsSelectable)
                item.setText(item.text() + f"\nMotivo: {company.block_reason}")
            self.company_list.addItem(item)

    def _select_companies_by_ids(self, company_ids: list[str]) -> None:
        wanted = {str(item).strip() for item in company_ids if str(item).strip()}
        for index in range(self.company_list.count()):
            item = self.company_list.item(index)
            if item.data(Qt.UserRole) in wanted:
                item.setSelected(True)
        self._refresh_certificate_status()

    def _select_job_companies(self) -> None:
        if not self.job or not self.job.company_ids:
            QMessageBox.information(self, "Job", "Nenhum job.json carregado com company_ids.")
            return
        self._select_companies_by_ids(self.job.company_ids)
        self.append_log("Empresas do job.json selecionadas na lista.")

    def import_certificate(self) -> None:
        pfx_path, _ = QFileDialog.getOpenFileName(self, "Selecionar certificado PFX", "", "Certificados (*.pfx);;Todos os arquivos (*)")
        if not pfx_path:
            return
        alias, ok = QInputDialog.getText(self, "Alias do Certificado", "Nome/alias para exibir na interface:")
        if not ok:
            return
        password, ok = QInputDialog.getText(
            self,
            "Senha do PFX",
            "Senha do certificado:",
            QLineEdit.Password,
        )
        if not ok:
            return
        try:
            metadata = certificate_auth.register_certificate(
                self.runtime,
                alias=alias,
                pfx_path=Path(pfx_path),
                password=password,
            )
            if metadata.thumbprint:
                self.certificate_registry_input.setText(metadata.thumbprint)
            self.append_log(f"Certificado importado com sucesso: {metadata.display_label()}")
            self._refresh_certificate_status()
        except Exception as exc:
            QMessageBox.critical(self, "Certificado", str(exc))

    def _selected_companies(self) -> list[CompanyRecord]:
        selected_ids = {item.data(Qt.UserRole) for item in self.company_list.selectedItems()}
        if not selected_ids and self.job_mode_checkbox.isChecked() and self.job and self.job.company_ids:
            selected_ids = set(self.job.company_ids)
        return [company for company in self.companies if company.company_id in selected_ids]

    def start_worker(self) -> None:
        if self.worker is not None and self.worker.isRunning():
            return
        selected = self._selected_companies()
        if not selected:
            QMessageBox.warning(self, "Empresas", "Selecione ao menos uma empresa elegível.")
            return
        self.worker = EcacMailboxWorker(
            self.runtime,
            companies=selected,
            certificate_thumbprint=self.certificate_registry_input.text().strip(),
            job=self.job if self.job_mode_checkbox.isChecked() else None,
            headless=False,
        )
        self.worker.log.connect(self.append_log)
        self.worker.status.connect(self.status_label.setText)
        self.worker.progress.connect(self._on_progress)
        self.worker.summary_ready.connect(self._on_summary)
        self.worker.error.connect(self._on_error)
        self.worker.finished.connect(self._on_worker_finished)
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.status_label.setText("Executando")
        self.dashboard.update_robot_presence(status="processing", robot_id=self.robot_dashboard_id or "")
        self.worker.start()

    def stop_worker(self) -> None:
        if self.worker is None:
            return
        self.append_log("Solicitação de parada enviada ao worker.")
        self.worker.request_stop()
        self.status_label.setText("Parando")

    def _on_progress(self, payload: object) -> None:
        if isinstance(payload, dict):
            current = payload.get("current", 0)
            total = payload.get("total", 0)
            company = payload.get("company_name", "")
            self.progress_label.setText(f"{current} / {total}")
            if company:
                self.status_label.setText(f"Executando: {company}")

    def _on_summary(self, summary: object) -> None:
        if not isinstance(summary, dict):
            return
        self.append_log(
            f"Resumo final: status={summary.get('status')} empresas={summary.get('total_companies')} mensagens={summary.get('total_messages')}"
        )
        self.status_label.setText(str(summary.get("status") or "Concluído"))
        self.progress_label.setText(f"{summary.get('total_companies', 0)} / {summary.get('total_companies', 0)}")
        self.dashboard.update_robot_presence(status="active", robot_id=self.robot_dashboard_id or "")

    def _on_error(self, message: str) -> None:
        self.append_log(f"Erro: {message}")
        QMessageBox.critical(self, "Execução", message)

        self.dashboard.update_robot_presence(status="active", robot_id=self.robot_dashboard_id or "")

    def _on_worker_finished(self) -> None:
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        self.dashboard.update_robot_presence(status="active", robot_id=self.robot_dashboard_id or "")

    def _on_robot_heartbeat(self) -> None:
        status = "processing" if self.worker is not None and self.worker.isRunning() else "active"
        self.dashboard.update_robot_presence(status=status, robot_id=self.robot_dashboard_id or "")

    def closeEvent(self, event) -> None:  # type: ignore[override]
        if self.worker is not None and self.worker.isRunning():
            self.worker.request_stop()
            self.worker.wait(3000)
        self.robot_heartbeat_timer.stop()
        self.dashboard.update_robot_presence(status="inactive", robot_id=self.robot_dashboard_id or "")
        self.runtime.mark_inactive()
        super().closeEvent(event)

# === ecac_caixa_postal_app ===
import argparse
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="e-CAC - Caixa Postal")
    parser.add_argument("--no-ui", action="store_true", help="Executa sem interface gráfica.")
    parser.add_argument("--job-mode", action="store_true", help="Força leitura de job.json.")
    parser.add_argument("--headless", action="store_true", help="Executa o navegador em headless.")
    parser.add_argument("--company-id", action="append", default=[], help="Restringe a execução a um company_id.")
    parser.add_argument("--certificate-thumbprint", default="", help="Thumbprint salvo localmente para priorizar um certificado.")
    return parser.parse_args()


def _run_cli(args: argparse.Namespace) -> int:
    runtime_env = build_runtime()
    job = runtime_env.json_runtime.load_job() if args.job_mode or args.company_id else runtime_env.json_runtime.load_job()
    dashboard = DashboardClient(runtime_env)
    robot_id = dashboard.register_robot_presence(status="active")
    office_context = dashboard.resolve_office_context(job)
    company_ids = args.company_id or (job.company_ids if args.job_mode and job else None)
    companies = dashboard.load_companies(office_context, job=job if args.job_mode else job, company_ids=company_ids)
    try:
        summary = run_mailbox_job(
            runtime_env,
            companies=companies,
            certificate_thumbprint=args.certificate_thumbprint,
            job=job if args.job_mode else job,
            headless=args.headless,
            log_callback=lambda line: print(line, flush=True),
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0 if summary.get("status") in {"completed", "partial"} else 1
    finally:
        dashboard.update_robot_presence(status="inactive", robot_id=robot_id or "")
        runtime_env.mark_inactive("cli_exit")


def main() -> int:
    args = parse_args()
    if args.no_ui:
        return _run_cli(args)

    from PySide6.QtWidgets import QApplication

    app = QApplication(sys.argv)
    runtime_env = build_runtime()
    window = MainWindow(runtime_env)
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
