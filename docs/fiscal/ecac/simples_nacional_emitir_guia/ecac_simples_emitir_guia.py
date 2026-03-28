from __future__ import annotations

import argparse
import importlib.util
import types
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import textwrap
import threading
import time
import traceback
import unicodedata
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urljoin

import requests

SCRIPT_DIR = Path(__file__).resolve().parent


_ECAC_BASE_SOURCE = 'from __future__ import annotations\n\n"""e-CAC - Caixa Postal\n\nImplementação consolidada em arquivo único.\n"""\n\n# === models ===\nimport base64\nfrom dataclasses import asdict, dataclass, field\nfrom pathlib import Path\nimport tempfile\nfrom typing import Any, Optional\n\n\n@dataclass(slots=True)\nclass RuntimePaths:\n    runtime_dir: Path\n    base_dir: Path\n    robots_root_dir: Path\n    shared_env_dir: Path\n    data_dir: Path\n    json_dir: Path\n    logs_dir: Path\n    chrome_profile_dir: Path\n    chrome_profile_backup_dir: Path\n    certificates_registry_path: Path\n    runtime_log_path: Path\n\n\n@dataclass(slots=True)\nclass OfficeContext:\n    office_id: str = ""\n    office_server_id: str = ""\n    base_path: str = ""\n    segment_path: str = ""\n    notes_mode: str = ""\n    source: str = ""\n\n\n@dataclass(slots=True)\nclass CompanyRecord:\n    company_id: str\n    name: str\n    document: str\n    active: bool = True\n    eligible: bool = True\n    block_reason: str = ""\n    source: str = "dashboard"\n    auth_mode: str = "password"\n    cert_password: str = ""\n    cert_blob_b64: str = ""\n    cert_path: str = ""\n    raw: dict[str, Any] = field(default_factory=dict)\n\n    @property\n    def search_text(self) -> str:\n        return f"{self.name} {self.document}".strip().lower()\n\n    @property\n    def has_valid_cnpj(self) -> bool:\n        return len(only_digits(self.document)) == 14\n\n    @property\n    def has_certificate_credentials(self) -> bool:\n        return bool(\n            str(self.cert_password or "").strip()\n            and (str(self.cert_blob_b64 or "").strip() or str(self.cert_path or "").strip())\n        )\n\n    def to_dict(self) -> dict[str, Any]:\n        payload = asdict(self)\n        payload["raw"] = dict(self.raw)\n        return payload\n\n\n@dataclass(slots=True)\nclass CertificateMetadata:\n    alias: str\n    subject: str\n    issuer: str = ""\n    thumbprint: str = ""\n    pfx_path: str = ""\n    source: str = "registry"\n\n    def display_label(self) -> str:\n        return self.alias or self.subject or self.thumbprint or "Sem nome"\n\n    def to_dict(self) -> dict[str, Any]:\n        return asdict(self)\n\n\n@dataclass(slots=True)\nclass JobPayload:\n    job_id: str = ""\n    execution_request_id: str = ""\n    office_id: str = ""\n    company_ids: list[str] = field(default_factory=list)\n    companies: list[dict[str, Any]] = field(default_factory=list)\n    raw: dict[str, Any] = field(default_factory=dict)\n\n\n@dataclass(slots=True)\nclass RobotMailboxConfig:\n    use_responsible_office: bool = False\n    responsible_office_company_id: str = ""\n    source: str = "default"\n\n    def to_dict(self) -> dict[str, Any]:\n        return asdict(self)\n\n\n@dataclass(slots=True)\nclass MessageRecord:\n    company_id: str\n    company_name: str\n    company_document: str\n    run_id: str\n    extracted_at: str\n    source_system: str\n    row_index: int\n    company_profile_context: str = ""\n    message_id: str = ""\n    subject: str = ""\n    sender: str = ""\n    category: str = ""\n    received_at: str = ""\n    sent_at: str = ""\n    posted_at: str = ""\n    read_status: str = ""\n    unread: Optional[bool] = None\n    priority: str = ""\n    snippet: str = ""\n    body: str = ""\n    attachments: list[dict[str, Any]] = field(default_factory=list)\n    raw_visible_text: str = ""\n    detail_visible_text: str = ""\n\n    def dedupe_key(self) -> str:\n        return "|".join(\n            [\n                self.message_id.strip(),\n                self.subject.strip(),\n                self.received_at.strip(),\n                self.posted_at.strip(),\n                self.raw_visible_text.strip(),\n            ]\n        )\n\n    def to_dict(self) -> dict[str, Any]:\n        return asdict(self)\n\n\n@dataclass(slots=True)\nclass CompanyRunResult:\n    company_id: str\n    company_name: str\n    company_document: str\n    context_type: str = "company"\n    status: str = "pending"\n    eligible: bool = True\n    block_reason: str = ""\n    profile_switched: bool = False\n    mailbox_opened: bool = False\n    company_profile_context: str = ""\n    messages: list[MessageRecord] = field(default_factory=list)\n    errors: list[str] = field(default_factory=list)\n    started_at: str = ""\n    finished_at: str = ""\n\n    def to_dict(self) -> dict[str, Any]:\n        payload = asdict(self)\n        payload["messages"] = [message.to_dict() for message in self.messages]\n        return payload\n\n\n@dataclass(slots=True)\nclass RunSummary:\n    run_id: str\n    started_at: str\n    finished_at: str = ""\n    status: str = "pending"\n    total_companies: int = 0\n    total_success: int = 0\n    total_failed: int = 0\n    total_messages: int = 0\n    interrupted_at_company: str = ""\n    company_results: list[CompanyRunResult] = field(default_factory=list)\n    responsible_office_result: Optional[CompanyRunResult] = None\n\n    def to_dict(self) -> dict[str, Any]:\n        payload = asdict(self)\n        payload["company_results"] = [item.to_dict() for item in self.company_results]\n        payload["responsible_office_result"] = (\n            self.responsible_office_result.to_dict() if self.responsible_office_result else None\n        )\n        return payload\n\n# === runtime ===\nimport json\nimport logging\nimport os\nimport re\nimport sys\nimport uuid\nfrom dataclasses import dataclass\nfrom datetime import datetime, timezone\nfrom pathlib import Path\nfrom typing import Any, Callable, Optional\n\n\nROBOT_TECHNICAL_ID = "ecac_caixa_postal"\nROBOT_DISPLAY_NAME = "e-CAC - Caixa Postal"\nHEARTBEAT_INTERVAL_SECONDS = 30\n\n\ndef utc_now_iso() -> str:\n    return datetime.now(timezone.utc).isoformat()\n\n\ndef only_digits(text: str) -> str:\n    return "".join(ch for ch in str(text or "") if ch.isdigit())\n\n\ndef format_cpf(cpf: str) -> str:\n    digits = only_digits(cpf)\n    if len(digits) != 11:\n        return digits\n    return f"{digits[0:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:11]}"\n\n\ndef format_cnpj(cnpj: str) -> str:\n    digits = only_digits(cnpj)\n    if len(digits) != 14:\n        return digits\n    return f"{digits[0:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:14]}"\n\n\ndef format_document(text: str) -> str:\n    digits = only_digits(text)\n    if len(digits) <= 11:\n        return format_cpf(digits)\n    return format_cnpj(digits)\n\n\ndef slugify(value: str) -> str:\n    text = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower())\n    return text.strip("_") or "item"\n\n\ndef read_json(path: Path, default: Any = None) -> Any:\n    try:\n        if path.exists():\n            return json.loads(path.read_text(encoding="utf-8"))\n    except Exception:\n        pass\n    return default\n\n\ndef write_json_atomic(path: Path, payload: dict[str, Any]) -> None:\n    path.parent.mkdir(parents=True, exist_ok=True)\n    temp_path = path.with_suffix(path.suffix + ".tmp")\n    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\\n", encoding="utf-8")\n    temp_path.replace(path)\n\n\ndef _load_env_file(path: Path) -> None:\n    if not path.exists():\n        return\n    try:\n        from dotenv import load_dotenv\n\n        load_dotenv(path, override=False)\n        return\n    except Exception:\n        pass\n\n    for raw_line in path.read_text(encoding="utf-8").splitlines():\n        line = raw_line.strip()\n        if not line or line.startswith("#") or "=" not in line:\n            continue\n        key, value = line.split("=", 1)\n        key = key.strip()\n        value = value.strip().strip(\'"\').strip("\'")\n        if key and key not in os.environ:\n            os.environ[key] = value\n\n\ndef resolve_runtime_dir() -> Path:\n    explicit = (os.environ.get("ROBOT_SCRIPT_DIR") or "").strip().rstrip("\\\\/")\n    if explicit:\n        return Path(explicit).resolve()\n    if getattr(sys, "frozen", False):\n        return Path(sys.executable).resolve().parent\n    return Path(__file__).resolve().parent\n\n\ndef resolve_base_dir(runtime_dir: Path) -> Path:\n    if getattr(sys, "frozen", False):\n        exe_dir = Path(sys.executable).resolve().parent\n        internal_dir = exe_dir / "_internal"\n        if internal_dir.is_dir():\n            return internal_dir\n        meipass_dir = getattr(sys, "_MEIPASS", None)\n        if meipass_dir:\n            return Path(meipass_dir).resolve()\n        return exe_dir\n    return runtime_dir\n\n\ndef resolve_robots_base_env_dir(base_dir: Path, runtime_dir: Path) -> Path:\n    candidates: list[Path] = []\n    robots_root = (os.environ.get("ROBOTS_ROOT_PATH") or "").strip()\n    robot_script_dir = (os.environ.get("ROBOT_SCRIPT_DIR") or "").strip()\n\n    if robots_root:\n        candidates.append(Path(robots_root))\n    if robot_script_dir:\n        candidates.append(Path(robot_script_dir).resolve().parent)\n    candidates.append(runtime_dir.parent)\n    candidates.append(base_dir.parent)\n\n    if getattr(sys, "frozen", False):\n        exe_dir = Path(sys.executable).resolve().parent\n        candidates.append(exe_dir)\n        candidates.append(exe_dir.parent)\n\n    seen: set[str] = set()\n    for candidate in candidates:\n        try:\n            resolved = candidate.resolve()\n        except Exception:\n            resolved = candidate\n        key = str(resolved).lower()\n        if key in seen:\n            continue\n        seen.add(key)\n        if (resolved / ".env").exists() or (resolved / ".env.example").exists():\n            return resolved\n\n    if robots_root:\n        return Path(robots_root).resolve()\n    return runtime_dir.parent.resolve()\n\n\ndef bootstrap_environment() -> RuntimePaths:\n    runtime_dir = resolve_runtime_dir()\n    base_dir = resolve_base_dir(runtime_dir)\n    shared_env_dir = resolve_robots_base_env_dir(base_dir, runtime_dir)\n\n    shared_env = shared_env_dir / ".env"\n    shared_env_example = shared_env_dir / ".env.example"\n    local_env = runtime_dir / ".env"\n    local_env_example = runtime_dir / ".env.example"\n\n    _load_env_file(shared_env if shared_env.exists() else shared_env_example)\n    _load_env_file(local_env if local_env.exists() else local_env_example)\n\n    if getattr(sys, "frozen", False):\n        exe_dir = Path(sys.executable).resolve().parent\n        if exe_dir != shared_env_dir:\n            _load_env_file(exe_dir / ".env")\n            _load_env_file(exe_dir / ".env.example")\n\n    data_dir = runtime_dir / "data"\n    json_dir = data_dir / "json"\n    logs_dir = data_dir / "logs"\n    chrome_profile_dir = data_dir / "chrome_profile"\n    chrome_profile_backup_dir = data_dir / "chrome_profile_backup"\n\n    for folder in (data_dir, json_dir, logs_dir, chrome_profile_dir, chrome_profile_backup_dir):\n        folder.mkdir(parents=True, exist_ok=True)\n\n    playwright_dir = data_dir / "ms-playwright"\n    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(playwright_dir))\n\n    return RuntimePaths(\n        runtime_dir=runtime_dir,\n        base_dir=base_dir,\n        robots_root_dir=shared_env_dir.parent if shared_env_dir.parent != shared_env_dir else shared_env_dir,\n        shared_env_dir=shared_env_dir,\n        data_dir=data_dir,\n        json_dir=json_dir,\n        logs_dir=logs_dir,\n        chrome_profile_dir=chrome_profile_dir,\n        chrome_profile_backup_dir=chrome_profile_backup_dir,\n        certificates_registry_path=json_dir / "certificates.json",\n        runtime_log_path=logs_dir / "runtime.log",\n    )\n\n\nclass RuntimeLogger:\n    def __init__(self, log_path: Path, sink: Optional[Callable[[str], None]] = None) -> None:\n        self.log_path = log_path\n        self.sink = sink\n        self._logger = logging.getLogger(f"ecac_caixa_postal::{log_path}")\n        self._logger.setLevel(logging.INFO)\n        self._logger.propagate = False\n        if not self._logger.handlers:\n            handler = logging.FileHandler(log_path, encoding="utf-8")\n            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))\n            self._logger.addHandler(handler)\n\n    def bind_sink(self, sink: Optional[Callable[[str], None]]) -> None:\n        self.sink = sink\n\n    def _emit(self, level: str, message: str) -> None:\n        text = str(message).rstrip()\n        getattr(self._logger, level.lower())(text)\n        line = f"[{datetime.now().strftime(\'%H:%M:%S\')}] [{level}] {text}"\n        if self.sink:\n            self.sink(line)\n\n    def info(self, message: str) -> None:\n        self._emit("INFO", message)\n\n    def warning(self, message: str) -> None:\n        self._emit("WARNING", message)\n\n    def error(self, message: str) -> None:\n        self._emit("ERROR", message)\n\n    def exception(self, message: str) -> None:\n        self._logger.exception(message)\n        line = f"[{datetime.now().strftime(\'%H:%M:%S\')}] [ERROR] {message}"\n        if self.sink:\n            self.sink(line)\n\n\nclass JsonRobotRuntime:\n    def __init__(self, technical_id: str, display_name: str, json_dir: Path) -> None:\n        self.technical_id = technical_id\n        self.display_name = display_name\n        self.json_dir = json_dir\n        self.job_path = json_dir / "job.json"\n        self.result_path = json_dir / "result.json"\n        self.heartbeat_path = json_dir / "heartbeat.json"\n\n    def register_robot(self, extra: Optional[dict[str, Any]] = None) -> str:\n        self.write_heartbeat(status="active", message="robot_registered", extra=extra)\n        return self.technical_id\n\n    def load_job(self) -> Optional[JobPayload]:\n        payload = read_json(self.job_path, default=None)\n        if not isinstance(payload, dict):\n            return None\n        execution_request_id = str(\n            payload.get("execution_request_id") or payload.get("job_id") or payload.get("id") or ""\n        ).strip()\n        if not execution_request_id:\n            return None\n        result_payload = read_json(self.result_path, default=None)\n        if isinstance(result_payload, dict):\n            existing = str(\n                result_payload.get("execution_request_id")\n                or result_payload.get("job_id")\n                or result_payload.get("event_id")\n                or ""\n            ).strip()\n            if existing and existing == execution_request_id:\n                return None\n        company_ids = [str(item) for item in payload.get("company_ids") or [] if str(item).strip()]\n        companies = payload.get("companies") if isinstance(payload.get("companies"), list) else []\n        return JobPayload(\n            job_id=str(payload.get("job_id") or payload.get("id") or execution_request_id),\n            execution_request_id=execution_request_id,\n            office_id=str(payload.get("office_id") or "").strip(),\n            company_ids=company_ids,\n            companies=[item for item in companies if isinstance(item, dict)],\n            raw=payload,\n        )\n\n    def write_heartbeat(\n        self,\n        *,\n        status: str,\n        current_job_id: Optional[str] = None,\n        current_execution_request_id: Optional[str] = None,\n        message: Optional[str] = None,\n        progress: Optional[dict[str, Any]] = None,\n        extra: Optional[dict[str, Any]] = None,\n    ) -> None:\n        payload: dict[str, Any] = {\n            "robot_technical_id": self.technical_id,\n            "display_name": self.display_name,\n            "status": status,\n            "updated_at": utc_now_iso(),\n            "current_job_id": current_job_id,\n            "current_execution_request_id": current_execution_request_id,\n            "message": message,\n            "progress": progress or {},\n        }\n        if extra:\n            payload.update(extra)\n        write_json_atomic(self.heartbeat_path, payload)\n\n    def write_result(\n        self,\n        *,\n        job: Optional[JobPayload],\n        success: bool,\n        summary: dict[str, Any],\n        payload: Optional[dict[str, Any]] = None,\n        error_message: Optional[str] = None,\n        company_results: Optional[list[dict[str, Any]]] = None,\n        responsible_office_result: Optional[dict[str, Any]] = None,\n    ) -> None:\n        execution_request_id = job.execution_request_id if job else None\n        job_id = job.job_id if job else None\n        event_id = execution_request_id or job_id or str(uuid.uuid4())\n        result_payload: dict[str, Any] = {\n            "event_id": event_id,\n            "job_id": job_id or event_id,\n            "execution_request_id": execution_request_id,\n            "robot_technical_id": self.technical_id,\n            "display_name": self.display_name,\n            "status": "completed" if success else "failed",\n            "started_at": summary.get("started_at"),\n            "finished_at": summary.get("finished_at") or utc_now_iso(),\n            "error_message": error_message,\n            "summary": summary,\n            "company_results": company_results or summary.get("company_results") or summary.get("companies") or [],\n            "responsible_office_result": responsible_office_result or summary.get("responsible_office_result"),\n            "payload": payload or {},\n        }\n        write_json_atomic(self.result_path, result_payload)\n        self.write_heartbeat(\n            status="active",\n            message="result_ready",\n            current_job_id=None,\n            current_execution_request_id=None,\n        )\n\n\n@dataclass(slots=True)\nclass RuntimeEnvironment:\n    paths: RuntimePaths\n    logger: RuntimeLogger\n    json_runtime: JsonRobotRuntime\n\n    def resolve_supabase_service_role_key(self) -> str:\n        for env_name in (\n            "SUPABASE_SERVICE_ROLE_KEY",\n            "SERVICE_ROLE_KEY",\n            "SUPABASE_KEY",\n            "SUPABASE_SECRET_KEY",\n            "NEXT_PUBLIC_SUPABASE_SERVICE_ROLE_KEY",\n        ):\n            value = (os.getenv(env_name) or "").strip()\n            if value:\n                return value\n        return ""\n\n    def generate_run_id(self) -> str:\n        return datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]\n\n    def mark_inactive(self, reason: str = "application_exit") -> None:\n        try:\n            self.json_runtime.write_heartbeat(status="inactive", message=reason)\n        except Exception:\n            pass\n\n\ndef build_runtime(sink: Optional[Callable[[str], None]] = None) -> RuntimeEnvironment:\n    paths = bootstrap_environment()\n    logger = RuntimeLogger(paths.runtime_log_path, sink=sink)\n    json_runtime = JsonRobotRuntime(ROBOT_TECHNICAL_ID, ROBOT_DISPLAY_NAME, paths.json_dir)\n    return RuntimeEnvironment(paths=paths, logger=logger, json_runtime=json_runtime)\n\n# === dashboard_client ===\nimport hashlib\nimport os\nfrom typing import Any, Iterable, Optional\n\nimport requests\n\n\n\nclass DashboardClient:\n    def __init__(self, runtime_env: RuntimeEnvironment) -> None:\n        self.runtime = runtime_env\n        self.supabase_url = (os.getenv("SUPABASE_URL") or "").strip()\n        self.supabase_key = runtime_env.resolve_supabase_service_role_key()\n        self.server_api_url = (\n            os.getenv("FOLDER_STRUCTURE_API_URL") or os.getenv("SERVER_API_URL") or ""\n        ).strip().rstrip("/")\n        self.connector_secret = (os.getenv("CONNECTOR_SECRET") or "").strip()\n        self._office_context: Optional[OfficeContext] = None\n        self._robot_config_cache: Optional[dict[str, Any]] = None\n        self._registered_robot_id: Optional[str] = None\n\n    def is_configured(self) -> bool:\n        return bool(self.supabase_url and self.supabase_key)\n\n    def _supabase(self):\n        if not self.supabase_url or not self.supabase_key:\n            raise RuntimeError("Supabase não configurado. Defina SUPABASE_URL e uma service role key.")\n        try:\n            from supabase import create_client\n        except Exception as exc:\n            raise RuntimeError("Biblioteca supabase não disponível no ambiente do robô.") from exc\n        return create_client(self.supabase_url, self.supabase_key)\n\n    def build_server_api_headers(self) -> dict[str, str]:\n        headers: dict[str, str] = {}\n        if self.server_api_url and "ngrok" in self.server_api_url.lower():\n            headers["ngrok-skip-browser-warning"] = "true"\n        if self.connector_secret:\n            hashed = hashlib.sha256(self.connector_secret.encode("utf-8")).hexdigest()\n            headers["Authorization"] = f"Bearer {hashed}"\n        return headers\n\n    def fetch_robot_config_from_api(self) -> Optional[dict[str, Any]]:\n        if not self.server_api_url:\n            return None\n        url = f"{self.server_api_url}/api/robot-config"\n        response = requests.get(\n            url,\n            params={"technical_id": ROBOT_TECHNICAL_ID},\n            headers=self.build_server_api_headers(),\n            timeout=20,\n        )\n        response.raise_for_status()\n        payload = response.json()\n        if not isinstance(payload, dict):\n            raise RuntimeError("Resposta inválida da API /api/robot-config.")\n        self._robot_config_cache = payload\n        return payload\n\n    def register_robot_presence(self, status: str = "active") -> Optional[str]:\n        try:\n            client = self._supabase()\n            api_cfg = self._robot_config_cache\n            if api_cfg is None:\n                try:\n                    api_cfg = self.fetch_robot_config_from_api()\n                except Exception:\n                    api_cfg = None\n            payload = {\n                "display_name": ROBOT_DISPLAY_NAME,\n                "status": status,\n                "last_heartbeat_at": utc_now_iso(),\n                "segment_path": str((api_cfg or {}).get("segment_path") or "").strip() or None,\n                "notes_mode": str((api_cfg or {}).get("notes_mode") or "").strip() or None,\n            }\n            response = client.table("robots").select("id").eq("technical_id", ROBOT_TECHNICAL_ID).limit(1).execute()\n            rows = getattr(response, "data", None) or []\n            if rows:\n                robot_id = str(rows[0].get("id") or "").strip()\n                if robot_id:\n                    client.table("robots").update(payload).eq("id", robot_id).execute()\n                    self._registered_robot_id = robot_id\n                    return robot_id\n            insert_payload = {"technical_id": ROBOT_TECHNICAL_ID, **payload}\n            inserted = client.table("robots").insert(insert_payload).execute()\n            inserted_rows = getattr(inserted, "data", None) or []\n            if inserted_rows:\n                robot_id = str(inserted_rows[0].get("id") or "").strip()\n                if robot_id:\n                    self._registered_robot_id = robot_id\n                    return robot_id\n            reread = client.table("robots").select("id").eq("technical_id", ROBOT_TECHNICAL_ID).limit(1).execute()\n            reread_rows = getattr(reread, "data", None) or []\n            if reread_rows:\n                robot_id = str(reread_rows[0].get("id") or "").strip()\n                self._registered_robot_id = robot_id or None\n                return robot_id or None\n            self.runtime.logger.warning("Falha ao registrar o robô na tabela robots: insert sem retorno.")\n            return None\n        except Exception as exc:\n            self.runtime.logger.warning(f"Falha ao registrar o robô na tabela robots: {exc}")\n            return None\n\n    def update_robot_presence(self, status: str = "active", robot_id: str = "") -> None:\n        try:\n            client = self._supabase()\n            update_payload = {\n                "status": status,\n                "last_heartbeat_at": utc_now_iso(),\n            }\n            target_id = str(robot_id or self._registered_robot_id or "").strip()\n            if target_id:\n                client.table("robots").update(update_payload).eq("id", target_id).execute()\n                return\n            client.table("robots").update(update_payload).eq("technical_id", ROBOT_TECHNICAL_ID).execute()\n        except Exception as exc:\n            self.runtime.logger.warning(f"Falha ao atualizar heartbeat do robô na tabela robots: {exc}")\n\n    def fetch_mailbox_runtime_config(self, office_context: OfficeContext) -> RobotMailboxConfig:\n        if not office_context.office_id:\n            return RobotMailboxConfig()\n        try:\n            client = self._supabase()\n            response = (\n                client.table("office_robot_configs")\n                .select("admin_settings")\n                .eq("office_id", office_context.office_id)\n                .eq("robot_technical_id", ROBOT_TECHNICAL_ID)\n                .limit(1)\n                .execute()\n            )\n            rows = getattr(response, "data", None) or []\n            admin_settings = rows[0].get("admin_settings") if rows else {}\n            if not isinstance(admin_settings, dict):\n                admin_settings = {}\n            return RobotMailboxConfig(\n                use_responsible_office=bool(admin_settings.get("use_responsible_office")),\n                responsible_office_company_id=str(admin_settings.get("responsible_office_company_id") or "").strip(),\n                source="office_robot_configs" if rows else "default",\n            )\n        except Exception as exc:\n            self.runtime.logger.warning(f"Falha ao carregar admin_settings do robô: {exc}")\n            return RobotMailboxConfig(source="default")\n\n    def _mailbox_external_message_id(self, message: MessageRecord) -> str:\n        candidate = str(message.message_id or "").strip()\n        if candidate:\n            return candidate\n        raw = "|".join(\n            [\n                str(message.company_id or "").strip(),\n                str(message.subject or "").strip(),\n                str(message.received_at or message.posted_at or "").strip(),\n                str(message.raw_visible_text or "").strip(),\n            ]\n        )\n        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]\n\n    def _mailbox_received_at_iso(self, message: MessageRecord) -> str:\n        raw = str(message.received_at or message.posted_at or message.sent_at or "").strip()\n        if not raw:\n            return message.extracted_at\n        for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%Y"):\n            try:\n                parsed = datetime.strptime(raw, fmt)\n                return parsed.replace(tzinfo=timezone.utc).isoformat()\n            except ValueError:\n                continue\n        return message.extracted_at\n\n    def _mailbox_status_tuple(self, message: MessageRecord) -> tuple[str, bool]:\n        read_status = str(message.read_status or "").strip().lower()\n        if message.unread is True:\n            return "novo", False\n        if message.unread is False:\n            return "lido", True\n        if any(token in read_status for token in ("não lida", "nao lida", "não visualizada", "nao visualizada", "nova")):\n            return "novo", False\n        if any(token in read_status for token in ("lida", "lido", "visualizada")):\n            return "lido", True\n        return "novo", False\n\n    def persist_mailbox_messages(\n        self,\n        office_context: OfficeContext,\n        result: CompanyRunResult,\n        *,\n        run_id: str,\n    ) -> int:\n        if not office_context.office_id or not result.messages:\n            return 0\n\n        try:\n            client = self._supabase()\n            payload_rows: list[dict[str, Any]] = []\n            external_ids: list[str] = []\n            for message in result.messages:\n                external_message_id = self._mailbox_external_message_id(message)\n                status, is_read = self._mailbox_status_tuple(message)\n                external_ids.append(external_message_id)\n                payload_rows.append(\n                    {\n                        "office_id": office_context.office_id,\n                        "company_id": result.company_id,\n                        "robot_technical_id": ROBOT_TECHNICAL_ID,\n                        "external_message_id": external_message_id,\n                        "subject": str(message.subject or "").strip() or "(sem assunto)",\n                        "sender_name": str(message.sender or "").strip() or None,\n                        "sender_document": None,\n                        "category": str(message.category or "").strip() or None,\n                        "status": status,\n                        "received_at": self._mailbox_received_at_iso(message),\n                        "fetched_at": str(message.extracted_at or utc_now_iso()).strip(),\n                        "is_read": is_read,\n                        "read_at": str(message.extracted_at or utc_now_iso()).strip() if is_read else None,\n                        "payload": {\n                            "run_id": run_id,\n                            "context_type": result.context_type,\n                            "company_profile_context": result.company_profile_context,\n                            "message_id": message.message_id,\n                            "subject": message.subject,\n                            "sender": message.sender,\n                            "category": message.category,\n                            "received_at_raw": message.received_at,\n                            "sent_at_raw": message.sent_at,\n                            "posted_at_raw": message.posted_at,\n                            "read_status": message.read_status,\n                            "unread": message.unread,\n                            "priority": message.priority,\n                            "preview": message.snippet,\n                            "body": message.body,\n                            "attachments": message.attachments,\n                            "raw_visible_text": message.raw_visible_text,\n                            "detail_visible_text": message.detail_visible_text,\n                            "source_system": message.source_system,\n                            "row_index": message.row_index,\n                            "extracted_at": message.extracted_at,\n                        },\n                    }\n                )\n\n            if external_ids:\n                client.table("ecac_mailbox_messages").delete().eq("office_id", office_context.office_id).eq(\n                    "company_id", result.company_id\n                ).eq("robot_technical_id", ROBOT_TECHNICAL_ID).in_("external_message_id", external_ids).execute()\n            client.table("ecac_mailbox_messages").insert(payload_rows).execute()\n            self.runtime.logger.info(\n                f"Supabase: {len(payload_rows)} mensagem(ns) da Caixa Postal sincronizada(s) para {result.company_name}."\n            )\n            return len(payload_rows)\n        except Exception as exc:\n            self.runtime.logger.warning(f"Falha ao sincronizar Caixa Postal no Supabase para {result.company_name}: {exc}")\n            return 0\n\n    def resolve_office_context(self, job: Optional[JobPayload] = None, force_refresh: bool = False) -> OfficeContext:\n        if self._office_context and not force_refresh:\n            return self._office_context\n\n        payload: Optional[dict[str, Any]] = None\n        if force_refresh or self._robot_config_cache is None:\n            try:\n                payload = self.fetch_robot_config_from_api()\n            except Exception as exc:\n                self.runtime.logger.warning(f"Falha ao consultar /api/robot-config: {exc}")\n        else:\n            payload = self._robot_config_cache\n\n        office_id = ""\n        office_server_id = ""\n        base_path = ""\n        segment_path = ""\n        notes_mode = ""\n        source = ""\n\n        if isinstance(payload, dict):\n            office_id = str(payload.get("office_id") or "").strip()\n            office_server_id = str(payload.get("office_server_id") or "").strip()\n            base_path = str(payload.get("base_path") or "").strip()\n            segment_path = str(payload.get("segment_path") or "").strip()\n            notes_mode = str(payload.get("notes_mode") or "").strip()\n            if office_id:\n                source = "server_api"\n\n        if not office_id and job:\n            office_id = job.office_id\n            if office_id:\n                source = "job_json"\n\n        if not office_id:\n            office_id = str(os.getenv("OFFICE_ID") or "").strip()\n            office_server_id = office_server_id or str(os.getenv("OFFICE_SERVER_ID") or "").strip()\n            if office_id:\n                source = "env"\n\n        context = OfficeContext(\n            office_id=office_id,\n            office_server_id=office_server_id,\n            base_path=base_path,\n            segment_path=segment_path,\n            notes_mode=notes_mode,\n            source=source,\n        )\n        self._office_context = context\n        return context\n\n    def _normalize_job_companies(\n        self,\n        job: Optional[JobPayload],\n        company_ids: Optional[Iterable[str]] = None,\n    ) -> list[CompanyRecord]:\n        if not job or not job.companies:\n            return []\n        wanted = {str(item).strip() for item in (company_ids or []) if str(item).strip()}\n        rows: list[CompanyRecord] = []\n        for row in job.companies:\n            company_id = str(row.get("company_id") or row.get("id") or "").strip()\n            if not company_id or (wanted and company_id not in wanted):\n                continue\n            name = str(row.get("name") or "").strip()\n            document = only_digits(row.get("document") or row.get("doc") or "")\n            active = bool(row.get("active", True))\n            eligible = bool(row.get("eligible", active))\n            block_reason = str(row.get("block_reason") or "").strip()\n            auth_mode = str(\n                row.get("auth_mode")\n                or ("certificate" if row.get("cert_blob_b64") or row.get("cert_path") else "password")\n            ).strip().lower()\n            if auth_mode not in {"password", "certificate"}:\n                auth_mode = "password"\n            if not active and not block_reason:\n                block_reason = "Empresa inativa no dashboard."\n            rows.append(\n                CompanyRecord(\n                    company_id=company_id,\n                    name=name,\n                    document=document,\n                    active=active,\n                    eligible=eligible and active,\n                    block_reason=block_reason,\n                    source="job_json",\n                    auth_mode=auth_mode,\n                    cert_password=str(row.get("cert_password") or "").strip(),\n                    cert_blob_b64=str(row.get("cert_blob_b64") or "").strip(),\n                    cert_path=str(row.get("cert_path") or "").strip(),\n                    raw=dict(row),\n                )\n            )\n        return sorted(rows, key=lambda item: item.name.lower())\n\n    def load_companies(\n        self,\n        office_context: OfficeContext,\n        *,\n        job: Optional[JobPayload] = None,\n        company_ids: Optional[Iterable[str]] = None,\n    ) -> list[CompanyRecord]:\n        snapshot_rows = self._normalize_job_companies(job, company_ids)\n        if snapshot_rows:\n            return snapshot_rows\n\n        if not office_context.office_id:\n            raise RuntimeError("office_id não resolvido. Verifique o conector da VM e /api/robot-config.")\n\n        client = self._supabase()\n        wanted_ids = [str(item).strip() for item in (company_ids or []) if str(item).strip()]\n        config_query = (\n            client.table("company_robot_config")\n            .select("company_id,enabled,settings,auth_mode")\n            .eq("office_id", office_context.office_id)\n            .eq("robot_technical_id", ROBOT_TECHNICAL_ID)\n            .eq("enabled", True)\n        )\n        if wanted_ids:\n            config_query = config_query.in_("company_id", wanted_ids)\n        config_rows = getattr(config_query.execute(), "data", None) or []\n\n        config_by_company: dict[str, dict[str, Any]] = {}\n        enabled_company_ids: list[str] = []\n        for row in config_rows:\n            company_id = str(row.get("company_id") or "").strip()\n            if not company_id:\n                continue\n            normalized_row = dict(row)\n            settings = normalized_row.get("settings")\n            if isinstance(settings, str):\n                try:\n                    settings = json.loads(settings)\n                except Exception:\n                    settings = {}\n            if not isinstance(settings, dict):\n                settings = {}\n            normalized_row["settings"] = settings\n            config_by_company[company_id] = normalized_row\n            enabled_company_ids.append(company_id)\n\n        target_company_ids = wanted_ids or enabled_company_ids\n        companies_query = (\n            client.table("companies")\n            .select("id,name,document,active,office_id,auth_mode,cert_blob_b64,cert_password")\n            .eq("office_id", office_context.office_id)\n            .order("name")\n        )\n        if target_company_ids:\n            companies_query = companies_query.in_("id", target_company_ids)\n        elif not enabled_company_ids:\n            companies_query = companies_query.eq("active", True)\n        company_rows = getattr(companies_query.execute(), "data", None) or []\n\n        normalized: list[CompanyRecord] = []\n        for row in company_rows:\n            company_id = str(row.get("id") or "").strip()\n            name = str(row.get("name") or "").strip()\n            document = only_digits(row.get("document") or "")\n            active = bool(row.get("active", False))\n            company_auth_mode = str(row.get("auth_mode") or ("certificate" if row.get("cert_blob_b64") else "password")).strip().lower()\n            if company_auth_mode not in {"password", "certificate"}:\n                company_auth_mode = "password"\n            cfg = config_by_company.get(company_id)\n            cfg_settings = dict((cfg or {}).get("settings") or {})\n            cfg_auth_mode = str((cfg or {}).get("auth_mode") or cfg_settings.get("auth_mode") or "").strip().lower()\n            if cfg_auth_mode not in {"", "password", "certificate"}:\n                cfg_auth_mode = ""\n            auth_mode = cfg_auth_mode or company_auth_mode\n            cert_password = str(cfg_settings.get("cert_password") or row.get("cert_password") or "").strip()\n            cert_blob_b64 = str(cfg_settings.get("cert_blob_b64") or row.get("cert_blob_b64") or "").strip()\n            cert_path = str(cfg_settings.get("cert_path") or "").strip()\n            if auth_mode != "certificate":\n                cert_password = ""\n                cert_blob_b64 = ""\n                cert_path = ""\n            if wanted_ids:\n                eligible = active\n                block_reason = "" if active else "Empresa inativa no dashboard."\n            elif enabled_company_ids:\n                eligible = active and bool(cfg)\n                block_reason = ""\n                if not active:\n                    block_reason = "Empresa inativa no dashboard."\n                elif not cfg:\n                    block_reason = "Empresa sem company_robot_config habilitado para este robô."\n            else:\n                eligible = active\n                block_reason = "" if active else "Empresa inativa no dashboard."\n            normalized.append(\n                CompanyRecord(\n                    company_id=company_id,\n                    name=name,\n                    document=document,\n                    active=active,\n                    eligible=eligible,\n                    block_reason=block_reason,\n                    source="job_scope" if wanted_ids else ("company_robot_config" if enabled_company_ids else "companies_fallback"),\n                    auth_mode=auth_mode,\n                    cert_password=cert_password,\n                    cert_blob_b64=cert_blob_b64,\n                    cert_path=cert_path,\n                    raw={"company": dict(row), "config": dict(cfg or {})},\n                )\n            )\n\n        if wanted_ids:\n            missing = {item for item in wanted_ids if item not in {company.company_id for company in normalized}}\n            for company_id in sorted(missing):\n                normalized.append(\n                    CompanyRecord(\n                        company_id=company_id,\n                        name=f"Empresa {company_id}",\n                        document="",\n                        active=False,\n                        eligible=False,\n                        block_reason="Empresa não encontrada no escopo do escritório.",\n                        source="missing",\n                        auth_mode="password",\n                        raw={},\n                    )\n                )\n\n        normalized.sort(key=lambda item: item.name.lower())\n        return normalized\n\n    def load_company_by_id(self, office_context: OfficeContext, company_id: str) -> Optional[CompanyRecord]:\n        company_id = str(company_id or "").strip()\n        if not company_id:\n            return None\n        rows = self.load_companies(office_context, job=None, company_ids=[company_id])\n        return rows[0] if rows else None\n\n# === certificate_auth ===\nimport json\nimport os\nimport platform\nimport re\nimport shutil\nimport subprocess\nimport textwrap\nfrom pathlib import Path\nfrom typing import Optional\n\n\n\ndef _ps_quote(value: str) -> str:\n    return str(value or "").replace("\'", "\'\'")\n\n\ndef ensure_windows_environment() -> None:\n    if platform.system().lower() != "windows":\n        raise RuntimeError("Este robô é Windows-only para o fluxo de certificado digital via PowerShell/UI Automation.")\n    if shutil.which("powershell") is None:\n        raise RuntimeError("PowerShell não encontrado no PATH. O fluxo de certificado do e-CAC depende dele.")\n\n\ndef run_powershell(script: str, timeout_ms: int = 60000) -> str:\n    ensure_windows_environment()\n    creationflags = 0\n    try:\n        creationflags = subprocess.CREATE_NO_WINDOW  # type: ignore[attr-defined]\n    except Exception:\n        creationflags = 0\n\n    result = subprocess.run(\n        [\n            "powershell",\n            "-NoProfile",\n            "-NonInteractive",\n            "-ExecutionPolicy",\n            "Bypass",\n            "-WindowStyle",\n            "Hidden",\n            "-Command",\n            script,\n        ],\n        capture_output=True,\n        text=True,\n        encoding="utf-8",\n        errors="ignore",\n        timeout=max(1, timeout_ms // 1000),\n        check=False,\n        creationflags=creationflags,\n    )\n    stdout = (result.stdout or "").strip()\n    stderr = (result.stderr or "").strip()\n    if result.returncode != 0:\n        message = f"PowerShell retornou código {result.returncode}."\n        if stderr:\n            message += f"\\nErro:\\n{stderr[-1800:]}"\n        elif stdout:\n            message += f"\\nSaída:\\n{stdout[-1800:]}"\n        raise RuntimeError(message)\n    return stdout\n\n\ndef load_registry(path: Path) -> list[CertificateMetadata]:\n    if not path.exists():\n        return []\n    payload = read_json(path, default=[])\n    if not isinstance(payload, list):\n        return []\n    certificates: list[CertificateMetadata] = []\n    for item in payload:\n        if not isinstance(item, dict):\n            continue\n        certificates.append(\n            CertificateMetadata(\n                alias=str(item.get("alias") or item.get("name") or "").strip(),\n                subject=str(item.get("subject") or "").strip(),\n                issuer=str(item.get("issuer") or "").strip(),\n                thumbprint=str(item.get("thumbprint") or "").strip(),\n                pfx_path=str(item.get("pfx_path") or "").strip(),\n                source=str(item.get("source") or "registry").strip(),\n            )\n        )\n    return certificates\n\n\ndef save_registry(path: Path, certificates: list[CertificateMetadata]) -> None:\n    rows = [item.to_dict() for item in certificates]\n    path.parent.mkdir(parents=True, exist_ok=True)\n    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\\n", encoding="utf-8")\n\n\ndef build_selector_candidates(subject: str, alias: str = "") -> list[str]:\n    candidates: list[str] = []\n    raw_values = [alias.strip(), subject.strip()]\n    for value in raw_values:\n        if value and value not in candidates:\n            candidates.append(value)\n\n    match = re.search(r"CN=([^,]+)", subject or "", flags=re.IGNORECASE)\n    if match:\n        cn = match.group(1).strip()\n        if cn and cn not in candidates:\n            candidates.append(cn)\n\n    for current in list(candidates):\n        normalized = current.replace("CN=", "").strip()\n        if normalized and normalized not in candidates:\n            candidates.append(normalized)\n        name_only = normalized.split(":")[0].strip()\n        if name_only and name_only not in candidates:\n            candidates.append(name_only)\n    return candidates\n\n\ndef import_pfx_and_get_metadata(pfx_path: Path, pfx_password: str) -> CertificateMetadata:\n    ensure_windows_environment()\n    if not pfx_path.exists():\n        raise FileNotFoundError(f"Arquivo PFX não encontrado: {pfx_path}")\n    script = textwrap.dedent(\n        f"""\n        $path = \'{_ps_quote(str(pfx_path))}\'\n        $password = ConvertTo-SecureString \'{_ps_quote(pfx_password)}\' -AsPlainText -Force\n        $cert = Import-PfxCertificate -FilePath $path -CertStoreLocation Cert:\\\\CurrentUser\\\\My -Password $password\n        if (-not $cert) {{\n            throw \'Falha ao importar o certificado para Cert:\\\\CurrentUser\\\\My.\'\n        }}\n        ($cert | Select-Object -First 1 Subject, Thumbprint, Issuer) | ConvertTo-Json -Compress\n        """\n    )\n    payload = json.loads(run_powershell(script))\n    return CertificateMetadata(\n        alias=pfx_path.stem,\n        subject=str(payload.get("Subject") or "").strip(),\n        issuer=str(payload.get("Issuer") or "").strip(),\n        thumbprint=str(payload.get("Thumbprint") or "").strip(),\n        pfx_path=str(pfx_path),\n        source="imported_pfx",\n    )\n\n\ndef find_certificate_in_store(subject: str, issuer: str = "", thumbprint: str = "") -> CertificateMetadata:\n    ensure_windows_environment()\n    filters: list[str] = []\n    if thumbprint:\n        filters.append(f"$_.Thumbprint -eq \'{_ps_quote(thumbprint)}\'")\n    if subject:\n        filters.append(f"$_.Subject -like \'*{_ps_quote(subject)}*\'")\n    if issuer:\n        filters.append(f"$_.Issuer -like \'*{_ps_quote(issuer)}*\'")\n    where_clause = " -and ".join(filters) if filters else "$true"\n    script = textwrap.dedent(\n        f"""\n        $cert = Get-ChildItem Cert:\\\\CurrentUser\\\\My |\n            Where-Object {{ {where_clause} }} |\n            Select-Object -First 1 Subject, Thumbprint, Issuer\n        if (-not $cert) {{\n            throw \'Certificado alvo não encontrado em Cert:\\\\CurrentUser\\\\My.\'\n        }}\n        $cert | ConvertTo-Json -Compress\n        """\n    )\n    payload = json.loads(run_powershell(script))\n    return CertificateMetadata(\n        alias="",\n        subject=str(payload.get("Subject") or "").strip(),\n        issuer=str(payload.get("Issuer") or "").strip(),\n        thumbprint=str(payload.get("Thumbprint") or "").strip(),\n        source="store",\n    )\n\n\ndef register_certificate(\n    runtime_env: RuntimeEnvironment,\n    *,\n    alias: str,\n    pfx_path: Path,\n    password: str,\n) -> CertificateMetadata:\n    metadata = import_pfx_and_get_metadata(pfx_path, password)\n    metadata.alias = alias.strip() or metadata.alias or pfx_path.stem\n    metadata.pfx_path = ""\n    metadata.source = "manual_import"\n    return metadata\n\n\ndef resolve_certificate_from_env() -> Optional[CertificateMetadata]:\n    pfx_path = (os.getenv("ECAC_CERT_PFX_PATH") or "").strip()\n    pfx_password = os.getenv("ECAC_CERT_PFX_PASSWORD") or ""\n    subject = (os.getenv("ECAC_CERT_SUBJECT") or "").strip()\n    issuer = (os.getenv("ECAC_CERT_ISSUER") or "").strip()\n\n    if pfx_path:\n        metadata = import_pfx_and_get_metadata(Path(pfx_path), pfx_password)\n        if subject:\n            metadata.subject = subject\n        if issuer:\n            metadata.issuer = issuer\n        metadata.alias = metadata.alias or Path(pfx_path).stem\n        metadata.source = "env_pfx"\n        return metadata\n\n    if subject:\n        metadata = find_certificate_in_store(subject=subject, issuer=issuer)\n        metadata.alias = metadata.alias or subject\n        metadata.source = "env_store"\n        return metadata\n    return None\n\n\ndef peek_certificate_configuration_from_env() -> Optional[CertificateMetadata]:\n    pfx_path = (os.getenv("ECAC_CERT_PFX_PATH") or "").strip()\n    subject = (os.getenv("ECAC_CERT_SUBJECT") or "").strip()\n    issuer = (os.getenv("ECAC_CERT_ISSUER") or "").strip()\n    if not any([pfx_path, subject, issuer]):\n        return None\n    return CertificateMetadata(\n        alias=Path(pfx_path).stem if pfx_path else (subject or "Certificado via .env"),\n        subject=subject,\n        issuer=issuer,\n        pfx_path=pfx_path,\n        source="env_hint",\n    )\n\n\ndef _decode_certificate_blob(cert_blob_b64: str) -> bytes:\n    raw = str(cert_blob_b64 or "").strip()\n    if not raw:\n        raise ValueError("cert_blob_b64 vazio.")\n    if "," in raw and raw.lower().startswith("data:"):\n        raw = raw.split(",", 1)[1]\n    raw = "".join(raw.split())\n    return base64.b64decode(raw)\n\n\ndef peek_certificate_configuration_from_dashboard(companies: list[CompanyRecord]) -> Optional[CertificateMetadata]:\n    for company in companies:\n        auth_mode = str(company.auth_mode or "").strip().lower()\n        if auth_mode != "certificate" and not company.cert_blob_b64 and not company.cert_path:\n            continue\n        if not company.cert_blob_b64 and not company.cert_path:\n            continue\n        return CertificateMetadata(\n            alias=f"{company.name} (dashboard)",\n            subject="",\n            issuer="",\n            thumbprint="",\n            pfx_path="",\n            source="dashboard_hint",\n        )\n    return None\n\n\ndef resolve_certificate_from_dashboard(\n    runtime_env: RuntimeEnvironment,\n    companies: list[CompanyRecord],\n) -> Optional[CertificateMetadata]:\n    for company in companies:\n        auth_mode = str(company.auth_mode or "").strip().lower()\n        if auth_mode != "certificate" and not company.cert_blob_b64 and not company.cert_path:\n            continue\n        if not company.cert_blob_b64 and not company.cert_path:\n            continue\n        cert_password = str(company.cert_password or "").strip()\n        if not cert_password:\n            raise RuntimeError(f"Senha do certificado não configurada no dashboard para {company.name}.")\n        if company.cert_blob_b64:\n            try:\n                cert_data = _decode_certificate_blob(company.cert_blob_b64)\n            except Exception as exc:\n                raise RuntimeError(f"Falha ao decodificar cert_blob_b64 do dashboard para {company.name}: {exc}") from exc\n        else:\n            cert_path = Path(str(company.cert_path or "")).expanduser()\n            if not cert_path.is_file():\n                raise RuntimeError(f"Caminho de certificado do dashboard inválido para {company.name}: {cert_path}")\n            cert_data = cert_path.read_bytes()\n\n        temp_file = tempfile.NamedTemporaryFile(\n            suffix=".pfx",\n            prefix=f"ecac_caixa_postal_{slugify(company.name or company.document)}_",\n            delete=False,\n        )\n        temp_pfx = Path(temp_file.name)\n        try:\n            temp_file.write(cert_data)\n            temp_file.flush()\n            temp_file.close()\n            metadata = import_pfx_and_get_metadata(temp_pfx, cert_password)\n        finally:\n            try:\n                temp_file.close()\n            except Exception:\n                pass\n            try:\n                if temp_pfx.exists():\n                    temp_pfx.unlink()\n            except Exception:\n                pass\n        metadata.alias = f"{company.name} (dashboard)"\n        metadata.pfx_path = ""\n        metadata.source = "dashboard_company"\n        return metadata\n    return None\n\n\ndef resolve_certificate(\n    runtime_env: RuntimeEnvironment,\n    companies: Optional[list[CompanyRecord]] = None,\n    preferred_thumbprint: str = "",\n) -> Optional[CertificateMetadata]:\n    env_certificate = resolve_certificate_from_env()\n    if env_certificate:\n        return env_certificate\n\n    if companies:\n        dashboard_certificate = resolve_certificate_from_dashboard(runtime_env, companies)\n        if dashboard_certificate:\n            return dashboard_certificate\n    return None\n\n\ndef wait_for_certificate_dialog(timeout_seconds: int = 12) -> bool:\n    script = textwrap.dedent(\n        f"""\n        Add-Type -AssemblyName UIAutomationClient\n        Add-Type -AssemblyName UIAutomationTypes\n        $deadline = (Get-Date).AddSeconds({timeout_seconds})\n        $root = [System.Windows.Automation.AutomationElement]::RootElement\n        $dialogTitle = \'Selecione um certificado\'\n        while ((Get-Date) -lt $deadline) {{\n            $condition = New-Object System.Windows.Automation.PropertyCondition(\n                [System.Windows.Automation.AutomationElement]::NameProperty,\n                $dialogTitle\n            )\n            $dialog = $root.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $condition)\n            if ($dialog) {{\n                Write-Output \'FOUND\'\n                exit 0\n            }}\n            Start-Sleep -Milliseconds 250\n        }}\n        Write-Output \'NOT_FOUND\'\n        """\n    )\n    return run_powershell(script, timeout_ms=(timeout_seconds + 5) * 1000).strip() == "FOUND"\n\n\ndef select_certificate_dialog(certificate: CertificateMetadata, timeout_ms: int = 45000) -> None:\n    targets = build_selector_candidates(certificate.subject, certificate.alias)\n    if not targets:\n        raise RuntimeError("Nenhum seletor de certificado disponível para automação da janela nativa.")\n\n    powershell_targets = "@(" + ", ".join("\'" + _ps_quote(item) + "\'" for item in targets) + ")"\n    script = textwrap.dedent(\n        f"""\n        Add-Type -AssemblyName UIAutomationClient\n        Add-Type -AssemblyName UIAutomationTypes\n        Add-Type @"\n        using System;\n        using System.Runtime.InteropServices;\n        public struct POINT {{\n          public int X;\n          public int Y;\n        }}\n        public class NativeCertUi {{\n          [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr hWnd);\n          [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);\n          [DllImport("user32.dll")] public static extern IntPtr GetForegroundWindow();\n          [DllImport("user32.dll")] public static extern bool GetCursorPos(out POINT lpPoint);\n          [DllImport("user32.dll")] public static extern bool SetCursorPos(int X, int Y);\n          [DllImport("user32.dll")] public static extern void mouse_event(uint dwFlags, uint dx, uint dy, uint dwData, UIntPtr dwExtraInfo);\n        }}\n        "@\n\n        $targets = {powershell_targets}\n        $dialogTitle = \'Selecione um certificado\'\n        $root = [System.Windows.Automation.AutomationElement]::RootElement\n        $deadline = (Get-Date).AddMilliseconds({timeout_ms})\n        $dialog = $null\n\n        while ((Get-Date) -lt $deadline -and -not $dialog) {{\n            $condition = New-Object System.Windows.Automation.PropertyCondition(\n                [System.Windows.Automation.AutomationElement]::NameProperty,\n                $dialogTitle\n            )\n            $dialog = $root.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $condition)\n            if (-not $dialog) {{ Start-Sleep -Milliseconds 250 }}\n        }}\n\n        if (-not $dialog) {{\n            throw \'Janela de selecao de certificado nao apareceu.\'\n        }}\n\n        function Matches-Target([string]$name, $targets) {{\n            if (-not $name) {{ return $false }}\n            foreach ($target in $targets) {{\n                if (-not $target) {{ continue }}\n                if ($name -eq $target -or $target.Contains($name) -or $name.Contains($target)) {{\n                    return $true\n                }}\n            }}\n            return $false\n        }}\n\n        function Get-OkButton($dialog) {{\n            $condition = New-Object System.Windows.Automation.PropertyCondition(\n                [System.Windows.Automation.AutomationElement]::NameProperty,\n                \'OK\'\n            )\n            return $dialog.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $condition)\n        }}\n\n        function Wait-Closed() {{\n            $closeDeadline = (Get-Date).AddSeconds(8)\n            while ((Get-Date) -lt $closeDeadline) {{\n                $condition = New-Object System.Windows.Automation.PropertyCondition(\n                    [System.Windows.Automation.AutomationElement]::NameProperty,\n                    $dialogTitle\n                )\n                $stillOpen = $root.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $condition)\n                if (-not $stillOpen) {{\n                    return $true\n                }}\n                Start-Sleep -Milliseconds 180\n            }}\n            return $false\n        }}\n\n        function Click-Center($element) {{\n            if (-not $element) {{ return $false }}\n            $rect = $element.Current.BoundingRectangle\n            if ($rect.Width -le 0 -or $rect.Height -le 0) {{ return $false }}\n            $x = [int]($rect.Left + ($rect.Width / 2))\n            $y = [int]($rect.Top + ($rect.Height / 2))\n            [NativeCertUi]::SetCursorPos($x, $y) | Out-Null\n            Start-Sleep -Milliseconds 80\n            [NativeCertUi]::mouse_event(0x0002, 0, 0, 0, [UIntPtr]::Zero)\n            Start-Sleep -Milliseconds 40\n            [NativeCertUi]::mouse_event(0x0004, 0, 0, 0, [UIntPtr]::Zero)\n            Start-Sleep -Milliseconds 180\n            return $true\n        }}\n\n        function Find-DialogList($dialog) {{\n            $all = $dialog.FindAll([System.Windows.Automation.TreeScope]::Descendants, [System.Windows.Automation.Condition]::TrueCondition)\n            foreach ($item in $all) {{\n                $type = [string]$item.Current.ControlType.ProgrammaticName\n                if ($type -in @(\'ControlType.DataGrid\', \'ControlType.List\', \'ControlType.Table\')) {{\n                    return $item\n                }}\n            }}\n            return $dialog\n        }}\n\n        function Find-ChromeWindow($root) {{\n            $wins = $root.FindAll([System.Windows.Automation.TreeScope]::Children, [System.Windows.Automation.Condition]::TrueCondition)\n            $preferred = @()\n            for ($i = 0; $i -lt $wins.Count; $i++) {{\n                $w = $wins.Item($i)\n                $name = [string]$w.Current.Name\n                if (-not $name) {{ continue }}\n                $lname = $name.ToLower()\n                if ($lname.Contains(\'gov.br\') -and ($lname.Contains(\'chrome\') -or $lname.Contains(\'edge\'))) {{\n                    $preferred += $w\n                }}\n            }}\n            if ($preferred.Count -gt 0) {{ return $preferred[0] }}\n            for ($i = 0; $i -lt $wins.Count; $i++) {{\n                $w = $wins.Item($i)\n                $name = [string]$w.Current.Name\n                if (-not $name) {{ continue }}\n                $lname = $name.ToLower()\n                if ($lname.Contains(\'google chrome\') -or $lname.Contains(\'chrome\') -or $lname.Contains(\'microsoft edge\') -or $lname.Contains(\'edge\')) {{\n                    return $w\n                }}\n            }}\n            return $null\n        }}\n\n        $previousWindow = [NativeCertUi]::GetForegroundWindow()\n        $cursor = New-Object POINT\n        [NativeCertUi]::GetCursorPos([ref]$cursor) | Out-Null\n\n        try {{\n            $listHost = Find-DialogList $dialog\n            if (-not (Click-Center $listHost)) {{\n                $chromeWindow = Find-ChromeWindow $root\n                if (-not $chromeWindow) {{\n                    throw \'Nao encontrei uma janela do Chrome/Edge para focar. Deixe o navegador visivel e tente novamente.\'\n                }}\n                $chromeHandle = [IntPtr]$chromeWindow.Current.NativeWindowHandle\n                if ($chromeHandle -eq [IntPtr]::Zero) {{\n                    throw \'A janela principal do Chrome nao possui handle valido.\'\n                }}\n                [NativeCertUi]::ShowWindow($chromeHandle, 5) | Out-Null\n                [NativeCertUi]::SetForegroundWindow($chromeHandle) | Out-Null\n                Start-Sleep -Milliseconds 150\n                $rect = $chromeWindow.Current.BoundingRectangle\n                $clickX = [int]($rect.Left + ($rect.Width * 0.48))\n                $clickY = [int]($rect.Top + ($rect.Height * 0.31))\n                [NativeCertUi]::SetCursorPos($clickX, $clickY) | Out-Null\n                Start-Sleep -Milliseconds 80\n                [NativeCertUi]::mouse_event(0x0002, 0, 0, 0, [UIntPtr]::Zero)\n                Start-Sleep -Milliseconds 40\n                [NativeCertUi]::mouse_event(0x0004, 0, 0, 0, [UIntPtr]::Zero)\n                Start-Sleep -Milliseconds 180\n            }}\n\n            $okButton = Get-OkButton $dialog\n            $wsh = New-Object -ComObject WScript.Shell\n            $wsh.SendKeys(\'{{HOME}}\')\n            Start-Sleep -Milliseconds 220\n\n            for ($step = 0; $step -lt 60; $step++) {{\n                $focusedName = \'\'\n                try {{\n                    $focused = [System.Windows.Automation.AutomationElement]::FocusedElement\n                    if ($focused) {{\n                        $focusedName = [string]$focused.Current.Name\n                    }}\n                }} catch {{}}\n                if (Matches-Target $focusedName $targets) {{\n                    if ($okButton) {{\n                        Click-Center $okButton | Out-Null\n                    }} else {{\n                        $wsh.SendKeys(\'~\')\n                    }}\n                    if (Wait-Closed) {{\n                        exit 0\n                    }}\n                    throw (\'O seletor permaneceu aberto apos confirmar o certificado: \' + $focusedName)\n                }}\n                if ($step -lt 59) {{\n                    $wsh.SendKeys(\'{{DOWN}}\')\n                    Start-Sleep -Milliseconds 180\n                }}\n            }}\n\n            throw \'Nao foi possivel alcancar o certificado alvo navegando pela lista nativa.\'\n        }} finally {{\n            [NativeCertUi]::SetCursorPos($cursor.X, $cursor.Y) | Out-Null\n            if ($previousWindow -ne [IntPtr]::Zero) {{\n                [NativeCertUi]::SetForegroundWindow($previousWindow) | Out-Null\n            }}\n        }}\n        """\n    )\n    run_powershell(script, timeout_ms=timeout_ms + 15000)\n\n\n\nimport types as _types_singlefile\ncertificate_auth = _types_singlefile.SimpleNamespace(\n    ensure_windows_environment=ensure_windows_environment,\n    run_powershell=run_powershell,\n    load_registry=load_registry,\n    save_registry=save_registry,\n    build_selector_candidates=build_selector_candidates,\n    import_pfx_and_get_metadata=import_pfx_and_get_metadata,\n    find_certificate_in_store=find_certificate_in_store,\n    register_certificate=register_certificate,\n    resolve_certificate_from_env=resolve_certificate_from_env,\n    peek_certificate_configuration_from_env=peek_certificate_configuration_from_env,\n    peek_certificate_configuration_from_dashboard=peek_certificate_configuration_from_dashboard,\n    resolve_certificate_from_dashboard=resolve_certificate_from_dashboard,\n    resolve_certificate=resolve_certificate,\n    wait_for_certificate_dialog=wait_for_certificate_dialog,\n    select_certificate_dialog=select_certificate_dialog,\n)\n\n# === ecac_browser ===\nimport os\nimport time\nfrom dataclasses import dataclass\nfrom pathlib import Path\nfrom typing import Optional\n\n\n\n@dataclass(slots=True)\nclass BrowserSession:\n    playwright: object\n    context: object\n    page: object\n    executable_path: str\n\n\ndef _playwright_browser_candidates() -> list[Path]:\n    root = Path(os.environ.get("PLAYWRIGHT_BROWSERS_PATH") or "").expanduser()\n    if not root.exists():\n        return []\n    candidates: list[Path] = []\n    for folder in sorted(root.glob("chromium-*"), reverse=True):\n        exe = folder / "chrome-win64" / "chrome.exe"\n        if exe.exists():\n            candidates.append(exe)\n            continue\n        exe = folder / "chrome-win" / "chrome.exe"\n        if exe.exists():\n            candidates.append(exe)\n    return candidates\n\n\ndef resolve_browser_executable(runtime_env: RuntimeEnvironment) -> Path:\n    base_dir = runtime_env.paths.base_dir\n    runtime_dir = runtime_env.paths.runtime_dir\n    data_dir = runtime_env.paths.data_dir\n    candidates = [\n        data_dir / "Chrome" / "chrome.exe",\n        data_dir / "_internal" / "Chrome" / "chrome.exe",\n        base_dir / "Chrome" / "chrome.exe",\n        base_dir / "_internal" / "Chrome" / "chrome.exe",\n        runtime_dir / "_internal" / "Chrome" / "chrome.exe",\n        runtime_dir / "Chrome" / "chrome.exe",\n        Path(os.environ.get("LOCALAPPDATA", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",\n        Path(os.environ.get("PROGRAMFILES", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",\n        Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",\n        Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",\n        Path(os.environ.get("PROGRAMFILES", "")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",\n        Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",\n    ]\n    candidates.extend(_playwright_browser_candidates())\n\n    seen: set[str] = set()\n    normalized: list[Path] = []\n    for candidate in candidates:\n        key = str(candidate.resolve(strict=False)).lower()\n        if key in seen:\n            continue\n        seen.add(key)\n        normalized.append(candidate)\n\n    for candidate in normalized:\n        if candidate.exists():\n            return candidate\n\n    checked = " | ".join(str(item) for item in normalized)\n    raise FileNotFoundError(f"Chrome/Edge não encontrado. Caminhos verificados: {checked}")\n\n\ndef _cleanup_profile_runtime_files(profile_dir: Path) -> None:\n    runtime_entries = [\n        "DevToolsActivePort",\n        "SingletonCookie",\n        "SingletonLock",\n        "SingletonSocket",\n        "lockfile",\n        "chrome_debug.log",\n    ]\n    for name in runtime_entries:\n        try:\n            target = profile_dir / name\n            if target.exists():\n                target.unlink()\n        except Exception:\n            pass\n    for nested_name in ("LOCK",):\n        try:\n            for target in profile_dir.rglob(nested_name):\n                if target.is_file():\n                    target.unlink()\n        except Exception:\n            pass\n\n\ndef _is_profile_healthy(profile_dir: Path) -> bool:\n    required = [\n        profile_dir / "Local State",\n        profile_dir / "Default" / "Preferences",\n    ]\n    return all(path.exists() for path in required)\n\n\ndef _copy_profile_tree(source_dir: Path, target_dir: Path) -> None:\n    ignore_names = {\n        "Cache",\n        "Code Cache",\n        "GPUCache",\n        "GrShaderCache",\n        "GraphiteDawnCache",\n        "DawnGraphiteCache",\n        "DawnWebGPUCache",\n        "ShaderCache",\n        "Crashpad",\n        "BrowserMetrics",\n        "BrowserMetrics-spare.pma",\n        "DevToolsActivePort",\n        "SingletonCookie",\n        "SingletonLock",\n        "SingletonSocket",\n        "lockfile",\n        "chrome_debug.log",\n        "LOCK",\n        "Cookies-journal",\n        "Network Persistent State",\n        "Session Storage",\n        "Sessions",\n        "shared_proto_db",\n    }\n\n    def _ignore(_dir: str, names: list[str]) -> set[str]:\n        return {name for name in names if name in ignore_names}\n\n    if target_dir.exists():\n        shutil.rmtree(target_dir, ignore_errors=True)\n    shutil.copytree(source_dir, target_dir, ignore=_ignore)\n    _cleanup_profile_runtime_files(target_dir)\n\n\ndef _remove_tree_with_retries(target_dir: Path) -> None:\n    for attempt in range(4):\n        try:\n            if target_dir.exists():\n                _cleanup_profile_runtime_files(target_dir)\n                shutil.rmtree(target_dir, ignore_errors=False)\n        except FileNotFoundError:\n            return\n        except Exception:\n            time.sleep(0.8 + attempt * 0.4)\n        if not target_dir.exists():\n            return\n    shutil.rmtree(target_dir, ignore_errors=True)\n    if target_dir.exists():\n        raise RuntimeError(f"Não foi possível remover completamente o perfil Chrome em {target_dir}")\n\n\ndef ensure_chrome_profile_ready(runtime_env: RuntimeEnvironment) -> None:\n    profile_dir = runtime_env.paths.chrome_profile_dir\n    backup_dir = runtime_env.paths.chrome_profile_backup_dir\n    profile_dir.parent.mkdir(parents=True, exist_ok=True)\n    backup_dir.parent.mkdir(parents=True, exist_ok=True)\n\n    profile_entries = list(profile_dir.iterdir()) if profile_dir.exists() else []\n    backup_entries = list(backup_dir.iterdir()) if backup_dir.exists() else []\n\n    if not profile_entries and backup_entries:\n        runtime_env.logger.info(f"Perfil de trabalho ausente. Restaurando a partir do backup: {backup_dir}")\n        _copy_profile_tree(backup_dir, profile_dir)\n        return\n\n    if profile_entries and not backup_entries:\n        runtime_env.logger.info(f"Criando backup inicial do perfil Chrome em: {backup_dir}")\n        _copy_profile_tree(profile_dir, backup_dir)\n        return\n\n    profile_dir.mkdir(parents=True, exist_ok=True)\n    backup_dir.mkdir(parents=True, exist_ok=True)\n\n\ndef _display_runtime_path(runtime_env: RuntimeEnvironment, path: Path) -> str:\n    try:\n        return str(path.relative_to(runtime_env.paths.runtime_dir)).replace("\\\\", "/")\n    except Exception:\n        return path.name\n\n\ndef prepare_chrome_working_profile(runtime_env: RuntimeEnvironment) -> None:\n    profile_dir = runtime_env.paths.chrome_profile_dir\n    backup_dir = runtime_env.paths.chrome_profile_backup_dir\n    ensure_chrome_profile_ready(runtime_env)\n    if _is_profile_healthy(profile_dir):\n        _cleanup_profile_runtime_files(profile_dir)\n        return\n    if _is_profile_healthy(backup_dir):\n        runtime_env.logger.info("Perfil principal indisponível. Restaurando a partir do backup estável.")\n        _remove_tree_with_retries(profile_dir)\n        profile_dir.parent.mkdir(parents=True, exist_ok=True)\n        _copy_profile_tree(backup_dir, profile_dir)\n        return\n    profile_dir.mkdir(parents=True, exist_ok=True)\n\n\ndef refresh_chrome_profile_backup(runtime_env: RuntimeEnvironment) -> None:\n    profile_dir = runtime_env.paths.chrome_profile_dir\n    backup_dir = runtime_env.paths.chrome_profile_backup_dir\n    if not _is_profile_healthy(profile_dir):\n        return\n    try:\n        _copy_profile_tree(profile_dir, backup_dir)\n        runtime_env.logger.info(f"Backup do perfil Chrome atualizado em: {backup_dir}")\n    except Exception as exc:\n        runtime_env.logger.warning(f"Falha ao atualizar backup do perfil Chrome: {exc}")\n\n\ndef launch_browser(runtime_env: RuntimeEnvironment, *, headless: bool = False) -> BrowserSession:\n    try:\n        from playwright.sync_api import sync_playwright\n    except Exception as exc:\n        raise RuntimeError("Playwright não está disponível no ambiente do robô.") from exc\n\n    executable = resolve_browser_executable(runtime_env)\n    prepare_chrome_working_profile(runtime_env)\n    runtime_env.logger.info(\n        "Chrome persistente configurado com "\n        f"executável={_display_runtime_path(runtime_env, executable)} "\n        f"perfil={_display_runtime_path(runtime_env, runtime_env.paths.chrome_profile_dir)}"\n    )\n    playwright = sync_playwright().start()\n    persistent_kwargs = dict(\n        user_data_dir=str(runtime_env.paths.chrome_profile_dir),\n        executable_path=str(executable),\n        headless=headless,\n        ignore_https_errors=True,\n        no_viewport=True,\n        ignore_default_args=[\n            "--disable-extensions",\n            "--disable-component-extensions-with-background-pages",\n        ],\n        args=[\n            "--no-first-run",\n            "--no-default-browser-check",\n            "--disable-blink-features=AutomationControlled",\n            "--disable-features=IsolateOrigins,site-per-process",\n            "--disable-dev-shm-usage",\n            "--no-sandbox",\n            "--disable-gpu",\n            "--disable-popup-blocking",\n            "--start-maximized",\n            "--ignore-certificate-errors",\n        ],\n    )\n    try:\n        context = playwright.chromium.launch_persistent_context(**persistent_kwargs)\n        page = context.pages[0] if context.pages else context.new_page()\n        page.set_default_timeout(30000)\n        context.set_default_navigation_timeout(60000)\n        return BrowserSession(\n            playwright=playwright,\n            context=context,\n            page=page,\n            executable_path=str(executable),\n        )\n    except Exception as exc:\n        runtime_env.logger.warning(f"Falha ao abrir navegador com perfil principal. Tentando restaurar backup: {exc}")\n        try:\n            if _is_profile_healthy(runtime_env.paths.chrome_profile_backup_dir):\n                _remove_tree_with_retries(runtime_env.paths.chrome_profile_dir)\n                runtime_env.paths.chrome_profile_dir.parent.mkdir(parents=True, exist_ok=True)\n                _copy_profile_tree(\n                    runtime_env.paths.chrome_profile_backup_dir,\n                    runtime_env.paths.chrome_profile_dir,\n                )\n                context = playwright.chromium.launch_persistent_context(**persistent_kwargs)\n                page = context.pages[0] if context.pages else context.new_page()\n                page.set_default_timeout(30000)\n                context.set_default_navigation_timeout(60000)\n                runtime_env.logger.info("Navegador reaberto com sucesso após restaurar o backup do perfil Chrome.")\n                return BrowserSession(\n                    playwright=playwright,\n                    context=context,\n                    page=page,\n                    executable_path=str(executable),\n                )\n        except Exception as retry_exc:\n            runtime_env.logger.warning(f"Falha ao restaurar backup do perfil Chrome: {retry_exc}")\n        playwright.stop()\n        raise\n\n\ndef close_browser(session: Optional[BrowserSession], runtime_env: Optional[RuntimeEnvironment] = None) -> None:\n    if not session:\n        return\n    try:\n        session.context.close()\n    except Exception:\n        pass\n    try:\n        session.playwright.stop()\n    except Exception:\n        pass\n    if runtime_env is not None:\n        refresh_chrome_profile_backup(runtime_env)\n\n# === ecac_service ===\nimport csv\nimport json\nimport re\nimport traceback\nfrom dataclasses import asdict\nfrom datetime import datetime, timezone\nfrom pathlib import Path\nfrom typing import Any, Callable, Optional\n\n\nLOGIN_URL = "https://cav.receita.fazenda.gov.br/autenticacao/login"\nSOURCE_SYSTEM = ROBOT_TECHNICAL_ID\n\n# Ajuste fino de layout: centralize mudanças do portal aqui.\nPROFILE_MENU_LABELS = [\n    "Alterar perfil de acesso",\n    "Perfil de acesso",\n    "Trocar perfil",\n    "Selecionar perfil",\n]\nPROFILE_MODAL_SELECTORS = [\n    "[role=\'dialog\']",\n    "dialog[open]",\n    ".modal.show",\n    ".modal-dialog",\n    ".ui-dialog",\n    ".mat-mdc-dialog-container",\n    ".mat-dialog-container",\n    ".p-dialog",\n    ".swal2-popup",\n    ".offcanvas.show",\n    ".popup.show",\n]\nPROFILE_INPUT_SELECTORS = [\n    "input[name*=\'cnpj\' i]",\n    "input[id*=\'cnpj\' i]",\n    "input[placeholder*=\'CNPJ\' i]",\n    "input[placeholder*=\'CPF\' i]",\n    "input[placeholder*=\'empresa\' i]",\n    "input[placeholder*=\'pesquisar\' i]",\n    "input[placeholder*=\'buscar\' i]",\n    "#txtCnpj",\n    "input[type=\'search\']",\n    "input[type=\'text\']",\n]\nPROFILE_CONFIRM_LABELS = ["OK", "Confirmar", "Pesquisar", "Buscar", "Selecionar", "Aplicar", "Alterar perfil", "Enviar"]\nMAILBOX_ENTRY_LABELS = [\n    "Você tem novas mensagens",\n    "Novas mensagens",\n    "Caixa Postal",\n    "Mensagens",\n    "Caixa postal",\n]\nMODAL_CLOSE_LABELS = ["Fechar", "OK", "Entendi", "Entendido", "Continuar", "Não quero ver novamente"]\nMESSAGE_ROW_SELECTORS = [\n    "table tbody tr",\n    "[role=\'row\']",\n    ".mat-row",\n    ".datatable-row-wrapper",\n    ".ui-datatable-data > tr",\n    ".table-responsive tbody tr",\n    ".lista-mensagens tr",\n]\nNEXT_PAGE_LABELS = ["Próximo", "Próxima", ">", "Avançar"]\nDETAIL_CLOSE_LABELS = ["Fechar", "Voltar", "X", "Cancelar"]\nBACK_TO_LIST_LABELS = ["Lista de Mensagens", "Lista de mensagens", "Mensagens", "Voltar", "Caixa Postal"]\n\n\ndef _first_line(text: str) -> str:\n    for line in str(text or "").splitlines():\n        line = line.strip()\n        if line:\n            return line\n    return ""\n\n\ndef _stack_tail(exc: BaseException) -> str:\n    return "".join(traceback.format_exception_only(type(exc), exc)).strip()\n\n\nclass ProfileSwitchError(RuntimeError):\n    pass\n\n\nclass ProfileSwitchNoPowerOfAttorneyError(ProfileSwitchError):\n    pass\n\n\nclass EcacMailboxAutomation:\n    def __init__(\n        self,\n        runtime_env: RuntimeEnvironment,\n        session: BrowserSession,\n        certificate: CertificateMetadata,\n        *,\n        stop_requested: Callable[[], bool],\n    ) -> None:\n        self.runtime = runtime_env\n        self.session = session\n        self.page = session.page\n        self.certificate = certificate\n        self.stop_requested = stop_requested\n        self._last_profile_field: Optional[Any] = None\n        self._last_profile_form: Optional[Any] = None\n\n    def _iter_scopes(self) -> list[Any]:\n        scopes = [self.page]\n        try:\n            scopes.extend(frame for frame in self.page.frames if frame != self.page.main_frame)\n        except Exception:\n            pass\n        unique: list[Any] = []\n        seen: set[int] = set()\n        for scope in scopes:\n            if id(scope) in seen:\n                continue\n            seen.add(id(scope))\n            unique.append(scope)\n        return unique\n\n    def _wait_until(\n        self,\n        predicate: Callable[[], bool],\n        *,\n        timeout_ms: int,\n        interval_ms: int = 250,\n    ) -> bool:\n        deadline = datetime.now().timestamp() + (timeout_ms / 1000)\n        last_error: Optional[Exception] = None\n        while datetime.now().timestamp() < deadline:\n            if self.stop_requested():\n                return False\n            try:\n                if predicate():\n                    return True\n            except Exception as exc:\n                last_error = exc\n            try:\n                self.page.wait_for_timeout(interval_ms)\n            except Exception:\n                pass\n        if last_error:\n            self.runtime.logger.warning(f"Espera expirou: {last_error}")\n        return False\n\n    def _body_text(self, scope: Optional[Any] = None) -> str:\n        target = scope or self.page\n        try:\n            return target.locator("body").inner_text(timeout=3000)\n        except Exception:\n            return ""\n\n    def _page_contains(self, text: str) -> bool:\n        lowered = text.lower()\n        for scope in self._iter_scopes():\n            try:\n                if lowered in self._body_text(scope).lower():\n                    return True\n            except Exception:\n                continue\n        return False\n\n    def _click_by_label(self, labels: list[str], *, timeout_ms: int = 7000) -> bool:\n        for label in labels:\n            pattern = re.compile(re.escape(label), re.IGNORECASE)\n            for scope in self._iter_scopes():\n                locators = [\n                    scope.get_by_role("button", name=pattern),\n                    scope.get_by_role("link", name=pattern),\n                    scope.get_by_text(pattern),\n                    scope.locator(f"[aria-label*=\'{label}\']"),\n                    scope.locator(f"text={label}"),\n                ]\n                for locator in locators:\n                    try:\n                        if locator.count() < 1:\n                            continue\n                        candidate = locator.first\n                        candidate.wait_for(state="visible", timeout=1000)\n                        candidate.click(timeout=timeout_ms, force=True)\n                        return True\n                    except Exception:\n                        continue\n        return False\n\n    def _log_step(self, message: str) -> None:\n        self.runtime.logger.info(message)\n\n    def _capture_debug_artifacts(self, prefix: str) -> None:\n        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")\n        safe_prefix = re.sub(r"[^A-Za-z0-9_.-]+", "_", prefix).strip("_") or "ecac_debug"\n        screenshot_path = self.runtime.paths.logs_dir / f"{safe_prefix}_{timestamp}.png"\n        html_path = self.runtime.paths.logs_dir / f"{safe_prefix}_{timestamp}.html"\n        try:\n            self.page.screenshot(path=str(screenshot_path), full_page=True)\n            self.runtime.logger.warning(f"Screenshot salvo para diagnóstico: {screenshot_path}")\n        except Exception as exc:\n            self.runtime.logger.warning(f"Falha ao salvar screenshot de diagnóstico: {exc}")\n        try:\n            html_path.write_text(self.page.content(), encoding="utf-8")\n            self.runtime.logger.warning(f"HTML salvo para diagnóstico: {html_path}")\n        except Exception as exc:\n            self.runtime.logger.warning(f"Falha ao salvar HTML de diagnóstico: {exc}")\n\n    def _visible_modal_scopes(self) -> list[Any]:\n        scopes: list[Any] = []\n        for scope in self._iter_scopes():\n            for selector in PROFILE_MODAL_SELECTORS:\n                try:\n                    locator = scope.locator(selector)\n                    count = locator.count()\n                except Exception:\n                    continue\n                for index in range(min(count, 4)):\n                    try:\n                        candidate = locator.nth(index)\n                        candidate.wait_for(state="visible", timeout=350)\n                        scopes.append(candidate)\n                    except Exception:\n                        continue\n        return scopes\n\n    def _profile_scopes(self) -> list[Any]:\n        modal_scopes = self._visible_modal_scopes()\n        return modal_scopes + [scope for scope in self._iter_scopes() if scope not in modal_scopes]\n\n    def _find_company_input(self, company: CompanyRecord) -> Optional[Any]:\n        digits = only_digits(company.document)\n        is_cnpj = len(digits) >= 14\n        preferred_pairs = []\n        if is_cnpj:\n            preferred_pairs = [\n                ("form#formPJ", "#txtNIPapel2"),\n                ("form#formMatriz", "#txtNIPapel3"),\n                ("form#formSucessora", "#txtNIPapel4"),\n                ("form#formEnteFederativoVinculado", "#txtNIPapel5"),\n            ]\n        else:\n            preferred_pairs = [("form#formPF", "#txtNIPapel1")]\n\n        for scope in self._profile_scopes():\n            for form_selector, field_selector in preferred_pairs:\n                try:\n                    form = scope.locator(form_selector).first\n                    if form.count() < 1:\n                        continue\n                    form.wait_for(state="visible", timeout=700)\n                    field = form.locator(field_selector).first\n                    field.wait_for(state="visible", timeout=700)\n                    field.scroll_into_view_if_needed(timeout=1000)\n                    self._last_profile_form = form\n                    return field\n                except Exception:\n                    continue\n\n        for scope in self._profile_scopes():\n            for selector in PROFILE_INPUT_SELECTORS:\n                locator = scope.locator(selector)\n                try:\n                    count = locator.count()\n                except Exception:\n                    continue\n                for index in range(min(count, 8)):\n                    field = locator.nth(index)\n                    try:\n                        field.wait_for(state="visible", timeout=500)\n                        field.scroll_into_view_if_needed(timeout=1000)\n                        try:\n                            self._last_profile_form = field.locator("xpath=ancestor::form[1]")\n                        except Exception:\n                            self._last_profile_form = None\n                        return field\n                    except Exception:\n                        continue\n        return None\n\n    def _fill_with_fallbacks(self, field: Any, value: str) -> bool:\n        attempts: list[Callable[[], None]] = [\n            lambda: (field.click(timeout=1500), field.fill(""), field.fill(value), field.press("Tab")),\n            lambda: field.evaluate(\n                """(el, val) => {\n                    el.focus();\n                    el.value = \'\';\n                    el.dispatchEvent(new Event(\'input\', { bubbles: true }));\n                    el.value = val;\n                    el.dispatchEvent(new Event(\'input\', { bubbles: true }));\n                    el.dispatchEvent(new Event(\'change\', { bubbles: true }));\n                    el.blur();\n                }""",\n                value,\n            ),\n            lambda: (field.click(timeout=1500), field.fill(""), field.type(value, delay=45), field.press("Tab")),\n        ]\n        for attempt in attempts:\n            try:\n                attempt()\n                current_value = ""\n                try:\n                    current_value = str(field.input_value(timeout=800) or "")\n                except Exception:\n                    pass\n                if only_digits(current_value) == only_digits(value):\n                    return True\n                field.dispatch_event("input")\n                field.dispatch_event("change")\n                try:\n                    current_value = str(field.input_value(timeout=800) or "")\n                except Exception:\n                    current_value = ""\n                if only_digits(current_value) == only_digits(value):\n                    return True\n            except Exception:\n                continue\n        return False\n\n    def _fill_company_search(self, company: CompanyRecord) -> bool:\n        self._last_profile_form = None\n        field = self._find_company_input(company)\n        if field is None:\n            return False\n        self._last_profile_field = field\n        digits = only_digits(company.document)\n        search_terms = [digits, format_document(company.document), company.name]\n        for term in search_terms:\n            if not term:\n                continue\n            if self._fill_with_fallbacks(field, term):\n                target_name = ""\n                try:\n                    target_name = field.get_attribute("id") or field.get_attribute("name") or ""\n                except Exception:\n                    pass\n                self._log_step(f"filled_cnpj term=\'{term}\' field=\'{target_name or \'unknown\'}\'")\n                return True\n        return False\n\n    def _select_company_in_profile(self, company: CompanyRecord) -> bool:\n        targets = [\n            company.name,\n            format_document(company.document),\n            only_digits(company.document),\n        ]\n        for scope in self._profile_scopes():\n            body = self._body_text(scope).lower()\n            for target in targets:\n                if target and target.lower() not in body:\n                    continue\n                pattern = re.compile(re.escape(target), re.IGNORECASE)\n                candidates = [\n                    scope.get_by_role("row", name=pattern),\n                    scope.get_by_role("option", name=pattern),\n                    scope.get_by_text(pattern),\n                    scope.locator(f"text={target}"),\n                ]\n                for locator in candidates:\n                    try:\n                        if locator.count() < 1:\n                            continue\n                        locator.first.click(timeout=5000, force=True)\n                        return True\n                    except Exception:\n                        continue\n        return False\n\n    def _confirm_profile_switch(self) -> bool:\n        if self._last_profile_form is not None:\n            try:\n                form = self._last_profile_form\n                submit_candidates = [\n                    form.locator("input.submit"),\n                    form.locator("input[type=\'button\']"),\n                    form.locator("input[type=\'submit\']"),\n                    form.get_by_role("button", name=re.compile("alterar|ok|confirmar|pesquisar", re.IGNORECASE)),\n                    form.get_by_text(re.compile("alterar|ok|confirmar|pesquisar", re.IGNORECASE)),\n                ]\n                for locator in submit_candidates:\n                    if locator.count() < 1:\n                        continue\n                    button = locator.first\n                    button.wait_for(state="visible", timeout=800)\n                    try:\n                        with self.page.expect_response(\n                            lambda response: response.request.method in {"POST", "GET"}\n                            and (\n                                "perfil" in response.url.lower()\n                                or "mudanca" in response.url.lower()\n                                or "captcha" in response.url.lower()\n                                or "alterar" in response.url.lower()\n                            ),\n                            timeout=6000,\n                        ):\n                            button.click(timeout=4000, force=True)\n                        self._log_step("postback_detected")\n                    except Exception:\n                        button.click(timeout=4000, force=True)\n                    try:\n                        self.page.wait_for_load_state("networkidle", timeout=7000)\n                    except Exception:\n                        pass\n                    self._log_step("clicked_confirm")\n                    return True\n            except Exception:\n                pass\n        for scope in self._profile_scopes():\n            for label in PROFILE_CONFIRM_LABELS:\n                pattern = re.compile(re.escape(label), re.IGNORECASE)\n                candidates = [\n                    scope.get_by_role("button", name=pattern),\n                    scope.get_by_role("link", name=pattern),\n                    scope.get_by_text(pattern),\n                    scope.locator(f"button:has-text(\'{label}\')"),\n                    scope.locator(".modal-footer button"),\n                ]\n                for locator in candidates:\n                    try:\n                        if locator.count() < 1:\n                            continue\n                        button = locator.first\n                        button.wait_for(state="visible", timeout=800)\n                        try:\n                            with self.page.expect_response(\n                                lambda response: response.request.method in {"POST", "GET"}\n                                and (\n                                    "perfil" in response.url.lower()\n                                    or "alterar" in response.url.lower()\n                                    or "represent" in response.url.lower()\n                                ),\n                                timeout=5000,\n                            ):\n                                button.click(timeout=4000, force=True)\n                            self._log_step("postback_detected")\n                        except Exception:\n                            button.click(timeout=4000, force=True)\n                        try:\n                            self.page.wait_for_load_state("networkidle", timeout=7000)\n                        except Exception:\n                            pass\n                        self._log_step("clicked_confirm")\n                        return True\n                    except Exception:\n                        continue\n        return False\n\n    def _extract_profile_modal_error(self) -> str:\n        error_candidates = [\n            ".mensagemErro",\n            ".erro",\n            "[role=\'alert\']",\n            ".ui-state-error",\n            ".alert",\n            ".alert-danger",\n        ]\n        for scope in self._visible_modal_scopes():\n            for selector in error_candidates:\n                try:\n                    locator = scope.locator(selector)\n                    count = locator.count()\n                except Exception:\n                    continue\n                for index in range(min(count, 6)):\n                    try:\n                        text = " ".join(locator.nth(index).inner_text(timeout=800).split())\n                    except Exception:\n                        continue\n                    if not text:\n                        continue\n                    lowered = text.lower()\n                    if any(marker in lowered for marker in ["atenção", "atencao", "erro", "não existe", "nao existe", "indisponível", "indisponivel"]):\n                        return text\n        return ""\n\n    def _current_profile_context(self) -> str:\n        snippets: list[str] = []\n        for scope in self._iter_scopes():\n            text = self._body_text(scope)\n            if not text:\n                continue\n            for line in text.splitlines():\n                line = line.strip()\n                if not line:\n                    continue\n                if "perfil" in line.lower() or "cnpj" in line.lower() or "titular" in line.lower():\n                    snippets.append(line)\n                if len(snippets) >= 6:\n                    break\n            if len(snippets) >= 6:\n                break\n        return " | ".join(snippets[:6])\n\n    def close_blocking_modals(self) -> None:\n        for _ in range(6):\n            clicked = False\n            modal_scopes = self._visible_modal_scopes()\n            for label in MODAL_CLOSE_LABELS:\n                clicked = self._click_by_label([label], timeout_ms=2000) or clicked\n            for scope in modal_scopes + self._iter_scopes():\n                try:\n                    locator = scope.locator("[aria-label*=\'Fechar\'], [aria-label*=\'close\'], .close, .btn-close")\n                    if locator.count() > 0:\n                        locator.first.click(timeout=1500, force=True)\n                        clicked = True\n                except Exception:\n                    continue\n            try:\n                self.page.keyboard.press("Escape")\n            except Exception:\n                pass\n            if not clicked:\n                break\n            self._wait_until(lambda: len(self._visible_modal_scopes()) == 0, timeout_ms=1500, interval_ms=150)\n\n    def is_authenticated(self) -> bool:\n        try:\n            if "/ecac/" in self.page.url and not self._page_contains("Acesso Gov BR"):\n                return True\n        except Exception:\n            pass\n        indicators = [\n            "Alterar perfil de acesso",\n            "Titular (Acesso GOV.BR por Certificado)",\n            "Caixa Postal",\n            "Mensagens",\n        ]\n        return any(self._page_contains(indicator) for indicator in indicators)\n\n    def _click_access_gov_br(self) -> None:\n        previous_url = self.page.url\n        for _ in range(5):\n            if self._click_by_label(["Acesso Gov BR"], timeout_ms=8000):\n                moved = self._wait_until(\n                    lambda: self.page.url != previous_url or "acesso.gov.br" in self.page.url or self._page_contains("Seu certificado digital"),\n                    timeout_ms=12000,\n                )\n                if moved:\n                    return\n            self.page.reload(wait_until="domcontentloaded")\n        raise RuntimeError("Falha ao acionar o botão \'Acesso Gov BR\'.")\n\n    def _click_cert_login(self) -> None:\n        for _ in range(4):\n            if self._click_by_label(["Seu certificado digital"], timeout_ms=10000):\n                if certificate_auth.wait_for_certificate_dialog(timeout_seconds=10):\n                    return\n            self.page.reload(wait_until="domcontentloaded")\n            self._wait_until(lambda: self._page_contains("Seu certificado digital"), timeout_ms=10000)\n        raise RuntimeError("A janela nativa de certificado não apareceu após clicar em \'Seu certificado digital\'.")\n\n    def login(self) -> None:\n        self.page.goto(LOGIN_URL, wait_until="domcontentloaded")\n        if self.is_authenticated():\n            self.close_blocking_modals()\n            return\n\n        self._click_access_gov_br()\n        self._wait_until(\n            lambda: "acesso.gov.br" in self.page.url or self._page_contains("Seu certificado digital"),\n            timeout_ms=20000,\n        )\n        self._click_cert_login()\n        certificate_auth.select_certificate_dialog(self.certificate)\n        logged = self._wait_until(self.is_authenticated, timeout_ms=90000, interval_ms=500)\n        if not logged:\n            raise RuntimeError("O e-CAC não confirmou a sessão autenticada dentro do timeout.")\n        self.close_blocking_modals()\n\n    def ensure_logged_in(self) -> None:\n        if self.is_authenticated():\n            return\n        self.runtime.logger.warning("Sessão do e-CAC não parece autenticada. Tentando reautenticação controlada.")\n        self.login()\n\n    def _profile_switch_targets(self, company: CompanyRecord) -> list[str]:\n        return [\n            company.name.lower(),\n            only_digits(company.document),\n            format_document(company.document).lower(),\n        ]\n\n    def _profile_switch_success(self, company: CompanyRecord) -> bool:\n        targets = [target for target in self._profile_switch_targets(company) if target]\n        if self._visible_modal_scopes():\n            return False\n        context_text = self._current_profile_context().lower()\n        body_text = self._body_text().lower()\n        return any(target in context_text or target in body_text for target in targets)\n\n    def _after_profile_switch_cleanup(self) -> str:\n        self.close_blocking_modals()\n        try:\n            self.page.wait_for_load_state("networkidle", timeout=5000)\n        except Exception:\n            pass\n        if self._is_message_detail_view():\n            self._log_step("modal_not_closable_navigated_to_message")\n            return "detail_message"\n        if self._message_rows_locator()[0] is not None:\n            return "mailbox_list"\n        return "portal"\n\n    def switch_company_profile(self, company: CompanyRecord) -> str:\n        self.ensure_logged_in()\n        last_error: Optional[Exception] = None\n        for attempt in range(1, 4):\n            try:\n                self.close_blocking_modals()\n                self._log_step(f"clicked_alterar_acesso attempt={attempt}")\n                if not self._click_by_label(PROFILE_MENU_LABELS, timeout_ms=7000):\n                    raise RuntimeError("Fluxo de \'Alterar perfil de acesso\' não foi localizado.")\n                visible = self._wait_until(\n                    lambda: len(self._visible_modal_scopes()) > 0 or self._find_company_input() is not None,\n                    timeout_ms=15000,\n                )\n                if not visible:\n                    raise RuntimeError("Modal de alteração de perfil não ficou visível.")\n                self._log_step("modal_visible")\n                if not self._fill_company_search(company):\n                    raise RuntimeError("Campo de busca do perfil não foi localizado ou não aceitou o CNPJ.")\n                selected = self._wait_until(lambda: self._select_company_in_profile(company), timeout_ms=4000, interval_ms=350)\n                if selected:\n                    self._log_step("profile_option_selected")\n                confirmed = self._confirm_profile_switch()\n                if not confirmed and not selected:\n                    raise RuntimeError("Botão de confirmação do perfil não foi localizado.")\n                if not confirmed:\n                    self._log_step("clicked_confirm skipped_button_not_found")\n                modal_closed = self._wait_until(\n                    lambda: len(self._visible_modal_scopes()) == 0 or bool(self._extract_profile_modal_error()),\n                    timeout_ms=20000,\n                    interval_ms=350,\n                )\n                if not modal_closed:\n                    raise RuntimeError("O modal de alteração de perfil permaneceu aberto após a confirmação.")\n                modal_error = self._extract_profile_modal_error()\n                if modal_error:\n                    lowered_error = modal_error.lower()\n                    if "não existe procuração eletrônica" in lowered_error or "nao existe procuracao eletronica" in lowered_error:\n                        raise ProfileSwitchNoPowerOfAttorneyError(\n                            f"Falha retornada pelo portal ao alterar perfil: {modal_error}"\n                        )\n                    raise ProfileSwitchError(f"Falha retornada pelo portal ao alterar perfil: {modal_error}")\n                cleanup_state = self._after_profile_switch_cleanup()\n                changed = self._wait_until(\n                    lambda: self._profile_switch_success(company),\n                    timeout_ms=15000,\n                    interval_ms=400,\n                )\n                context = self._current_profile_context()\n                if not changed:\n                    raise RuntimeError(\n                        "A troca de perfil não foi validada visualmente no portal para a empresa alvo."\n                    )\n                self._log_step(f"profile_switched attempt={attempt} state={cleanup_state}")\n                return context\n            except ProfileSwitchNoPowerOfAttorneyError as exc:\n                last_error = exc\n                self.runtime.logger.warning(f"profile_switch_failed attempt={attempt} company={company.name}: {exc}")\n                self._capture_debug_artifacts(f"profile_switch_failed_{company.company_id}_attempt_{attempt}")\n                self.close_blocking_modals()\n                raise\n            except Exception as exc:\n                last_error = exc\n                self.runtime.logger.warning(f"profile_switch_failed attempt={attempt} company={company.name}: {exc}")\n                self._capture_debug_artifacts(f"profile_switch_failed_{company.company_id}_attempt_{attempt}")\n                self.close_blocking_modals()\n                if attempt < 3:\n                    try:\n                        self.page.wait_for_timeout(600 * attempt)\n                    except Exception:\n                        pass\n        raise RuntimeError(str(last_error or "Falha desconhecida na troca de perfil."))\n\n    def open_mailbox(self) -> None:\n        self.close_blocking_modals()\n        if self._is_message_detail_view() or self._message_rows_locator()[0] is not None:\n            return\n        if self._click_by_label(MAILBOX_ENTRY_LABELS, timeout_ms=8000):\n            ready = self._wait_until(\n                lambda: self._message_rows_locator()[0] is not None or self._is_message_detail_view() or self._page_contains("mensagem"),\n                timeout_ms=15000,\n            )\n            if ready:\n                self.close_blocking_modals()\n                return\n        raise RuntimeError("A entrada da Caixa Postal / Mensagens não foi localizada.")\n\n    def _is_message_detail_view(self) -> bool:\n        text = self._body_text().lower()\n        indicators = [\n            "id da mensagem",\n            "tipo de comunicação",\n            "tipo de comunicacao",\n            "exibição até",\n            "exibicao ate",\n            "destinatário",\n            "destinatario",\n            "lista de mensagens",\n        ]\n        return sum(1 for indicator in indicators if indicator in text) >= 3\n\n    def _return_to_message_list(self) -> None:\n        if self._message_rows_locator()[0] is not None:\n            return\n        if self._click_by_label(BACK_TO_LIST_LABELS, timeout_ms=3000):\n            self._wait_until(lambda: self._message_rows_locator()[0] is not None, timeout_ms=8000, interval_ms=300)\n        elif self._is_message_detail_view():\n            try:\n                self.page.go_back(wait_until="domcontentloaded", timeout=8000)\n            except Exception:\n                pass\n            self._wait_until(lambda: self._message_rows_locator()[0] is not None, timeout_ms=8000, interval_ms=300)\n\n    def _message_rows_locator(self) -> tuple[Optional[Any], Optional[str], Optional[Any]]:\n        for scope in self._iter_scopes():\n            for selector in MESSAGE_ROW_SELECTORS:\n                locator = scope.locator(selector)\n                try:\n                    count = locator.count()\n                except Exception:\n                    continue\n                if count <= 0:\n                    continue\n                return locator, selector, scope\n        return None, None, None\n\n    def _parse_message(self, company: CompanyRecord, run_id: str, index: int, text: str, detail_text: str = "") -> MessageRecord:\n        clean_text = " ".join(str(text or "").split())\n        lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]\n        all_text = detail_text or clean_text\n        date_match = re.findall(r"\\b\\d{2}/\\d{2}/\\d{4}(?:\\s+\\d{2}:\\d{2})?\\b", all_text)\n        protocol_match = re.search(r"\\b\\d{8,}\\b", all_text)\n        sender_match = re.search(r"(?:Origem|Remetente|Enviado por)\\s*[:\\-]\\s*(.+)", all_text, flags=re.IGNORECASE)\n        category_match = re.search(r"(?:Categoria|Tipo)\\s*[:\\-]\\s*(.+)", all_text, flags=re.IGNORECASE)\n        status_match = re.search(r"(?:Lida|Não lida|Nao lida|Lido|Não Visualizada)", all_text, flags=re.IGNORECASE)\n        priority_match = re.search(r"(?:Prioridade|Urgência|Urgencia)\\s*[:\\-]\\s*(.+)", all_text, flags=re.IGNORECASE)\n\n        subject = ""\n        if lines:\n            non_header = [line for line in lines if line.lower() not in {"assunto", "data", "tipo", "origem"}]\n            subject = non_header[0] if non_header else lines[0]\n        attachments: list[dict[str, Any]] = []\n        for match in re.findall(r"([A-Za-z0-9_\\- ]+\\.(?:pdf|xml|zip|docx?|xlsx?))", all_text, flags=re.IGNORECASE):\n            attachments.append({"name": match.strip()})\n\n        return MessageRecord(\n            company_id=company.company_id,\n            company_name=company.name,\n            company_document=format_document(company.document),\n            run_id=run_id,\n            extracted_at=utc_now_iso(),\n            source_system=SOURCE_SYSTEM,\n            row_index=index,\n            company_profile_context=self._current_profile_context(),\n            message_id=protocol_match.group(0) if protocol_match else "",\n            subject=subject,\n            sender=sender_match.group(1).strip() if sender_match else "",\n            category=category_match.group(1).strip() if category_match else "",\n            received_at=date_match[0] if date_match else "",\n            sent_at=date_match[1] if len(date_match) > 1 else "",\n            posted_at=date_match[0] if date_match else "",\n            read_status=status_match.group(0) if status_match else "",\n            unread=("não" in status_match.group(0).lower() or "nao" in status_match.group(0).lower()) if status_match else None,\n            priority=priority_match.group(1).strip() if priority_match else "",\n            snippet=_first_line(clean_text),\n            body=detail_text.strip(),\n            attachments=attachments,\n            raw_visible_text=clean_text,\n            detail_visible_text=detail_text.strip(),\n        )\n\n    def _parse_current_detail_message(self, company: CompanyRecord, run_id: str, index: int) -> Optional[MessageRecord]:\n        detail_text = self._collect_detail_text("")\n        if not detail_text:\n            detail_text = self._body_text()\n        normalized = " ".join(detail_text.split())\n        if not normalized or not self._is_message_detail_view():\n            return None\n        message = self._parse_message(company, run_id, index, normalized, detail_text)\n        if not message.subject:\n            message.subject = _first_line(detail_text)\n        return message\n\n    def _collect_detail_text(self, original_snapshot: str) -> str:\n        overlay_selectors = [\n            "[role=\'dialog\']",\n            "dialog[open]",\n            ".modal.show",\n            ".modal-dialog",\n            ".ui-dialog",\n            ".mat-mdc-dialog-container",\n            ".mat-dialog-container",\n            ".p-dialog",\n            ".swal2-popup",\n            ".offcanvas.show",\n        ]\n        for scope in self._iter_scopes():\n            for selector in overlay_selectors:\n                try:\n                    locator = scope.locator(selector)\n                    if locator.count() <= 0:\n                        continue\n                    for idx in range(min(locator.count(), 3)):\n                        candidate = locator.nth(idx)\n                        if not candidate.is_visible():\n                            continue\n                        text = " ".join(candidate.inner_text(timeout=1500).split())\n                        if text and text != " ".join(original_snapshot.split()):\n                            return text\n                except Exception:\n                    continue\n\n        for frame in self.page.frames:\n            if frame == self.page.main_frame:\n                continue\n            text = " ".join(self._body_text(frame).split())\n            if text and text != " ".join(original_snapshot.split()):\n                return text\n\n        current = " ".join(self._body_text().split())\n        if current and current != " ".join(original_snapshot.split()):\n            return current\n        return ""\n\n    def _dismiss_detail_view(self) -> None:\n        for label in DETAIL_CLOSE_LABELS:\n            self._click_by_label([label], timeout_ms=1500)\n        try:\n            self.page.keyboard.press("Escape")\n        except Exception:\n            pass\n        self._wait_until(\n            lambda: not self._is_message_detail_view() or self._message_rows_locator()[0] is not None,\n            timeout_ms=2000,\n            interval_ms=150,\n        )\n\n    def _open_row_detail(self, row: Any) -> str:\n        original_snapshot = self._body_text()\n        click_attempts: list[Callable[[], None]] = []\n        try:\n            clickable = row.locator("a, button, [role=\'button\'], [role=\'link\']")\n            if clickable.count() > 0:\n                click_attempts.append(lambda: clickable.first.click(timeout=3000, force=True))\n        except Exception:\n            pass\n        click_attempts.extend(\n            [\n                lambda: row.click(timeout=4000, force=True),\n                lambda: row.dblclick(timeout=4000),\n            ]\n        )\n        try:\n            click_attempts.append(lambda: (row.focus(), self.page.keyboard.press("Enter")))\n        except Exception:\n            pass\n\n        for attempt in click_attempts:\n            try:\n                attempt()\n            except Exception:\n                continue\n            opened = self._wait_until(\n                lambda: bool(self._collect_detail_text(original_snapshot)) or self._page_contains("Fechar"),\n                timeout_ms=6000,\n            )\n            detail_text = self._collect_detail_text(original_snapshot)\n            if opened and detail_text:\n                return detail_text\n            self._dismiss_detail_view()\n        return ""\n\n    def _click_next_page(self) -> bool:\n        return self._click_by_label(NEXT_PAGE_LABELS, timeout_ms=4000)\n\n    def extract_first_messages(self, company: CompanyRecord, run_id: str, limit: int = 20) -> list[MessageRecord]:\n        if self._is_message_detail_view():\n            self._log_step("message_detail_detected_before_list")\n        locator, _, _ = self._message_rows_locator()\n        if locator is None and not self._is_message_detail_view():\n            raise RuntimeError("Nenhuma tabela/lista de mensagens visível foi encontrada na Caixa Postal.")\n\n        messages: list[MessageRecord] = []\n        seen: set[str] = set()\n        if self._is_message_detail_view():\n            current_message = self._parse_current_detail_message(company, run_id, 1)\n            if current_message is not None:\n                seen.add(current_message.dedupe_key())\n                messages.append(current_message)\n                self._log_step(f"message_extracted: {current_message.message_id or current_message.subject}")\n            self._return_to_message_list()\n            locator, _, _ = self._message_rows_locator()\n            if locator is None and len(messages) >= limit:\n                return messages[:limit]\n            if locator is None and not messages:\n                raise RuntimeError("A mensagem pós-troca foi aberta, mas a lista da Caixa Postal não pôde ser restaurada.")\n\n        page_cycles = 0\n        while len(messages) < limit and page_cycles < 4:\n            try:\n                row_count = locator.count()\n            except Exception:\n                row_count = 0\n            if row_count <= 0:\n                break\n            for index in range(row_count):\n                if len(messages) >= limit or self.stop_requested():\n                    break\n                row = locator.nth(index)\n                try:\n                    if not row.is_visible():\n                        continue\n                    cells = row.locator("td, th, [role=\'cell\'], [role=\'gridcell\']").all_inner_texts()\n                    raw_text = "\\n".join(item.strip() for item in cells if item.strip()) or row.inner_text(timeout=2000)\n                    if not raw_text.strip():\n                        continue\n                    lowered = raw_text.lower()\n                    if "assunto" in lowered and "data" in lowered and len(raw_text) < 120:\n                        continue\n                    self._log_step(f"message_opened: index {len(messages) + 1}")\n                    detail_text = self._open_row_detail(row)\n                    message = self._parse_message(company, run_id, len(messages) + 1, raw_text, detail_text)\n                    key = message.dedupe_key()\n                    if key in seen:\n                        self._return_to_message_list()\n                        continue\n                    seen.add(key)\n                    messages.append(message)\n                    self._log_step(f"message_extracted: {message.message_id or message.subject}")\n                    self._return_to_message_list()\n                except Exception as exc:\n                    self.runtime.logger.warning(f"Falha ao extrair linha de mensagem: {exc}")\n                    self._return_to_message_list()\n                    continue\n            if len(messages) >= limit:\n                break\n            if not self._click_next_page():\n                break\n            page_cycles += 1\n            try:\n                self.page.wait_for_load_state("networkidle", timeout=5000)\n            except Exception:\n                pass\n            locator, _, _ = self._message_rows_locator()\n            if locator is None:\n                break\n        return messages[:limit]\n\n    def collect_current_mailbox(\n        self,\n        company: CompanyRecord,\n        run_id: str,\n        *,\n        switch_profile: bool = False,\n        context_type: str = "company",\n        raise_on_error: bool = False,\n    ) -> CompanyRunResult:\n        result = CompanyRunResult(\n            company_id=company.company_id,\n            company_name=company.name,\n            company_document=format_document(company.document),\n            context_type=context_type,\n            eligible=company.eligible,\n            block_reason=company.block_reason,\n            started_at=utc_now_iso(),\n        )\n        if not company.eligible:\n            result.status = "blocked"\n            result.errors.append(company.block_reason or "Empresa não elegível.")\n            result.finished_at = utc_now_iso()\n            return result\n\n        try:\n            if switch_profile:\n                profile_context = self.switch_company_profile(company)\n                result.profile_switched = True\n                result.company_profile_context = profile_context\n            else:\n                result.company_profile_context = self._current_profile_context()\n            self.open_mailbox()\n            result.mailbox_opened = True\n            result.messages = self.extract_first_messages(company, run_id, limit=20)\n            result.company_profile_context = self._current_profile_context()\n            result.status = "success" if result.messages else "empty"\n        except Exception as exc:\n            result.status = "error"\n            result.errors.append(_stack_tail(exc))\n            self.runtime.logger.warning(f"Empresa {company.name}: {exc}")\n            if raise_on_error:\n                raise\n        finally:\n            result.finished_at = utc_now_iso()\n        return result\n\n\ndef _apply_mailbox_eligibility(\n    companies: list[CompanyRecord],\n    mailbox_config: RobotMailboxConfig,\n) -> list[CompanyRecord]:\n    normalized: list[CompanyRecord] = []\n    for company in companies:\n        updated = CompanyRecord(**company.to_dict())\n        updated.block_reason = ""\n        if mailbox_config.use_responsible_office:\n            updated.eligible = updated.has_valid_cnpj\n            if not updated.eligible:\n                updated.block_reason = "Empresa sem CNPJ válido para operar via escritório responsável."\n        else:\n            updated.eligible = updated.has_certificate_credentials and str(updated.auth_mode or "").strip().lower() == "certificate"\n            if not updated.eligible:\n                updated.block_reason = "Empresa sem certificado digital configurado no dashboard."\n        normalized.append(updated)\n    return normalized\n\n\ndef _resolve_company_certificate(runtime_env: RuntimeEnvironment, company: CompanyRecord) -> CertificateMetadata:\n    metadata = resolve_certificate_from_dashboard(runtime_env, [company])\n    if not metadata:\n        raise RuntimeError(f"Certificado digital não encontrado para {company.name}.")\n    return metadata\n\n\ndef _company_has_own_certificate(company: CompanyRecord) -> bool:\n    return bool(company.has_certificate_credentials and str(company.auth_mode or "").strip().lower() == "certificate")\n\n\ndef _build_company_error_result(\n    company: CompanyRecord,\n    *,\n    error_messages: list[str],\n    context_type: str = "company",\n) -> CompanyRunResult:\n    return CompanyRunResult(\n        company_id=company.company_id,\n        company_name=company.name,\n        company_document=format_document(company.document),\n        context_type=context_type,\n        status="error",\n        eligible=company.eligible,\n        block_reason=company.block_reason,\n        errors=[message for message in error_messages if message],\n        started_at=utc_now_iso(),\n        finished_at=utc_now_iso(),\n    )\n\n\ndef _resolve_responsible_office_company(\n    dashboard_client: DashboardClient,\n    office_context: OfficeContext,\n    companies: list[CompanyRecord],\n    mailbox_config: RobotMailboxConfig,\n) -> CompanyRecord:\n    responsible_id = str(mailbox_config.responsible_office_company_id or "").strip()\n    if not responsible_id:\n        raise RuntimeError("Configuração dinâmica inválida: responsible_office_company_id não definido.")\n    company = next((item for item in companies if item.company_id == responsible_id), None)\n    if company is None:\n        company = dashboard_client.load_company_by_id(office_context, responsible_id)\n    if company is None:\n        raise RuntimeError("Empresa definida como escritório responsável não foi encontrada no dashboard.")\n    if not company.has_valid_cnpj:\n        raise RuntimeError("A empresa escolhida como escritório responsável precisa ter CNPJ válido.")\n    if not company.has_certificate_credentials or str(company.auth_mode or "").strip().lower() != "certificate":\n        raise RuntimeError("A empresa escolhida como escritório responsável precisa ter certificado digital configurado.")\n    company.eligible = True\n    company.block_reason = ""\n    return company\n\n\ndef _finalize_summary(\n    summary: dict[str, Any],\n    company_results: list[CompanyRunResult],\n    responsible_office_result: Optional[CompanyRunResult] = None,\n) -> dict[str, Any]:\n    total_messages = sum(len(result.messages) for result in company_results)\n    if responsible_office_result:\n        total_messages += len(responsible_office_result.messages)\n    total_success = sum(1 for result in company_results if result.status in {"success", "empty"})\n    total_failed = sum(1 for result in company_results if result.status not in {"success", "empty"})\n    status = summary.get("status") or "completed"\n    if total_failed and total_success:\n        status = "partial"\n    elif total_failed and not total_success:\n        status = "failed"\n    elif responsible_office_result and responsible_office_result.status not in {"success", "empty"} and company_results:\n        status = "partial"\n    elif responsible_office_result and responsible_office_result.status not in {"success", "empty"}:\n        status = "failed"\n    elif status == "stopped":\n        status = "partial"\n    else:\n        status = "completed"\n    summary.update(\n        {\n            "status": status,\n            "total_companies": len(company_results),\n            "total_contexts_processed": len(company_results) + (1 if responsible_office_result else 0),\n            "total_success": total_success,\n            "total_failed": total_failed,\n            "total_messages": total_messages,\n            "company_results": [result.to_dict() for result in company_results],\n            "responsible_office_result": responsible_office_result.to_dict() if responsible_office_result else None,\n        }\n    )\n    return summary\n\n\ndef execute_mailbox_run(\n    runtime_env: RuntimeEnvironment,\n    *,\n    companies: list[CompanyRecord],\n    dashboard_client: DashboardClient,\n    office_context: OfficeContext,\n    mailbox_config: RobotMailboxConfig,\n    job: Optional[JobPayload] = None,\n    stop_requested: Callable[[], bool],\n    progress_callback: Optional[Callable[[dict[str, Any]], None]] = None,\n    headless: bool = False,\n) -> dict[str, Any]:\n    if not companies:\n        raise RuntimeError("Nenhuma empresa informada pelo job.json para execução.")\n\n    run_id = runtime_env.generate_run_id()\n    summary: dict[str, Any] = {\n        "run_id": run_id,\n        "started_at": utc_now_iso(),\n        "finished_at": "",\n        "status": "processing",\n        "robot_technical_id": ROBOT_TECHNICAL_ID,\n        "execution_request_id": job.execution_request_id if job else None,\n        "job_id": job.job_id if job else None,\n        "use_responsible_office": mailbox_config.use_responsible_office,\n        "responsible_office_company_id": mailbox_config.responsible_office_company_id or None,\n        "interrupted_at_company": "",\n    }\n    total_steps = len(companies) + (1 if mailbox_config.use_responsible_office else 0)\n    runtime_env.json_runtime.write_heartbeat(\n        status="processing",\n        current_job_id=job.job_id if job else None,\n        current_execution_request_id=job.execution_request_id if job else None,\n        message="run_started",\n        progress={"current": 0, "total": total_steps},\n        extra={"run_id": run_id},\n    )\n\n    company_results: list[CompanyRunResult] = []\n    responsible_office_result: Optional[CompanyRunResult] = None\n\n    if mailbox_config.use_responsible_office:\n        responsible_company = _resolve_responsible_office_company(\n            dashboard_client,\n            office_context,\n            companies,\n            mailbox_config,\n        )\n        target_companies = [company for company in companies if company.company_id != responsible_company.company_id]\n        deferred_certificate_fallbacks: list[tuple[CompanyRecord, str]] = []\n        responsible_certificate = _resolve_company_certificate(runtime_env, responsible_company)\n\n        def _open_authenticated_automation(certificate: CertificateMetadata, company_label: str) -> tuple[BrowserSession, EcacMailboxAutomation]:\n            session = launch_browser(runtime_env, headless=headless)\n            runtime_env.logger.info(f"Navegador iniciado em {session.executable_path} para {company_label}")\n            automation = EcacMailboxAutomation(\n                runtime_env,\n                session,\n                certificate,\n                stop_requested=stop_requested,\n            )\n            automation.login()\n            return session, automation\n\n        session: Optional[BrowserSession] = None\n        automation: Optional[EcacMailboxAutomation] = None\n        try:\n            session, automation = _open_authenticated_automation(responsible_certificate, responsible_company.name)\n            responsible_office_result = automation.collect_current_mailbox(\n                responsible_company,\n                run_id,\n                switch_profile=False,\n                context_type="responsible_office",\n            )\n            dashboard_client.persist_mailbox_messages(\n                office_context,\n                responsible_office_result,\n                run_id=run_id,\n            )\n            runtime_env.json_runtime.write_heartbeat(\n                status="processing",\n                current_job_id=job.job_id if job else None,\n                current_execution_request_id=job.execution_request_id if job else None,\n                message=f"processing_company:{responsible_company.company_id}",\n                progress={"current": 1, "total": total_steps, "company_id": responsible_company.company_id},\n                extra={"run_id": run_id, "company_name": responsible_company.name, "context_type": "responsible_office"},\n            )\n            if progress_callback:\n                progress_callback({"current": 1, "total": total_steps, "company_name": responsible_company.name})\n\n            for index, company in enumerate(target_companies, start=2):\n                if stop_requested():\n                    summary["status"] = "stopped"\n                    summary["interrupted_at_company"] = company.name\n                    break\n                if automation is None:\n                    try:\n                        session, automation = _open_authenticated_automation(responsible_certificate, responsible_company.name)\n                    except Exception as exc:\n                        runtime_env.logger.warning(\n                            f"Não foi possível reabrir a sessão do escritório responsável antes de {company.name}: {exc}"\n                        )\n                        if _company_has_own_certificate(company):\n                            fallback_session: Optional[BrowserSession] = None\n                            try:\n                                company_certificate = _resolve_company_certificate(runtime_env, company)\n                                fallback_session, fallback_automation = _open_authenticated_automation(company_certificate, company.name)\n                                result = fallback_automation.collect_current_mailbox(\n                                    company,\n                                    run_id,\n                                    switch_profile=False,\n                                    context_type="company",\n                                    raise_on_error=True,\n                                )\n                            except Exception as fallback_exc:\n                                result = _build_company_error_result(\n                                    company,\n                                    error_messages=[_stack_tail(exc), _stack_tail(fallback_exc)],\n                                )\n                            finally:\n                                close_browser(fallback_session, runtime_env)\n                            company_results.append(result)\n                            dashboard_client.persist_mailbox_messages(\n                                office_context,\n                                result,\n                                run_id=run_id,\n                            )\n                            continue\n                        result = _build_company_error_result(\n                            company,\n                            error_messages=[_stack_tail(exc)],\n                        )\n                        company_results.append(result)\n                        dashboard_client.persist_mailbox_messages(\n                            office_context,\n                            result,\n                            run_id=run_id,\n                        )\n                        continue\n                runtime_env.logger.info(f"Processando empresa por procuração: {company.name}")\n                runtime_env.json_runtime.write_heartbeat(\n                    status="processing",\n                    current_job_id=job.job_id if job else None,\n                    current_execution_request_id=job.execution_request_id if job else None,\n                    message=f"processing_company:{company.company_id}",\n                    progress={"current": index, "total": total_steps, "company_id": company.company_id},\n                    extra={"run_id": run_id, "company_name": company.name},\n                )\n                if progress_callback:\n                    progress_callback({"current": index, "total": total_steps, "company_name": company.name})\n                try:\n                    result = automation.collect_current_mailbox(\n                        company,\n                        run_id,\n                        switch_profile=True,\n                        context_type="company",\n                        raise_on_error=True,\n                    )\n                except ProfileSwitchNoPowerOfAttorneyError as exc:\n                    if _company_has_own_certificate(company):\n                        runtime_env.logger.warning(\n                            f"Empresa {company.name} sem procuração via escritório responsável. "\n                            "Empresa será tentada novamente no final com certificado próprio."\n                        )\n                        deferred_certificate_fallbacks.append((company, _stack_tail(exc)))\n                        continue\n                    else:\n                        runtime_env.logger.warning(\n                            f"Empresa {company.name} sem procuração via escritório responsável e sem certificado próprio. "\n                            "Seguindo para a próxima empresa."\n                        )\n                        result = _build_company_error_result(\n                            company,\n                            error_messages=[\n                                _stack_tail(exc),\n                                "Empresa sem certificado digital próprio para fallback após falta de procuração.",\n                            ],\n                        )\n                except Exception as exc:\n                    result = _build_company_error_result(\n                        company,\n                        error_messages=[_stack_tail(exc)],\n                    )\n                company_results.append(result)\n                dashboard_client.persist_mailbox_messages(\n                    office_context,\n                    result,\n                    run_id=run_id,\n                )\n        finally:\n            close_browser(session, runtime_env)\n\n        for deferred_company, deferred_error in deferred_certificate_fallbacks:\n            if stop_requested():\n                summary["status"] = "stopped"\n                summary["interrupted_at_company"] = deferred_company.name\n                break\n            runtime_env.logger.info(\n                f"Processando empresa com certificado próprio após falta de procuração: {deferred_company.name}"\n            )\n            runtime_env.json_runtime.write_heartbeat(\n                status="processing",\n                current_job_id=job.job_id if job else None,\n                current_execution_request_id=job.execution_request_id if job else None,\n                message=f"fallback_company:{deferred_company.company_id}",\n                progress={"current": len(company_results) + 1, "total": total_steps, "company_id": deferred_company.company_id},\n                extra={"run_id": run_id, "company_name": deferred_company.name, "mode": "own_certificate_fallback"},\n            )\n            fallback_session: Optional[BrowserSession] = None\n            try:\n                company_certificate = _resolve_company_certificate(runtime_env, deferred_company)\n                fallback_session, fallback_automation = _open_authenticated_automation(company_certificate, deferred_company.name)\n                result = fallback_automation.collect_current_mailbox(\n                    deferred_company,\n                    run_id,\n                    switch_profile=False,\n                    context_type="company",\n                    raise_on_error=True,\n                )\n                result.errors.insert(0, deferred_error)\n            except Exception as fallback_exc:\n                result = _build_company_error_result(\n                    deferred_company,\n                    error_messages=[deferred_error, _stack_tail(fallback_exc)],\n                )\n            finally:\n                close_browser(fallback_session, runtime_env)\n            company_results.append(result)\n            dashboard_client.persist_mailbox_messages(\n                office_context,\n                result,\n                run_id=run_id,\n            )\n    else:\n        for index, company in enumerate(companies, start=1):\n            if stop_requested():\n                summary["status"] = "stopped"\n                summary["interrupted_at_company"] = company.name\n                break\n            runtime_env.json_runtime.write_heartbeat(\n                status="processing",\n                current_job_id=job.job_id if job else None,\n                current_execution_request_id=job.execution_request_id if job else None,\n                message=f"processing_company:{company.company_id}",\n                progress={"current": index, "total": total_steps, "company_id": company.company_id},\n                extra={"run_id": run_id, "company_name": company.name},\n            )\n            if progress_callback:\n                progress_callback({"current": index, "total": total_steps, "company_name": company.name})\n            if not company.eligible:\n                company_results.append(\n                    CompanyRunResult(\n                        company_id=company.company_id,\n                        company_name=company.name,\n                        company_document=format_document(company.document),\n                        context_type="company",\n                        status="blocked",\n                        eligible=False,\n                        block_reason=company.block_reason,\n                        errors=[company.block_reason or "Empresa não elegível para execução isolada."],\n                        started_at=utc_now_iso(),\n                        finished_at=utc_now_iso(),\n                    )\n                )\n                continue\n            session = None\n            try:\n                certificate = _resolve_company_certificate(runtime_env, company)\n                session = launch_browser(runtime_env, headless=headless)\n                runtime_env.logger.info(f"Navegador iniciado em {session.executable_path} para {company.name}")\n                automation = EcacMailboxAutomation(\n                    runtime_env,\n                    session,\n                    certificate,\n                    stop_requested=stop_requested,\n                )\n                automation.login()\n                result = automation.collect_current_mailbox(company, run_id, switch_profile=False, context_type="company")\n                company_results.append(result)\n                dashboard_client.persist_mailbox_messages(\n                    office_context,\n                    result,\n                    run_id=run_id,\n                )\n            finally:\n                close_browser(session, runtime_env)\n\n    summary["finished_at"] = utc_now_iso()\n    summary = _finalize_summary(summary, company_results, responsible_office_result)\n    runtime_env.json_runtime.write_result(\n        job=job,\n        success=summary["status"] in {"completed", "partial"},\n        summary=summary,\n        error_message=None if summary["status"] != "failed" else "Falha geral na execução do robô.",\n        company_results=[result.to_dict() for result in company_results],\n        responsible_office_result=responsible_office_result.to_dict() if responsible_office_result else None,\n        payload={"mailbox_config": mailbox_config.to_dict()},\n    )\n    return summary\n\n# === worker ===\nimport threading\nfrom datetime import datetime, timezone\nfrom typing import Any, Callable, Optional\n\n\ntry:\n    from PySide6.QtCore import QThread, Signal\nexcept Exception:  # pragma: no cover - CLI mode sem PySide6\n    QThread = object  # type: ignore[misc,assignment]\n\n    class Signal:  # type: ignore[override]\n        def __init__(self, *args: Any, **kwargs: Any) -> None:\n            pass\n\n        def emit(self, *args: Any, **kwargs: Any) -> None:\n            return\n\n\ndef run_mailbox_job(\n    runtime_env: RuntimeEnvironment,\n    *,\n    companies: list[CompanyRecord],\n    job: Optional[JobPayload] = None,\n    headless: bool = False,\n    stop_requested: Optional[Callable[[], bool]] = None,\n    log_callback: Optional[Callable[[str], None]] = None,\n    progress_callback: Optional[Callable[[dict[str, Any]], None]] = None,\n) -> dict[str, Any]:\n    stop_requested = stop_requested or (lambda: False)\n    if log_callback:\n        runtime_env.logger.bind_sink(log_callback)\n    certificate_auth.ensure_windows_environment()\n    if not job:\n        raise RuntimeError("job.json obrigatório não encontrado para o robô da Caixa Postal.")\n\n    dashboard_client = DashboardClient(runtime_env)\n    office_context = dashboard_client.resolve_office_context(job)\n    mailbox_config = dashboard_client.fetch_mailbox_runtime_config(office_context)\n    dashboard_robot_id = dashboard_client.register_robot_presence(status="processing")\n    runtime_env.json_runtime.register_robot(\n        extra={\n            "office_id": office_context.office_id,\n            "office_server_id": office_context.office_server_id,\n            "office_source": office_context.source,\n        }\n    )\n    if not office_context.office_id:\n        raise RuntimeError("office_id não resolvido. Verifique /api/robot-config, CONNECTOR_SECRET e a VM do escritório.")\n\n    scoped_companies = _apply_mailbox_eligibility(companies, mailbox_config)\n    if not scoped_companies:\n        raise RuntimeError("Nenhuma empresa carregada a partir do job.json.")\n\n    if not mailbox_config.use_responsible_office and not any(company.eligible for company in scoped_companies):\n        raise RuntimeError("Nenhuma empresa do job possui certificado digital configurado.")\n\n    heartbeat_state: dict[str, Any] = {\n        "current": 0,\n        "total": len(scoped_companies) + (1 if mailbox_config.use_responsible_office else 0),\n        "company_name": "",\n    }\n    stop_flag = threading.Event()\n\n    def heartbeat_loop() -> None:\n        while not stop_flag.wait(HEARTBEAT_INTERVAL_SECONDS):\n            runtime_env.json_runtime.write_heartbeat(\n                status="processing",\n                current_job_id=job.job_id if job else None,\n                current_execution_request_id=job.execution_request_id if job else None,\n                message="heartbeat",\n                progress=dict(heartbeat_state),\n                extra={"office_id": office_context.office_id, "office_server_id": office_context.office_server_id},\n            )\n            dashboard_client.update_robot_presence(status="processing", robot_id=dashboard_robot_id or "")\n\n    def on_progress(payload: dict[str, Any]) -> None:\n        heartbeat_state.update(payload)\n        if progress_callback:\n            progress_callback(payload)\n\n    heartbeat_thread = threading.Thread(target=heartbeat_loop, name="ecac-mailbox-heartbeat", daemon=True)\n    heartbeat_thread.start()\n    try:\n        summary = execute_mailbox_run(\n            runtime_env,\n            companies=scoped_companies,\n            dashboard_client=dashboard_client,\n            office_context=office_context,\n            mailbox_config=mailbox_config,\n            job=job,\n            stop_requested=stop_requested,\n            progress_callback=on_progress,\n            headless=headless,\n        )\n        return summary\n    except Exception as exc:\n        failed_summary = {\n            "run_id": runtime_env.generate_run_id(),\n            "started_at": utc_now_iso(),\n            "finished_at": utc_now_iso(),\n            "status": "failed",\n            "robot_technical_id": "ecac_caixa_postal",\n            "total_companies": len(scoped_companies),\n            "total_success": 0,\n            "total_failed": len(scoped_companies),\n            "total_messages": 0,\n            "error": str(exc),\n            "company_results": [],\n            "responsible_office_result": None,\n        }\n        runtime_env.json_runtime.write_result(\n            job=job,\n            success=False,\n            summary=failed_summary,\n            error_message=str(exc),\n            company_results=[],\n            responsible_office_result=None,\n            payload={"mailbox_config": mailbox_config.to_dict()},\n        )\n        runtime_env.json_runtime.write_heartbeat(\n            status="active",\n            message="result_ready",\n            extra={"office_id": office_context.office_id, "office_server_id": office_context.office_server_id},\n        )\n        dashboard_client.update_robot_presence(status="active", robot_id=dashboard_robot_id or "")\n        raise\n    finally:\n        stop_flag.set()\n        heartbeat_thread.join(timeout=2)\n        if not stop_requested():\n            dashboard_client.update_robot_presence(status="active", robot_id=dashboard_robot_id or "")\n\n\nclass EcacMailboxWorker(QThread):  # type: ignore[misc]\n    log = Signal(str)\n    status = Signal(str)\n    progress = Signal(object)\n    summary_ready = Signal(object)\n    error = Signal(str)\n\n    def __init__(\n        self,\n        runtime_env: RuntimeEnvironment,\n        *,\n        companies: list[CompanyRecord],\n        job: Optional[JobPayload] = None,\n        headless: bool = False,\n    ) -> None:\n        super().__init__()\n        self.runtime = runtime_env\n        self.companies = companies\n        self.job = job\n        self.headless = headless\n        self._stop_requested = False\n\n    def request_stop(self) -> None:\n        self._stop_requested = True\n        try:\n            self.requestInterruption()\n        except Exception:\n            pass\n\n    def run(self) -> None:\n        try:\n            self.status.emit("Executando")\n            summary = run_mailbox_job(\n                self.runtime,\n                companies=self.companies,\n                job=self.job,\n                headless=self.headless,\n                stop_requested=lambda: self._stop_requested,\n                log_callback=self.log.emit,\n                progress_callback=self.progress.emit,\n            )\n            self.summary_ready.emit(summary)\n            self.status.emit("Concluído")\n        except Exception as exc:\n            self.error.emit(str(exc))\n            self.status.emit("Falha")\n\n# === ui_main ===\nimport os\nfrom pathlib import Path\nfrom typing import Optional\n\n\nfrom PySide6.QtCore import Qt, QTimer\nfrom PySide6.QtGui import QColor\nfrom PySide6.QtWidgets import (\n    QAbstractItemView,\n    QApplication,\n    QGroupBox,\n    QHBoxLayout,\n    QLabel,\n    QLineEdit,\n    QListWidget,\n    QListWidgetItem,\n    QMainWindow,\n    QMessageBox,\n    QPushButton,\n    QTextEdit,\n    QVBoxLayout,\n    QWidget,\n)\n\n\nclass MainWindow(QMainWindow):\n    def __init__(self, runtime_env: RuntimeEnvironment) -> None:\n        super().__init__()\n        self.runtime = runtime_env\n        self.dashboard = DashboardClient(runtime_env)\n        self.robot_dashboard_id: Optional[str] = None\n        self.job: Optional[JobPayload] = self.runtime.json_runtime.load_job()\n        self.office_context = self.dashboard.resolve_office_context(self.job)\n        self.mailbox_config = RobotMailboxConfig()\n        self.worker: Optional[EcacMailboxWorker] = None\n        self.companies: list[CompanyRecord] = []\n        self.filtered_companies: list[CompanyRecord] = []\n        self.robot_heartbeat_timer = QTimer(self)\n        self.robot_heartbeat_timer.setInterval(30000)\n        self.robot_heartbeat_timer.timeout.connect(self._on_robot_heartbeat)\n\n        self.setWindowTitle("e-CAC - Caixa Postal")\n        self.resize(1180, 760)\n        self._build_ui()\n        self.runtime.logger.bind_sink(self.append_log)\n        self._load_initial_state()\n        self.robot_dashboard_id = self.dashboard.register_robot_presence(status="active")\n        self.robot_heartbeat_timer.start()\n\n    def _build_ui(self) -> None:\n        central = QWidget()\n        central.setObjectName("centralRoot")\n        self.setCentralWidget(central)\n        layout = QVBoxLayout(central)\n        layout.setContentsMargins(18, 18, 18, 18)\n        layout.setSpacing(14)\n\n        title = QLabel("e-CAC - Caixa Postal")\n        title.setStyleSheet("font-size: 24px; font-weight: 700; color: #14213d;")\n        layout.addWidget(title)\n\n        subtitle = QLabel("Autenticação via certificado do operador, troca de perfil por empresa e extração da Caixa Postal.")\n        subtitle.setStyleSheet("color: #4b5563; font-size: 13px;")\n        subtitle.setWordWrap(True)\n        layout.addWidget(subtitle)\n\n        top_row = QHBoxLayout()\n        top_row.setSpacing(14)\n        layout.addLayout(top_row)\n\n        companies_box = QGroupBox("Empresas")\n        companies_layout = QVBoxLayout(companies_box)\n        self.search_input = QLineEdit()\n        self.search_input.setPlaceholderText("Filtrar por nome ou CNPJ")\n        self.search_input.textChanged.connect(self._apply_company_filter)\n        companies_layout.addWidget(self.search_input)\n\n        self.company_list = QListWidget()\n        self.company_list.setSelectionMode(QAbstractItemView.NoSelection)\n        self.company_list.setAlternatingRowColors(True)\n        self.company_list.setUniformItemSizes(False)\n        companies_layout.addWidget(self.company_list, 1)\n\n        top_row.addWidget(companies_box, 2)\n\n        side_column = QVBoxLayout()\n        side_column.setSpacing(12)\n        top_row.addLayout(side_column, 1)\n\n        self.status_label = QLabel("Pronto")\n        self.progress_label = QLabel("0 / 0")\n        self.office_label = QLabel(self.office_context.office_id or "office_id não resolvido")\n        self.job_label = QLabel("job.json não carregado")\n        status_panel = QWidget()\n        status_layout = QVBoxLayout(status_panel)\n        status_layout.setContentsMargins(0, 0, 0, 0)\n        status_layout.setSpacing(6)\n        status_layout.addWidget(QLabel("Status"))\n        status_layout.addWidget(self.status_label)\n        status_layout.addWidget(QLabel("Progresso"))\n        status_layout.addWidget(self.progress_label)\n        status_layout.addWidget(QLabel("office_id"))\n        status_layout.addWidget(self.office_label)\n        status_layout.addWidget(QLabel("job_id"))\n        status_layout.addWidget(self.job_label)\n        side_column.addWidget(status_panel)\n\n        controls = QHBoxLayout()\n        self.start_button = QPushButton("Iniciar")\n        self.start_button.clicked.connect(self.start_worker)\n        self.stop_button = QPushButton("Parar")\n        self.stop_button.clicked.connect(self.stop_worker)\n        self.stop_button.setEnabled(False)\n        self.clear_log_button = QPushButton("Limpar log")\n        controls.addWidget(self.start_button)\n        controls.addWidget(self.stop_button)\n        controls.addWidget(self.clear_log_button)\n        side_column.addLayout(controls)\n        side_column.addStretch(1)\n\n        log_box = QGroupBox("Logs")\n        log_layout = QVBoxLayout(log_box)\n        self.log_panel = QTextEdit()\n        self.log_panel.setReadOnly(True)\n        self.log_panel.setLineWrapMode(QTextEdit.NoWrap)\n        self.log_panel.setPlaceholderText("Logs da execução aparecerão aqui.")\n        self.clear_log_button.clicked.connect(self.log_panel.clear)\n        log_layout.addWidget(self.log_panel)\n        layout.addWidget(log_box, 1)\n\n        self.setStyleSheet(\n            """\n            QMainWindow, QWidget#centralRoot {\n                background: #eef2f6;\n                color: #18212f;\n            }\n            QLabel {\n                color: #18212f;\n            }\n            QGroupBox {\n                font-weight: 700;\n                color: #14213d;\n                border: 1px solid #cfd8e3;\n                border-radius: 12px;\n                margin-top: 12px;\n                padding-top: 10px;\n                background: #ffffff;\n            }\n            QGroupBox::title {\n                left: 12px;\n                padding: 0 6px;\n                color: #0f172a;\n                background: #ffffff;\n            }\n            QPushButton {\n                background: #0b5cab;\n                color: #ffffff;\n                border: 1px solid #0a4f95;\n                border-radius: 8px;\n                padding: 9px 14px;\n                font-weight: 600;\n            }\n            QPushButton:hover {\n                background: #0e6dcb;\n            }\n            QPushButton:pressed {\n                background: #094a8c;\n            }\n            QPushButton:disabled {\n                background: #b8c2cc;\n                color: #eef2f6;\n                border-color: #b8c2cc;\n            }\n            QLineEdit, QListWidget, QTextEdit {\n                background: #ffffff;\n                color: #111827;\n                border: 1px solid #c7d2df;\n                border-radius: 8px;\n                padding: 8px;\n                selection-background-color: #0b5cab;\n                selection-color: #ffffff;\n            }\n            QLineEdit:focus, QListWidget:focus, QTextEdit:focus {\n                border: 1px solid #0b5cab;\n            }\n            QCheckBox {\n                color: #18212f;\n                spacing: 8px;\n            }\n            QListWidget {\n                alternate-background-color: #f7fafc;\n                outline: 0;\n            }\n            QListWidget::item {\n                color: #111827;\n                background: transparent;\n                border-radius: 6px;\n                padding: 8px 6px;\n                margin: 2px;\n            }\n            QListWidget::item:selected {\n                background: #dbeafe;\n                color: #0f172a;\n                border: 1px solid #93c5fd;\n            }\n            QListWidget::item:hover {\n                background: #eff6ff;\n                color: #0f172a;\n            }\n            QTextEdit {\n                background: #0f172a;\n                color: #e5eef8;\n                border: 1px solid #1e293b;\n                font-family: Consolas, \'Courier New\', monospace;\n            }\n            """\n        )\n\n    def append_log(self, message: str) -> None:\n        self.log_panel.append(message)\n\n    def _load_initial_state(self) -> None:\n        self.append_log("Carregando estado inicial do robô.")\n        self.reload_companies()\n        if self.job:\n            self.job_label.setText(self.job.job_id or self.job.execution_request_id or "(sem job_id)")\n        else:\n            self.job_label.setText("job.json pendente ou já consumido")\n\n    def reload_companies(self) -> None:\n        try:\n            self.job = self.runtime.json_runtime.load_job()\n            self.office_context = self.dashboard.resolve_office_context(self.job, force_refresh=True)\n            company_ids = self.job.company_ids or None if self.job else None\n            self.mailbox_config = self.dashboard.fetch_mailbox_runtime_config(self.office_context)\n            self.companies = self.dashboard.load_companies(self.office_context, job=self.job, company_ids=company_ids)\n            self.companies = _apply_mailbox_eligibility(self.companies, self.mailbox_config)\n            self.office_label.setText(self.office_context.office_id or "office_id não resolvido")\n            self._apply_company_filter()\n            self.append_log(f"{len(self.companies)} empresas carregadas do dashboard.")\n            if self.job:\n                self.job_label.setText(self.job.job_id or self.job.execution_request_id or "(sem job_id)")\n                self.status_label.setText("Pronto para executar job")\n                self.progress_label.setText(f"0 / {len(self.companies)}")\n                self.start_button.setEnabled(True)\n            else:\n                self.job_label.setText("job.json pendente ou já consumido")\n                self.status_label.setText("Conectado ao dashboard")\n                self.progress_label.setText(f"0 / {len(self.companies)}")\n                self.append_log("Nenhum job.json pendente encontrado. Empresas exibidas a partir do dashboard.")\n                self.start_button.setEnabled(bool(self.companies))\n            self.append_log(\n                "Configuração dinâmica: "\n                + (\n                    "com escritório responsável."\n                    if self.mailbox_config.use_responsible_office\n                    else "sem escritório responsável."\n                )\n            )\n        except Exception as exc:\n            self.append_log(f"Falha ao carregar empresas: {exc}")\n            self.companies = []\n            self._apply_company_filter()\n            self.start_button.setEnabled(False)\n\n    def _apply_company_filter(self) -> None:\n        search = self.search_input.text().strip().lower()\n        self.filtered_companies = []\n        self.company_list.clear()\n        for company in self.companies:\n            if search and search not in company.search_text:\n                continue\n            self.filtered_companies.append(company)\n            item = QListWidgetItem(\n                f"{company.name}\\n{format_document(company.document)} | {\'Elegível\' if company.eligible else \'Bloqueada\'}"\n            )\n            item.setData(Qt.UserRole, company.company_id)\n            if company.block_reason:\n                item.setToolTip(company.block_reason)\n            if not company.eligible:\n                item.setForeground(QColor("#8b1e1e"))\n                item.setFlags(item.flags() & ~Qt.ItemIsSelectable)\n                item.setText(item.text() + f"\\nMotivo: {company.block_reason}")\n            self.company_list.addItem(item)\n\n    def _build_manual_job(self) -> JobPayload:\n        execution_id = f"manual_{uuid.uuid4().hex}"\n        company_ids = [company.company_id for company in self.companies if str(company.company_id).strip()]\n        company_rows = [company.to_dict() for company in self.companies]\n        return JobPayload(\n            job_id=execution_id,\n            execution_request_id=execution_id,\n            office_id=self.office_context.office_id,\n            company_ids=company_ids,\n            companies=company_rows,\n            raw={\n                "job_id": execution_id,\n                "execution_request_id": execution_id,\n                "office_id": self.office_context.office_id,\n                "company_ids": company_ids,\n                "companies": company_rows,\n                "source": "manual_ui",\n            },\n        )\n\n    def start_worker(self) -> None:\n        if self.worker is not None and self.worker.isRunning():\n            return\n        if not self.companies:\n            QMessageBox.warning(self, "Empresas", "Nenhuma empresa foi carregada do dashboard.")\n            return\n        execution_job = self.job\n        if not execution_job:\n            if not self.office_context.office_id:\n                QMessageBox.warning(self, "Escritório", "office_id não resolvido. Recarregue o dashboard antes de iniciar.")\n                return\n            execution_job = self._build_manual_job()\n            self.append_log("Nenhum job.json pendente encontrado. Iniciando execução manual com as empresas do dashboard.")\n            self.job_label.setText(execution_job.job_id)\n        if not execution_job.company_ids and not execution_job.companies:\n            QMessageBox.warning(self, "Empresas", "Nenhuma empresa foi informada para a execução.")\n            return\n        self.worker = EcacMailboxWorker(\n            self.runtime,\n            companies=list(self.companies),\n            job=execution_job,\n            headless=False,\n        )\n        self.worker.log.connect(self.append_log)\n        self.worker.status.connect(self.status_label.setText)\n        self.worker.progress.connect(self._on_progress)\n        self.worker.summary_ready.connect(self._on_summary)\n        self.worker.error.connect(self._on_error)\n        self.worker.finished.connect(self._on_worker_finished)\n        self.start_button.setEnabled(False)\n        self.stop_button.setEnabled(True)\n        self.status_label.setText("Executando")\n        self.dashboard.update_robot_presence(status="processing", robot_id=self.robot_dashboard_id or "")\n        self.worker.start()\n\n    def stop_worker(self) -> None:\n        if self.worker is None:\n            return\n        self.append_log("Solicitação de parada enviada ao worker.")\n        self.worker.request_stop()\n        self.status_label.setText("Parando")\n\n    def _on_progress(self, payload: object) -> None:\n        if isinstance(payload, dict):\n            current = payload.get("current", 0)\n            total = payload.get("total", 0)\n            company = payload.get("company_name", "")\n            self.progress_label.setText(f"{current} / {total}")\n            if company:\n                self.status_label.setText(f"Executando: {company}")\n\n    def _on_summary(self, summary: object) -> None:\n        if not isinstance(summary, dict):\n            return\n        self.append_log(\n            f"Resumo final: status={summary.get(\'status\')} contextos={summary.get(\'total_contexts_processed\', summary.get(\'total_companies\'))} mensagens={summary.get(\'total_messages\')}"\n        )\n        self.status_label.setText(str(summary.get("status") or "Concluído"))\n        total_contexts = int(summary.get("total_contexts_processed", summary.get("total_companies", 0)) or 0)\n        self.progress_label.setText(f"{total_contexts} / {total_contexts}")\n        self.dashboard.update_robot_presence(status="active", robot_id=self.robot_dashboard_id or "")\n\n    def _on_error(self, message: str) -> None:\n        self.append_log(f"Erro: {message}")\n        QMessageBox.critical(self, "Execução", message)\n\n        self.dashboard.update_robot_presence(status="active", robot_id=self.robot_dashboard_id or "")\n\n    def _on_worker_finished(self) -> None:\n        self.start_button.setEnabled(True)\n        self.stop_button.setEnabled(False)\n        self.dashboard.update_robot_presence(status="active", robot_id=self.robot_dashboard_id or "")\n\n    def _on_robot_heartbeat(self) -> None:\n        status = "processing" if self.worker is not None and self.worker.isRunning() else "active"\n        self.dashboard.update_robot_presence(status=status, robot_id=self.robot_dashboard_id or "")\n\n    def closeEvent(self, event) -> None:  # type: ignore[override]\n        if self.worker is not None and self.worker.isRunning():\n            self.worker.request_stop()\n            self.worker.wait(3000)\n        self.robot_heartbeat_timer.stop()\n        self.dashboard.update_robot_presence(status="inactive", robot_id=self.robot_dashboard_id or "")\n        self.runtime.mark_inactive()\n        super().closeEvent(event)\n\n# === ecac_caixa_postal_app ===\nimport argparse\nimport json\nimport sys\nfrom pathlib import Path\n\nSCRIPT_DIR = Path(__file__).resolve().parent\nif str(SCRIPT_DIR) not in sys.path:\n    sys.path.insert(0, str(SCRIPT_DIR))\n\n\n\ndef parse_args() -> argparse.Namespace:\n    parser = argparse.ArgumentParser(description="e-CAC - Caixa Postal")\n    parser.add_argument("--no-ui", action="store_true", help="Executa sem interface gráfica.")\n    parser.add_argument("--headless", action="store_true", help="Executa o navegador em headless.")\n    return parser.parse_args()\n\n\ndef _run_cli(args: argparse.Namespace) -> int:\n    runtime_env = build_runtime()\n    job = runtime_env.json_runtime.load_job()\n    if not job:\n        raise RuntimeError("job.json obrigatório não encontrado para o robô da Caixa Postal.")\n    dashboard = DashboardClient(runtime_env)\n    robot_id = dashboard.register_robot_presence(status="active")\n    office_context = dashboard.resolve_office_context(job)\n    companies = dashboard.load_companies(office_context, job=job, company_ids=job.company_ids or None)\n    try:\n        summary = run_mailbox_job(\n            runtime_env,\n            companies=companies,\n            job=job,\n            headless=args.headless,\n            log_callback=lambda line: print(line, flush=True),\n        )\n        print(json.dumps(summary, ensure_ascii=False, indent=2))\n        return 0 if summary.get("status") in {"completed", "partial"} else 1\n    finally:\n        dashboard.update_robot_presence(status="inactive", robot_id=robot_id or "")\n        runtime_env.mark_inactive("cli_exit")\n\n\ndef main() -> int:\n    args = parse_args()\n    if args.no_ui:\n        return _run_cli(args)\n\n    from PySide6.QtWidgets import QApplication\n\n    app = QApplication(sys.argv)\n    runtime_env = build_runtime()\n    window = MainWindow(runtime_env)\n    window.show()\n    return app.exec()\n\n\nif __name__ == "__main__":\n    raise SystemExit(main())\n\n'

def _load_ecac_base():
    cache_key = "_ecac_caixa_postal_singlefile_module"
    if cache_key in sys.modules:
        return sys.modules[cache_key]

    module = types.ModuleType(cache_key)
    module.__file__ = str(SCRIPT_DIR / "ecac_caixa_postal_embedded.py")
    sys.modules[cache_key] = module
    exec(_ECAC_BASE_SOURCE.lstrip("\ufeff"), module.__dict__)
    return module


ecac_base = _load_ecac_base()

RuntimePaths = ecac_base.RuntimePaths
OfficeContext = ecac_base.OfficeContext
CompanyRecord = ecac_base.CompanyRecord
CertificateMetadata = ecac_base.CertificateMetadata
JobPayload = ecac_base.JobPayload
RuntimeLogger = ecac_base.RuntimeLogger
JsonRobotRuntime = ecac_base.JsonRobotRuntime
RuntimeEnvironment = ecac_base.RuntimeEnvironment
BrowserSession = ecac_base.BrowserSession

utc_now_iso = ecac_base.utc_now_iso
only_digits = ecac_base.only_digits
format_document = ecac_base.format_document
slugify = ecac_base.slugify
read_json = ecac_base.read_json
write_json_atomic = ecac_base.write_json_atomic
launch_browser = ecac_base.launch_browser
close_browser = ecac_base.close_browser
resolve_certificate_from_dashboard = ecac_base.resolve_certificate_from_dashboard
resolve_certificate_from_env = ecac_base.resolve_certificate_from_env
import_pfx_and_get_metadata = ecac_base.import_pfx_and_get_metadata

LOGIN_URL = ecac_base.LOGIN_URL
PGDAS_ENTRY_URL = "https://cav.receita.fazenda.gov.br/ecac/Aplicacao.aspx?id=10009&origem=menu"

ROBOT_SEGMENT_PATH_DEFAULT = ""

PROFILE_MENU_LABELS = list(ecac_base.PROFILE_MENU_LABELS)
PGDAS_READY_MARKERS = [
    "PGDAS-D",
    "DEFIS",
    "Declaracao Mensal",
    "Declaracao mensal",
    "Consultar Declaracoes",
    "Consultar declaracoes",
]
HCAPTCHA_ERROR_URL_TOKEN = "/Erro/93003"
DEFIS_ENTRY_URL = "https://sinac.cav.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/defis.app/entrada.aspx"
LOGIN_HCAPTCHA_RELOAD_LIMIT = 5
LOGIN_HCAPTCHA_EXTENSION_WAIT_TIMEOUT_MS = 120000
LOGIN_BROWSER_RESET_LIMIT = 3
CAPTCHA_SOLVER_EXTENSION_ID = "eihghbeaaeedpcojhbghbocnkcponaeo"
CAPTCHA_SOLVER_EXTENSION_NAME = "Skill Up Agency LTD - Captcha Solver"


def _stack_tail(exc: BaseException) -> str:
    return "".join(traceback.format_exception_only(type(exc), exc)).strip()


def _normalize_portal_text(value: str) -> str:
    raw = str(value or "")
    decomposed = unicodedata.normalize("NFKD", raw)
    without_accents = "".join(char for char in decomposed if not unicodedata.combining(char))
    return " ".join(without_accents.split()).lower()


def _extract_document_from_subject(subject: str) -> str:
    match = re.search(r":(\d{11,14})", str(subject or ""))
    if match:
        return match.group(1)
    return ""


def _extract_common_name_from_subject(subject: str) -> str:
    match = re.search(r"CN=([^,]+)", str(subject or ""), flags=re.IGNORECASE)
    if match:
        common_name = str(match.group(1) or "").strip()
        common_name = re.sub(r":\d{11,14}$", "", common_name).strip()
        return common_name
    return ""


def _read_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json_dict(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def _captcha_solver_extension_status(profile_root: Path) -> tuple[bool, str]:
    root = Path(profile_root)
    secure_preferences_path = root / "Default" / "Secure Preferences"
    settings = (
        (_read_json_dict(secure_preferences_path).get("extensions") or {}).get("settings") or {}
    )
    if not isinstance(settings, dict):
        return False, "Secure Preferences sem registro de extensoes."
    extension_entry = settings.get(CAPTCHA_SOLVER_EXTENSION_ID)
    if not isinstance(extension_entry, dict):
        return False, f"Extensao {CAPTCHA_SOLVER_EXTENSION_NAME} nao registrada no perfil."
    disable_reasons = extension_entry.get("disable_reasons") or []
    if isinstance(disable_reasons, list) and disable_reasons:
        return False, f"Extensao {CAPTCHA_SOLVER_EXTENSION_NAME} desabilitada: {disable_reasons}."
    extension_root = root / "Default" / "Extensions" / CAPTCHA_SOLVER_EXTENSION_ID
    if not extension_root.exists():
        return False, f"Diretorio da extensao {CAPTCHA_SOLVER_EXTENSION_NAME} ausente em {extension_root}."
    versions = sorted([item.name for item in extension_root.iterdir() if item.is_dir()])
    version = versions[-1] if versions else "sem_versao"
    return True, f"{CAPTCHA_SOLVER_EXTENSION_NAME} pronta (id={CAPTCHA_SOLVER_EXTENSION_ID}, versao={version})."


def _sync_captcha_solver_extension_from_bundle(runtime_env: RuntimeEnvironment) -> bool:
    bundled_profile_root = SCRIPT_DIR / "data" / "chrome_profile_backup"
    target_profile_root = Path(runtime_env.paths.chrome_profile_backup_dir)
    try:
        if bundled_profile_root.resolve() == target_profile_root.resolve():
            return False
    except Exception:
        pass
    source_status, _ = _captcha_solver_extension_status(bundled_profile_root)
    if not source_status:
        return False

    copied = False
    source_extension_root = bundled_profile_root / "Default" / "Extensions" / CAPTCHA_SOLVER_EXTENSION_ID
    target_extension_root = target_profile_root / "Default" / "Extensions" / CAPTCHA_SOLVER_EXTENSION_ID
    if source_extension_root.exists():
        target_extension_root.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source_extension_root, target_extension_root, dirs_exist_ok=True)
        copied = True

    for relative_name in ("Extension Rules", "Extension Scripts", "Extension State"):
        source_dir = bundled_profile_root / "Default" / relative_name
        target_dir = target_profile_root / "Default" / relative_name
        if source_dir.exists():
            shutil.copytree(source_dir, target_dir, dirs_exist_ok=True)
            copied = True

    source_secure_preferences_path = bundled_profile_root / "Default" / "Secure Preferences"
    target_secure_preferences_path = target_profile_root / "Default" / "Secure Preferences"
    source_secure_preferences = _read_json_dict(source_secure_preferences_path)
    target_secure_preferences = _read_json_dict(target_secure_preferences_path)
    source_settings = (
        (source_secure_preferences.get("extensions") or {}).get("settings") or {}
    )
    extension_entry = source_settings.get(CAPTCHA_SOLVER_EXTENSION_ID)
    if isinstance(extension_entry, dict):
        target_extensions = target_secure_preferences.setdefault("extensions", {})
        if not isinstance(target_extensions, dict):
            target_extensions = {}
            target_secure_preferences["extensions"] = target_extensions
        target_settings = target_extensions.setdefault("settings", {})
        if not isinstance(target_settings, dict):
            target_settings = {}
            target_extensions["settings"] = target_settings
        target_settings[CAPTCHA_SOLVER_EXTENSION_ID] = extension_entry
        _write_json_dict(target_secure_preferences_path, target_secure_preferences)
        copied = True

    return copied


def _ensure_captcha_solver_extension_ready(runtime_env: RuntimeEnvironment) -> bool:
    profile_backup_dir = Path(runtime_env.paths.chrome_profile_backup_dir)
    ready, detail = _captcha_solver_extension_status(profile_backup_dir)
    if ready:
        runtime_env.logger.info(detail)
        return True
    if _sync_captcha_solver_extension_from_bundle(runtime_env):
        ready, detail = _captcha_solver_extension_status(profile_backup_dir)
        if ready:
            runtime_env.logger.info(f"Extensao solver restaurada no perfil Chrome: {detail}")
            return True
    runtime_env.logger.warning(
        f"Extensao solver indisponivel no perfil Chrome: {detail}"
    )
    return False


def sanitize_company_folder(name: str) -> str:
    normalized = str(name or "").strip()
    normalized = re.sub(r'[<>:"/\\|?*]', "_", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip().rstrip(" .")
    return normalized or "Empresa"


def _normalize_folder_token(value: str) -> str:
    raw = str(value or "")
    decomposed = unicodedata.normalize("NFKD", raw)
    without_accents = "".join(char for char in decomposed if not unicodedata.combining(char))
    collapsed = re.sub(r"[^a-zA-Z0-9]+", " ", without_accents.lower())
    return " ".join(collapsed.split())


def _resolve_runtime_dir_for_robot(technical_id: str, runtime_dir: Optional[Path]) -> Optional[Path]:
    if runtime_dir is not None:
        return Path(runtime_dir).resolve()

    robots_root = str(os.getenv("ROBOTS_ROOT_PATH") or "").strip()
    if robots_root:
        return (Path(robots_root).resolve() / technical_id).resolve()

    home_robots_root = Path.home() / "Documents" / "ROBOS"
    home_technical_dir = (home_robots_root / technical_id).resolve()
    if home_technical_dir.exists():
        return home_technical_dir

    legacy_candidates = [
        home_robots_root / "simples_nacional_emitir_guia",
        home_robots_root / "simples_nacional_consulta_extratos_defis",
        home_robots_root / "simples_nacional_debitos",
    ]
    for candidate in legacy_candidates:
        resolved = candidate.resolve()
        if resolved.exists() and (resolved / Path(__file__).name).exists():
            return resolved

    current_dir = SCRIPT_DIR.resolve()
    parent_dir = current_dir.parent
    if parent_dir.name.strip().lower() == "robos":
        return (parent_dir / technical_id).resolve()

    return None


def build_runtime(
    technical_id: str,
    display_name: str,
    *,
    sink: Optional[Callable[[str], None]] = None,
    runtime_dir: Optional[Path] = None,
) -> RuntimeEnvironment:
    runtime_dir = _resolve_runtime_dir_for_robot(technical_id, runtime_dir)
    previous = os.environ.get("ROBOT_SCRIPT_DIR")
    if runtime_dir is not None:
        os.environ["ROBOT_SCRIPT_DIR"] = str(runtime_dir)
    try:
        paths = ecac_base.bootstrap_environment()
    finally:
        if runtime_dir is not None:
            if previous is None:
                os.environ.pop("ROBOT_SCRIPT_DIR", None)
            else:
                os.environ["ROBOT_SCRIPT_DIR"] = previous
    logger = RuntimeLogger(paths.runtime_log_path, sink=sink)
    json_runtime = JsonRobotRuntime(technical_id, display_name, paths.json_dir)
    return RuntimeEnvironment(paths=paths, logger=logger, json_runtime=json_runtime)


_embedded_close_browser = close_browser
_external_chrome_processes: dict[int, subprocess.Popen[Any]] = {}
_external_chrome_browsers: dict[int, Any] = {}


def _remove_tree_force(path: Path) -> None:
    target = Path(path)
    if not target.exists():
        return

    def _onerror(func, failing_path, exc_info):
        try:
            os.chmod(failing_path, stat.S_IWRITE)
        except Exception:
            pass
        try:
            func(failing_path)
        except Exception:
            pass

    shutil.rmtree(target, onerror=_onerror)


def _resolve_portable_chrome_exe(runtime_env: RuntimeEnvironment) -> Path:
    chrome_exe = Path(runtime_env.paths.data_dir) / "Chrome" / "chrome.exe"
    if not chrome_exe.exists():
        raise RuntimeError(f"Chrome portátil obrigatório não encontrado em {chrome_exe}.")
    return chrome_exe


def _allocate_local_debug_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _terminate_external_chrome_process(proc: Optional[subprocess.Popen[Any]]) -> None:
    if proc is None:
        return
    try:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)
    except Exception:
        try:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)
        except Exception:
            pass


def _terminate_runtime_portable_chrome_processes(runtime_env: RuntimeEnvironment) -> None:
    chrome_exe = _resolve_portable_chrome_exe(runtime_env)
    powershell_script = textwrap.dedent(
        f"""
        $target = {chrome_exe.as_posix()!r}
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object {{ $_.ExecutablePath -eq $target }} |
            ForEach-Object {{
                try {{
                    Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop
                }} catch {{
                }}
            }}
        """
    )
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-Command", powershell_script],
            check=False,
            timeout=15,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception:
        pass


def _wait_for_cdp_ready(proc: subprocess.Popen[Any], port: int, timeout_seconds: float = 20.0) -> dict[str, Any]:
    deadline = time.time() + max(timeout_seconds, 3.0)
    cdp_url = f"http://127.0.0.1:{port}/json/version"
    last_error = ""
    while time.time() < deadline:
        exit_code = proc.poll()
        if exit_code is not None:
            raise RuntimeError(f"Chrome portátil encerrou antes do CDP subir. Código de saída: {exit_code}.")
        try:
            response = requests.get(cdp_url, timeout=0.8)
            if response.ok:
                payload = response.json() if response.content else {}
                if isinstance(payload, dict) and payload.get("webSocketDebuggerUrl"):
                    return payload
        except Exception as exc:
            last_error = str(exc)
        time.sleep(0.25)
    raise TimeoutError(f"Timeout ao aguardar CDP do Chrome portátil na porta {port}. {last_error}".strip())


def _is_internal_chrome_url(url: str) -> bool:
    lowered = str(url or "").strip().lower()
    return lowered.startswith(("chrome://", "devtools://", "edge://"))


def _resolve_automation_page(context: Any):
    pages = list(getattr(context, "pages", []) or [])
    for page in pages:
        try:
            if not _is_internal_chrome_url(page.url):
                page.bring_to_front()
                return page
        except Exception:
            continue

    page = context.new_page()
    try:
        page.bring_to_front()
    except Exception:
        pass
    return page


def _rebuild_chrome_profile_from_backup(
    runtime_env: RuntimeEnvironment,
    *,
    require_backup: bool,
) -> Path:
    profile_dir = Path(runtime_env.paths.chrome_profile_dir)
    backup_dir = Path(runtime_env.paths.chrome_profile_backup_dir)

    if profile_dir.exists():
        _terminate_runtime_portable_chrome_processes(runtime_env)
        _remove_tree_force(profile_dir)
        if profile_dir.exists():
            time.sleep(0.5)
            _remove_tree_force(profile_dir)

    if not backup_dir.exists():
        if require_backup:
            raise RuntimeError(
                f"Backup obrigatório do perfil não encontrado em {backup_dir}."
            )
        profile_dir.mkdir(parents=True, exist_ok=True)
        return profile_dir

    shutil.copytree(backup_dir, profile_dir, dirs_exist_ok=profile_dir.exists())
    return profile_dir


def launch_browser(runtime_env: RuntimeEnvironment, *, headless: bool = False) -> BrowserSession:
    from playwright.sync_api import sync_playwright

    _ensure_captcha_solver_extension_ready(runtime_env)
    chrome_exe = _resolve_portable_chrome_exe(runtime_env)
    profile_dir = _rebuild_chrome_profile_from_backup(runtime_env, require_backup=True)
    port = _allocate_local_debug_port()
    runtime_env.logger.info(
        f"Chrome persistente configurado com executável=data/Chrome/chrome.exe perfil=data/chrome_profile porta_cdp={port}"
    )

    chrome_args = [
        f"--remote-debugging-port={port}",
        "--remote-debugging-address=127.0.0.1",
        f"--user-data-dir={profile_dir}",
        "--no-first-run",
        "--no-default-browser-check",
        "--remote-allow-origins=*",
        "--disable-blink-features=AutomationControlled",
        "--disable-features=IsolateOrigins,site-per-process",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-popup-blocking",
        "--start-maximized",
        "--ignore-certificate-errors",
        "--no-sandbox",
    ]

    chrome_proc = subprocess.Popen(
        [str(chrome_exe), *chrome_args],
        cwd=str(chrome_exe.parent),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )

    playwright = None
    try:
        _wait_for_cdp_ready(chrome_proc, port, timeout_seconds=20.0)
        playwright = sync_playwright().start()
        browser = playwright.chromium.connect_over_cdp(f"http://127.0.0.1:{port}", timeout=20000)
        contexts = getattr(browser, "contexts", []) or []
        context = contexts[0] if contexts else browser.new_context(ignore_https_errors=True, viewport={"width": 1600, "height": 1200})
        try:
            context.set_default_timeout(120000)
        except Exception:
            pass
        try:
            context.set_default_navigation_timeout(120000)
        except Exception:
            pass
        page = _resolve_automation_page(context)
        session = BrowserSession(
            playwright=playwright,
            context=context,
            page=page,
            executable_path=str(chrome_exe),
        )
        _external_chrome_processes[id(session)] = chrome_proc
        _external_chrome_browsers[id(session)] = browser
        return session
    except Exception:
        if playwright is not None:
            try:
                playwright.stop()
            except Exception:
                pass
        _terminate_external_chrome_process(chrome_proc)
        try:
            _terminate_runtime_portable_chrome_processes(runtime_env)
            time.sleep(0.35)
            _rebuild_chrome_profile_from_backup(runtime_env, require_backup=False)
        except Exception:
            pass
        raise


def close_browser(session: Optional[BrowserSession], runtime_env: Optional[RuntimeEnvironment] = None) -> None:
    session_id = id(session) if session is not None else None
    browser = _external_chrome_browsers.pop(session_id, None) if session_id is not None else None
    chrome_proc = _external_chrome_processes.pop(session_id, None) if session_id is not None else None
    try:
        try:
            if session is not None and getattr(session, "page", None) is not None:
                session.page.close()
        except Exception:
            pass
        try:
            if session is not None and getattr(session, "context", None) is not None:
                session.context.close()
        except Exception:
            pass
        try:
            if browser is not None:
                browser.close()
        except Exception:
            pass
        try:
            if session is not None and getattr(session, "playwright", None) is not None:
                session.playwright.stop()
        except Exception:
            pass
        if browser is None and chrome_proc is None:
            _embedded_close_browser(session, runtime_env)
    finally:
        _terminate_external_chrome_process(chrome_proc)
        if runtime_env is not None:
            try:
                _terminate_runtime_portable_chrome_processes(runtime_env)
                time.sleep(0.35)
                _rebuild_chrome_profile_from_backup(runtime_env, require_backup=False)
            except Exception as exc:
                runtime_env.logger.warning(f"Falha ao restaurar chrome_profile a partir do backup: {exc}")


def cleanup_runtime_artifacts(runtime_env: RuntimeEnvironment) -> None:
    try:
        shutdown_runtime_logger(runtime_env)
    except Exception:
        pass

    data_dir = getattr(runtime_env.paths, "data_dir", None)
    logs_dir = getattr(runtime_env.paths, "logs_dir", None)
    if data_dir:
        try:
            shutil.rmtree(Path(data_dir) / "output", ignore_errors=True)
        except Exception:
            pass
        try:
            for gitkeep in Path(data_dir).rglob(".gitkeep"):
                gitkeep.unlink(missing_ok=True)
        except Exception:
            pass
    if logs_dir:
        try:
            shutil.rmtree(Path(logs_dir), ignore_errors=True)
        except Exception:
            pass

    try:
        stop_path = get_stop_signal_path(runtime_env)
        if stop_path.exists():
            stop_path.unlink()
    finally:
        try:
            _terminate_runtime_portable_chrome_processes(runtime_env)
            time.sleep(0.35)
            _rebuild_chrome_profile_from_backup(runtime_env, require_backup=False)
        except Exception as exc:
            runtime_env.logger.warning(f"Falha ao restaurar chrome_profile a partir do backup: {exc}")


class DashboardClient:
    def __init__(self, runtime_env: RuntimeEnvironment, technical_id: str, display_name: str) -> None:
        self.runtime = runtime_env
        self.technical_id = technical_id
        self.display_name = display_name
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
            raise RuntimeError("Supabase nao configurado. Defina SUPABASE_URL e uma service role key.")
        try:
            from supabase import create_client
        except Exception as exc:
            raise RuntimeError("Biblioteca supabase nao disponivel no ambiente do robo.") from exc
        return create_client(self.supabase_url, self.supabase_key)

    def build_server_api_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.server_api_url and "ngrok" in self.server_api_url.lower():
            headers["ngrok-skip-browser-warning"] = "true"
        if self.connector_secret:
            import hashlib

            hashed = hashlib.sha256(self.connector_secret.encode("utf-8")).hexdigest()
            headers["Authorization"] = f"Bearer {hashed}"
        return headers

    def fetch_robot_config_from_api(self) -> Optional[dict[str, Any]]:
        if not self.server_api_url:
            return None
        url = f"{self.server_api_url}/api/robot-config"
        response = requests.get(
            url,
            params={"technical_id": self.technical_id},
            headers=self.build_server_api_headers(),
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("Resposta invalida da API /api/robot-config.")
        self._robot_config_cache = payload
        return payload

    def fetch_robot_runtime_metadata(self) -> dict[str, Any]:
        try:
            client = self._supabase()
            response = (
                client.table("robots")
                .select("segment_path,notes_mode")
                .eq("technical_id", self.technical_id)
                .limit(1)
                .execute()
            )
            rows = getattr(response, "data", None) or []
            if not rows:
                return {}
            row = rows[0] if isinstance(rows[0], dict) else {}
            return {
                "segment_path": str(row.get("segment_path") or "").strip() or None,
                "notes_mode": str(row.get("notes_mode") or "").strip() or None,
            }
        except Exception as exc:
            self.runtime.logger.warning(f"Falha ao consultar segment_path do robo na tabela robots: {exc}")
            return {}

    def derive_segment_path_from_folder_structure(self, payload: Optional[dict[str, Any]]) -> str:
        nodes = payload.get("folder_structure") if isinstance(payload, dict) else None
        if not isinstance(nodes, list):
            return ""

        if self.technical_id == "ecac_simples_consulta_extratos_defis":
            candidates = {
                "extrato do simples e defis",
                "extratos e defis",
                "consulta extratos e defis",
                "consulta de extratos e defis",
            }
        elif self.technical_id == "ecac_simples_emitir_guia":
            candidates = {
                "emitir guia das",
                "guia do simples nacional",
                "simples nacional emitir guia",
                "emissao de guia das",
            }
        else:
            candidates = {_normalize_folder_token(self.display_name), _normalize_folder_token(self.technical_id)}
        candidates = {item for item in candidates if item}
        if not candidates:
            return ""

        indexed: dict[str, dict[str, Any]] = {}
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_id = str(node.get("id") or "").strip()
            if node_id:
                indexed[node_id] = node

        matches: list[tuple[int, int, str]] = []
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_name = str(node.get("name") or "").strip()
            node_slug = str(node.get("slug") or "").strip()
            name_norm = _normalize_folder_token(node_name)
            slug_norm = _normalize_folder_token(node_slug)
            score = 0
            for candidate in candidates:
                if not candidate:
                    continue
                if name_norm == candidate or slug_norm == candidate:
                    score = max(score, 3)
                elif candidate in name_norm or candidate in slug_norm:
                    score = max(score, 2)
                elif name_norm in candidate or slug_norm in candidate:
                    score = max(score, 1)
            if score <= 0:
                continue

            path_names: list[str] = []
            current = node
            guard = 0
            while isinstance(current, dict) and guard < 20:
                current_name = str(current.get("name") or "").strip()
                if current_name:
                    path_names.append(current_name)
                parent_id = str(current.get("parent_id") or "").strip()
                current = indexed.get(parent_id) if parent_id else None
                guard += 1
            if path_names:
                matches.append((score, len(path_names), "/".join(reversed(path_names))))

        if not matches:
            return ""
        matches.sort(key=lambda item: (item[0], item[1], len(item[2])), reverse=True)
        return matches[0][2]

    def register_robot_presence(self, status: str = "active") -> Optional[str]:
        try:
            client = self._supabase()
            api_cfg = self._robot_config_cache
            if api_cfg is None:
                try:
                    api_cfg = self.fetch_robot_config_from_api()
                except Exception:
                    api_cfg = None
            segment_path = str((api_cfg or {}).get("segment_path") or "").strip()
            if not segment_path:
                segment_path = self.derive_segment_path_from_folder_structure(api_cfg)
            payload = {
                "display_name": self.display_name,
                "status": status,
                "last_heartbeat_at": utc_now_iso(),
                "segment_path": segment_path or None,
                "notes_mode": str((api_cfg or {}).get("notes_mode") or "").strip() or None,
            }
            response = client.table("robots").select("id").eq("technical_id", self.technical_id).limit(1).execute()
            rows = getattr(response, "data", None) or []
            if rows:
                robot_id = str(rows[0].get("id") or "").strip()
                if robot_id:
                    client.table("robots").update(payload).eq("id", robot_id).execute()
                    self._registered_robot_id = robot_id
                    return robot_id
            inserted = client.table("robots").insert({"technical_id": self.technical_id, **payload}).execute()
            inserted_rows = getattr(inserted, "data", None) or []
            if inserted_rows:
                robot_id = str(inserted_rows[0].get("id") or "").strip()
                if robot_id:
                    self._registered_robot_id = robot_id
                    return robot_id
        except Exception as exc:
            self.runtime.logger.warning(f"Falha ao registrar o robo na tabela robots: {exc}")
        return None

    def update_robot_presence(self, status: str = "active", robot_id: str = "") -> None:
        try:
            client = self._supabase()
            payload = {
                "status": status,
                "last_heartbeat_at": utc_now_iso(),
            }
            target_id = str(robot_id or self._registered_robot_id or "").strip()
            if target_id:
                client.table("robots").update(payload).eq("id", target_id).execute()
                return
            client.table("robots").update(payload).eq("technical_id", self.technical_id).execute()
        except Exception as exc:
            self.runtime.logger.warning(f"Falha ao atualizar heartbeat do robo na tabela robots: {exc}")

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
            if not segment_path:
                segment_path = self.derive_segment_path_from_folder_structure(payload)
            notes_mode = str(payload.get("notes_mode") or "").strip()
            if office_id:
                source = "server_api"

        if not segment_path or not notes_mode:
            robot_cfg = self.fetch_robot_runtime_metadata()
            if not segment_path:
                segment_path = str(robot_cfg.get("segment_path") or "").strip()
            if not notes_mode:
                notes_mode = str(robot_cfg.get("notes_mode") or "").strip()

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

    def fetch_admin_settings(self, office_context: OfficeContext) -> dict[str, Any]:
        if not office_context.office_id:
            return {}
        try:
            client = self._supabase()
            response = (
                client.table("office_robot_configs")
                .select("admin_settings")
                .eq("office_id", office_context.office_id)
                .eq("robot_technical_id", self.technical_id)
                .limit(1)
                .execute()
            )
            rows = getattr(response, "data", None) or []
            settings = rows[0].get("admin_settings") if rows else {}
            return settings if isinstance(settings, dict) else {}
        except Exception as exc:
            self.runtime.logger.warning(f"Falha ao carregar admin_settings do robo: {exc}")
            return {}

    def fetch_responsible_office_config(self, office_context: OfficeContext) -> ResponsibleOfficeConfig:
        admin_settings = self.fetch_admin_settings(office_context)
        return ResponsibleOfficeConfig(
            use_responsible_office=bool(admin_settings.get("use_responsible_office")),
            responsible_office_company_id=str(admin_settings.get("responsible_office_company_id") or "").strip(),
            source="office_robot_configs" if admin_settings else "default",
        )

    def load_company_by_id(self, office_context: OfficeContext, company_id: str) -> Optional[CompanyRecord]:
        target_id = str(company_id or "").strip()
        if not office_context.office_id or not target_id:
            return None

        client = self._supabase()
        config_response = (
            client.table("company_robot_config")
            .select("company_id,enabled,settings,auth_mode")
            .eq("office_id", office_context.office_id)
            .eq("robot_technical_id", self.technical_id)
            .eq("company_id", target_id)
            .limit(1)
            .execute()
        )
        config_rows = getattr(config_response, "data", None) or []
        cfg_row = dict(config_rows[0]) if config_rows else {}
        cfg_settings = cfg_row.get("settings")
        if isinstance(cfg_settings, str):
            try:
                cfg_settings = json.loads(cfg_settings)
            except Exception:
                cfg_settings = {}
        if not isinstance(cfg_settings, dict):
            cfg_settings = {}

        response = (
            client.table("companies")
            .select("id,name,document,active,office_id,auth_mode,cert_blob_b64,cert_password")
            .eq("office_id", office_context.office_id)
            .eq("id", target_id)
            .limit(1)
            .execute()
        )
        rows = getattr(response, "data", None) or []
        if not rows:
            return None

        row = dict(rows[0])
        active = bool(row.get("active", True))
        cfg_auth_mode = str(cfg_row.get("auth_mode") or "").strip().lower()
        company_auth_mode = str(
            row.get("auth_mode")
            or ("certificate" if row.get("cert_blob_b64") else "password")
        ).strip().lower()
        auth_mode = cfg_auth_mode or company_auth_mode
        if auth_mode not in {"password", "certificate"}:
            auth_mode = "password"
        cert_password = str(cfg_settings.get("cert_password") or row.get("cert_password") or "").strip()
        cert_blob_b64 = str(cfg_settings.get("cert_blob_b64") or row.get("cert_blob_b64") or "").strip()
        cert_path = str(cfg_settings.get("cert_path") or "").strip()
        block_reason = ""
        if not active:
            block_reason = "Empresa inativa no dashboard."

        return CompanyRecord(
            company_id=str(row.get("id") or "").strip(),
            name=str(row.get("name") or "").strip(),
            document=only_digits(row.get("document") or ""),
            active=active,
            eligible=active,
            block_reason=block_reason,
            source="dashboard",
            auth_mode=auth_mode,
            cert_password=cert_password,
            cert_blob_b64=cert_blob_b64,
            cert_path=cert_path,
            raw={"company": row, "config": cfg_row},
        )

    def _normalize_job_companies(
        self,
        job: Optional[JobPayload],
        company_ids: Optional[list[str]] = None,
    ) -> list[CompanyRecord]:
        if not job or not job.companies:
            return []
        wanted = {str(item).strip() for item in (company_ids or []) if str(item).strip()}
        rows: list[CompanyRecord] = []
        for row in job.companies:
            company_id = str(row.get("company_id") or row.get("id") or "").strip()
            if not company_id or (wanted and company_id not in wanted):
                continue
            active = bool(row.get("active", True))
            auth_mode = str(
                row.get("auth_mode")
                or ("certificate" if row.get("cert_blob_b64") or row.get("cert_path") else "password")
            ).strip().lower()
            if auth_mode not in {"password", "certificate"}:
                auth_mode = "password"
            block_reason = str(row.get("block_reason") or "").strip()
            if not active and not block_reason:
                block_reason = "Empresa inativa no dashboard."
            rows.append(
                CompanyRecord(
                    company_id=company_id,
                    name=str(row.get("name") or "").strip(),
                    document=only_digits(row.get("document") or row.get("doc") or ""),
                    active=active,
                    eligible=bool(row.get("eligible", active)) and active,
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
        company_ids: Optional[list[str]] = None,
    ) -> list[CompanyRecord]:
        snapshot_rows = self._normalize_job_companies(job, company_ids)
        if snapshot_rows:
            return snapshot_rows

        if not office_context.office_id:
            raise RuntimeError("office_id nao resolvido. Verifique o conector da VM e /api/robot-config.")

        client = self._supabase()
        wanted_ids = [str(item).strip() for item in (company_ids or []) if str(item).strip()]

        config_query = (
            client.table("company_robot_config")
            .select("company_id,enabled,settings,auth_mode")
            .eq("office_id", office_context.office_id)
            .eq("robot_technical_id", self.technical_id)
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
            normalized_row["settings"] = settings if isinstance(settings, dict) else {}
            config_by_company[company_id] = normalized_row
            enabled_company_ids.append(company_id)

        target_company_ids = wanted_ids or enabled_company_ids
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
            cfg_row = config_by_company.get(company_id, {})
            cfg_settings = cfg_row.get("settings") if isinstance(cfg_row.get("settings"), dict) else {}
            active = bool(row.get("active", True))
            cfg_auth_mode = str(cfg_row.get("auth_mode") or "").strip().lower()
            company_auth_mode = str(
                row.get("auth_mode")
                or ("certificate" if row.get("cert_blob_b64") else "password")
            ).strip().lower()
            auth_mode = cfg_auth_mode or company_auth_mode
            if auth_mode not in {"password", "certificate"}:
                auth_mode = "password"
            cert_password = str(cfg_settings.get("cert_password") or row.get("cert_password") or "").strip()
            cert_blob_b64 = str(cfg_settings.get("cert_blob_b64") or row.get("cert_blob_b64") or "").strip()
            cert_path = str(cfg_settings.get("cert_path") or "").strip()
            eligible = bool(cfg_row.get("enabled", True)) and active
            block_reason = ""
            if not active:
                block_reason = "Empresa inativa no dashboard."
            normalized.append(
                CompanyRecord(
                    company_id=company_id,
                    name=str(row.get("name") or "").strip(),
                    document=only_digits(row.get("document") or ""),
                    active=active,
                    eligible=eligible,
                    block_reason=block_reason,
                    source="dashboard",
                    auth_mode=auth_mode,
                    cert_password=cert_password,
                    cert_blob_b64=cert_blob_b64,
                    cert_path=cert_path,
                    raw={"company": dict(row), "config": cfg_row},
                )
            )
        return normalized


class HCaptchaVisibleError(RuntimeError):
    pass


class LoginBrowserResetRequiredError(RuntimeError):
    pass


def _ps_quote(value: str) -> str:
    return str(value or "").replace("'", "''")


def select_certificate_dialog_strong(certificate: CertificateMetadata, timeout_ms: int = 45000) -> None:
    candidates = ecac_base.certificate_auth.build_selector_candidates(certificate.subject, certificate.alias)
    if not candidates:
        raise RuntimeError("Nenhum seletor de certificado disponivel para a janela nativa.")

    powershell_targets = "@(" + ", ".join("'" + _ps_quote(item) + "'" for item in candidates) + ")"
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
        $deadline = (Get-Date).AddMilliseconds({timeout_ms})
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
            throw 'Janela de selecao de certificado nao apareceu.'
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

        function Get-OkButton($dialog) {{
            $condition = New-Object System.Windows.Automation.PropertyCondition(
                [System.Windows.Automation.AutomationElement]::NameProperty,
                'OK'
            )
            return $dialog.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $condition)
        }}

        function Wait-Closed($root, $dialogTitle) {{
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

        function Click-Center($element) {{
            if (-not $element) {{ return $false }}
            $rect = $element.Current.BoundingRectangle
            if ($rect.Width -le 0 -or $rect.Height -le 0) {{ return $false }}
            $x = [int]($rect.Left + ($rect.Width / 2))
            $y = [int]($rect.Top + ($rect.Height / 2))
            [NativeCertUi]::SetCursorPos($x, $y) | Out-Null
            Start-Sleep -Milliseconds 80
            [NativeCertUi]::mouse_event(0x0002, 0, 0, 0, [UIntPtr]::Zero)
            Start-Sleep -Milliseconds 40
            [NativeCertUi]::mouse_event(0x0004, 0, 0, 0, [UIntPtr]::Zero)
            Start-Sleep -Milliseconds 180
            return $true
        }}

        function Find-DialogList($dialog) {{
            $all = $dialog.FindAll([System.Windows.Automation.TreeScope]::Descendants, [System.Windows.Automation.Condition]::TrueCondition)
            foreach ($item in $all) {{
                $type = [string]$item.Current.ControlType.ProgrammaticName
                if ($type -in @('ControlType.DataGrid', 'ControlType.List', 'ControlType.Table')) {{
                    return $item
                }}
            }}
            return $dialog
        }}

        function Find-ChromeWindow($root) {{
            $wins = $root.FindAll([System.Windows.Automation.TreeScope]::Children, [System.Windows.Automation.Condition]::TrueCondition)
            $preferred = @()
            for ($i = 0; $i -lt $wins.Count; $i++) {{
                $w = $wins.Item($i)
                $name = [string]$w.Current.Name
                if (-not $name) {{ continue }}
                $lname = $name.ToLower()
                if ($lname.Contains('gov.br') -and ($lname.Contains('chrome') -or $lname.Contains('edge'))) {{
                    $preferred += $w
                }}
            }}
            if ($preferred.Count -gt 0) {{ return $preferred[0] }}
            for ($i = 0; $i -lt $wins.Count; $i++) {{
                $w = $wins.Item($i)
                $name = [string]$w.Current.Name
                if (-not $name) {{ continue }}
                $lname = $name.ToLower()
                if ($lname.Contains('google chrome') -or $lname.Contains('chrome') -or $lname.Contains('microsoft edge') -or $lname.Contains('edge')) {{
                    return $w
                }}
            }}
            return $null
        }}

        $previousWindow = [NativeCertUi]::GetForegroundWindow()
        $cursor = New-Object POINT
        [NativeCertUi]::GetCursorPos([ref]$cursor) | Out-Null

        try {{
            $listHost = Find-DialogList $dialog
            if (-not (Click-Center $listHost)) {{
                $chromeWindow = Find-ChromeWindow $root
                if (-not $chromeWindow) {{
                    throw 'Nao encontrei uma janela do Chrome/Edge para focar. Deixe o navegador visivel e tente novamente.'
                }}
                $chromeHandle = [IntPtr]$chromeWindow.Current.NativeWindowHandle
                if ($chromeHandle -eq [IntPtr]::Zero) {{
                    throw 'A janela principal do Chrome nao possui handle valido.'
                }}
                [NativeCertUi]::ShowWindow($chromeHandle, 5) | Out-Null
                [NativeCertUi]::SetForegroundWindow($chromeHandle) | Out-Null
                Start-Sleep -Milliseconds 150
                $rect = $chromeWindow.Current.BoundingRectangle
                $clickX = [int]($rect.Left + ($rect.Width * 0.48))
                $clickY = [int]($rect.Top + ($rect.Height * 0.31))
                [NativeCertUi]::SetCursorPos($clickX, $clickY) | Out-Null
                Start-Sleep -Milliseconds 80
                [NativeCertUi]::mouse_event(0x0002, 0, 0, 0, [UIntPtr]::Zero)
                Start-Sleep -Milliseconds 40
                [NativeCertUi]::mouse_event(0x0004, 0, 0, 0, [UIntPtr]::Zero)
                Start-Sleep -Milliseconds 180
            }}

            $okButton = Get-OkButton $dialog
            $wsh = New-Object -ComObject WScript.Shell
            $wsh.SendKeys('{{HOME}}')
            Start-Sleep -Milliseconds 220

            for ($step = 0; $step -lt 60; $step++) {{
                $focusedName = ''
                try {{
                    $focused = [System.Windows.Automation.AutomationElement]::FocusedElement
                    if ($focused) {{
                        $focusedName = [string]$focused.Current.Name
                    }}
                }} catch {{}}
                if (Matches-Target $focusedName $targets) {{
                    if ($okButton) {{
                        Click-Center $okButton | Out-Null
                    }} else {{
                        $wsh.SendKeys('~')
                    }}
                    if (Wait-Closed $root $dialogTitle) {{
                        exit 0
                    }}
                    throw ('O seletor permaneceu aberto apos confirmar o certificado: ' + $focusedName)
                }}
                if ($step -lt 59) {{
                    $wsh.SendKeys('{{DOWN}}')
                    Start-Sleep -Milliseconds 180
                }}
            }}

            throw 'Nao foi possivel alcancar o certificado alvo navegando pela lista nativa.'
        }} finally {{
            [NativeCertUi]::SetCursorPos($cursor.X, $cursor.Y) | Out-Null
            if ($previousWindow -ne [IntPtr]::Zero) {{
                [NativeCertUi]::SetForegroundWindow($previousWindow) | Out-Null
            }}
        }}
        """
    )
    ecac_base.certificate_auth.run_powershell(script, timeout_ms=timeout_ms + 15000)


ecac_base.certificate_auth.select_certificate_dialog = select_certificate_dialog_strong


@dataclass(slots=True)
class CompanyExecutionResult:
    company_id: str
    company_name: str
    company_document: str
    status: str = "pending"
    eligible: bool = True
    block_reason: str = ""
    profile_switched: bool = False
    company_profile_context: str = ""
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    files: list[dict[str, Any]] = field(default_factory=list)
    records: list[dict[str, Any]] = field(default_factory=list)
    flags: dict[str, Any] = field(default_factory=dict)
    started_at: str = ""
    finished_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RobotRunSummary:
    run_id: str
    technical_id: str
    display_name: str
    started_at: str
    finished_at: str = ""
    status: str = "pending"
    company_results: list[CompanyExecutionResult] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "technical_id": self.technical_id,
            "display_name": self.display_name,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status": self.status,
            "company_results": [item.to_dict() for item in self.company_results],
            "metadata": self.metadata,
        }


@dataclass(slots=True)
class ResponsibleOfficeConfig:
    use_responsible_office: bool = False
    responsible_office_company_id: str = ""
    source: str = "default"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class SimplesEcacAutomation(ecac_base.EcacMailboxAutomation):
    def __init__(
        self,
        runtime_env: RuntimeEnvironment,
        session: BrowserSession,
        certificate: CertificateMetadata,
        *,
        stop_requested: Callable[[], bool],
        headless: bool,
        allow_manual_captcha: bool = True,
        manual_captcha_timeout_ms: int = 180000,
    ) -> None:
        super().__init__(runtime_env, session, certificate, stop_requested=stop_requested)
        self.headless = headless
        self.allow_manual_captcha = allow_manual_captcha
        self.manual_captcha_timeout_ms = manual_captcha_timeout_ms
        self.expected_document = _extract_document_from_subject(certificate.subject)
        self.captcha_solver_available = _ensure_captcha_solver_extension_ready(runtime_env)

    def _capture_debug(self, prefix: str) -> None:
        try:
            self._capture_debug_artifacts(prefix)
        except Exception:
            pass

    def _bring_browser_to_front(self) -> None:
        try:
            self.page.bring_to_front()
        except Exception:
            pass
        try:
            self.page.evaluate(
                """() => {
                    try {
                        window.focus();
                    } catch (error) {
                    }
                    try {
                        document.body?.focus?.();
                    } catch (error) {
                    }
                    return true;
                }"""
            )
        except Exception:
            pass

    def _goto_login_with_retry(self, timeout_ms: int = 20000) -> None:
        last_error: Optional[Exception] = None
        for _ in range(3):
            try:
                self._bring_browser_to_front()
                self.page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=timeout_ms)
                return
            except Exception as exc:
                last_error = exc
                try:
                    self.page.wait_for_timeout(250)
                except Exception:
                    time.sleep(0.25)
        raise RuntimeError(str(last_error or "Falha ao abrir a pagina inicial do e-CAC."))

    def _on_gov_br_identity_page(self) -> bool:
        current_url = str(self.page.url or "").lower()
        if "acesso.gov.br" in current_url:
            return True
        markers = (
            "Seu certificado digital",
            "Certificado digital",
            "Identifique-se no gov.br",
            "Numero do CPF",
            "Número do CPF",
        )
        return any(self._page_contains(marker) for marker in markers)

    def _click_access_gov_br(self, timeout_ms: int = 12000) -> None:
        deadline = time.time() + max(timeout_ms, 1000) / 1000
        pattern = re.compile(r"^\s*Acesso Gov BR\s*$", re.IGNORECASE)
        while time.time() < deadline:
            current_url = str(self.page.url or "").lower()
            if "acesso.gov.br" in current_url or self._on_gov_br_identity_page():
                return
            self._bring_browser_to_front()
            candidates = [
                self.page.get_by_role("button", name=pattern),
                self.page.get_by_text(pattern),
                self.page.get_by_role("link", name=pattern),
            ]
            for locator in candidates:
                try:
                    count = locator.count()
                except Exception:
                    continue
                for index in range(min(count, 6)):
                    candidate = locator.nth(index)
                    if not self._click_locator_candidate(candidate, timeout_ms=250):
                        continue
                    try:
                        self._wait_until(
                            lambda: "acesso.gov.br" in str(self.page.url or "").lower()
                            or self._page_contains("Seu certificado digital"),
                            timeout_ms=2500,
                            interval_ms=75,
                        )
                    except Exception:
                        pass
                    current_url = str(self.page.url or "").lower()
                    if "acesso.gov.br" in current_url or self._page_contains("Seu certificado digital"):
                        return
            try:
                clicked = bool(
                    self.page.evaluate(
                        """() => {
                            const normalize = (value) =>
                                String(value || '')
                                    .normalize('NFD')
                                    .replace(/[\\u0300-\\u036f]/g, '')
                                    .replace(/\\s+/g, ' ')
                                    .trim()
                                    .toLowerCase();
                            const nodes = Array.from(document.querySelectorAll('button, a, [role="button"], [role="link"], span, div'));
                            const isVisible = (el) => {
                                if (!el) return false;
                                const style = window.getComputedStyle(el);
                                const rect = el.getBoundingClientRect();
                                return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
                            };
                            for (const node of nodes) {
                                if (!isVisible(node)) continue;
                                const text = normalize(node.innerText || node.textContent || node.getAttribute('aria-label') || '');
                                if (text !== 'acesso gov br') continue;
                                node.click();
                                return true;
                            }
                            return false;
                        }"""
                    )
                )
                if clicked:
                    try:
                        self._wait_until(
                            lambda: "acesso.gov.br" in str(self.page.url or "").lower()
                            or self._page_contains("Seu certificado digital"),
                            timeout_ms=2500,
                            interval_ms=75,
                        )
                    except Exception:
                        pass
                    current_url = str(self.page.url or "").lower()
                    if "acesso.gov.br" in current_url or self._page_contains("Seu certificado digital"):
                        return
            except Exception:
                pass
            try:
                self.page.wait_for_timeout(75)
            except Exception:
                time.sleep(0.075)
        raise RuntimeError("Falha ao acionar o botão 'Acesso Gov BR'.")

    def _profile_switch_success_relaxed(self, company: CompanyRecord) -> bool:
        targets = [target for target in self._profile_switch_targets(company) if target]
        if not targets:
            return False
        context_text = self._current_profile_context().lower()
        body_text = self._body_text().lower()
        return any(target in context_text or target in body_text for target in targets)

    def _context_matches_company(self, company: CompanyRecord, context: str = "") -> bool:
        current_context = str(context or self._current_profile_context() or "").strip()
        if not current_context:
            return False
        current_lower = current_context.lower()
        document = only_digits(company.document)
        if document and document in only_digits(current_context):
            return True
        return bool(company.name and company.name.lower() in current_lower)

    def _extract_profile_modal_error(self) -> str:
        generic_tokens = {"atencao:", "atenção:", "atencao", "atenção"}
        ignored_fragments = {
            "desculpe, o e-cac nao pode carregar o menu de servicos.",
            "desculpe, o e-cac não pôde carregar o menu de serviços.",
            "clique aqui para tentar novamente",
            "por favor, tente novamente.",
        }
        collected: list[str] = []
        selectors = [
            ".erro",
            ".erro .mensagemErro",
            ".erro p",
            ".erro span",
            ".infoCpfBloqueado",
            ".infoCpfBloqueado span",
            "[role='alert']",
            ".ui-state-error",
            ".validation-summary-errors",
        ]
        scopes = list(self._profile_scopes()) or [self.page]
        for scope in scopes:
            for selector in selectors:
                locator = scope.locator(selector)
                try:
                    count = locator.count()
                except Exception:
                    continue
                for index in range(min(count, 8)):
                    candidate = locator.nth(index)
                    raw_text = ""
                    try:
                        if not candidate.is_visible():
                            continue
                    except Exception:
                        continue
                    try:
                        raw_text = " ".join((candidate.inner_text(timeout=120) or "").split())
                    except Exception:
                        raw_text = ""
                    if not raw_text:
                        try:
                            raw_text = str(
                                candidate.evaluate(
                                    """el => String(
                                        el?.innerText ||
                                        el?.textContent ||
                                        ''
                                    ).replace(/\\s+/g, ' ').trim()"""
                                )
                                or ""
                            ).strip()
                        except Exception:
                            raw_text = ""
                    normalized = " ".join(str(raw_text or "").split()).strip()
                    lowered = _normalize_portal_text(normalized)
                    if (
                        not normalized
                        or lowered in generic_tokens
                        or any(fragment in lowered for fragment in ignored_fragments)
                    ):
                        continue
                    collected.append(normalized)
        deduped: list[str] = []
        seen: set[str] = set()
        for text in collected:
            key = _normalize_portal_text(text)
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(text)
        return " ".join(deduped).strip()

    def _raise_profile_switch_modal_error(self, modal_error: str) -> None:
        lowered_error = _normalize_portal_text(modal_error)
        if (
            "nao existe procuracao eletronica" in lowered_error
            or "opcao indisponivel para esse procurador" in lowered_error
            or "opcao indisponivel para este procurador" in lowered_error
        ):
            raise ecac_base.ProfileSwitchNoPowerOfAttorneyError(
                f"Falha retornada pelo portal ao alterar perfil: {modal_error}"
            )
        raise ecac_base.ProfileSwitchError(f"Falha retornada pelo portal ao alterar perfil: {modal_error}")

    def _profile_modal_visible(self, company: Optional[CompanyRecord] = None) -> bool:
        profile_markers = [
            "alterar perfil",
            "perfil de acesso",
            "responsavel legal",
            "responsável legal",
            "procurador",
            "empresa filial",
            "sucessora",
        ]
        for scope in self._visible_modal_scopes():
            try:
                text = " ".join(self._body_text(scope).split()).lower()
            except Exception:
                text = ""
            if any(marker in text for marker in profile_markers):
                return True
            if company and company.document:
                digits = only_digits(company.document)
                if digits and digits in only_digits(text):
                    return True
            try:
                if scope.locator("#txtNIPapel1, #txtNIPapel2, #txtNIPapel3, #txtNIPapel4, #txtNIPapel5, #txtCnpj").count() > 0:
                    return True
            except Exception:
                continue
        return False

    def _is_valid_profile_input(self, field: Any) -> bool:
        attributes: dict[str, str] = {}
        for attr_name in ("id", "name", "placeholder", "title", "aria-label", "class"):
            try:
                attributes[attr_name] = str(field.get_attribute(attr_name) or "")
            except Exception:
                attributes[attr_name] = ""
        attrs_text = " ".join(attributes.values()).lower()
        if any(token in attrs_text for token in ("txtpesquisar", "frmpesquisar", "localizar serviço", "localizar servico")):
            return False
        try:
            if field.locator("xpath=ancestor::form[@id='frmPesquisar' or @name='pesquisa']").count() > 0:
                return False
        except Exception:
            pass
        accepted_tokens = ("nipapel", "cnpj", "cpf", "empresa", "documento", "papel", "txtcnpj")
        if any(token in attrs_text for token in accepted_tokens):
            return True
        try:
            form = field.locator("xpath=ancestor::form[1]").first
            form_id = str(form.get_attribute("id") or "").lower()
            form_name = str(form.get_attribute("name") or "").lower()
            form_text = " ".join(self._body_text(form).split()).lower()
        except Exception:
            form_id = ""
            form_name = ""
            form_text = ""
        if any(token in form_id or token in form_name for token in ("formpj", "formpf", "formmatriz", "formsucessora", "formentefederativovinculado")):
            return True
        return "perfil" in form_text and "acesso" in form_text

    def _find_company_input(self, company: CompanyRecord, timeout_ms: int = 1800) -> Optional[Any]:
        digits = only_digits(company.document)
        is_cnpj = len(digits) >= 14
        preferred_pairs = []
        if is_cnpj:
            preferred_pairs = [
                ("form#formPJ", "#txtNIPapel2"),
                ("form#formMatriz", "#txtNIPapel3"),
                ("form#formSucessora", "#txtNIPapel4"),
                ("form#formEnteFederativoVinculado", "#txtNIPapel5"),
            ]
        else:
            preferred_pairs = [("form#formPF", "#txtNIPapel1")]

        fallback_selectors = [
            "input[id*='NIPapel' i]",
            "input[name*='NIPapel' i]",
            "input[id*='cnpj' i]",
            "input[name*='cnpj' i]",
            "input[id*='cpf' i]",
            "input[name*='cpf' i]",
            "input[placeholder*='CNPJ' i]",
            "input[placeholder*='CPF' i]",
            "input[placeholder*='empresa' i]",
            "#txtCnpj",
        ]

        deadline = time.time() + max(timeout_ms, 250) / 1000
        while time.time() < deadline:
            for scope in self._profile_scopes():
                for form_selector, field_selector in preferred_pairs:
                    try:
                        form = scope.locator(form_selector).first
                        if form.count() < 1 or not form.is_visible():
                            continue
                        field = form.locator(field_selector).first
                        if field.count() < 1 or not field.is_visible():
                            continue
                        if not self._is_valid_profile_input(field):
                            continue
                        try:
                            field.scroll_into_view_if_needed(timeout=120)
                        except Exception:
                            pass
                        self._last_profile_form = form
                        return field
                    except Exception:
                        continue

            for scope in self._profile_scopes():
                for selector in fallback_selectors:
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
                            if not self._is_valid_profile_input(field):
                                continue
                            try:
                                field.scroll_into_view_if_needed(timeout=120)
                            except Exception:
                                pass
                            try:
                                self._last_profile_form = field.locator("xpath=ancestor::form[1]")
                            except Exception:
                                self._last_profile_form = None
                            return field
                        except Exception:
                            continue
            try:
                self.page.wait_for_timeout(75)
            except Exception:
                time.sleep(0.075)
        return None

    def _profile_async_postback_active(self) -> bool:
        try:
            return bool(
                self.page.evaluate(
                    """() => {
                        try {
                            const prm = window.Sys?.WebForms?.PageRequestManager?.getInstance?.();
                            return !!(prm && prm.get_isInAsyncPostBack && prm.get_isInAsyncPostBack());
                        } catch (error) {
                            return false;
                        }
                    }"""
                )
            )
        except Exception:
            return False

    def _profile_confirm_ready(self) -> bool:
        labels = [
            "Confirmar",
            "OK",
            "Ok",
            "Continuar",
            "Alterar perfil",
            "Selecionar",
        ]
        for scope in self._profile_scopes():
            for label in labels:
                pattern = re.compile(re.escape(label), re.IGNORECASE)
                candidates = [
                    scope.get_by_role("button", name=pattern),
                    scope.get_by_role("link", name=pattern),
                    scope.get_by_text(pattern),
                    scope.locator("button"),
                    scope.locator("input[type='submit'], input[type='button']"),
                ]
                for locator in candidates:
                    try:
                        count = locator.count()
                    except Exception:
                        continue
                    for index in range(min(count, 6)):
                        candidate = locator.nth(index)
                        try:
                            if candidate.is_visible():
                                return True
                        except Exception:
                            continue
        return False

    def _wait_profile_postback_complete(self, company: CompanyRecord, timeout_ms: int = 900) -> bool:
        deadline = time.time() + max(timeout_ms, 250) / 1000
        saw_postback = False
        while time.time() < deadline:
            modal_error = self._extract_profile_modal_error()
            if modal_error:
                self._log_step("postback_detected")
                return True
            if self._profile_async_postback_active():
                saw_postback = True
            elif saw_postback:
                self._log_step("postback_detected")
                return True
            if not self._profile_modal_visible(company):
                self._log_step("postback_detected")
                return True
            if self._profile_confirm_ready():
                if saw_postback:
                    self._log_step("postback_detected")
                return True
            try:
                self.page.wait_for_timeout(75)
            except Exception:
                time.sleep(0.075)
        if saw_postback:
            self._log_step("postback_detected")
        return True

    def _fill_company_search(self, company: CompanyRecord) -> bool:
        digits = only_digits(company.document)
        if not digits:
            return False
        field = self._find_company_input(company, timeout_ms=3000)
        if field is None:
            return False
        field_id = ""
        try:
            field_id = str(field.get_attribute("id") or field.get_attribute("name") or "").strip()
        except Exception:
            field_id = ""
        try:
            field.click(timeout=200)
        except Exception:
            pass
        try:
            field.fill("", timeout=200)
        except Exception:
            try:
                field.evaluate("(el) => { if (el) { el.value = ''; } }")
            except Exception:
                pass
        try:
            field.fill(digits, timeout=300)
        except Exception:
            try:
                field.evaluate(
                    """(el, value) => {
                        if (!el) {
                            return false;
                        }
                        el.focus();
                        el.value = value;
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                        return true;
                    }""",
                    digits,
                )
            except Exception:
                return False
        try:
            current_value = only_digits(field.input_value(timeout=150))
        except Exception:
            current_value = ""
        if current_value != digits:
            try:
                field.evaluate(
                    """(el, value) => {
                        if (!el) {
                            return false;
                        }
                        el.focus();
                        el.value = value;
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                        return true;
                    }""",
                    digits,
                )
            except Exception:
                return False
        try:
            field.dispatch_event("change")
        except Exception:
            pass
        try:
            field.press("Tab", timeout=150)
        except Exception:
            try:
                field.evaluate("(el) => { if (el) { el.blur(); } }")
            except Exception:
                pass
        self._log_step(f"filled_cnpj term='{digits}' field='{field_id or 'unknown'}'")
        self._wait_profile_postback_complete(company, timeout_ms=900)
        return True

    def _select_company_in_profile(self, company: CompanyRecord) -> bool:
        targets = [
            only_digits(company.document),
            _normalize_portal_text(company.name),
        ]
        targets = [item for item in targets if item]
        if not targets:
            return False
        for scope in self._profile_scopes():
            candidates = [
                scope.locator("tr"),
                scope.locator("li"),
                scope.locator("label"),
                scope.locator("input[type='radio'], input[type='checkbox']"),
            ]
            for locator in candidates:
                try:
                    count = locator.count()
                except Exception:
                    continue
                for index in range(min(count, 12)):
                    candidate = locator.nth(index)
                    try:
                        if not candidate.is_visible():
                            continue
                        text = _normalize_portal_text(
                            " ".join((candidate.inner_text(timeout=120) or "").split())
                        )
                    except Exception:
                        text = ""
                    if text and not any(target in text for target in targets):
                        continue
                    try:
                        checked = str(candidate.get_attribute("checked") or "").strip().lower()
                        aria_checked = str(candidate.get_attribute("aria-checked") or "").strip().lower()
                        if checked in {"true", "checked"} or aria_checked == "true":
                            return True
                    except Exception:
                        pass
                    if self._click_locator_candidate(candidate, timeout_ms=250):
                        return True
        return False

    def _confirm_profile_switch(self) -> bool:
        labels = [
            "Alterar",
            "Confirmar",
            "OK",
            "Ok",
            "Continuar",
            "Alterar perfil",
            "Selecionar",
        ]
        scopes = []
        if getattr(self, "_last_profile_form", None) is not None:
            scopes.append(self._last_profile_form)
            fast_candidates = [
                "input[type='submit'][value*='Alterar' i]",
                "input[type='button'][value*='Alterar' i]",
                "button",
                "input[type='submit']",
                "input[type='button']",
            ]
            for selector in fast_candidates:
                locator = self._last_profile_form.locator(selector)
                try:
                    count = locator.count()
                except Exception:
                    continue
                for index in range(min(count, 4)):
                    candidate = locator.nth(index)
                    try:
                        text = _normalize_portal_text(
                            " ".join(
                                (
                                    candidate.get_attribute("value")
                                    or candidate.get_attribute("title")
                                    or candidate.inner_text(timeout=80)
                                    or ""
                                ).split()
                            )
                        )
                    except Exception:
                        text = ""
                    if text and not any(_normalize_portal_text(label) in text for label in labels):
                        continue
                    if self._click_locator_candidate(candidate, timeout_ms=180):
                        self._log_step("clicked_confirm")
                        return True
        scopes.extend(self._profile_scopes())
        for scope in scopes:
            for label in labels:
                pattern = re.compile(re.escape(label), re.IGNORECASE)
                candidates = [
                    scope.get_by_role("button", name=pattern),
                    scope.get_by_role("link", name=pattern),
                    scope.get_by_text(pattern),
                    scope.locator("button"),
                    scope.locator("input[type='submit'], input[type='button']"),
                ]
                for locator in candidates:
                    try:
                        count = locator.count()
                    except Exception:
                        continue
                    for index in range(min(count, 8)):
                        candidate = locator.nth(index)
                        try:
                            text = _normalize_portal_text(
                                " ".join(
                                    (
                                        candidate.inner_text(timeout=120)
                                        or candidate.get_attribute("value")
                                        or candidate.get_attribute("title")
                                        or ""
                                    ).split()
                                )
                            )
                        except Exception:
                            text = ""
                        if text and label and _normalize_portal_text(label) not in text:
                            continue
                        if self._click_locator_candidate(candidate, timeout_ms=180):
                            self._log_step("clicked_confirm")
                            return True
        return False

    def required_mailbox_notice_visible(self) -> bool:
        scopes = self._visible_modal_scopes() or list(self._iter_scopes())
        for scope in scopes:
            text = " ".join(self._body_text(scope).split()).lower()
            if "mensagens importantes" in text and "caixa postal" in text and (
                "nao lidas" in text or "não lidas" in text
            ):
                return True
        return False

    def _click_locator_candidate(self, locator: Any, *, timeout_ms: int = 4000) -> bool:
        deadline = time.time() + max(timeout_ms, 250) / 1000
        while time.time() < deadline:
            try:
                if not locator.is_visible():
                    raise RuntimeError("locator_not_visible")
                try:
                    locator.scroll_into_view_if_needed(timeout=150)
                except Exception:
                    pass
                locator.click(timeout=250, force=True)
                return True
            except Exception:
                pass
            try:
                clicked = bool(
                    locator.evaluate(
                        """el => {
                            if (!el) {
                                return false;
                            }
                            const style = window.getComputedStyle(el);
                            const rect = el.getBoundingClientRect();
                            if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity || '1') <= 0) {
                                return false;
                            }
                            if (rect.width <= 0 || rect.height <= 0) {
                                return false;
                            }
                            el.click();
                            return true;
                        }"""
                    )
                )
                if clicked:
                    return True
            except Exception:
                pass
            try:
                self.page.wait_for_timeout(75)
            except Exception:
                time.sleep(0.075)
        return False

    def _open_required_mailbox_from_notice(self) -> bool:
        labels = ["Ir para a Caixa Postal", "Caixa Postal"]
        for scope in self._visible_modal_scopes() or [self.page]:
            for label in labels:
                pattern = re.compile(re.escape(label), re.IGNORECASE)
                candidates = [
                    scope.get_by_role("button", name=pattern),
                    scope.get_by_role("link", name=pattern),
                    scope.get_by_text(pattern),
                    scope.locator(".ui-dialog-buttonpane button"),
                    scope.locator(".ui-dialog-buttonset button"),
                    scope.locator("button"),
                    scope.locator("a"),
                ]
                for locator in candidates:
                    try:
                        count = locator.count()
                    except Exception:
                        continue
                    for index in range(min(count, 6)):
                        candidate = locator.nth(index)
                        try:
                            text = _normalize_portal_text(" ".join((candidate.inner_text(timeout=700) or "").split()))
                        except Exception:
                            text = ""
                        if "caixa postal" not in text:
                            continue
                        if self._click_locator_candidate(candidate, timeout_ms=4000):
                            return True
        try:
            clicked = bool(
                self.page.evaluate(
                    """labels => {
                        const normalize = (value) =>
                            String(value || '')
                                .normalize('NFD')
                                .replace(/[\u0300-\u036f]/g, '')
                                .replace(/\\s+/g, ' ')
                                .trim()
                                .toLowerCase();
                        const wanted = (labels || []).map(normalize);
                        const dialogs = Array.from(document.querySelectorAll('.ui-dialog, [role="dialog"], dialog, .modal.show'));
                        const roots = dialogs.length ? dialogs : [document.body];
                        const isVisible = (el) => {
                            if (!el) return false;
                            const style = window.getComputedStyle(el);
                            const rect = el.getBoundingClientRect();
                            return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
                        };
                        for (const root of roots) {
                            const buttons = Array.from(root.querySelectorAll('button, a, input[type="button"], input[type="submit"]'));
                            for (const button of buttons) {
                                const text = normalize(button.innerText || button.value || button.getAttribute('aria-label') || '');
                                if (!isVisible(button) || !text) continue;
                                if (!wanted.some((label) => text.includes(label) || text.includes('caixa postal'))) continue;
                                button.click();
                                return true;
                            }
                        }
                        return false;
                    }""",
                    labels,
                )
            )
            if clicked:
                return True
        except Exception:
            pass
        try:
            self.page.goto(
                "https://cav.receita.fazenda.gov.br/eCAC/Aplicacao.aspx?id=00006",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            return True
        except Exception:
            return False

    def _apply_unread_mailbox_filter(self) -> bool:
        labels = ["Não lidas", "Nao lidas", "Não visualizadas", "Nao visualizadas"]
        for scope in self._iter_scopes():
            for label in labels:
                pattern = re.compile(re.escape(label), re.IGNORECASE)
                interactive_locators = [
                    scope.get_by_role("radio", name=pattern),
                    scope.get_by_role("checkbox", name=pattern),
                    scope.get_by_role("tab", name=pattern),
                    scope.get_by_role("button", name=pattern),
                    scope.get_by_role("link", name=pattern),
                    scope.get_by_text(pattern),
                ]
                for locator in interactive_locators:
                    try:
                        if locator.count() < 1:
                            continue
                        candidate = locator.first
                        try:
                            if not candidate.is_visible():
                                continue
                        except Exception:
                            pass
                        for attr_name in ("aria-checked", "aria-selected", "aria-pressed", "checked"):
                            try:
                                state = str(candidate.get_attribute(attr_name) or "").strip().lower()
                            except Exception:
                                state = ""
                            if state in {"true", "checked"}:
                                return True
                        if self._click_locator_candidate(candidate, timeout_ms=2500):
                            try:
                                self.page.wait_for_load_state("networkidle", timeout=4000)
                            except Exception:
                                pass
                            try:
                                self.page.wait_for_timeout(500)
                            except Exception:
                                time.sleep(0.5)
                            state_confirmed = False
                            for attr_name in ("aria-checked", "aria-selected", "aria-pressed", "checked"):
                                try:
                                    state = str(candidate.get_attribute(attr_name) or "").strip().lower()
                                except Exception:
                                    state = ""
                                if state in {"true", "checked"}:
                                    state_confirmed = True
                                    break
                            if state_confirmed:
                                return True
                    except Exception:
                        continue
            try:
                selects = scope.locator("select")
                select_count = selects.count()
            except Exception:
                select_count = 0
            for index in range(min(select_count, 4)):
                select = selects.nth(index)
                try:
                    options = select.locator("option")
                    option_count = options.count()
                except Exception:
                    option_count = 0
                for option_index in range(min(option_count, 12)):
                    option = options.nth(option_index)
                    try:
                        text = " ".join((option.inner_text(timeout=500) or "").split())
                    except Exception:
                        continue
                    if not any(_normalize_portal_text(label) == _normalize_portal_text(text) for label in labels):
                        continue
                    try:
                        value = str(option.get_attribute("value") or "").strip()
                    except Exception:
                        value = ""
                    try:
                        if value:
                            select.select_option(value=value, timeout=2500)
                        else:
                            select.select_option(label=text, timeout=2500)
                        try:
                            self.page.wait_for_load_state("networkidle", timeout=4000)
                        except Exception:
                            pass
                        return True
                    except Exception:
                        continue
        return False

    def required_mailbox_notice_visible(self) -> bool:
        scopes = self._visible_modal_scopes() or list(self._iter_scopes())
        for scope in scopes:
            text = _normalize_portal_text(" ".join(self._body_text(scope).split()))
            if "mensagens importantes" in text and "caixa postal" in text and "nao lidas" in text:
                return True
        return False

    def _mailbox_row_requires_read(self, text: str) -> bool:
        normalized = " ".join(str(text or "").split())
        lowered = _normalize_portal_text(normalized)
        if not lowered:
            return False
        if "assunto" in lowered and "data" in lowered and len(lowered) < 120:
            return False
        unread_tokens = ["nao lida", "nao lido", "novo"]
        important_tokens = ["importante", "prioridade", "urgente"]
        return "!" in normalized or any(token in lowered for token in unread_tokens + important_tokens)

    def _mailbox_row_requires_read(self, text: str) -> bool:
        normalized = " ".join(str(text or "").split())
        lowered = normalized.lower()
        if not lowered:
            return False
        if "assunto" in lowered and "data" in lowered and len(lowered) < 120:
            return False
        unread_tokens = ["nao lida", "não lida", "nao lido", "não lido", "novo"]
        important_tokens = ["importante", "prioridade", "urgente"]
        return "!" in normalized or any(token in lowered for token in unread_tokens + important_tokens)

    def _mailbox_row_requires_read(self, text: str) -> bool:
        normalized = " ".join(str(text or "").split())
        lowered = _normalize_portal_text(normalized)
        if not lowered:
            return False
        if "assunto" in lowered and "data" in lowered and len(lowered) < 120:
            return False
        unread_tokens = ["nao lida", "nao lido", "novo"]
        important_tokens = ["importante", "prioridade", "urgente"]
        return "!" in normalized or any(token in lowered for token in unread_tokens + important_tokens)

    def mark_required_mailbox_messages_as_read(self, limit: int = 30) -> int:
        opened = 0
        seen_rows: set[str] = set()
        locator, _, _ = self._message_rows_locator()

        if locator is None and self._is_message_detail_view():
            opened += 1
            try:
                self.page.wait_for_timeout(1000)
            except Exception:
                time.sleep(1.0)
            self._return_to_message_list()
            locator, _, _ = self._message_rows_locator()

        filter_applied = self._apply_unread_mailbox_filter()
        page_cycles = 0
        while locator is not None and opened < limit and page_cycles < 8:
            try:
                row_count = locator.count()
            except Exception:
                row_count = 0
            if row_count <= 0:
                break

            target_index: Optional[int] = None
            fallback_index: Optional[int] = None
            target_text = ""
            for index in range(row_count):
                row = locator.nth(index)
                try:
                    if not row.is_visible():
                        continue
                    cells = row.locator("td, th, [role='cell'], [role='gridcell']").all_inner_texts()
                    raw_text = "\n".join(item.strip() for item in cells if item.strip()) or row.inner_text(timeout=1500)
                    raw_text = " ".join(raw_text.split())
                except Exception:
                    continue
                if not raw_text or raw_text in seen_rows:
                    continue
                if self._mailbox_row_requires_read(raw_text):
                    target_index = index
                    target_text = raw_text
                    break
                if not filter_applied and fallback_index is None and page_cycles == 0:
                    fallback_index = index
                    target_text = raw_text

            if target_index is None and fallback_index is not None:
                target_index = fallback_index

            if target_index is None:
                if not self._click_next_page():
                    break
                page_cycles += 1
                try:
                    self.page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass
                if filter_applied:
                    self._apply_unread_mailbox_filter()
                locator, _, _ = self._message_rows_locator()
                continue

            seen_rows.add(target_text)
            locator_missing = False
            try:
                row = locator.nth(target_index)
                detail_text = self._open_row_detail(row)
                if detail_text or self._is_message_detail_view():
                    opened += 1
                    try:
                        self.page.wait_for_timeout(1200)
                    except Exception:
                        time.sleep(1.2)
            finally:
                self.close_blocking_modals()
                self._return_to_message_list()
                if filter_applied:
                    self._apply_unread_mailbox_filter()
                locator, _, _ = self._message_rows_locator()
                locator_missing = locator is None
            if locator_missing:
                break

        return opened

    def resolve_required_mailbox_notice(self) -> bool:
        if not self.required_mailbox_notice_visible():
            return False
        self.runtime.logger.info("Modal de leitura obrigatoria da Caixa Postal detectado.")
        if not self._open_required_mailbox_from_notice():
            raise RuntimeError("O modal de leitura obrigatoria apareceu, mas o acesso para a Caixa Postal nao foi localizado.")
        ready = self._wait_until(
            lambda: self._message_rows_locator()[0] is not None or self._is_message_detail_view(),
            timeout_ms=20000,
            interval_ms=400,
        )
        if not ready:
            raise RuntimeError("A Caixa Postal nao ficou disponivel apos o modal de leitura obrigatoria.")
        opened = self.mark_required_mailbox_messages_as_read(limit=30)
        self.runtime.logger.info(f"Leitura obrigatoria da Caixa Postal concluida; {opened} mensagem(ns) aberta(s).")
        try:
            self.page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
        except Exception:
            pass
        self.close_blocking_modals()
        return True

    def _open_required_mailbox_from_notice(self) -> bool:
        labels = ["Ir para a Caixa Postal", "Caixa Postal"]
        for scope in self._visible_modal_scopes() or [self.page]:
            for label in labels:
                pattern = re.compile(re.escape(label), re.IGNORECASE)
                candidates = [
                    scope.get_by_role("button", name=pattern),
                    scope.get_by_role("link", name=pattern),
                    scope.get_by_text(pattern),
                    scope.locator(".ui-dialog-buttonpane button"),
                    scope.locator(".ui-dialog-buttonset button"),
                    scope.locator("button"),
                    scope.locator("a"),
                ]
                for locator in candidates:
                    try:
                        count = locator.count()
                    except Exception:
                        continue
                    for index in range(min(count, 8)):
                        candidate = locator.nth(index)
                        try:
                            text = _normalize_portal_text(" ".join((candidate.inner_text(timeout=700) or "").split()))
                        except Exception:
                            text = ""
                        if "caixa postal" not in text:
                            continue
                        if self._click_locator_candidate(candidate, timeout_ms=4000):
                            return True
        try:
            clicked = bool(
                self.page.evaluate(
                    """labels => {
                        const normalize = (value) =>
                            String(value || '')
                                .normalize('NFD')
                                .replace(/[\\u0300-\\u036f]/g, '')
                                .replace(/\\s+/g, ' ')
                                .trim()
                                .toLowerCase();
                        const wanted = (labels || []).map(normalize);
                        const dialogs = Array.from(document.querySelectorAll('.ui-dialog, [role="dialog"], dialog, .modal.show'));
                        const roots = dialogs.length ? dialogs : [document.body];
                        const isVisible = (el) => {
                            if (!el) return false;
                            const style = window.getComputedStyle(el);
                            const rect = el.getBoundingClientRect();
                            return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
                        };
                        for (const root of roots) {
                            const buttons = Array.from(root.querySelectorAll('button, a, input[type="button"], input[type="submit"]'));
                            for (const button of buttons) {
                                const text = normalize(button.innerText || button.value || button.getAttribute('aria-label') || '');
                                if (!isVisible(button) || !text) continue;
                                if (!wanted.some((label) => text.includes(label) || text.includes('caixa postal'))) continue;
                                button.click();
                                return true;
                            }
                        }
                        return false;
                    }""",
                    labels,
                )
            )
            if clicked:
                return True
        except Exception:
            pass
        try:
            self.page.goto(
                "https://cav.receita.fazenda.gov.br/eCAC/Aplicacao.aspx?id=00006",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            return True
        except Exception:
            return False

    def _apply_unread_mailbox_filter(self) -> bool:
        labels = ["Nao lidas", "Não lidas", "Nao visualizadas", "Não visualizadas"]
        normalized_labels = {_normalize_portal_text(label) for label in labels}

        def _is_selected(candidate: Any) -> bool:
            for attr_name in ("aria-checked", "aria-selected", "aria-pressed", "checked"):
                try:
                    state = str(candidate.get_attribute(attr_name) or "").strip().lower()
                except Exception:
                    state = ""
                if state in {"true", "checked"}:
                    return True
            try:
                class_name = str(candidate.get_attribute("class") or "").strip().lower()
            except Exception:
                class_name = ""
            return any(token in class_name for token in ("active", "selected", "checked", "ui-state-active"))

        for scope in self._iter_scopes():
            for label in labels:
                pattern = re.compile(re.escape(label), re.IGNORECASE)
                interactive_locators = [
                    scope.get_by_role("radio", name=pattern),
                    scope.get_by_role("checkbox", name=pattern),
                    scope.get_by_role("tab", name=pattern),
                    scope.get_by_role("button", name=pattern),
                    scope.get_by_role("link", name=pattern),
                    scope.get_by_text(pattern),
                ]
                for locator in interactive_locators:
                    try:
                        if locator.count() < 1:
                            continue
                        candidate = locator.first
                        if _is_selected(candidate):
                            return True
                        if self._click_locator_candidate(candidate, timeout_ms=2500):
                            try:
                                self.page.wait_for_load_state("networkidle", timeout=4000)
                            except Exception:
                                pass
                            try:
                                self.page.wait_for_timeout(700)
                            except Exception:
                                time.sleep(0.7)
                            return True
                    except Exception:
                        continue
            try:
                selects = scope.locator("select")
                select_count = selects.count()
            except Exception:
                select_count = 0
            for index in range(min(select_count, 4)):
                select = selects.nth(index)
                try:
                    options = select.locator("option")
                    option_count = options.count()
                except Exception:
                    option_count = 0
                for option_index in range(min(option_count, 12)):
                    option = options.nth(option_index)
                    try:
                        text = " ".join((option.inner_text(timeout=500) or "").split())
                    except Exception:
                        continue
                    if _normalize_portal_text(text) not in normalized_labels:
                        continue
                    try:
                        value = str(option.get_attribute("value") or "").strip()
                    except Exception:
                        value = ""
                    try:
                        if value:
                            select.select_option(value=value, timeout=2500)
                        else:
                            select.select_option(label=text, timeout=2500)
                        try:
                            self.page.wait_for_load_state("networkidle", timeout=4000)
                        except Exception:
                            pass
                        return True
                    except Exception:
                        continue
        try:
            clicked = bool(
                self.page.evaluate(
                    """labels => {
                        const normalize = (value) =>
                            String(value || '')
                                .normalize('NFD')
                                .replace(/[\\u0300-\\u036f]/g, '')
                                .replace(/\\s+/g, ' ')
                                .trim()
                                .toLowerCase();
                        const wanted = new Set((labels || []).map(normalize));
                        const isVisible = (el) => {
                            if (!el) return false;
                            const style = window.getComputedStyle(el);
                            const rect = el.getBoundingClientRect();
                            return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
                        };
                        const textFrom = (el) => normalize(
                            el?.innerText ||
                            el?.textContent ||
                            el?.value ||
                            el?.getAttribute?.('aria-label') ||
                            el?.getAttribute?.('title') ||
                            ''
                        );
                        const candidates = Array.from(document.querySelectorAll('button, a, label, span, div, li, input, option'));
                        for (const element of candidates) {
                            if (!isVisible(element)) continue;
                            const text = textFrom(element);
                            if (!text || !wanted.has(text)) continue;
                            if (element.tagName === 'OPTION' && element.parentElement?.tagName === 'SELECT') {
                                element.parentElement.value = element.value;
                                element.parentElement.dispatchEvent(new Event('change', { bubbles: true }));
                                return true;
                            }
                            if (element.tagName === 'LABEL') {
                                element.click();
                                element.dispatchEvent(new Event('change', { bubbles: true }));
                                const clickedText = textFrom(element);
                                if (wanted.has(clickedText)) {
                                    return true;
                                }
                                const htmlFor = element.getAttribute('for');
                                if (htmlFor) {
                                    const target = document.getElementById(htmlFor);
                                    if (target) {
                                        target.click();
                                        target.dispatchEvent(new Event('change', { bubbles: true }));
                                        return true;
                                    }
                                }
                            }
                            element.click();
                            element.dispatchEvent(new Event('change', { bubbles: true }));
                            return true;
                        }
                        return false;
                    }""",
                    list(normalized_labels),
                )
            )
            if clicked:
                try:
                    self.page.wait_for_load_state("networkidle", timeout=4000)
                except Exception:
                    pass
                try:
                    self.page.wait_for_timeout(700)
                except Exception:
                    time.sleep(0.7)
                return True
        except Exception:
            pass
        return False

    def required_mailbox_notice_visible(self) -> bool:
        scopes = self._visible_modal_scopes() or list(self._iter_scopes())
        for scope in scopes:
            text = _normalize_portal_text(" ".join(self._body_text(scope).split()))
            if "mensagens importantes" in text and "caixa postal" in text:
                return True
        try:
            clicked_label_exists = self.page.get_by_text(re.compile("Ir para a Caixa Postal", re.IGNORECASE)).count() > 0
        except Exception:
            clicked_label_exists = False
        page_text = _normalize_portal_text(" ".join(self._body_text().split()))
        return clicked_label_exists and "caixa postal" in page_text and "mensagens importantes" in page_text

    def _mailbox_row_requires_read(self, text: str) -> bool:
        normalized = " ".join(str(text or "").split())
        lowered = _normalize_portal_text(normalized)
        if not lowered:
            return False
        if "assunto" in lowered and "data" in lowered and len(lowered) < 120:
            return False
        unread_tokens = ["nao lida", "nao lido", "novo"]
        important_tokens = ["importante", "prioridade", "urgente"]
        return "!" in normalized or any(token in lowered for token in unread_tokens + important_tokens)

    def mark_required_mailbox_messages_as_read(self, limit: int = 30) -> int:
        def _visible_rows_locator() -> Optional[Any]:
            locator, _, _ = self._message_rows_locator()
            if locator is None:
                return None
            try:
                count = locator.count()
            except Exception:
                return None
            for index in range(min(count, 8)):
                try:
                    if locator.nth(index).is_visible():
                        return locator
                except Exception:
                    continue
            return None

        opened = 0
        seen_rows: set[str] = set()

        if self._is_message_detail_view():
            try:
                self.page.wait_for_timeout(1000)
            except Exception:
                time.sleep(1.0)
            self._return_to_message_list()

        filter_applied = self._apply_unread_mailbox_filter()
        if not filter_applied:
            self.runtime.logger.warning("Nao foi possivel aplicar o filtro de mensagens nao lidas na Caixa Postal.")
            return 0

        locator = _visible_rows_locator()
        page_cycles = 0
        while locator is not None and opened < limit and page_cycles < 8:
            try:
                row_count = locator.count()
            except Exception:
                row_count = 0
            if row_count <= 0:
                break

            target_index: Optional[int] = None
            target_text = ""
            for index in range(row_count):
                row = locator.nth(index)
                try:
                    if not row.is_visible():
                        continue
                    cells = row.locator("td, th, [role='cell'], [role='gridcell']").all_inner_texts()
                    raw_text = "\n".join(item.strip() for item in cells if item.strip()) or row.inner_text(timeout=1500)
                    raw_text = " ".join(raw_text.split())
                except Exception:
                    continue
                if not raw_text or raw_text in seen_rows:
                    continue
                normalized_text = _normalize_portal_text(raw_text)
                if "assunto" in normalized_text and "data" in normalized_text and len(normalized_text) < 120:
                    continue
                if "nenhum registro" in normalized_text or "nenhuma mensagem" in normalized_text:
                    continue
                if self._mailbox_row_requires_read(raw_text) or filter_applied:
                    target_index = index
                    target_text = raw_text
                    break

            if target_index is None:
                if not self._click_next_page():
                    break
                page_cycles += 1
                try:
                    self.page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass
                if filter_applied:
                    self._apply_unread_mailbox_filter()
                locator = _visible_rows_locator()
                continue

            seen_rows.add(target_text)
            locator_missing = False
            try:
                row = locator.nth(target_index)
                detail_text = self._open_row_detail(row)
                if detail_text or self._is_message_detail_view():
                    opened += 1
                    try:
                        self.page.wait_for_timeout(1200)
                    except Exception:
                        time.sleep(1.2)
            finally:
                self.close_blocking_modals()
                if self._is_message_detail_view() or _visible_rows_locator() is None:
                    try:
                        self._click_by_label(ecac_base.BACK_TO_LIST_LABELS, timeout_ms=3000)
                    except Exception:
                        pass
                self._return_to_message_list()
                if filter_applied:
                    self._apply_unread_mailbox_filter()
                locator = _visible_rows_locator()
                locator_missing = locator is None
            if locator_missing:
                break

        return opened

    def resolve_required_mailbox_notice(self) -> bool:
        if not self.required_mailbox_notice_visible():
            return False
        self.runtime.logger.info("Modal de leitura obrigatoria da Caixa Postal detectado.")
        if not self._open_required_mailbox_from_notice():
            raise RuntimeError("O modal de leitura obrigatoria apareceu, mas o acesso para a Caixa Postal nao foi localizado.")
        ready = self._wait_until(
            lambda: self._message_rows_locator()[0] is not None or self._is_message_detail_view(),
            timeout_ms=20000,
            interval_ms=400,
        )
        if not ready:
            raise RuntimeError("A Caixa Postal nao ficou disponivel apos o modal de leitura obrigatoria.")
        opened = self.mark_required_mailbox_messages_as_read(limit=30)
        if opened <= 0 and self.required_mailbox_notice_visible():
            raise RuntimeError("Nao foi possivel concluir a leitura das mensagens nao lidas da Caixa Postal.")
        self.runtime.logger.info(f"Leitura obrigatoria da Caixa Postal concluida; {opened} mensagem(ns) aberta(s).")
        try:
            self.page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
        except Exception:
            pass
        self.close_blocking_modals()
        return True

    def is_authenticated(self) -> bool:
        if super().is_authenticated():
            return True
        html = ""
        try:
            html = self.page.content()
        except Exception:
            pass
        return "Titular (Acesso GOV.BR por Certificado)" in html or "Responsavel Legal" in html

    def _current_profile_context(self) -> str:
        try:
            locator = self.page.locator("#informacao-perfil span")
            count = locator.count()
        except Exception:
            count = 0
        if count > 0:
            snippets: list[str] = []
            for index in range(min(count, 6)):
                try:
                    text = " ".join(locator.nth(index).inner_text(timeout=1200).split())
                except Exception:
                    continue
                if text:
                    snippets.append(text)
            if snippets:
                return " | ".join(snippets)
        context = super()._current_profile_context()
        if context:
            return context
        try:
            html = self.page.content()
        except Exception:
            return ""
        snippets: list[str] = []
        for marker in (
            "Titular (Acesso GOV.BR por Certificado)",
            "Responsavel Legal",
            "Responsável Legal",
        ):
            match = re.search(rf"{re.escape(marker)}.*?</span>", html, flags=re.IGNORECASE | re.DOTALL)
            if match:
                clean = re.sub(r"<[^>]+>", " ", match.group(0))
                snippets.append(" ".join(clean.split()))
        return " | ".join(snippets)

    def certificate_context_matches(self) -> bool:
        if not self.expected_document:
            return True
        html = ""
        try:
            html = self.page.content()
        except Exception:
            return False
        return self.expected_document in only_digits(html)

    def _find_frame_by_url_tokens(self, *tokens: str):
        wanted = [str(token or "").strip().lower() for token in tokens if str(token or "").strip()]
        if not wanted:
            return None
        for frame in self.page.frames:
            url = str(getattr(frame, "url", "") or "").lower()
            if any(token in url for token in wanted):
                return frame
        return None

    def _hcaptcha_visible_in_dom(self) -> bool:
        try:
            return bool(
                self.page.locator("body").evaluate(
                    """() => {
                        const nodes = Array.from(
                            document.querySelectorAll(
                                "iframe[src*='hcaptcha'], iframe[title*='hCaptcha'], .h-captcha, [data-sitekey][data-size]"
                            )
                        );
                        return nodes.some((node) => {
                            const rect = node.getBoundingClientRect();
                            const style = window.getComputedStyle(node);
                            if (!style || style.display === "none" || style.visibility === "hidden") {
                                return false;
                            }
                            if (Number(style.opacity || "1") <= 0) {
                                return false;
                            }
                            if (rect.width < 100 || rect.height < 50) {
                                return false;
                            }
                            return rect.bottom > 0 &&
                                rect.right > 0 &&
                                rect.top < (window.innerHeight || document.documentElement.clientHeight) &&
                                rect.left < (window.innerWidth || document.documentElement.clientWidth);
                        });
                    }"""
                )
            )
        except Exception:
            return False

    def captcha_visible(self) -> bool:
        try:
            if HCAPTCHA_ERROR_URL_TOKEN.lower() in self.page.url.lower():
                return True
        except Exception:
            pass
        if self._hcaptcha_visible_in_dom():
            return True
        markers = [
            "Por favor, tente novamente.",
            "Verificar",
            "Desafio hCaptcha",
            "hCaptcha",
        ]
        for scope in [self.page]:
            text = self._body_text(scope)
            lowered = _normalize_portal_text(text)
            if any(_normalize_portal_text(marker) in lowered for marker in markers):
                return True
        return False

    def _reload_after_captcha(self, stage_label: str) -> None:
        self.runtime.logger.warning(
            f"hCaptcha detectado na etapa {stage_label}; recarregando a pagina para nova tentativa."
        )
        self._bring_browser_to_front()
        try:
            self.page.reload(wait_until="domcontentloaded", timeout=30000)
        except Exception:
            current_url = str(self.page.url or "").strip()
            if not current_url:
                raise
            self.page.goto(current_url, wait_until="domcontentloaded", timeout=30000)

    def _wait_for_solver_extension(self, stage_label: str, timeout_ms: int = LOGIN_HCAPTCHA_EXTENSION_WAIT_TIMEOUT_MS) -> None:
        if not self.captcha_solver_available:
            raise LoginBrowserResetRequiredError(
                f"hCaptcha persistiu na etapa {stage_label} e a extensao {CAPTCHA_SOLVER_EXTENSION_NAME} nao esta disponivel no perfil Chrome."
            )
        self.runtime.logger.warning(
            f"hCaptcha persistiu na etapa {stage_label}; aguardando a extensao resolver por ate {int(timeout_ms / 1000)}s."
        )
        deadline = time.time() + max(timeout_ms, 1000) / 1000
        while time.time() < deadline:
            if self.stop_requested():
                raise RuntimeError("Execucao interrompida enquanto aguardava a extensao resolver o hCaptcha.")
            if not self.captcha_visible():
                self.runtime.logger.info(f"hCaptcha liberado na etapa {stage_label}; fluxo retomado.")
                return
            try:
                self.page.wait_for_timeout(250)
            except Exception:
                time.sleep(0.25)
        raise LoginBrowserResetRequiredError(
            f"A extensao nao resolveu o hCaptcha na etapa {stage_label} dentro de {int(timeout_ms / 1000)}s."
        )

    def _advance_to_gov_br_with_retry_budget(self) -> None:
        last_error: Optional[Exception] = None
        if self._on_gov_br_identity_page() and not self.captcha_visible():
            self.runtime.logger.info("Tela do GOV.BR ja estava aberta; pulando clique em 'Acesso Gov BR'.")
            return
        for attempt in range(1, LOGIN_HCAPTCHA_RELOAD_LIMIT + 2):
            if self._on_gov_br_identity_page() and not self.captcha_visible():
                self.runtime.logger.info("Tela do GOV.BR detectada durante a tentativa; seguindo para o certificado.")
                return
            self._click_access_gov_br()
            self._wait_until(
                lambda: "acesso.gov.br" in str(self.page.url or "").lower()
                or self._page_contains("Seu certificado digital")
                or self.captcha_visible(),
                timeout_ms=30000,
                interval_ms=75,
            )
            current_url = str(self.page.url or "").lower()
            if "acesso.gov.br" in current_url or self._page_contains("Seu certificado digital"):
                if not self.captcha_visible():
                    return
                last_error = HCaptchaVisibleError("hCaptcha exibido ao tentar acessar o GOV.BR.")
            elif self.captcha_visible():
                last_error = HCaptchaVisibleError("hCaptcha exibido ao tentar acessar o GOV.BR.")
            else:
                last_error = RuntimeError("A navegacao para o GOV.BR nao ficou pronta apos o clique.")

            if isinstance(last_error, HCaptchaVisibleError):
                if attempt <= LOGIN_HCAPTCHA_RELOAD_LIMIT:
                    self.runtime.logger.warning(
                        f"hcaptcha_reload stage=gov_br attempt={attempt}/{LOGIN_HCAPTCHA_RELOAD_LIMIT}"
                    )
                    self._reload_after_captcha("gov_br")
                    continue
                self._wait_for_solver_extension("gov_br")
                current_url = str(self.page.url or "").lower()
                if "acesso.gov.br" in current_url or self._page_contains("Seu certificado digital"):
                    return
                continue
            raise last_error
        raise RuntimeError(str(last_error or "Falha ao avancar para o GOV.BR."))

    def handle_captcha_if_present(self) -> str:
        if not self.captcha_visible():
            return "not_present"
        self.runtime.logger.warning("hCaptcha detectado durante o fluxo do e-CAC.")
        if self.headless or not self.allow_manual_captcha:
            raise HCaptchaVisibleError("hCaptcha visivel sem suporte a resolucao manual neste modo de execucao.")
        deadline = time.time() + (self.manual_captcha_timeout_ms / 1000)
        while time.time() < deadline:
            if self.stop_requested():
                raise RuntimeError("Execucao interrompida enquanto aguardava resolucao manual do hCaptcha.")
            if self.is_authenticated() and not self.captcha_visible():
                self.runtime.logger.info("hCaptcha nao esta mais visivel; fluxo retomado.")
                return "resolved"
            try:
                self.page.wait_for_timeout(250)
            except Exception:
                time.sleep(0.25)
        raise HCaptchaVisibleError("O hCaptcha permaneceu visivel apos o timeout configurado.")

    def _click_cert_login_fast(self) -> bool:
        self._bring_browser_to_front()
        patterns = [
            re.compile(r"^\s*Seu certificado digital\s*$", re.IGNORECASE),
            re.compile(r"^\s*Certificado digital\s*$", re.IGNORECASE),
        ]
        for pattern in patterns:
            candidates = [
                self.page.get_by_role("button", name=pattern),
                self.page.get_by_role("link", name=pattern),
                self.page.get_by_text(pattern),
            ]
            for locator in candidates:
                try:
                    count = locator.count()
                except Exception:
                    continue
                for index in range(min(count, 6)):
                    candidate = locator.nth(index)
                    if self._click_locator_candidate(candidate, timeout_ms=2000):
                        self.runtime.logger.info("Login por certificado acionado.")
                        return True
        try:
            self._bring_browser_to_front()
            clicked = bool(
                self.page.evaluate(
                    """() => {
                        const normalize = (value) =>
                            String(value || '')
                                .normalize('NFD')
                                .replace(/[\u0300-\u036f]/g, '')
                                .replace(/\\s+/g, ' ')
                                .trim()
                                .toLowerCase();
                        const wanted = new Set(['seu certificado digital', 'certificado digital']);
                        const nodes = Array.from(document.querySelectorAll('button, a, [role="button"], [role="link"], div, span'));
                        const isVisible = (el) => {
                            if (!el) return false;
                            const style = window.getComputedStyle(el);
                            const rect = el.getBoundingClientRect();
                            return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
                        };
                        for (const node of nodes) {
                            if (!isVisible(node)) continue;
                            const text = normalize(node.innerText || node.textContent || node.getAttribute('aria-label') || '');
                            if (!wanted.has(text)) continue;
                            node.click();
                            return true;
                        }
                        return false;
                    }"""
                )
            )
            if clicked:
                self.runtime.logger.info("Login por certificado acionado via fallback DOM.")
                return True
        except Exception:
            pass
        return False

    def _wait_for_cert_login_ready(self, timeout_ms: int = 6000) -> bool:
        text_ready = self._wait_until(
            lambda: self._page_contains("Seu certificado digital") or self._page_contains("Certificado digital"),
            timeout_ms=timeout_ms,
            interval_ms=75,
        )
        if not text_ready:
            return False
        if self._click_cert_login_fast():
            return True
        return self._wait_until(
            lambda: self._click_cert_login_fast(),
            timeout_ms=1500,
            interval_ms=75,
        )

    def _refresh_certificate_login_page(self) -> None:
        self.runtime.logger.warning("Janela de certificado nao apareceu; atualizando a pagina do GOV.BR para tentar novamente.")
        self._bring_browser_to_front()
        try:
            self.page.reload(wait_until="domcontentloaded", timeout=30000)
        except Exception:
            current_url = str(self.page.url or "").strip()
            if current_url:
                self.page.goto(current_url, wait_until="domcontentloaded", timeout=30000)
            else:
                raise
        self._wait_until(
            lambda: self._page_contains("Seu certificado digital") or self._page_contains("Certificado digital"),
            timeout_ms=10000,
            interval_ms=75,
        )

    def _open_certificate_dialog_with_refresh(self, timeout_ms: int = 25000) -> None:
        deadline = time.time() + max(timeout_ms, 5000) / 1000
        attempt = 0
        captcha_attempts = 0
        last_error: Optional[Exception] = None
        while time.time() < deadline:
            attempt += 1
            if not self._wait_for_cert_login_ready(timeout_ms=3500):
                last_error = RuntimeError("A opcao de login por certificado nao ficou disponivel.")
            else:
                try:
                    self._bring_browser_to_front()
                    try:
                        self.page.wait_for_timeout(150)
                    except Exception:
                        time.sleep(0.15)
                    if self.captcha_visible():
                        raise HCaptchaVisibleError("hCaptcha exibido no lugar da janela do certificado.")
                    select_certificate_dialog_strong(self.certificate, timeout_ms=2500)
                    return
                except Exception as exc:
                    last_error = exc
                    if self.captcha_visible():
                        last_error = HCaptchaVisibleError("hCaptcha exibido no lugar da janela do certificado.")
                    message = str(exc or "")
                    if (
                        "Janela de selecao de certificado nao apareceu" not in message
                        and "hCaptcha exibido no lugar da janela do certificado" not in message
                    ):
                        raise
            if time.time() >= deadline:
                break
            if isinstance(last_error, HCaptchaVisibleError):
                if captcha_attempts < LOGIN_HCAPTCHA_RELOAD_LIMIT:
                    captcha_attempts += 1
                    self.runtime.logger.warning(
                        f"hcaptcha_reload stage=certificado_digital attempt={captcha_attempts}/{LOGIN_HCAPTCHA_RELOAD_LIMIT}"
                    )
                    self._refresh_certificate_login_page()
                    continue
                self._wait_for_solver_extension("certificado_digital")
                continue
            self._refresh_certificate_login_page()
        raise RuntimeError(str(last_error or "A janela de selecao do certificado nao apareceu apos atualizar a pagina."))

    def login(self) -> None:
        if self.is_authenticated() and self.certificate_context_matches():
            self.resolve_required_mailbox_notice()
            self.close_blocking_modals()
            return
        last_error: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                self._goto_login_with_retry()
                if self.is_authenticated() and self.certificate_context_matches():
                    self.resolve_required_mailbox_notice()
                    self.close_blocking_modals()
                    return
                self._advance_to_gov_br_with_retry_budget()
                self._open_certificate_dialog_with_refresh()
                deadline = time.time() + 120
                while time.time() < deadline:
                    if self.is_authenticated():
                        if not self.certificate_context_matches():
                            raise RuntimeError(
                                "Sessao autenticada com contexto diferente do certificado alvo."
                            )
                        self.resolve_required_mailbox_notice()
                        self.close_blocking_modals()
                        return
                    if self.captcha_visible():
                        self._wait_for_solver_extension("certificado_digital")
                    if HCAPTCHA_ERROR_URL_TOKEN.lower() in self.page.url.lower():
                        self._wait_for_solver_extension("certificado_digital")
                    try:
                        self.page.wait_for_timeout(250)
                    except Exception:
                        time.sleep(0.25)
                raise RuntimeError("O e-CAC nao confirmou a sessao autenticada dentro do timeout.")
            except Exception as exc:
                last_error = exc
                self.runtime.logger.warning(f"login_failed attempt={attempt}: {exc}")
                self._capture_debug(f"login_failed_attempt_{attempt}")
                if isinstance(exc, LoginBrowserResetRequiredError):
                    raise
                try:
                    self.page.goto(
                        "https://cav.receita.fazenda.gov.br/autenticacao/Login/Logout",
                        wait_until="domcontentloaded",
                        timeout=30000,
                    )
                except Exception:
                    pass
                if attempt < 3:
                    time.sleep(0.35 * attempt)
        raise RuntimeError(str(last_error or "Falha desconhecida no login do e-CAC."))

    def ensure_logged_in(self) -> None:
        if self.is_authenticated() and self.certificate_context_matches():
            self.resolve_required_mailbox_notice()
            return
        self.login()

    def _finalize_profile_switch(self, company: CompanyRecord, initial_state: str = "") -> bool:
        deadline = time.time() + 15.0
        state = initial_state
        mailbox_processed = False
        while time.time() < deadline:
            if self._profile_switch_success(company) or self._profile_switch_success_relaxed(company):
                return True
            if self.required_mailbox_notice_visible():
                self._log_step("profile_switch_mailbox_notice")
                self.resolve_required_mailbox_notice()
                mailbox_processed = True
                state = ""
                self.return_to_ecac_home()
                continue
            locator, _, _ = self._message_rows_locator()
            if self._is_message_detail_view() or locator is not None or state in {"detail_message", "mailbox_list"}:
                self._log_step(f"profile_switch_mailbox_state state={state or 'unknown'}")
                opened = self.mark_required_mailbox_messages_as_read(limit=12)
                self._log_step(f"profile_switch_mailbox_read opened={opened}")
                mailbox_processed = True
                state = ""
                try:
                    self.page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
                except Exception:
                    pass
                self.close_blocking_modals()
                self.return_to_ecac_home()
                continue
            if mailbox_processed:
                self.close_blocking_modals()
            try:
                self.page.wait_for_timeout(120)
            except Exception:
                time.sleep(0.12)
        return self._profile_switch_success(company) or self._profile_switch_success_relaxed(company)

    def switch_company_profile(self, company: CompanyRecord) -> str:
        self.ensure_logged_in()
        last_error: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                self.close_blocking_modals()
                if not self.click_anywhere(PROFILE_MENU_LABELS, timeout_ms=1800):
                    raise RuntimeError("Fluxo de 'Alterar perfil de acesso' nao foi localizado.")
                self._log_step(f"clicked_alterar_acesso attempt={attempt}")
                visible = self._wait_until(
                    lambda: self._profile_modal_visible(company) or self._find_company_input(company) is not None,
                    timeout_ms=5000,
                    interval_ms=75,
                )
                if not visible:
                    raise RuntimeError("Modal de alteracao de perfil nao ficou visivel.")
                self._log_step("modal_visible")
                if not self._fill_company_search(company):
                    raise RuntimeError("Campo de busca do perfil nao foi localizado ou nao aceitou o CNPJ.")
                modal_error = self._extract_profile_modal_error()
                if modal_error:
                    self._raise_profile_switch_modal_error(modal_error)
                selected = self._wait_until(
                    lambda: self._select_company_in_profile(company),
                    timeout_ms=900,
                    interval_ms=75,
                )
                if selected:
                    self._log_step("profile_option_selected")
                confirmed = self._confirm_profile_switch()
                if not confirmed and not selected:
                    raise RuntimeError("Botao de confirmacao do perfil nao foi localizado.")
                if not confirmed:
                    self._log_step("clicked_confirm skipped_button_not_found")
                modal_closed = self._wait_until(
                    lambda: (
                        bool(self._extract_profile_modal_error())
                        or self.required_mailbox_notice_visible()
                        or self._is_message_detail_view()
                        or self._message_rows_locator()[0] is not None
                        or self._profile_switch_success_relaxed(company)
                        or not self._profile_modal_visible(company)
                    ),
                    timeout_ms=2500,
                    interval_ms=75,
                )
                modal_error = self._extract_profile_modal_error()
                if modal_error:
                    self._raise_profile_switch_modal_error(modal_error)
                modal_error = self._extract_profile_modal_error()
                if modal_error:
                    lowered_error = modal_error.lower()
                    if "não existe procuração eletrônica" in lowered_error or "nao existe procuracao eletronica" in lowered_error:
                        raise ecac_base.ProfileSwitchNoPowerOfAttorneyError(
                            f"Falha retornada pelo portal ao alterar perfil: {modal_error}"
                        )
                    raise ecac_base.ProfileSwitchError(f"Falha retornada pelo portal ao alterar perfil: {modal_error}")
                cleanup_state = "portal"
                if modal_closed:
                    cleanup_state = self._after_profile_switch_cleanup()
                else:
                    self._log_step("profile_modal_still_processing")
                changed = self._finalize_profile_switch(company, cleanup_state)
                context = self._current_profile_context()
                if not changed and self._profile_switch_success_relaxed(company):
                    self._log_step(f"profile_switched_relaxed attempt={attempt} state={cleanup_state}")
                    return context
                if not changed:
                    raise RuntimeError(
                        "A troca de perfil nao foi validada visualmente no portal para a empresa alvo."
                    )
                self._log_step(f"profile_switched attempt={attempt} state={cleanup_state}")
                return context
            except ecac_base.ProfileSwitchNoPowerOfAttorneyError as exc:
                last_error = exc
                self.runtime.logger.warning(f"profile_switch_failed attempt={attempt} company={company.name}: {exc}")
                self._capture_debug_artifacts(f"profile_switch_failed_{company.company_id}_attempt_{attempt}")
                self.close_blocking_modals()
                raise
            except Exception as exc:
                last_error = exc
                self.runtime.logger.warning(f"profile_switch_failed attempt={attempt} company={company.name}: {exc}")
                self._capture_debug_artifacts(f"profile_switch_failed_{company.company_id}_attempt_{attempt}")
                self.close_blocking_modals()
                if attempt < 3:
                    try:
                        self.page.wait_for_timeout(250 * attempt)
                    except Exception:
                        pass
        raise RuntimeError(str(last_error or "Falha desconhecida ao trocar o perfil da empresa."))

    def return_to_ecac_home(self) -> None:
        self.ensure_logged_in()
        current_url = str(getattr(self.page, "url", "") or "").lower()
        current_context = self._current_profile_context()
        if (
            "cav.receita.fazenda.gov.br/ecac/" in current_url
            and "aplicacao.aspx?id=" not in current_url
            and current_context
            and not self.required_mailbox_notice_visible()
        ):
            self.close_blocking_modals()
            return
        try:
            self.page.goto("https://cav.receita.fazenda.gov.br/ecac/", wait_until="domcontentloaded", timeout=120000)
        except Exception:
            return
        if self.required_mailbox_notice_visible():
            self.resolve_required_mailbox_notice()
        self.close_blocking_modals()

    def ensure_company_profile(self, company: CompanyRecord) -> tuple[bool, str]:
        self.ensure_logged_in()
        current_context = self._current_profile_context()
        if self._context_matches_company(company, current_context):
            return False, current_context
        self.return_to_ecac_home()
        self.resolve_required_mailbox_notice()
        current_context = self._current_profile_context()
        if self._context_matches_company(company, current_context):
            return False, current_context
        switched_context = self.switch_company_profile(company)
        self.resolve_required_mailbox_notice()
        return True, self._current_profile_context() or switched_context

    def open_pgdas_defis(self) -> str:
        self.ensure_logged_in()
        self.page.goto(PGDAS_ENTRY_URL, wait_until="domcontentloaded", timeout=120000)
        if self.required_mailbox_notice_visible():
            self.resolve_required_mailbox_notice()
            self.page.goto(PGDAS_ENTRY_URL, wait_until="domcontentloaded", timeout=120000)
        if self.captcha_visible():
            self.handle_captcha_if_present()
        iframe_ready = self._wait_until(
            lambda: self.page.locator("#frmApp").count() > 0
            or self._find_frame_by_url_tokens("pgdasd2018.app", "simplesnacional") is not None,
            timeout_ms=120000,
            interval_ms=200,
        )
        if not iframe_ready:
            self._capture_debug("pgdas_iframe_unavailable")
            raise RuntimeError("Iframe principal do PGDAS-D / DEFIS nao ficou disponivel.")
        frame = self._find_frame_by_url_tokens("pgdasd2018.app", "simplesnacional")
        if frame is not None:
            return str(getattr(frame, "url", "") or "").strip()
        try:
            return str(self.page.locator("#frmApp").first.get_attribute("src") or "").strip()
        except Exception:
            return ""

    def wait_for_pgdas_frame(self, timeout_ms: int = 120000):
        deadline = time.time() + (timeout_ms / 1000)
        while time.time() < deadline:
            frame = self._find_frame_by_url_tokens("pgdasd2018", "simplesnacional")
            if frame is not None:
                return frame
            try:
                self.page.wait_for_timeout(200)
            except Exception:
                time.sleep(0.2)
        return None

    def wait_for_pgdas_ready(self, timeout_ms: int = 120000) -> Any:
        frame = self.wait_for_pgdas_frame(timeout_ms=timeout_ms)
        if frame is None:
            raise RuntimeError("Frame do PGDAS-D / DEFIS nao foi localizado.")
        deadline = time.time() + (timeout_ms / 1000)
        while time.time() < deadline:
            try:
                text = frame.locator("body").inner_text(timeout=1200)
            except Exception:
                text = ""
            lowered = text.lower()
            if any(marker.lower() in lowered for marker in PGDAS_READY_MARKERS):
                return frame
            try:
                self.page.wait_for_timeout(200)
            except Exception:
                time.sleep(0.2)
        return frame

    def wait_for_defis_frame(self, timeout_ms: int = 120000):
        deadline = time.time() + (timeout_ms / 1000)
        while time.time() < deadline:
            frame = self._find_frame_by_url_tokens("defis.app")
            if frame is not None:
                return frame
            try:
                self.page.wait_for_timeout(200)
            except Exception:
                time.sleep(0.2)
        return None

    def click_anywhere(self, labels: list[str], *, timeout_ms: int = 7000) -> bool:
        deadline = time.time() + max(timeout_ms, 250) / 1000
        while time.time() < deadline:
            for scope in self._iter_scopes():
                try:
                    if click_labels_in_scope(scope, labels, timeout_ms=250):
                        return True
                except Exception:
                    continue
            remaining_ms = max(250, int((deadline - time.time()) * 1000))
            try:
                if self._click_by_label(labels, timeout_ms=min(remaining_ms, 800)):
                    return True
            except Exception:
                pass
            try:
                self.page.wait_for_timeout(75)
            except Exception:
                time.sleep(0.075)
        return False

    def fill_first_visible(self, selectors: list[str], value: str, *, scopes: Optional[list[Any]] = None) -> bool:
        deadline = time.time() + 8.0
        while time.time() < deadline:
            for scope in scopes or self._iter_scopes():
                for selector in selectors:
                    locator = scope.locator(selector)
                    try:
                        count = locator.count()
                    except Exception:
                        continue
                    for index in range(min(count, 6)):
                        field = locator.nth(index)
                        try:
                            if not field.is_visible():
                                continue
                            field.click(timeout=250)
                            field.fill("")
                            field.fill(value)
                            try:
                                field.press("Tab")
                            except Exception:
                                pass
                            return True
                        except Exception:
                            try:
                                filled = bool(
                                    field.evaluate(
                                        """(el, nextValue) => {
                                            if (!el) {
                                                return false;
                                            }
                                            const style = window.getComputedStyle(el);
                                            const rect = el.getBoundingClientRect();
                                            if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity || '1') <= 0) {
                                                return false;
                                            }
                                            if (rect.width <= 0 || rect.height <= 0) {
                                                return false;
                                            }
                                            el.focus();
                                            el.value = '';
                                            el.dispatchEvent(new Event('input', { bubbles: true }));
                                            el.value = String(nextValue ?? '');
                                            el.dispatchEvent(new Event('input', { bubbles: true }));
                                            el.dispatchEvent(new Event('change', { bubbles: true }));
                                            return true;
                                        }""",
                                        value,
                                    )
                                )
                                if filled:
                                    return True
                            except Exception:
                                continue
            try:
                self.page.wait_for_timeout(75)
            except Exception:
                time.sleep(0.075)
        return False

    def collect_scope_text(self, scope: Any) -> str:
        try:
            return scope.locator("body").inner_text(timeout=1500)
        except Exception:
            return ""

    def navigate_scope(self, scope: Any, url: str, *, timeout_ms: int = 120000) -> None:
        scope.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            self.page.wait_for_load_state("networkidle", timeout=1500)
        except Exception:
            pass

    def submit_consulta_year(self, frame: Any, year: int) -> None:
        year_value = str(int(year))
        form_submitted = False
        try:
            form_submitted = bool(
                frame.locator("form[action$='/Consulta']").evaluate(
                    """(form, year) => {
                        if (!form) {
                            return false;
                        }
                        const input = form.querySelector("#ano");
                        if (!input) {
                            return false;
                        }
                        input.value = String(year);
                        input.dispatchEvent(new Event("input", { bubbles: true }));
                        input.dispatchEvent(new Event("change", { bubbles: true }));
                        form.submit();
                        return true;
                    }""",
                    year_value,
                )
            )
        except Exception:
            form_submitted = False
        if not form_submitted:
            field = frame.locator("#ano").first
            field.wait_for(state="visible", timeout=10000)
            field.click(timeout=5000)
            field.fill(year_value, timeout=5000)
            frame.get_by_role("button", name=re.compile("Consultar", re.IGNORECASE)).click(timeout=15000, force=True)
        updated = self._wait_until(
            lambda: f"declaracoes de {year_value}" in _normalize_portal_text(self.collect_scope_text(frame)),
            timeout_ms=25000,
            interval_ms=400,
        )
        if not updated:
            raise RuntimeError(f"A consulta do ano-calendario {year_value} nao atualizou a tela do PGDAS-D.")

    def collect_extrato_targets(self, frame: Any) -> list[dict[str, Any]]:
        try:
            html = frame.content()
        except Exception:
            return []
        rows: list[dict[str, Any]] = []
        section_pattern = re.compile(
            r"PA\s*(\d{2})/(\d{4})</th>(.*?)(?=PA\s*\d{2}/\d{4}</th>|Vers[aã]o\s+\d|\Z)",
            flags=re.IGNORECASE | re.DOTALL,
        )
        for month, year, section_html in section_pattern.findall(html):
            hrefs = re.findall(
                r'href="([^"]*?/Consulta/Extrato[^"]+)"',
                section_html,
                flags=re.IGNORECASE,
            )
            section_text = re.sub(r"<[^>]+>", " ", section_html)
            rows.append(
                {
                    "competencia": f"{int(year):04d}-{int(month):02d}",
                    "pa": f"{int(month):02d}/{int(year):04d}",
                    "href": unescape(hrefs[-1]) if hrefs else "",
                    "extrato_count": len(hrefs),
                    "raw_text": " ".join(f"PA {int(month):02d}/{int(year):04d} {section_text}".split()),
                }
            )
        normalized: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in rows or []:
            if not isinstance(item, dict):
                continue
            competencia = str(item.get("competencia") or "").strip()
            if not competencia or competencia in seen:
                continue
            seen.add(competencia)
            normalized.append(
                {
                    "competencia": competencia,
                    "pa": str(item.get("pa") or "").strip(),
                    "href": str(item.get("href") or "").strip(),
                    "extrato_count": int(item.get("extrato_count") or 0),
                    "raw_text": str(item.get("raw_text") or "").strip(),
                }
            )
        normalized.sort(key=lambda row: row["competencia"])
        return normalized

    def collect_extrato_targets(self, frame: Any) -> list[dict[str, Any]]:
        try:
            html = frame.content()
        except Exception:
            return []
        rows: list[dict[str, Any]] = []
        pa_pattern = re.compile(r"PA\s*0?(\d{1,2})/(\d{4})", flags=re.IGNORECASE)
        href_pattern = re.compile(r'href=(["\'])([^"\']*?/Consulta/Extrato[^"\']+)\1', flags=re.IGNORECASE)
        pa_matches = list(pa_pattern.finditer(html))
        href_matches = list(href_pattern.finditer(html))
        grouped: dict[str, dict[str, Any]] = {}
        pa_index = 0
        current_pa: Optional[re.Match[str]] = None
        for href_match in href_matches:
            while pa_index < len(pa_matches) and pa_matches[pa_index].start() < href_match.start():
                current_pa = pa_matches[pa_index]
                pa_index += 1
            if current_pa is None:
                continue
            month = int(current_pa.group(1))
            year = int(current_pa.group(2))
            competencia = f"{year:04d}-{month:02d}"
            pa = f"{month:02d}/{year:04d}"
            row = grouped.setdefault(
                competencia,
                {"competencia": competencia, "pa": pa, "hrefs": [], "raw_text": ""},
            )
            row["hrefs"].append(unescape(href_match.group(2)))
            if not row["raw_text"]:
                context_html = html[current_pa.start():min(len(html), href_match.end() + 1200)]
                row["raw_text"] = " ".join(re.sub(r"<[^>]+>", " ", context_html).split())
        for competencia, item in grouped.items():
            hrefs = [str(value or "").strip() for value in item.get("hrefs") or [] if str(value or "").strip()]
            rows.append(
                {
                    "competencia": competencia,
                    "pa": str(item.get("pa") or "").strip(),
                    "href": hrefs[-1] if hrefs else "",
                    "extrato_count": len(hrefs),
                    "raw_text": str(item.get("raw_text") or "").strip(),
                }
            )
        normalized: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in rows or []:
            if not isinstance(item, dict):
                continue
            competencia = str(item.get("competencia") or "").strip()
            if not competencia or competencia in seen:
                continue
            seen.add(competencia)
            normalized.append(
                {
                    "competencia": competencia,
                    "pa": str(item.get("pa") or "").strip(),
                    "href": str(item.get("href") or "").strip(),
                    "extrato_count": int(item.get("extrato_count") or 0),
                    "raw_text": str(item.get("raw_text") or "").strip(),
                }
            )
        normalized.sort(key=lambda row: row["competencia"])
        return normalized

    def collect_defis_year_options(self, frame: Any) -> list[str]:
        try:
            values = frame.evaluate(
                """() => Array.from(
                    document.querySelectorAll("input[type='radio'][name='ctl00$conteudo$AnoC']")
                ).map((input) => (input.value || "").trim()).filter(Boolean)"""
            )
        except Exception:
            values = []
        years = sorted({value for value in values if re.fullmatch(r"20\d{2}", value)})
        return years

    def open_defis_year(self, frame: Any, year: str) -> None:
        year_value = str(year).strip()
        radio = frame.locator(f"input[type='radio'][value='{year_value}']").first
        radio.wait_for(state="attached", timeout=10000)
        radio.check(timeout=10000)
        clicked = False
        try:
            clicked = bool(
                frame.evaluate(
                    """() => {
                        const link = Array.from(document.querySelectorAll("a[href]")).find((anchor) =>
                            (anchor.getAttribute("href") || "").includes("lnkContinuar")
                        );
                        if (!link) {
                            return false;
                        }
                        link.click();
                        return true;
                    }"""
                )
            )
        except Exception:
            clicked = False
        if not clicked:
            frame.get_by_text("Continuar", exact=True).click(timeout=15000, force=True)
        ready = self._wait_until(
            lambda: "ja existe uma declaracao" in _normalize_portal_text(self.collect_scope_text(frame))
            or "declarar imprimir ajuda sair" in _normalize_portal_text(self.collect_scope_text(frame)),
            timeout_ms=25000,
            interval_ms=400,
        )
        if not ready:
            raise RuntimeError(f"A DEFIS do ano {year_value} nao abriu a tela de detalhes.")

    def open_defis_print_page(self, frame: Any) -> None:
        clicked = False
        try:
            clicked = bool(
                frame.evaluate(
                    """() => {
                        const link = Array.from(document.querySelectorAll("a[href]")).find((anchor) =>
                            (anchor.textContent || "").trim() === "Imprimir" ||
                            (anchor.getAttribute("href") || "").includes("Imprimir()")
                        );
                        if (!link) {
                            return false;
                        }
                        link.click();
                        return true;
                    }"""
                )
            )
        except Exception:
            clicked = False
        if not clicked:
            frame.get_by_text("Imprimir", exact=True).click(timeout=15000, force=True)
        ready = self._wait_until(
            lambda: "relacao de declaracoes transmitidas" in _normalize_portal_text(self.collect_scope_text(frame))
            or "imprimir.aspx" in str(getattr(frame, "url", "") or "").lower(),
            timeout_ms=25000,
            interval_ms=400,
        )
        if not ready:
            raise RuntimeError("A tela de impressao da DEFIS nao foi carregada.")

    def collect_defis_print_rows(self, frame: Any) -> list[dict[str, Any]]:
        try:
            rows = frame.evaluate(
                """() => {
                    return Array.from(document.querySelectorAll("tr")).map((row) => {
                        const cells = Array.from(row.querySelectorAll("td"));
                        if (cells.length < 6) {
                            return null;
                        }
                        const year = (cells[0].innerText || "").trim();
                        if (!/^20\\d{2}$/.test(year)) {
                            return null;
                        }
                        const links = Array.from(row.querySelectorAll("a[href]")).map((anchor) => anchor.getAttribute("href") || "");
                        const iddec = row.querySelector("a[iddec]")?.getAttribute("iddec") || (cells[cells.length - 1].innerText || "").trim();
                        return {
                            year,
                            iddec,
                            recibo_href: links[0] || "",
                            declaracao_href: links[1] || "",
                            raw_text: (row.innerText || "").replace(/\\s+/g, " ").trim(),
                        };
                    }).filter(Boolean);
                }"""
            )
        except Exception:
            return []
        normalized: list[dict[str, Any]] = []
        for item in rows or []:
            if not isinstance(item, dict):
                continue
            year = str(item.get("year") or "").strip()
            if not re.fullmatch(r"20\d{2}", year):
                continue
            normalized.append(
                {
                    "year": year,
                    "iddec": str(item.get("iddec") or "").strip(),
                    "recibo_href": str(item.get("recibo_href") or "").strip(),
                    "declaracao_href": str(item.get("declaracao_href") or "").strip(),
                    "raw_text": str(item.get("raw_text") or "").strip(),
                }
            )
        return normalized

    def expect_download_href(
        self,
        scope: Any,
        href: str,
        output_path: Path,
        *,
        timeout_ms: int = 45000,
        fallback_capture_pdf: bool = False,
    ) -> Optional[Path]:
        target_href = str(href or "").strip()
        if not target_href:
            return None
        output_path.parent.mkdir(parents=True, exist_ok=True)
        base_url = str(getattr(scope, "url", "") or self.page.url or "").strip()
        absolute_url = urljoin(base_url, target_href)

        def _capture_current_page_pdf() -> Optional[Path]:
            marker_attr = "data-codex-prev-height"
            resized_iframe = False
            try:
                if scope is not self.page:
                    try:
                        frame_height = int(
                            scope.evaluate(
                                """() => {
                                    const body = document.body;
                                    const doc = document.documentElement;
                                    return Math.max(
                                        body ? body.scrollHeight : 0,
                                        body ? body.offsetHeight : 0,
                                        doc ? doc.clientHeight : 0,
                                        doc ? doc.scrollHeight : 0,
                                        doc ? doc.offsetHeight : 0
                                    );
                                }"""
                            )
                        )
                    except Exception:
                        frame_height = 0
                    if frame_height > 0:
                        try:
                            self.page.locator("#frmApp").evaluate(
                                """(el, payload) => {
                                    el.setAttribute(payload.attr, el.style.height || "");
                                    el.style.height = `${payload.height}px`;
                                }""",
                                {"attr": marker_attr, "height": max(frame_height + 64, 1200)},
                            )
                            resized_iframe = True
                            self.page.wait_for_timeout(250)
                        except Exception:
                            resized_iframe = False
                dimensions = self.page.evaluate(
                    """() => {
                        const body = document.body;
                        const doc = document.documentElement;
                        return {
                            width: Math.max(
                                body ? body.scrollWidth : 0,
                                body ? body.offsetWidth : 0,
                                doc ? doc.clientWidth : 0,
                                doc ? doc.scrollWidth : 0,
                                doc ? doc.offsetWidth : 0
                            ),
                            height: Math.max(
                                body ? body.scrollHeight : 0,
                                body ? body.offsetHeight : 0,
                                doc ? doc.clientHeight : 0,
                                doc ? doc.scrollHeight : 0,
                                doc ? doc.offsetHeight : 0
                            )
                        };
                    }"""
                )
                width_px = max(1024, min(int(dimensions.get("width") or 1280) + 32, 2200))
                height_px = max(1400, min(int(dimensions.get("height") or 2200) + 64, 18000))
                try:
                    self.page.emulate_media(media="screen")
                except Exception:
                    pass
                self.page.pdf(
                    path=str(output_path),
                    width=f"{width_px}px",
                    height=f"{height_px}px",
                    print_background=True,
                    margin={"top": "12px", "right": "12px", "bottom": "12px", "left": "12px"},
                )
                return output_path if output_path.exists() else None
            finally:
                if resized_iframe:
                    try:
                        self.page.locator("#frmApp").evaluate(
                            """(el, attr) => {
                                const previous = el.getAttribute(attr) || "";
                                el.style.height = previous;
                                el.removeAttribute(attr);
                            }""",
                            marker_attr,
                        )
                    except Exception:
                        pass

        def _build_requests_session(target_url: str) -> requests.Session:
            http = requests.Session()
            try:
                user_agent = str(self.page.evaluate("() => navigator.userAgent") or "").strip()
            except Exception:
                user_agent = ""
            if user_agent:
                http.headers["User-Agent"] = user_agent
            http.headers["Referer"] = base_url or target_url
            try:
                cookies = self.session.context.cookies([target_url])
            except Exception:
                cookies = []
            for cookie in cookies:
                name = str(cookie.get("name") or "").strip()
                if not name:
                    continue
                http.cookies.set(
                    name,
                    str(cookie.get("value") or ""),
                    domain=str(cookie.get("domain") or "").lstrip(".") or None,
                    path=str(cookie.get("path") or "/") or "/",
                )
            return http

        def _render_html_to_pdf(html_text: str) -> Optional[Path]:
            browser = None
            context = None
            render_page = None
            prepared_html = str(html_text or "")
            if "<head" in prepared_html.lower():
                prepared_html = re.sub(
                    r"(<head[^>]*>)",
                    rf"\1<base href=\"{absolute_url}\">",
                    prepared_html,
                    count=1,
                    flags=re.IGNORECASE,
                )
            else:
                prepared_html = f"<base href=\"{absolute_url}\">{prepared_html}"
            try:
                browser = self.session.playwright.chromium.launch(
                    headless=True,
                    executable_path=self.session.executable_path or None,
                    args=["--disable-gpu", "--ignore-certificate-errors", "--no-sandbox"],
                )
                context = browser.new_context(ignore_https_errors=True, viewport={"width": 1600, "height": 1200})
                render_page = context.new_page()
                render_page.set_content(prepared_html, wait_until="domcontentloaded")
                try:
                    render_page.evaluate(
                        """() => {
                            const normalize = (value) => String(value || '')
                                .normalize('NFKD')
                                .replace(/[\\u0300-\\u036f]/g, '')
                                .toLowerCase()
                                .replace(/\\s+/g, ' ')
                                .trim();
                            const selectors = [
                                "div.container[style*='width: 98%']",
                                "div.container",
                                ".container-fluid .container",
                                ".container-fluid",
                            ];
                            const candidates = [];
                            for (const selector of selectors) {
                                for (const element of Array.from(document.querySelectorAll(selector))) {
                                    const text = normalize(element.innerText || '');
                                    if (!text || !text.includes('extrato detalhado')) continue;
                                    candidates.push(element);
                                }
                            }
                            const target = candidates.sort((a, b) => (b.innerText || '').length - (a.innerText || '').length)[0];
                            if (!target) {
                                return false;
                            }
                            const targetWidth = Math.max(
                                Math.ceil(target.getBoundingClientRect().width || 0),
                                Math.ceil(target.scrollWidth || 0),
                                900
                            );
                            const clone = target.cloneNode(true);
                            const wrapper = document.createElement('div');
                            wrapper.id = 'codex-print-root';
                            wrapper.style.margin = '0 auto';
                            wrapper.style.width = `${targetWidth}px`;
                            wrapper.style.maxWidth = 'none';
                            wrapper.style.padding = '0';
                            wrapper.style.background = '#ffffff';
                            wrapper.appendChild(clone);
                            document.body.innerHTML = '';
                            document.body.style.margin = '0';
                            document.body.style.padding = '8px';
                            document.body.style.background = '#ffffff';
                            document.body.appendChild(wrapper);
                            return true;
                        }"""
                    )
                except Exception:
                    pass
                try:
                    render_page.emulate_media(media="screen")
                except Exception:
                    pass
                dimensions = render_page.evaluate(
                    """() => {
                        const root = document.querySelector('#codex-print-root') || document.body;
                        const body = document.body;
                        const doc = document.documentElement;
                        return {
                            width: Math.max(
                                root ? root.scrollWidth : 0,
                                root ? root.offsetWidth : 0,
                                body ? body.scrollWidth : 0,
                                body ? body.offsetWidth : 0,
                                doc ? doc.clientWidth : 0,
                                doc ? doc.scrollWidth : 0,
                                doc ? doc.offsetWidth : 0
                            ),
                            height: Math.max(
                                root ? root.scrollHeight : 0,
                                root ? root.offsetHeight : 0,
                                body ? body.scrollHeight : 0,
                                body ? body.offsetHeight : 0,
                                doc ? doc.clientHeight : 0,
                                doc ? doc.scrollHeight : 0,
                                doc ? doc.offsetHeight : 0
                            )
                        };
                    }"""
                )
                width_px = max(1024, min(int(dimensions.get("width") or 1280) + 32, 2200))
                height_px = max(1400, min(int(dimensions.get("height") or 2200) + 64, 18000))
                render_page.pdf(
                    path=str(output_path),
                    width=f"{width_px}px",
                    height=f"{height_px}px",
                    print_background=True,
                    margin={"top": "12px", "right": "12px", "bottom": "12px", "left": "12px"},
                )
                return output_path if output_path.exists() else None
            finally:
                try:
                    if render_page is not None:
                        render_page.close()
                except Exception:
                    pass
                try:
                    if context is not None:
                        context.close()
                except Exception:
                    pass
                try:
                    if browser is not None:
                        browser.close()
                except Exception:
                    pass

        if target_href.lower().startswith("javascript:"):
            try:
                with self.page.expect_download(timeout=timeout_ms) as download_info:
                    clicked = bool(
                        scope.evaluate(
                            """href => {
                                const candidates = Array.from(document.querySelectorAll("a[href]")).filter((anchor) =>
                                    (anchor.getAttribute("href") || "").trim() === href
                                );
                                const link = candidates.length ? candidates[candidates.length - 1] : null;
                                if (!link) {
                                    return false;
                                }
                                link.click();
                                return true;
                            }""",
                            target_href,
                        )
                    )
                if not clicked:
                    return None
                download = download_info.value
                download.save_as(str(output_path))
                return output_path
            except Exception:
                return None

        try:
            http = _build_requests_session(absolute_url)
            response = http.get(
                absolute_url,
                timeout=max(10, timeout_ms // 1000),
                allow_redirects=True,
            )
            response.raise_for_status()
        except Exception:
            return None

        content_type = str(response.headers.get("content-type") or "").lower()
        content_disposition = str(response.headers.get("content-disposition") or "").lower()
        payload_bytes = response.content or b""
        if (
            "application/pdf" in content_type
            or ".pdf" in content_disposition
            or payload_bytes.startswith(b"%PDF")
        ):
            output_path.write_bytes(payload_bytes)
            return output_path

        if not fallback_capture_pdf:
            return None

        try:
            html_text = response.text
        except Exception:
            html_text = payload_bytes.decode("utf-8", errors="ignore")
        normalized = _normalize_portal_text(html_text)
        if "extrato detalhado" not in normalized:
            return None
        self.runtime.logger.info(
            f"Extrato sem download detectado em {absolute_url}; salvando a pagina completa em PDF."
        )
        return _render_html_to_pdf(html_text)

    def expect_download_click(
        self,
        scope: Any,
        labels: list[str],
        output_path: Path,
        *,
        timeout_ms: int = 45000,
    ) -> Optional[Path]:
        for label in labels:
            pattern = re.compile(re.escape(label), re.IGNORECASE)
            locators = [
                scope.get_by_role("button", name=pattern),
                scope.get_by_role("link", name=pattern),
                scope.get_by_text(pattern),
            ]
            for locator in locators:
                try:
                    if locator.count() < 1:
                        continue
                    with self.page.expect_download(timeout=timeout_ms) as download_info:
                        locator.first.click(timeout=timeout_ms, force=True)
                    download = download_info.value
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    download.save_as(str(output_path))
                    return output_path
                except Exception:
                    try:
                        def _pdf_response(response: Any) -> bool:
                            headers = response.headers or {}
                            content_type = str(headers.get("content-type") or headers.get("Content-Type") or "").lower()
                            disposition = str(headers.get("content-disposition") or headers.get("Content-Disposition") or "").lower()
                            return (
                                "pdf" in content_type
                                or "attachment" in disposition
                                or str(response.url or "").lower().endswith(".pdf")
                            )

                        with self.page.expect_response(_pdf_response, timeout=timeout_ms) as response_info:
                            locator.first.click(timeout=timeout_ms, force=True)
                        response = response_info.value
                        body = response.body()
                        if body:
                            output_path.parent.mkdir(parents=True, exist_ok=True)
                            output_path.write_bytes(body)
                            return output_path if output_path.exists() else None
                    except Exception:
                        continue
        return None


def cleanup_old_outputs(root_dir: Path) -> None:
    cutoff = time.time() - (24 * 60 * 60)
    if not root_dir.exists():
        return
    for child in root_dir.iterdir():
        try:
            if child.stat().st_mtime >= cutoff:
                continue
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)
        except Exception:
            continue


def ensure_company_output_dir(
    runtime_env: RuntimeEnvironment,
    office_context: OfficeContext,
    company: CompanyRecord,
) -> Path:
    base_path = str(getattr(office_context, "base_path", "") or "").strip()
    segment_path = str(getattr(office_context, "segment_path", "") or "").strip()
    if base_path:
        company_folder = sanitize_company_folder(company.name)
        if not segment_path:
            output_dir = Path(base_path) / company_folder
            output_dir.mkdir(parents=True, exist_ok=True)
            runtime_env.logger.warning(
                "segment_path nao foi resolvido no SaaS; salvando apenas em base_path/empresa."
            )
            runtime_env.logger.info(f"Diretorio de saida resolvido para {company.name}: {output_dir}")
            return output_dir
        relative_dir = f"{company_folder}/{segment_path.strip().replace(chr(92), '/')}".replace("\\", "/")
        output_dir = Path(base_path) / relative_dir.replace("/", os.sep)
        output_dir.mkdir(parents=True, exist_ok=True)
        runtime_env.logger.info(f"Diretorio de saida resolvido para {company.name}: {output_dir}")
        return output_dir
    output_root = runtime_env.paths.data_dir / "output"
    cleanup_old_outputs(output_root)
    day_dir = output_root / datetime.now().strftime("%Y-%m-%d")
    company_dir = day_dir / f"{slugify(company.name)}_{only_digits(company.document)}"
    company_dir.mkdir(parents=True, exist_ok=True)
    runtime_env.logger.info(f"Diretorio de saida resolvido para {company.name}: {company_dir}")
    return company_dir


def shutdown_runtime_logger(runtime_env: RuntimeEnvironment) -> None:
    logger = getattr(runtime_env, "logger", None)
    if logger is None:
        return
    try:
        logger.bind_sink(None)
    except Exception:
        pass
    internal_logger = getattr(logger, "_logger", None)
    if internal_logger is None:
        return
    handlers = list(getattr(internal_logger, "handlers", []) or [])
    for handler in handlers:
        try:
            handler.flush()
        except Exception:
            pass
        try:
            handler.close()
        except Exception:
            pass
        try:
            internal_logger.removeHandler(handler)
        except Exception:
            pass


def cleanup_runtime_artifacts(runtime_env: RuntimeEnvironment) -> None:
    try:
        shutdown_runtime_logger(runtime_env)
    except Exception:
        pass

    data_dir = getattr(runtime_env.paths, "data_dir", None)
    logs_dir = getattr(runtime_env.paths, "logs_dir", None)
    if data_dir:
        try:
            shutil.rmtree(Path(data_dir) / "output", ignore_errors=True)
        except Exception:
            pass
        try:
            for gitkeep in Path(data_dir).rglob(".gitkeep"):
                gitkeep.unlink(missing_ok=True)
        except Exception:
            pass
    if logs_dir:
        try:
            shutil.rmtree(Path(logs_dir), ignore_errors=True)
        except Exception:
            pass

    try:
        stop_path = get_stop_signal_path(runtime_env)
        if stop_path.exists():
            stop_path.unlink()
    finally:
        try:
            _rebuild_chrome_profile_from_backup(runtime_env, require_backup=False)
        except Exception as exc:
            runtime_env.logger.warning(f"Falha ao restaurar chrome_profile a partir do backup: {exc}")


def get_stop_signal_path(runtime_env: RuntimeEnvironment) -> Path:
    return Path(runtime_env.paths.json_dir) / "stop.json"


def read_stop_signal(runtime_env: RuntimeEnvironment) -> Optional[dict[str, Any]]:
    payload = read_json(get_stop_signal_path(runtime_env), default=None)
    return payload if isinstance(payload, dict) else None


def clear_stop_signal(runtime_env: RuntimeEnvironment) -> None:
    try:
        get_stop_signal_path(runtime_env).unlink(missing_ok=True)
    except Exception:
        pass


def ensure_default_certificate(
    runtime_env: RuntimeEnvironment,
    companies: list[CompanyRecord],
    *,
    pfx_path: str = "",
    pfx_password: str = "",
    preferred_company: Optional[CompanyRecord] = None,
) -> CertificateMetadata:
    if pfx_path:
        return import_pfx_and_get_metadata(Path(pfx_path), pfx_password)
    if preferred_company is not None:
        preferred_certificate = resolve_certificate_from_dashboard(runtime_env, [preferred_company])
        if preferred_certificate:
            return preferred_certificate
        raise RuntimeError(
            "O escritorio responsavel configurado precisa ter certificado digital valido no dashboard."
        )
    env_certificate = resolve_certificate_from_env()
    if env_certificate:
        return env_certificate
    dashboard_certificate = resolve_certificate_from_dashboard(runtime_env, companies)
    if dashboard_certificate:
        return dashboard_certificate
    raise RuntimeError("Nenhum certificado digital foi resolvido para o fluxo do e-CAC.")


def load_job_or_manual_payload(runtime_env: RuntimeEnvironment, args: argparse.Namespace) -> Optional[JobPayload]:
    explicit_company_args = any(
        str(getattr(args, attr, "") or "").strip()
        for attr in ("company_id", "company_name", "company_document")
    )
    if getattr(args, "job_mode", False) or (getattr(args, "no_ui", False) and not explicit_company_args):
        return runtime_env.json_runtime.load_job()
    return None


def build_company_from_args(args: argparse.Namespace) -> Optional[CompanyRecord]:
    company_document = only_digits(getattr(args, "company_document", "") or "")
    company_name = str(getattr(args, "company_name", "") or "").strip()
    company_id = str(getattr(args, "company_id", "") or "").strip() or company_document or company_name
    if not company_document and not company_name:
        return None
    return CompanyRecord(
        company_id=company_id,
        name=company_name or company_document,
        document=company_document,
        active=True,
        eligible=True,
        source="cli",
        raw={},
    )


def build_company_from_certificate(
    certificate: CertificateMetadata,
    *,
    source: str = "certificate_default",
) -> Optional[CompanyRecord]:
    company_document = only_digits(_extract_document_from_subject(certificate.subject))
    company_name = _extract_common_name_from_subject(certificate.subject) or str(certificate.alias or "").strip()
    company_id = company_document or company_name
    if not company_id:
        return None
    return CompanyRecord(
        company_id=company_id,
        name=company_name or company_document,
        document=company_document,
        active=True,
        eligible=True,
        source=source,
        raw={
            "certificate_subject": certificate.subject,
            "certificate_alias": certificate.alias,
            "certificate_source": certificate.source,
        },
    )


def write_result_and_heartbeat(
    runtime_env: RuntimeEnvironment,
    job: Optional[JobPayload],
    summary: RobotRunSummary,
    *,
    payload: Optional[dict[str, Any]] = None,
    error_message: Optional[str] = None,
) -> None:
    runtime_env.json_runtime.write_result(
        job=job,
        success=summary.status in {"completed", "partial"},
        summary=summary.to_dict(),
        payload=payload or {},
        error_message=error_message,
        company_results=[item.to_dict() for item in summary.company_results],
        responsible_office_result=None,
    )


def start_heartbeat_loop(
    runtime_env: RuntimeEnvironment,
    job: Optional[JobPayload],
    dashboard: DashboardClient,
    robot_id: str,
    stop_event: threading.Event,
    state: dict[str, Any],
) -> threading.Thread:
    def _loop() -> None:
        while not stop_event.is_set():
            runtime_env.json_runtime.write_heartbeat(
                status="processing",
                current_job_id=job.job_id if job else None,
                current_execution_request_id=job.execution_request_id if job else None,
                message="heartbeat",
                progress=dict(state),
            )
            dashboard.update_robot_presence(status="processing", robot_id=robot_id)
            stop_event.wait(30)

    thread = threading.Thread(target=_loop, name=f"{dashboard.technical_id}-heartbeat", daemon=True)
    thread.start()
    return thread


def default_company_payload(company: CompanyRecord, extra: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    payload = {
        "company_id": company.company_id,
        "company_name": company.name,
        "company_document": format_document(company.document),
    }
    if extra:
        payload.update(extra)
    return payload


def resolve_responsible_office_company(
    dashboard: DashboardClient,
    office_context: OfficeContext,
    companies: list[CompanyRecord],
    config: ResponsibleOfficeConfig,
) -> CompanyRecord:
    responsible_id = str(config.responsible_office_company_id or "").strip()
    if not responsible_id:
        raise RuntimeError("Configuracao dinamica invalida: responsible_office_company_id nao definido.")

    company = next((item for item in companies if item.company_id == responsible_id), None)
    if company is None:
        company = dashboard.load_company_by_id(office_context, responsible_id)
    if company is None:
        raise RuntimeError("Empresa definida como escritorio responsavel nao foi encontrada no dashboard.")
    if len(only_digits(company.document)) != 14:
        raise RuntimeError("A empresa escolhida como escritorio responsavel precisa ter CNPJ valido.")

    has_certificate = bool(
        str(company.cert_password or "").strip()
        and (str(company.cert_blob_b64 or "").strip() or str(company.cert_path or "").strip())
    )
    if not has_certificate:
        raise RuntimeError(
            "A empresa escolhida como escritorio responsavel precisa ter certificado digital configurado."
        )

    company.eligible = True
    company.block_reason = ""
    return company


def apply_execution_eligibility(
    companies: list[CompanyRecord],
    config: ResponsibleOfficeConfig,
) -> list[CompanyRecord]:
    normalized: list[CompanyRecord] = []
    for company in companies:
        updated = CompanyRecord(**company.to_dict())
        updated.block_reason = ""
        if config.use_responsible_office:
            updated.eligible = updated.has_valid_cnpj
            if not updated.eligible:
                updated.block_reason = "Empresa sem CNPJ valido para operar via escritorio responsavel."
        elif updated.source in {"cli", "certificate_default", "ui_certificate_default"}:
            updated.eligible = True
        else:
            updated.eligible = updated.has_certificate_credentials
            if not updated.eligible:
                updated.block_reason = "Empresa sem certificado digital configurado no dashboard."
        normalized.append(updated)
    return normalized


def resolve_company_certificate_for_execution(
    runtime_env: RuntimeEnvironment,
    company: CompanyRecord,
    *,
    pfx_path: str = "",
    pfx_password: str = "",
) -> CertificateMetadata:
    if pfx_path:
        return import_pfx_and_get_metadata(Path(pfx_path), pfx_password)
    if company.has_certificate_credentials:
        metadata = resolve_certificate_from_dashboard(runtime_env, [company])
        if metadata:
            return metadata
        raise RuntimeError(f"Certificado digital nao encontrado para {company.name}.")
    env_certificate = resolve_certificate_from_env()
    if env_certificate:
        return env_certificate
    raise RuntimeError(f"Nenhum certificado digital foi resolvido para {company.name}.")


class RobotRunner:
    def __init__(
        self,
        *,
        technical_id: str,
        display_name: str,
        args: argparse.Namespace,
        job: Optional[JobPayload],
        runtime_dir: Optional[Path],
        process_company: Callable[[SimplesEcacAutomation, CompanyRecord, Path, argparse.Namespace], CompanyExecutionResult],
    ) -> None:
        self.technical_id = technical_id
        self.display_name = display_name
        self.args = args
        self.job = job
        self.process_company = process_company
        self.runtime = build_runtime(technical_id, display_name, sink=lambda line: print(line, flush=True), runtime_dir=runtime_dir)
        self.dashboard = DashboardClient(self.runtime, technical_id, display_name)
        self.stop_requested = False
        self._active_session_lock = threading.Lock()
        self._active_session: Optional[BrowserSession] = None
        self._active_company_label = ""

    def _register_active_session(self, session: Optional[BrowserSession], company_label: str) -> None:
        if session is None:
            return
        with self._active_session_lock:
            self._active_session = session
            self._active_company_label = str(company_label or "").strip()

    def _release_active_session(self, session: Optional[BrowserSession]) -> None:
        if session is None:
            return
        with self._active_session_lock:
            if self._active_session is session:
                self._active_session = None
                self._active_company_label = ""

    def request_stop(self) -> None:
        self.stop_requested = True
        session: Optional[BrowserSession] = None
        company_label = ""
        with self._active_session_lock:
            session = self._active_session
            company_label = self._active_company_label
            self._active_session = None
            self._active_company_label = ""
        if session is None:
            return
        try:
            self.runtime.logger.info(
                f"Fechando o navegador ativo para interromper a execucao: {company_label or 'sessao atual'}."
            )
        except Exception:
            pass
        try:
            close_browser(session, self.runtime)
        except Exception as exc:
            self.runtime.logger.warning(f"Falha ao fechar o navegador durante a parada remota: {exc}")

    def resolve_companies(self, job: Optional[JobPayload]) -> tuple[OfficeContext, list[CompanyRecord]]:
        cli_company = build_company_from_args(self.args)
        office_context = self.dashboard.resolve_office_context(job, force_refresh=True)
        if cli_company is not None:
            return office_context, [cli_company]
        if not getattr(self.args, "job_mode", False) and not office_context.office_id:
            certificate = ensure_default_certificate(
                self.runtime,
                [],
                pfx_path=str(getattr(self.args, "pfx_path", "") or ""),
                pfx_password=str(getattr(self.args, "pfx_password", "") or ""),
            )
            certificate_company = build_company_from_certificate(certificate)
            if certificate_company is not None:
                self.runtime.logger.info(
                    "Execucao manual sem empresa informada e sem office_id; usando a empresa do certificado como contexto local."
                )
                return office_context, [certificate_company]
        companies = self.dashboard.load_companies(
            office_context,
            job=job,
            company_ids=job.company_ids if job else None,
        )
        if not companies:
            raise RuntimeError("Nenhuma empresa foi resolvida para a execucao.")
        return office_context, companies

    def run(self) -> dict[str, Any]:
        job = self.job or load_job_or_manual_payload(self.runtime, self.args)
        effective_args = build_effective_execution_args(self.args, job)
        robot_id = self.dashboard.register_robot_presence(status="processing") or ""
        self.runtime.json_runtime.register_robot({"display_name": self.display_name})
        run_id = self.runtime.generate_run_id()
        started_at = utc_now_iso()
        stop_event = threading.Event()
        heartbeat_state = {"current": 0, "total": 0, "company_name": ""}
        heartbeat_thread: Optional[threading.Thread] = None

        try:
            office_context, companies = self.resolve_companies(job)
            responsible_office_config = self.dashboard.fetch_responsible_office_config(office_context)
            companies = apply_execution_eligibility(companies, responsible_office_config)
            auth_company: Optional[CompanyRecord] = None
            if responsible_office_config.use_responsible_office:
                auth_company = resolve_responsible_office_company(
                    self.dashboard,
                    office_context,
                    companies,
                    responsible_office_config,
                )
            total_steps = len(companies)
            heartbeat_state["total"] = total_steps
            heartbeat_thread = start_heartbeat_loop(
                self.runtime,
                job,
                self.dashboard,
                robot_id,
                stop_event,
                heartbeat_state,
            )
            results: list[CompanyExecutionResult] = []
            processed_contexts = 0
            pfx_path = str(getattr(self.args, "pfx_path", "") or "")
            pfx_password = str(getattr(self.args, "pfx_password", "") or "")

            def _open_authenticated_automation(certificate: CertificateMetadata, company_label: str) -> tuple[BrowserSession, SimplesEcacAutomation]:
                last_error: Optional[Exception] = None
                for browser_attempt in range(1, LOGIN_BROWSER_RESET_LIMIT + 1):
                    session = launch_browser(self.runtime, headless=bool(getattr(self.args, "headless", False)))
                    self._register_active_session(session, company_label)
                    self.runtime.logger.info(f"Navegador iniciado em {session.executable_path} para {company_label}")
                    try:
                        automation = SimplesEcacAutomation(
                            self.runtime,
                            session,
                            certificate,
                            stop_requested=lambda: self.stop_requested,
                            headless=bool(getattr(self.args, "headless", False)),
                            allow_manual_captcha=not bool(getattr(self.args, "headless", False)),
                        )
                        automation.login()
                        if self.stop_requested:
                            raise RuntimeError("Execucao interrompida pelo usuario.")
                        return session, automation
                    except LoginBrowserResetRequiredError as exc:
                        last_error = exc
                        self.runtime.logger.warning(
                            f"login_browser_reset attempt={browser_attempt}/{LOGIN_BROWSER_RESET_LIMIT} company={company_label}: {exc}"
                        )
                        self._release_active_session(session)
                        close_browser(session, self.runtime)
                        if browser_attempt >= LOGIN_BROWSER_RESET_LIMIT:
                            break
                        time.sleep(min(1.5 * browser_attempt, 5.0))
                        continue
                    except Exception as exc:
                        last_error = exc
                        self._release_active_session(session)
                        close_browser(session, self.runtime)
                        raise
                raise RuntimeError(str(last_error or f"Falha ao autenticar no e-CAC para {company_label}."))

            def _session_is_usable(session: Optional[BrowserSession]) -> bool:
                if session is None:
                    return False
                try:
                    return not bool(session.page.is_closed())
                except Exception:
                    return False

            def _blocked_result(company: CompanyRecord) -> CompanyExecutionResult:
                return CompanyExecutionResult(
                    company_id=company.company_id,
                    company_name=company.name,
                    company_document=format_document(company.document),
                    status="blocked",
                    eligible=False,
                    block_reason=company.block_reason,
                    errors=[company.block_reason or "Empresa nao elegivel."],
                    started_at=utc_now_iso(),
                    finished_at=utc_now_iso(),
                )

            def _error_result(company: CompanyRecord, message: str) -> CompanyExecutionResult:
                return CompanyExecutionResult(
                    company_id=company.company_id,
                    company_name=company.name,
                    company_document=format_document(company.document),
                    status="error",
                    eligible=company.eligible,
                    block_reason=company.block_reason,
                    errors=[message],
                    started_at=utc_now_iso(),
                    finished_at=utc_now_iso(),
                )

            def _interrupted_result(company: CompanyRecord) -> CompanyExecutionResult:
                return CompanyExecutionResult(
                    company_id=company.company_id,
                    company_name=company.name,
                    company_document=format_document(company.document),
                    status="interrupted",
                    eligible=company.eligible,
                    block_reason=company.block_reason,
                    warnings=["Execucao interrompida pelo usuario."],
                    started_at=utc_now_iso(),
                    finished_at=utc_now_iso(),
                )

            def _process_company_with_automation(
                automation: SimplesEcacAutomation,
                company: CompanyRecord,
                *,
                switch_profile: bool,
            ) -> CompanyExecutionResult:
                result = CompanyExecutionResult(
                    company_id=company.company_id,
                    company_name=company.name,
                    company_document=format_document(company.document),
                    eligible=company.eligible,
                    block_reason=company.block_reason,
                    started_at=utc_now_iso(),
                )
                if not company.eligible:
                    return _blocked_result(company)
                try:
                    if switch_profile:
                        switched, context = automation.ensure_company_profile(company)
                    else:
                        automation.ensure_logged_in()
                        switched, context = False, automation._current_profile_context()
                    result.profile_switched = switched
                    result.company_profile_context = context
                    company_dir = ensure_company_output_dir(self.runtime, office_context, company)
                    processed = self.process_company(automation, company, company_dir, effective_args)
                    processed.profile_switched = switched
                    processed.company_profile_context = processed.company_profile_context or context
                    processed.started_at = result.started_at
                    processed.finished_at = utc_now_iso()
                    return processed
                except Exception as exc:
                    if self.stop_requested:
                        self.runtime.logger.info(f"company_interrupted {company.name}: execucao interrompida pelo usuario.")
                        interrupted = _interrupted_result(company)
                        interrupted.profile_switched = result.profile_switched
                        interrupted.company_profile_context = result.company_profile_context
                        interrupted.started_at = result.started_at
                        interrupted.finished_at = utc_now_iso()
                        return interrupted
                    try:
                        if automation is not None and _session_is_usable(getattr(automation, "session", None)):
                            automation.close_blocking_modals()
                            automation.return_to_ecac_home()
                    except Exception as recovery_exc:
                        self.runtime.logger.warning(f"company_recovery_failed {company.name}: {recovery_exc}")
                    self.runtime.logger.warning(f"company_failed {company.name}: {exc}")
                    errored = _error_result(company, _stack_tail(exc))
                    errored.profile_switched = result.profile_switched
                    errored.company_profile_context = result.company_profile_context
                    return errored

            if responsible_office_config.use_responsible_office and auth_company is not None:
                office_certificate = resolve_company_certificate_for_execution(
                    self.runtime,
                    auth_company,
                    pfx_path=pfx_path,
                    pfx_password=pfx_password,
                )
                session: Optional[BrowserSession] = None
                automation: Optional[SimplesEcacAutomation] = None
                try:
                    session, automation = _open_authenticated_automation(office_certificate, auth_company.name)
                    for company in companies:
                        if self.stop_requested:
                            self.runtime.logger.info("Execucao interrompida pelo usuario.")
                            break
                        if not _session_is_usable(session):
                            if session is not None:
                                self._release_active_session(session)
                                close_browser(session, self.runtime)
                            session, automation = _open_authenticated_automation(office_certificate, auth_company.name)
                        processed_contexts += 1
                        heartbeat_state.update({"current": processed_contexts, "company_name": company.name, "company_id": company.company_id})
                        should_switch_profile = company.company_id != auth_company.company_id
                        results.append(
                            _process_company_with_automation(
                                automation,
                                company,
                                switch_profile=should_switch_profile,
                            )
                        )
                finally:
                    self._release_active_session(session)
                    close_browser(session, self.runtime)
            else:
                for company in companies:
                    if self.stop_requested:
                        self.runtime.logger.info("Execucao interrompida pelo usuario.")
                        break
                    processed_contexts += 1
                    heartbeat_state.update({"current": processed_contexts, "company_name": company.name, "company_id": company.company_id})
                    if not company.eligible:
                        results.append(_blocked_result(company))
                        continue
                    session: Optional[BrowserSession] = None
                    try:
                        company_certificate = resolve_company_certificate_for_execution(
                            self.runtime,
                            company,
                            pfx_path=pfx_path,
                            pfx_password=pfx_password,
                        )
                        session, automation = _open_authenticated_automation(company_certificate, company.name)
                        results.append(_process_company_with_automation(automation, company, switch_profile=False))
                    except Exception as exc:
                        if self.stop_requested:
                            self.runtime.logger.info(f"company_interrupted {company.name}: execucao interrompida pelo usuario.")
                            results.append(_interrupted_result(company))
                            break
                        self.runtime.logger.warning(f"company_failed {company.name}: {exc}")
                        results.append(_error_result(company, _stack_tail(exc)))
                    finally:
                        self._release_active_session(session)
                        close_browser(session, self.runtime)

            completed = sum(1 for item in results if item.status in {"success", "partial", "empty"})
            failed = sum(1 for item in results if item.status not in {"success", "partial", "empty"})
            status = "completed" if failed == 0 else ("partial" if completed else "failed")
            summary = RobotRunSummary(
                run_id=run_id,
                technical_id=self.technical_id,
                display_name=self.display_name,
                started_at=started_at,
                finished_at=utc_now_iso(),
                status=status,
                company_results=results,
                metadata={
                    "office_id": office_context.office_id,
                    "job_id": job.job_id if job else "",
                    "execution_request_id": job.execution_request_id if job else "",
                    "use_responsible_office": responsible_office_config.use_responsible_office,
                    "responsible_office_company_id": responsible_office_config.responsible_office_company_id,
                    "auth_company_id": auth_company.company_id if auth_company else "",
                    "auth_company_name": auth_company.name if auth_company else "",
                    "auth_company_document": format_document(auth_company.document) if auth_company else "",
                    "total_companies": len(results),
                    "completed": completed,
                    "failed": failed,
                },
            )
            write_result_and_heartbeat(self.runtime, job, summary)
            return summary.to_dict()
        except Exception as exc:
            summary = RobotRunSummary(
                run_id=run_id,
                technical_id=self.technical_id,
                display_name=self.display_name,
                started_at=started_at,
                finished_at=utc_now_iso(),
                status="failed",
                company_results=[],
                metadata={"error": _stack_tail(exc)},
            )
            write_result_and_heartbeat(self.runtime, job, summary, error_message=_stack_tail(exc))
            raise
        finally:
            stop_event.set()
            if heartbeat_thread is not None:
                heartbeat_thread.join(timeout=2)
            self.dashboard.update_robot_presence(status="inactive", robot_id=robot_id)
            self.runtime.mark_inactive("runner_exit")
            cleanup_runtime_artifacts(self.runtime)


def build_common_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--no-ui", action="store_true", help="Compatibilidade com o padrao orquestrado.")
    parser.add_argument("--job-mode", action="store_true", help="Le job.json do diretorio data/json.")
    parser.add_argument("--headless", action="store_true", help="Executa o navegador sem UI.")
    parser.add_argument("--pfx-path", default="", help="Caminho explicito do .pfx.")
    parser.add_argument("--pfx-password", default="", help="Senha do .pfx.")
    parser.add_argument("--company-id", default="", help="ID da empresa para execucao manual.")
    parser.add_argument("--company-name", default="", help="Nome da empresa para execucao manual.")
    parser.add_argument("--company-document", default="", help="Documento da empresa para execucao manual.")
    return parser

ROBOT_TECHNICAL_ID = "ecac_simples_emitir_guia"
ROBOT_DISPLAY_NAME = "e-CAC - Simples Nacional - Emitir Guia DAS"

DECLARACAO_MENSAL_LABELS = ["Declaracao Mensal", "Declaracao mensal", "Declaracao mensal do PGDAS-D"]
GERAR_DAS_LABELS = ["Gerar DAS", "Emitir DAS", "Geracao do DAS"]
PERIODO_APURACAO_LABELS = ["Informe o periodo de apuracao", "Periodo de Apuracao", "Período de Apuração"]
COMPETENCIA_SELECTORS = [
    "input[name*='periodo' i]",
    "input[id*='periodo' i]",
    "input[name*='pa' i]",
    "input[id*='pa' i]",
    "input[name*='compet' i]",
    "input[id*='compet' i]",
    "input[placeholder*='MM/AAAA' i]",
    "input[aria-label*='periodo' i]",
]
RECALCULO_SELECTORS = [
    "input[name*='data' i]",
    "input[id*='data' i]",
    "input[placeholder*='DD/MM/AAAA' i]",
    "input[aria-label*='data' i]",
]
CONTINUAR_LABELS = ["Continuar", "Prosseguir", "Confirmar", "OK"]
CONSOLIDAR_OUTRA_DATA_LABELS = ["Consolidar para outra data", "Consolidar", "Outra data"]
DOWNLOAD_DAS_LABELS = ["Gerar DAS", "Imprimir DAS", "Baixar DAS", "Download DAS"]
SEM_VALOR_MESSAGE = "não é possível gerar DAS, não há valor devido para o período informado"
DAS_DECLARACAO_NAO_TRANSMITIDA_MESSAGE = "MSG_E0117 - Não existe declaração transmitida para este PA."


def parse_args() -> argparse.Namespace:
    parser = build_common_parser(ROBOT_DISPLAY_NAME)
    parser.add_argument("--competencia", default="", help="Competencia no formato MM/AAAA ou AAAA-MM.")
    parser.add_argument("--data-vencimento", default="", help="Nova data para recalculo no formato DD/MM/AAAA.")
    parser.add_argument("--dry-run", action="store_true", help="Executa sem emitir o DAS.")
    parser.add_argument("--real-generate", action="store_true", help="Executa a emissao real do DAS.")
    return parser.parse_args()


def _normalize_job_competencia(value: Any) -> str:
    raw = str(value or "").strip()
    match = re.fullmatch(r"(\d{4})-(\d{2})", raw)
    if match:
        return f"{match.group(2)}/{match.group(1)}"
    match = re.fullmatch(r"(\d{2})/(\d{4})", raw)
    if match:
        return raw
    return ""


def _normalize_job_due_date(value: Any) -> str:
    raw = str(value or "").strip()
    match = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if match:
        return f"{match.group(3)}/{match.group(2)}/{match.group(1)}"
    match = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", raw)
    if match:
        return raw
    return ""


def build_effective_execution_args(args: argparse.Namespace, job: Optional[JobPayload]) -> argparse.Namespace:
    effective = argparse.Namespace(**vars(args))
    raw = job.raw if job and isinstance(job.raw, dict) else {}
    if not raw:
        return effective
    settings = raw.get("settings") if isinstance(raw.get("settings"), dict) else {}
    job_action = str(raw.get("action") or "").strip().lower()
    job_mode = str(raw.get("mode") or "").strip().lower()
    competencia = _normalize_job_competencia(
        raw.get("competence")
        or raw.get("competencia")
        or settings.get("competence")
        or settings.get("competencia")
    )
    data_vencimento = _normalize_job_due_date(
        raw.get("recalculate_due_date")
        or raw.get("data_vencimento")
        or raw.get("due_date")
        or settings.get("recalculate_due_date")
        or settings.get("data_vencimento")
        or settings.get("due_date")
    )
    if not job_action:
        job_action = str(settings.get("action") or "").strip().lower()
    if not job_mode:
        job_mode = str(settings.get("mode") or "").strip().lower()
    if competencia:
        effective.competencia = competencia
    if data_vencimento:
        effective.data_vencimento = data_vencimento
    if job_action == "simples_emitir_guia":
        effective.real_generate = True
        effective.dry_run = False
    if job_mode == "recalcular" and data_vencimento:
        effective.data_vencimento = data_vencimento
    return effective


def _split_competencia(value: str) -> tuple[str, str]:
    normalized = _normalize_job_competencia(value)
    match = re.fullmatch(r"(\d{2})/(\d{4})", normalized)
    if not match:
        return "", ""
    return match.group(1), match.group(2)


def _select_first_visible_option(scope: Any, selectors: list[str], values: list[str]) -> bool:
    normalized_values = [str(item or "").strip() for item in values if str(item or "").strip()]
    if not normalized_values:
        return False
    for selector in selectors:
        locator = scope.locator(selector)
        try:
            count = locator.count()
        except Exception:
            continue
        for index in range(min(count, 6)):
            field = locator.nth(index)
            try:
                field.wait_for(state="visible", timeout=1200)
            except Exception:
                continue
            for candidate in normalized_values:
                try:
                    field.select_option(value=candidate, timeout=1500)
                    return True
                except Exception:
                    pass
                try:
                    field.select_option(label=candidate, timeout=1500)
                    return True
                except Exception:
                    pass
    return False


def click_labels_in_scope(scope: Any, labels: list[str], *, timeout_ms: int = 7000) -> bool:
    normalized_labels = [_normalize_portal_text(label) for label in labels if str(label or "").strip()]
    if not normalized_labels:
        return False

    def _is_match(text: str) -> bool:
        normalized_text = _normalize_portal_text(text)
        if not normalized_text:
            return False
        return any(
            normalized_text == label
            or normalized_text.startswith(f"{label} ")
            or normalized_text.endswith(f" {label}")
            or f" {label} " in f" {normalized_text} "
            for label in normalized_labels
        )

    deadline = time.time() + max(timeout_ms, 250) / 1000
    candidate_locators = [
        scope.locator("button"),
        scope.locator("a"),
        scope.locator("input[type='button']"),
        scope.locator("input[type='submit']"),
        scope.locator("[role='button']"),
        scope.locator("[role='link']"),
    ]
    while time.time() < deadline:
        for locator in candidate_locators:
            try:
                count = locator.count()
            except Exception:
                continue
            for index in range(min(count, 20)):
                candidate = locator.nth(index)
                try:
                    if not candidate.is_visible():
                        continue
                except Exception:
                    continue
                try:
                    text = (
                        candidate.get_attribute("value")
                        or candidate.get_attribute("aria-label")
                        or candidate.inner_text(timeout=200)
                        or candidate.text_content(timeout=200)
                        or ""
                    )
                except Exception:
                    text = ""
                if not _is_match(text):
                    continue
                try:
                    candidate.scroll_into_view_if_needed(timeout=150)
                except Exception:
                    pass
                try:
                    candidate.click(timeout=250, force=True)
                    return True
                except Exception:
                    try:
                        clicked = bool(
                            candidate.evaluate(
                                """(el) => {
                                    if (!el) {
                                        return false;
                                    }
                                    const style = window.getComputedStyle(el);
                                    const rect = el.getBoundingClientRect();
                                    if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity || '1') <= 0) {
                                        return false;
                                    }
                                    if (rect.width <= 0 || rect.height <= 0) {
                                        return false;
                                    }
                                    el.click();
                                    return true;
                                }"""
                            )
                        )
                        if clicked:
                            return True
                    except Exception:
                        continue

        try:
            clicked = bool(
                scope.evaluate(
                    """labels => {
                        const normalize = (value) =>
                            String(value || '')
                                .normalize('NFD')
                                .replace(/[\\u0300-\\u036f]/g, '')
                                .replace(/\\s+/g, ' ')
                                .trim()
                                .toLowerCase();
                        const wanted = (labels || []).map(normalize).filter(Boolean);
                        if (!wanted.length) return false;
                        const isVisible = (el) => {
                            if (!el) return false;
                            const style = window.getComputedStyle(el);
                            const rect = el.getBoundingClientRect();
                            return style.display !== 'none'
                                && style.visibility !== 'hidden'
                                && Number(style.opacity || '1') > 0
                                && rect.width > 0
                                && rect.height > 0;
                        };
                        const nodes = Array.from(document.querySelectorAll(
                            'button, a, input[type="button"], input[type="submit"], [role="button"], [role="link"]'
                        ));
                        for (const node of nodes) {
                            if (!isVisible(node)) continue;
                            const text = normalize(
                                node.value || node.getAttribute('aria-label') || node.innerText || node.textContent || ''
                            );
                            if (!text) continue;
                            const matched = wanted.some((label) =>
                                text === label
                                || text.startsWith(`${label} `)
                                || text.endsWith(` ${label}`)
                                || text.includes(` ${label} `)
                            );
                            if (!matched) continue;
                            node.click();
                            return true;
                        }
                        return false;
                    }""",
                    labels,
                )
            )
            if clicked:
                return True
        except Exception:
            pass
        try:
            scope.wait_for_timeout(75)
        except Exception:
            time.sleep(0.075)
    return False


def fill_competencia_periodo(automation: SimplesEcacAutomation, frame: Any, competencia: str) -> bool:
    normalized = _normalize_job_competencia(competencia)
    month, year = _split_competencia(normalized)
    if not normalized:
        return False

    for candidate in (normalized, normalized.replace("/", "")):
        if automation.fill_first_visible(COMPETENCIA_SELECTORS, candidate, scopes=[frame]):
            return True

    month_input_ok = automation.fill_first_visible(
        [
            "input[name*='mes' i]",
            "input[id*='mes' i]",
            "input[name*='month' i]",
            "input[id*='month' i]",
        ],
        month,
        scopes=[frame],
    )
    year_input_ok = automation.fill_first_visible(
        [
            "input[name*='ano' i]",
            "input[id*='ano' i]",
            "input[name*='year' i]",
            "input[id*='year' i]",
        ],
        year,
        scopes=[frame],
    )
    if month_input_ok and year_input_ok:
        return True

    month_select_ok = _select_first_visible_option(
        frame,
        [
            "select[name*='mes' i]",
            "select[id*='mes' i]",
            "select[name*='month' i]",
            "select[id*='month' i]",
        ],
        [month, str(int(month)), f"{int(month):d}"],
    )
    year_select_ok = _select_first_visible_option(
        frame,
        [
            "select[name*='ano' i]",
            "select[id*='ano' i]",
            "select[name*='year' i]",
            "select[id*='year' i]",
        ],
        [year],
    )
    if month_select_ok and year_select_ok:
        return True

    return False


def _extract_portal_message(text: str, expected_message: str) -> str:
    normalized_text = _normalize_portal_text(text)
    normalized_expected = _normalize_portal_text(expected_message)
    if normalized_expected not in normalized_text:
        return ""
    return expected_message


def _extract_known_portal_message(text: str, expected_messages: list[str]) -> str:
    raw_text = str(text or "")
    if not raw_text.strip():
        return ""
    lines = [" ".join(line.split()) for line in raw_text.splitlines() if " ".join(line.split())]
    for expected_message in expected_messages:
        normalized_expected = _normalize_portal_text(expected_message)
        if not normalized_expected:
            continue
        for line in lines:
            if normalized_expected in _normalize_portal_text(line):
                return line
    normalized_text = _normalize_portal_text(raw_text)
    for expected_message in expected_messages:
        normalized_expected = _normalize_portal_text(expected_message)
        if normalized_expected and normalized_expected in normalized_text:
            return expected_message
    return ""


def collect_das_summary(frame: Any) -> dict[str, str]:
    try:
        text = " ".join(frame.locator("body").inner_text(timeout=4000).split())
    except Exception:
        text = ""
    summary = {
        "raw_text": text,
        "saldo_devedor": "",
        "data_vencimento": "",
        "validade_calculo": "",
    }
    patterns = {
        "saldo_devedor": r"saldo\s+devedor[^0-9]*(\d{1,3}(?:\.\d{3})*,\d{2})",
        "data_vencimento": r"vencimento[^0-9]*(\d{2}/\d{2}/\d{4})",
        "validade_calculo": r"(?:validade\s+do\s+calculo|c[aá]lculo\s+v[aá]lido\s+at[eé])[^0-9]*(\d{2}/\d{2}/\d{4})",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            summary[key] = str(match.group(1) or "").strip()
    return summary


def _extract_pdf_text(pdf_path: Path) -> str:
    reader_cls = None
    try:
        from pypdf import PdfReader as _PdfReader  # type: ignore

        reader_cls = _PdfReader
    except Exception:
        try:
            from PyPDF2 import PdfReader as _PdfReader  # type: ignore

            reader_cls = _PdfReader
        except Exception:
            return ""

    try:
        reader = reader_cls(str(pdf_path))
    except Exception:
        return ""

    chunks: list[str] = []
    for page in getattr(reader, "pages", []):
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        if text.strip():
            chunks.append(text)
    return "\n".join(chunks)


def collect_das_pdf_summary(pdf_path: Path) -> dict[str, str]:
    text = " ".join(_extract_pdf_text(pdf_path).split())
    summary = {
        "raw_text": text,
        "data_vencimento": "",
        "pagar_ate": "",
    }
    if not text:
        return summary

    due_date_patterns = [
        r"cnpj\s+raz[aã]o\s+social\s+.+?\s+(?:[a-z]{3,12}/\d{4}|\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+c[oó]digo\s+principal",
        r"per[ií]odo\s+de\s+apura[cç][aã]o\s+data\s+de\s+vencimento\s+n[uú]mero\s+do\s+documento\s+.+?\s+(?:[a-z]{3,12}/\d{4}|\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})",
    ]
    for pattern in due_date_patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            summary["data_vencimento"] = str(match.group(1) or "").strip()
            break

    pagar_ate_match = re.search(
        r"pagar(?:\s+este\s+documento)?\s+at[eé][^0-9]*(\d{2}/\d{2}/\d{4})",
        text,
        flags=re.IGNORECASE,
    )
    if pagar_ate_match:
        summary["pagar_ate"] = str(pagar_ate_match.group(1) or "").strip()

    return summary


def process_company(automation: SimplesEcacAutomation, company: CompanyRecord, company_dir: Path, args: argparse.Namespace) -> CompanyExecutionResult:
    result = CompanyExecutionResult(
        company_id=company.company_id,
        company_name=company.name,
        company_document=company.document,
        status="pending",
        eligible=True,
    )
    competencia = _normalize_job_competencia(getattr(args, "competencia", "") or "")
    nova_data_vencimento = _normalize_job_due_date(getattr(args, "data_vencimento", "") or "")
    dry_run = bool(getattr(args, "dry_run", False) or not getattr(args, "real_generate", False))
    if not competencia:
        raise RuntimeError("Competencia nao informada para emissao do DAS.")

    automation.runtime.logger.info(
        f"{company.name} [{format_document(company.document)}] | Abrindo PGDAS-D para emitir DAS da competencia {competencia}."
    )

    automation.open_pgdas_defis()
    result.flags["pgdas_opened"] = True
    frame = automation.wait_for_pgdas_frame()
    if frame is None:
        raise RuntimeError("Frame do PGDAS-D nao ficou disponivel para emissao do DAS.")

    automation.runtime.logger.info("PGDAS carregado; navegando para Gerar DAS.")
    result.flags["declaracao_mensal_opened"] = automation.click_anywhere(DECLARACAO_MENSAL_LABELS, timeout_ms=6000)
    try:
        result.flags["gerar_das_opened"] = bool(
            frame.evaluate(
                """labels => {
                    const normalize = (value) => String(value || '')
                        .normalize('NFD')
                        .replace(/[\\u0300-\\u036f]/g, '')
                        .replace(/\\s+/g, ' ')
                        .trim()
                        .toLowerCase();
                    const wanted = (labels || []).map(normalize);
                    const nodes = Array.from(document.querySelectorAll('a, button, [role="button"], [role="link"]'));
                    const exact = nodes.find((node) => {
                        const text = normalize(node.innerText || node.textContent || '');
                        return wanted.some((label) => text === normalize(label));
                    });
                    const partial = nodes.find((node) => {
                        const text = normalize(node.innerText || node.textContent || '');
                        return wanted.some((label) => text.includes(normalize(label)));
                    });
                    const target = exact || partial;
                    if (!target) return false;
                    target.click();
                    return true;
                }""",
                GERAR_DAS_LABELS,
            )
        )
    except Exception:
        result.flags["gerar_das_opened"] = False
    if not result.flags["gerar_das_opened"]:
        result.flags["gerar_das_opened"] = automation.click_anywhere(GERAR_DAS_LABELS, timeout_ms=6000)
    automation.runtime.logger.info(
        f"Menu Gerar DAS acionado declaracao_mensal_opened={result.flags['declaracao_mensal_opened']} gerar_das_opened={result.flags['gerar_das_opened']}."
    )
    ready = automation._wait_until(
        lambda: any(
            token in _normalize_portal_text(automation.collect_scope_text(frame))
            for token in (
                "periodo de apuracao",
                "informe o periodo de apuracao",
                "informe o periodo para geracao do das",
                "geracao do das",
            )
        ),
        timeout_ms=15000,
        interval_ms=250,
    )
    if not ready:
        raise RuntimeError("A tela 'Informe o periodo de apuracao' nao foi carregada.")

    automation.runtime.logger.info(f"Tela de periodo de apuracao pronta; preenchendo competencia {competencia}.")
    result.flags["competencia_informada"] = fill_competencia_periodo(automation, frame, competencia)
    if not result.flags["competencia_informada"]:
        automation._capture_debug("emitir_guia_competencia_fill_failed")
        raise RuntimeError(f"Nao foi possivel preencher a competencia {competencia}.")

    result.flags["continuar_clicked"] = click_labels_in_scope(frame, CONTINUAR_LABELS, timeout_ms=6000)
    if not result.flags["continuar_clicked"]:
        result.flags["continuar_clicked"] = automation.click_anywhere(CONTINUAR_LABELS, timeout_ms=6000)
    automation.runtime.logger.info(
        f"Competencia preenchida; continuar_clicked={result.flags['continuar_clicked']}. Aguardando retorno do portal."
    )
    known_portal_messages = [
        SEM_VALOR_MESSAGE,
        DAS_DECLARACAO_NAO_TRANSMITIDA_MESSAGE,
    ]
    summary_ready = automation._wait_until(
        lambda: any(
            token in _normalize_portal_text(automation.collect_scope_text(frame))
            for token in (
                _normalize_portal_text(SEM_VALOR_MESSAGE),
                _normalize_portal_text(DAS_DECLARACAO_NAO_TRANSMITIDA_MESSAGE),
                "msg_e0117",
                "saldo devedor",
                "consolidar para outra data",
                "validade do calculo",
                "calculo valido ate",
                "data de vencimento",
            )
        ),
        timeout_ms=12000,
        interval_ms=250,
    )
    body_text = automation.collect_scope_text(frame)
    portal_message = _extract_known_portal_message(body_text, known_portal_messages)
    if portal_message == SEM_VALOR_MESSAGE:
        automation.runtime.logger.info("Portal retornou mensagem de sem valor devido para a competencia informada.")
        result.status = "empty"
        result.records.append(
            default_company_payload(
                company,
                {
                    "competencia": competencia,
                    "portal_message": portal_message,
                    "data_vencimento": "",
                    "validade_calculo": "",
                    "saldo_devedor": "",
                },
            )
        )
        automation.return_to_ecac_home()
        return result
    if portal_message:
        automation.runtime.logger.warning(f"Portal retornou mensagem impeditiva para a competencia: {portal_message}")
        result.status = "error"
        result.errors.append(portal_message)
        result.records.append(
            default_company_payload(
                company,
                {
                    "competencia": competencia,
                    "portal_message": portal_message,
                    "data_vencimento": "",
                    "validade_calculo": "",
                    "saldo_devedor": "",
                },
            )
        )
        automation.return_to_ecac_home()
        return result
    if not summary_ready:
        automation._capture_debug("emitir_guia_resumo_nao_carregado")
        raise RuntimeError("A tela de resumo do DAS nao foi carregada apos informar a competencia.")

    summary = collect_das_summary(frame)
    automation.runtime.logger.info(
        "Resumo do DAS carregado "
        f"saldo_devedor={summary.get('saldo_devedor') or ''} "
        f"data_vencimento={summary.get('data_vencimento') or ''} "
        f"validade_calculo={summary.get('validade_calculo') or ''}."
    )
    result.flags["saldo_devedor"] = summary.get("saldo_devedor") or ""
    result.flags["data_vencimento"] = summary.get("data_vencimento") or ""
    result.flags["validade_calculo"] = summary.get("validade_calculo") or ""

    if nova_data_vencimento:
        result.flags["recalculo_opened"] = click_labels_in_scope(
            frame,
            CONSOLIDAR_OUTRA_DATA_LABELS,
            timeout_ms=6000,
        )
        if not result.flags["recalculo_opened"]:
            result.flags["recalculo_opened"] = automation.click_anywhere(
                CONSOLIDAR_OUTRA_DATA_LABELS,
                timeout_ms=6000,
            )
        if not result.flags["recalculo_opened"]:
            raise RuntimeError("A opcao 'Consolidar para outra data' nao foi localizada.")
        if not automation.fill_first_visible(RECALCULO_SELECTORS, nova_data_vencimento, scopes=[frame]):
            raise RuntimeError(f"Nao foi possivel preencher a nova data {nova_data_vencimento}.")
        result.flags["recalculo_confirmado"] = click_labels_in_scope(
            frame,
            CONTINUAR_LABELS + ["Consolidar"],
            timeout_ms=6000,
        )
        if not result.flags["recalculo_confirmado"]:
            result.flags["recalculo_confirmado"] = automation.click_anywhere(
                CONTINUAR_LABELS + ["Consolidar"],
                timeout_ms=6000,
            )
        applied = automation._wait_until(
            lambda: nova_data_vencimento in automation.collect_scope_text(frame),
            timeout_ms=15000,
            interval_ms=250,
        )
        if not applied:
            raise RuntimeError(f"A nova data {nova_data_vencimento} nao foi aplicada pelo portal.")
        summary = collect_das_summary(frame)
        result.flags["validade_calculo"] = summary.get("validade_calculo") or nova_data_vencimento

    if dry_run:
        result.status = "partial"
        result.warnings.append("Dry-run habilitado; a emissao real do DAS nao foi executada.")
        result.records.append(
            default_company_payload(
                company,
                {
                    "competencia": competencia,
                    "data_vencimento": summary.get("data_vencimento") or "",
                    "validade_calculo": summary.get("validade_calculo") or "",
                    "saldo_devedor": summary.get("saldo_devedor") or "",
                },
            )
        )
        automation.return_to_ecac_home()
        return result

    download_path = company_dir / f"DAS - {competencia.replace('/', '-')}.pdf"
    automation.runtime.logger.info(f"Tentando capturar o DAS em {download_path}.")
    saved = automation.expect_download_click(frame, DOWNLOAD_DAS_LABELS, download_path)
    if saved is not None and saved.exists():
        pdf_summary = collect_das_pdf_summary(saved)
        if pdf_summary.get("data_vencimento"):
            summary["data_vencimento"] = pdf_summary["data_vencimento"]
            result.flags["data_vencimento_pdf"] = pdf_summary["data_vencimento"]
        if pdf_summary.get("pagar_ate"):
            result.flags["pagar_ate"] = pdf_summary["pagar_ate"]
        if pdf_summary.get("data_vencimento") or pdf_summary.get("pagar_ate"):
            automation.runtime.logger.info(
                "PDF do DAS lido "
                f"data_vencimento={pdf_summary.get('data_vencimento') or ''} "
                f"pagar_ate={pdf_summary.get('pagar_ate') or ''}."
            )
        result.files.append({"kind": "das", "path": str(saved), "filename": saved.name})
        result.status = "success"
    else:
        result.status = "partial"
        result.warnings.append("O portal carregou o resumo do DAS, mas o download nao foi capturado.")

    result.records.append(
        default_company_payload(
            company,
            {
                "competencia": competencia,
                "data_vencimento": summary.get("data_vencimento") or "",
                "validade_calculo": summary.get("validade_calculo") or "",
                "saldo_devedor": summary.get("saldo_devedor") or "",
                "pagar_ate": result.flags.get("pagar_ate") or "",
                "portal_message": "",
            },
        )
    )
    automation.return_to_ecac_home()
    return result


try:
    from PySide6.QtCore import QThread, QTimer, Signal
    from PySide6.QtWidgets import (
        QApplication,
        QAbstractItemView,
        QCheckBox,
        QFormLayout,
        QGroupBox,
        QHBoxLayout,
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
    )
except Exception:
    QApplication = None  # type: ignore[assignment]
    QThread = object  # type: ignore[misc,assignment]
    Signal = lambda *args, **kwargs: object()  # type: ignore[misc,assignment]


class SimplesWorker(QThread):  # type: ignore[misc]
    log = Signal(str)
    status = Signal(str)
    progress = Signal(object)
    summary_ready = Signal(object)
    error = Signal(str)

    def __init__(self, args: argparse.Namespace, companies: list[CompanyRecord], job: Optional[JobPayload]) -> None:
        super().__init__()
        self.args = args
        self.companies = companies
        self.job = job
        self._stop_requested = False
        self.runner: Optional[RobotRunner] = None

    def request_stop(self) -> None:
        self._stop_requested = True
        if self.runner is not None:
            self.runner.request_stop()

    def run(self) -> None:
        try:
            self.status.emit("Executando")
            runner = RobotRunner(
                technical_id=ROBOT_TECHNICAL_ID,
                display_name=ROBOT_DISPLAY_NAME,
                args=self.args,
                job=self.job,
                runtime_dir=None,
                process_company=process_company,
            )
            self.runner = runner
            runner.runtime.logger.bind_sink(self.log.emit)
            summary = runner.run()
            self.summary_ready.emit(summary)
            self.status.emit("Concluido")
        except Exception as exc:
            self.error.emit(str(exc))
            self.status.emit("Falha")
        finally:
            self.runner = None


class MainWindow(QMainWindow):
    def __init__(self, runtime_env: RuntimeEnvironment) -> None:
        super().__init__()
        self.runtime = runtime_env
        clear_stop_signal(self.runtime)
        self.dashboard = DashboardClient(runtime_env, ROBOT_TECHNICAL_ID, ROBOT_DISPLAY_NAME)
        self.job = self.runtime.json_runtime.load_job()
        self._last_job_execution_request_id = self._job_execution_request_id(self.job)
        self.office_context = self.dashboard.resolve_office_context(self.job)
        self.worker: Optional[SimplesWorker] = None
        self.companies: list[CompanyRecord] = []
        self.filtered_companies: list[CompanyRecord] = []
        self.robot_dashboard_id: Optional[str] = None
        self.robot_heartbeat_timer = QTimer(self)
        self.robot_heartbeat_timer.setInterval(30000)
        self.robot_heartbeat_timer.timeout.connect(self._on_robot_heartbeat)
        self.job_poll_timer = QTimer(self)
        self.job_poll_timer.setInterval(5000)
        self.job_poll_timer.timeout.connect(self._on_job_poll)
        self.setWindowTitle(ROBOT_DISPLAY_NAME)
        self.resize(1180, 760)
        self._build_ui()
        self.runtime.logger.bind_sink(self.append_log)
        self.runtime.json_runtime.register_robot({"display_name": ROBOT_DISPLAY_NAME})
        self.robot_dashboard_id = self.dashboard.register_robot_presence(status="active")
        self.reload_companies()
        self.robot_heartbeat_timer.start()
        self.job_poll_timer.start()
        if self.job:
            QTimer.singleShot(250, lambda: self.start_worker(auto_triggered=True))

    def _build_ui(self) -> None:
        central = QWidget(); self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.addWidget(QLabel(ROBOT_DISPLAY_NAME))
        top = QHBoxLayout(); layout.addLayout(top)
        left = QGroupBox("Empresas"); left_l = QVBoxLayout(left)
        self.search_input = QLineEdit(); self.search_input.textChanged.connect(self._apply_company_filter); left_l.addWidget(self.search_input)
        self.company_list = QListWidget(); self.company_list.setSelectionMode(QAbstractItemView.NoSelection); left_l.addWidget(self.company_list)
        top.addWidget(left, 2)
        right_box = QWidget(); right = QVBoxLayout(right_box)
        self.status_label = QLabel("Pronto"); self.progress_label = QLabel("0 / 0"); self.office_label = QLabel(self.office_context.office_id or "office_id nao resolvido")
        for label, widget in (("Status", self.status_label), ("Progresso", self.progress_label), ("office_id", self.office_label)):
            right.addWidget(QLabel(label)); right.addWidget(widget)
        controls = QHBoxLayout(); self.start_button = QPushButton("Iniciar"); self.start_button.clicked.connect(self.start_worker); self.stop_button = QPushButton("Parar"); self.stop_button.clicked.connect(self.stop_worker); self.stop_button.setEnabled(False); controls.addWidget(self.start_button); controls.addWidget(self.stop_button); right.addLayout(controls)
        top.addWidget(right_box, 1)
        self.log_panel = QTextEdit(); self.log_panel.setReadOnly(True); layout.addWidget(self.log_panel)

    def append_log(self, message: str) -> None:
        self.log_panel.append(message)

    def _job_execution_request_id(self, job: Optional[JobPayload]) -> str:
        if not job:
            return ""
        return str(job.execution_request_id or job.job_id or "").strip()

    def _fallback_manual_company(self) -> list[CompanyRecord]:
        try:
            certificate = ensure_default_certificate(self.runtime, [])
            company = build_company_from_certificate(certificate, source="ui_certificate_default")
            return [company] if company else []
        except Exception:
            return []

    def reload_companies(self) -> None:
        self.job = self.runtime.json_runtime.load_job()
        self.office_context = self.dashboard.resolve_office_context(self.job, force_refresh=True)
        try:
            if self.job:
                self.companies = self.dashboard.load_companies(self.office_context, job=self.job, company_ids=self.job.company_ids or None)
            else:
                self.companies = self.dashboard.load_companies(self.office_context, job=None, company_ids=None)
        except Exception:
            self.companies = self._fallback_manual_company()
        self.progress_label.setText(f"0 / {len(self.companies)}")
        self.start_button.setEnabled(bool(self.companies))
        self._apply_company_filter()

    def _apply_company_filter(self) -> None:
        search = self.search_input.text().strip().lower(); self.filtered_companies = []; self.company_list.clear()
        for company in self.companies:
            if search and search not in company.search_text: continue
            self.filtered_companies.append(company)
            self.company_list.addItem(QListWidgetItem(f"{company.name}\n{format_document(company.document)}"))

    def _build_args(self) -> argparse.Namespace:
        return argparse.Namespace(
            no_ui=False,
            job_mode=bool(self.job),
            headless=False,
            pfx_path="",
            pfx_password="",
            company_id="",
            company_name="",
            company_document="",
            competencia="",
            data_vencimento="",
            dry_run=True,
            real_generate=False,
        )

    def _build_manual_job(self) -> JobPayload:
        execution_id = f"manual_{uuid.uuid4().hex}"; company_ids = [c.company_id for c in self.companies if str(c.company_id).strip()]; company_rows = [c.to_dict() for c in self.companies]
        return JobPayload(job_id=execution_id, execution_request_id=execution_id, office_id=self.office_context.office_id, company_ids=company_ids, companies=company_rows, raw={"job_id": execution_id, "execution_request_id": execution_id, "office_id": self.office_context.office_id, "company_ids": company_ids, "companies": company_rows, "source": "manual_ui"})

    def start_worker(self, auto_triggered: bool = False) -> None:
        if self.worker is not None and self.worker.isRunning():
            return
        execution_job = self.job or self._build_manual_job()
        if not self.companies:
            self.reload_companies()
        if not self.companies:
            self.append_log("Nenhuma empresa carregada para a execucao automatica." if auto_triggered else "Nenhuma empresa carregada para a execucao.")
            if not auto_triggered:
                QMessageBox.warning(self, "Empresas", "Nenhuma empresa foi carregada do dashboard.")
            return
        self.job = execution_job
        self._last_job_execution_request_id = self._job_execution_request_id(execution_job)
        self.worker = SimplesWorker(self._build_args(), list(self.companies), execution_job)
        self.worker.log.connect(self.append_log)
        self.worker.status.connect(self.status_label.setText)
        self.worker.progress.connect(self._on_progress)
        self.worker.summary_ready.connect(self._on_summary)
        self.worker.error.connect(self._on_worker_error)
        self.worker.finished.connect(self._on_worker_finished)
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.dashboard.update_robot_presence(status="processing", robot_id=self.robot_dashboard_id or "")
        if auto_triggered:
            self.append_log(f"Job detectado automaticamente: {self._last_job_execution_request_id}. Iniciando execucao.")
        self.worker.start()

    def stop_worker(self) -> None:
        if self.worker is not None:
            self.worker.request_stop()
            self.status_label.setText("Parando")

    def _on_progress(self, payload: object) -> None:
        if isinstance(payload, dict): self.progress_label.setText(f"{payload.get('current', 0)} / {payload.get('total', 0)}")

    def _on_summary(self, summary: object) -> None:
        if isinstance(summary, dict): self.status_label.setText(str(summary.get("status") or "Concluido"))

    def _on_worker_error(self, message: str) -> None:
        self.append_log(f"Erro: {message}")
        self.status_label.setText("Falha")
        self.dashboard.update_robot_presence(status="active", robot_id=self.robot_dashboard_id or "")

    def _on_worker_finished(self) -> None:
        self.start_button.setEnabled(bool(self.companies))
        self.stop_button.setEnabled(False)
        self.worker = None
        self.dashboard.update_robot_presence(status="active", robot_id=self.robot_dashboard_id or "")
        self.reload_companies()
        pending_execution_request_id = self._job_execution_request_id(self.job)
        if pending_execution_request_id and pending_execution_request_id != self._last_job_execution_request_id:
            self.append_log(
                f"Novo job pendente detectado apos a conclusao: {pending_execution_request_id}. Iniciando automaticamente."
            )
            QTimer.singleShot(250, lambda: self.start_worker(auto_triggered=True))

    def _consume_stop_signal(self) -> bool:
        payload = read_stop_signal(self.runtime)
        if not payload:
            return False
        clear_stop_signal(self.runtime)
        reason = str(payload.get("reason") or "Solicitacao de parada recebida do SaaS.").strip()
        self.append_log(f"Sinal remoto de parada recebido: {reason}")
        try:
            self.runtime.json_runtime.job_path.unlink(missing_ok=True)
        except Exception:
            pass
        self.job = None
        self._last_job_execution_request_id = ""
        if self.worker is not None and self.worker.isRunning():
            self.stop_worker()
        else:
            self.status_label.setText("Pronto")
            self.runtime.json_runtime.write_heartbeat(status="active", message="stop_signal_consumed")
            self.dashboard.update_robot_presence(status="active", robot_id=self.robot_dashboard_id or "")
            self.reload_companies()
        return True

    def _on_job_poll(self) -> None:
        if self._consume_stop_signal():
            return
        if self.worker is not None and self.worker.isRunning():
            return
        pending_job = self.runtime.json_runtime.load_job()
        pending_execution_request_id = self._job_execution_request_id(pending_job)
        if not pending_execution_request_id:
            return
        if pending_execution_request_id == self._last_job_execution_request_id:
            return
        self.job = pending_job
        self.reload_companies()
        if self.job and self._job_execution_request_id(self.job) == pending_execution_request_id:
            self.start_worker(auto_triggered=True)

    def _on_robot_heartbeat(self) -> None:
        self._consume_stop_signal()
        status = "processing" if self.worker is not None and self.worker.isRunning() else "active"
        self.runtime.json_runtime.write_heartbeat(status=status, message="ui_heartbeat")
        self.dashboard.update_robot_presence(status=status, robot_id=self.robot_dashboard_id or "")

    def closeEvent(self, event) -> None:  # type: ignore[override]
        if self.worker is not None and self.worker.isRunning():
            self.worker.request_stop()
            self.worker.wait(3000)
        self.robot_heartbeat_timer.stop()
        self.job_poll_timer.stop()
        self.dashboard.update_robot_presence(status="inactive", robot_id=self.robot_dashboard_id or "")
        self.runtime.mark_inactive()
        cleanup_runtime_artifacts(self.runtime)
        super().closeEvent(event)


def _run_cli(args: argparse.Namespace) -> int:
    runner = RobotRunner(
        technical_id=ROBOT_TECHNICAL_ID,
        display_name=ROBOT_DISPLAY_NAME,
        args=args,
        job=None,
        runtime_dir=None,
        process_company=process_company,
    )
    summary = runner.run(); print(json.dumps(summary, ensure_ascii=False, indent=2)); return 0 if summary.get("status") in {"completed", "partial"} else 1


def main() -> int:
    args = parse_args()
    if args.no_ui: return _run_cli(args)
    if QApplication is None: raise RuntimeError("PySide6 nao esta disponivel para a interface grafica.")
    app = QApplication(sys.argv); runtime_env = build_runtime(ROBOT_TECHNICAL_ID, ROBOT_DISPLAY_NAME, runtime_dir=None); window = MainWindow(runtime_env); window.show(); return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
