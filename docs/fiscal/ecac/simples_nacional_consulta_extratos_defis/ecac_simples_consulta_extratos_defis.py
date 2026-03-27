from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import shutil
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


def _load_ecac_base():
    cache_key = "_ecac_caixa_postal_singlefile_module"
    if cache_key in sys.modules:
        return sys.modules[cache_key]

    base_path = (
        Path(__file__).resolve().parents[3]
        / "ecac"
        / "caixa postal"
        / "ecac_caixa_postal.py"
    )
    spec = importlib.util.spec_from_file_location(cache_key, base_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Nao foi possivel carregar o modulo base do e-CAC em {base_path}.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[cache_key] = module
    spec.loader.exec_module(module)
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


def build_runtime(
    technical_id: str,
    display_name: str,
    *,
    sink: Optional[Callable[[str], None]] = None,
    runtime_dir: Optional[Path] = None,
) -> RuntimeEnvironment:
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
          [DllImport("user32.dll")] public static extern bool SetCursorPos(int X, int Y);
          [DllImport("user32.dll")] public static extern void mouse_event(uint dwFlags, uint dx, uint dy, uint dwData, UIntPtr dwExtraInfo);
          [DllImport("user32.dll")] public static extern bool GetCursorPos(out POINT lpPoint);
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
            throw 'Janela "Selecione um certificado" nao apareceu dentro do timeout.'
        }}

        function Matches-Target([string]$name, $targets) {{
            if (-not $name) {{ return $false }}
            foreach ($target in $targets) {{
                if (-not $target) {{ continue }}
                if ($name -eq $target -or $name.Contains($target) -or $target.Contains($name)) {{
                    return $true
                }}
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
            Start-Sleep -Milliseconds 25
            [NativeCertUi]::mouse_event(0x0004, 0, 0, 0, [UIntPtr]::Zero)
            Start-Sleep -Milliseconds 180
            return $true
        }}

        function Wait-Closed($root, $dialogTitle) {{
            $closeDeadline = (Get-Date).AddSeconds(12)
            while ((Get-Date) -lt $closeDeadline) {{
                $condition = New-Object System.Windows.Automation.PropertyCondition(
                    [System.Windows.Automation.AutomationElement]::NameProperty,
                    $dialogTitle
                )
                $stillOpen = $root.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $condition)
                if (-not $stillOpen) {{
                    return $true
                }}
                Start-Sleep -Milliseconds 200
            }}
            return $false
        }}

        $cursor = New-Object POINT
        [NativeCertUi]::GetCursorPos([ref]$cursor) | Out-Null
        try {{
            while ((Get-Date) -lt $deadline) {{
                $all = $dialog.FindAll(
                    [System.Windows.Automation.TreeScope]::Descendants,
                    [System.Windows.Automation.Condition]::TrueCondition
                )
                $matches = @()
                $okButton = $null
                for ($i = 0; $i -lt $all.Count; $i++) {{
                    $item = $all.Item($i)
                    $type = [string]$item.Current.ControlType.ProgrammaticName
                    if ($type -eq 'ControlType.DataItem' -and (Matches-Target ([string]$item.Current.Name) $targets)) {{
                        $matches += $item
                    }}
                    if ($type -eq 'ControlType.Button' -and [string]$item.Current.Name -eq 'OK') {{
                        $okButton = $item
                    }}
                }}

                if ($matches -and $matches.Count -gt 0 -and $okButton) {{
                    foreach ($row in $matches) {{
                        Click-Center $row | Out-Null
                        Click-Center $row | Out-Null
                        Click-Center $okButton | Out-Null
                        if (Wait-Closed $root $dialogTitle) {{
                            exit 0
                        }}
                    }}
                }}
                Start-Sleep -Milliseconds 250
            }}
        }} finally {{
            [NativeCertUi]::SetCursorPos($cursor.X, $cursor.Y) | Out-Null
        }}

        throw 'Nao foi possivel selecionar o certificado alvo na janela nativa.'
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

    def _capture_debug(self, prefix: str) -> None:
        try:
            self._capture_debug_artifacts(prefix)
        except Exception:
            pass

    def _profile_switch_success_relaxed(self, company: CompanyRecord) -> bool:
        targets = [target for target in self._profile_switch_targets(company) if target]
        if not targets:
            return False
        context_text = self._current_profile_context().lower()
        body_text = self._body_text().lower()
        return any(target in context_text or target in body_text for target in targets)

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

    def _find_company_input(self, company: CompanyRecord) -> Optional[Any]:
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

        for scope in self._profile_scopes():
            for form_selector, field_selector in preferred_pairs:
                try:
                    form = scope.locator(form_selector).first
                    if form.count() < 1:
                        continue
                    form.wait_for(state="visible", timeout=700)
                    field = form.locator(field_selector).first
                    field.wait_for(state="visible", timeout=700)
                    if not self._is_valid_profile_input(field):
                        continue
                    field.scroll_into_view_if_needed(timeout=1000)
                    self._last_profile_form = form
                    return field
                except Exception:
                    continue

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
                        field.wait_for(state="visible", timeout=500)
                        if not self._is_valid_profile_input(field):
                            continue
                        field.scroll_into_view_if_needed(timeout=1000)
                        try:
                            self._last_profile_form = field.locator("xpath=ancestor::form[1]")
                        except Exception:
                            self._last_profile_form = None
                        return field
                    except Exception:
                        continue
        return None

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
        try:
            locator.wait_for(state="visible", timeout=min(timeout_ms, 1500))
        except Exception:
            pass
        try:
            locator.scroll_into_view_if_needed(timeout=1200)
        except Exception:
            pass
        try:
            locator.click(timeout=timeout_ms, force=True)
            return True
        except Exception:
            pass
        try:
            locator.evaluate(
                """el => {
                    if (!el) {
                        return false;
                    }
                    el.click();
                    return true;
                }"""
            )
            return True
        except Exception:
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
        labels = ["Nao lidas", "Nao visualizadas"]
        normalized_labels = {_normalize_portal_text(label) for label in labels}
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
        locator = _visible_rows_locator()

        if locator is None and self._is_message_detail_view():
            opened += 1
            try:
                self.page.wait_for_timeout(1000)
            except Exception:
                time.sleep(1.0)
            self._return_to_message_list()
            locator = _visible_rows_locator()

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
                if fallback_index is None and (filter_applied or page_cycles == 0):
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
                self.page.wait_for_timeout(750)
            except Exception:
                time.sleep(0.75)
        raise HCaptchaVisibleError("O hCaptcha permaneceu visivel apos o timeout configurado.")

    def login(self) -> None:
        if self.is_authenticated() and self.certificate_context_matches():
            self.resolve_required_mailbox_notice()
            self.close_blocking_modals()
            return
        last_error: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                self.page.goto(LOGIN_URL, wait_until="domcontentloaded")
                if self.is_authenticated() and self.certificate_context_matches():
                    self.resolve_required_mailbox_notice()
                    self.close_blocking_modals()
                    return
                self._click_access_gov_br()
                self._wait_until(
                    lambda: "acesso.gov.br" in self.page.url or self._page_contains("Seu certificado digital"),
                    timeout_ms=30000,
                )
                self.page.wait_for_timeout(3500)
                self._click_cert_login()
                select_certificate_dialog_strong(self.certificate)
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
                        self.handle_captcha_if_present()
                    if HCAPTCHA_ERROR_URL_TOKEN.lower() in self.page.url.lower():
                        self.handle_captcha_if_present()
                    try:
                        self.page.wait_for_timeout(500)
                    except Exception:
                        time.sleep(0.5)
                raise RuntimeError("O e-CAC nao confirmou a sessao autenticada dentro do timeout.")
            except Exception as exc:
                last_error = exc
                self.runtime.logger.warning(f"login_failed attempt={attempt}: {exc}")
                self._capture_debug(f"login_failed_attempt_{attempt}")
                try:
                    self.page.goto(
                        "https://cav.receita.fazenda.gov.br/autenticacao/Login/Logout",
                        wait_until="domcontentloaded",
                        timeout=30000,
                    )
                except Exception:
                    pass
                if attempt < 3:
                    time.sleep(1.0 * attempt)
        raise RuntimeError(str(last_error or "Falha desconhecida no login do e-CAC."))

    def ensure_logged_in(self) -> None:
        if self.is_authenticated() and self.certificate_context_matches():
            self.resolve_required_mailbox_notice()
            return
        self.login()

    def _finalize_profile_switch(self, company: CompanyRecord, initial_state: str = "") -> bool:
        deadline = time.time() + 45.0
        state = initial_state
        mailbox_processed = False
        while time.time() < deadline:
            if self._profile_switch_success(company):
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
                self.page.wait_for_timeout(500)
            except Exception:
                time.sleep(0.5)
        return self._profile_switch_success(company)

    def switch_company_profile(self, company: CompanyRecord) -> str:
        self.ensure_logged_in()
        last_error: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                self.close_blocking_modals()
                self._log_step(f"clicked_alterar_acesso attempt={attempt}")
                if not self._click_by_label(PROFILE_MENU_LABELS, timeout_ms=7000):
                    raise RuntimeError("Fluxo de 'Alterar perfil de acesso' nao foi localizado.")
                visible = self._wait_until(
                    lambda: self._profile_modal_visible(company) or self._find_company_input(company) is not None,
                    timeout_ms=15000,
                )
                if not visible:
                    raise RuntimeError("Modal de alteracao de perfil nao ficou visivel.")
                self._log_step("modal_visible")
                if not self._fill_company_search(company):
                    raise RuntimeError("Campo de busca do perfil nao foi localizado ou nao aceitou o CNPJ.")
                selected = self._wait_until(
                    lambda: self._select_company_in_profile(company),
                    timeout_ms=4000,
                    interval_ms=350,
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
                    timeout_ms=20000,
                    interval_ms=350,
                )
                if not modal_closed:
                    raise RuntimeError("O modal de alteracao de perfil permaneceu aberto apos a confirmacao.")
                modal_error = self._extract_profile_modal_error()
                if modal_error:
                    lowered_error = modal_error.lower()
                    if "não existe procuração eletrônica" in lowered_error or "nao existe procuracao eletronica" in lowered_error:
                        raise ecac_base.ProfileSwitchNoPowerOfAttorneyError(
                            f"Falha retornada pelo portal ao alterar perfil: {modal_error}"
                        )
                    raise ecac_base.ProfileSwitchError(f"Falha retornada pelo portal ao alterar perfil: {modal_error}")
                cleanup_state = self._after_profile_switch_cleanup()
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
                        self.page.wait_for_timeout(600 * attempt)
                    except Exception:
                        pass
        raise RuntimeError(str(last_error or "Falha desconhecida ao trocar o perfil da empresa."))

    def return_to_ecac_home(self) -> None:
        self.ensure_logged_in()
        try:
            self.page.goto("https://cav.receita.fazenda.gov.br/ecac/", wait_until="domcontentloaded", timeout=120000)
        except Exception:
            return
        if self.required_mailbox_notice_visible():
            self.resolve_required_mailbox_notice()
        self.close_blocking_modals()

    def ensure_company_profile(self, company: CompanyRecord) -> tuple[bool, str]:
        self.ensure_logged_in()
        self.return_to_ecac_home()
        self.resolve_required_mailbox_notice()
        current_context = self._current_profile_context()
        if company.document and only_digits(company.document) and only_digits(company.document) in only_digits(current_context):
            return False, current_context
        if company.name and company.name.lower() in current_context.lower():
            return False, current_context
        switched_context = self.switch_company_profile(company)
        self.return_to_ecac_home()
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
            interval_ms=500,
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
                self.page.wait_for_timeout(500)
            except Exception:
                time.sleep(0.5)
        return None

    def wait_for_pgdas_ready(self, timeout_ms: int = 120000) -> Any:
        frame = self.wait_for_pgdas_frame(timeout_ms=timeout_ms)
        if frame is None:
            raise RuntimeError("Frame do PGDAS-D / DEFIS nao foi localizado.")
        deadline = time.time() + (timeout_ms / 1000)
        while time.time() < deadline:
            try:
                text = frame.locator("body").inner_text(timeout=2000)
            except Exception:
                text = ""
            lowered = text.lower()
            if any(marker.lower() in lowered for marker in PGDAS_READY_MARKERS):
                return frame
            try:
                self.page.wait_for_timeout(500)
            except Exception:
                time.sleep(0.5)
        return frame

    def wait_for_defis_frame(self, timeout_ms: int = 120000):
        deadline = time.time() + (timeout_ms / 1000)
        while time.time() < deadline:
            frame = self._find_frame_by_url_tokens("defis.app")
            if frame is not None:
                return frame
            try:
                self.page.wait_for_timeout(500)
            except Exception:
                time.sleep(0.5)
        return None

    def click_anywhere(self, labels: list[str], *, timeout_ms: int = 7000) -> bool:
        return self._click_by_label(labels, timeout_ms=timeout_ms)

    def fill_first_visible(self, selectors: list[str], value: str, *, scopes: Optional[list[Any]] = None) -> bool:
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
                        field.wait_for(state="visible", timeout=1200)
                        field.click(timeout=1200)
                        field.fill("")
                        field.fill(value)
                        try:
                            field.press("Tab")
                        except Exception:
                            pass
                        return True
                    except Exception:
                        continue
        return False

    def collect_scope_text(self, scope: Any) -> str:
        try:
            return scope.locator("body").inner_text(timeout=4000)
        except Exception:
            return ""

    def navigate_scope(self, scope: Any, url: str, *, timeout_ms: int = 120000) -> None:
        scope.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            self.page.wait_for_load_state("networkidle", timeout=5000)
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
    if getattr(args, "job_mode", False):
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
        runtime_dir: Optional[Path],
        process_company: Callable[[SimplesEcacAutomation, CompanyRecord, Path, argparse.Namespace], CompanyExecutionResult],
    ) -> None:
        self.technical_id = technical_id
        self.display_name = display_name
        self.args = args
        self.process_company = process_company
        self.runtime = build_runtime(technical_id, display_name, sink=lambda line: print(line, flush=True), runtime_dir=runtime_dir)
        self.dashboard = DashboardClient(self.runtime, technical_id, display_name)
        self.stop_requested = False

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
        job = load_job_or_manual_payload(self.runtime, self.args)
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
            total_steps = len(companies) + (1 if auth_company and all(item.company_id != auth_company.company_id for item in companies) else 0)
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
                session = launch_browser(self.runtime, headless=bool(getattr(self.args, "headless", False)))
                self.runtime.logger.info(f"Navegador iniciado em {session.executable_path} para {company_label}")
                automation = SimplesEcacAutomation(
                    self.runtime,
                    session,
                    certificate,
                    stop_requested=lambda: self.stop_requested,
                    headless=bool(getattr(self.args, "headless", False)),
                    allow_manual_captcha=not bool(getattr(self.args, "headless", False)),
                )
                automation.login()
                return session, automation

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
                    processed = self.process_company(automation, company, company_dir, self.args)
                    processed.profile_switched = switched
                    processed.company_profile_context = processed.company_profile_context or context
                    processed.started_at = result.started_at
                    processed.finished_at = utc_now_iso()
                    return processed
                except Exception as exc:
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
                try:
                    session, automation = _open_authenticated_automation(office_certificate, auth_company.name)
                    processed_contexts += 1
                    heartbeat_state.update({"current": processed_contexts, "company_name": auth_company.name, "company_id": auth_company.company_id})
                    results.append(_process_company_with_automation(automation, auth_company, switch_profile=False))
                    for company in companies:
                        if company.company_id == auth_company.company_id:
                            continue
                        if self.stop_requested:
                            self.runtime.logger.info("Execucao interrompida pelo usuario.")
                            break
                        processed_contexts += 1
                        heartbeat_state.update({"current": processed_contexts, "company_name": company.name, "company_id": company.company_id})
                        results.append(_process_company_with_automation(automation, company, switch_profile=True))
                finally:
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
                        self.runtime.logger.warning(f"company_failed {company.name}: {exc}")
                        results.append(_error_result(company, _stack_tail(exc)))
                    finally:
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

ROBOT_TECHNICAL_ID = "ecac_simples_consulta_extratos_defis"
ROBOT_DISPLAY_NAME = "e-CAC - Simples Nacional - Consulta Extratos e DEFIS"

CONSULTAR_DECLARACOES_LABELS = ["Consultar Declaracoes", "Consultar declaracoes", "Consultar declaracao"]
DEFIS_LABELS = ["DEFIS", "Declaracao Anual", "Declaracao anual", "Imprimir DEFIS"]
EXTRATO_DOWNLOAD_LABELS = ["Imprimir extrato", "Extrato", "Baixar extrato"]
DECLARACAO_DOWNLOAD_LABELS = ["Declaracao", "Imprimir declaracao"]
RECIBO_DOWNLOAD_LABELS = ["Recibo", "Imprimir recibo"]


def parse_args() -> argparse.Namespace:
    return build_common_parser(ROBOT_DISPLAY_NAME).parse_args()


def _rows_with_pa(frame) -> list[tuple[str, Any]]:
    rows: list[tuple[str, Any]] = []
    for locator in (frame.locator("tr"), frame.locator("[role='row']"), frame.locator("li"), frame.locator(".linha")):
        try:
            count = locator.count()
        except Exception:
            continue
        for index in range(min(count, 400)):
            row = locator.nth(index)
            try:
                text = " ".join(row.inner_text(timeout=1000).split())
            except Exception:
                continue
            if re.search(r"PA\s*0?\d{1,2}/\d{4}", text, flags=re.IGNORECASE):
                rows.append((text, row))
    return rows


def process_company(automation: SimplesEcacAutomation, company: CompanyRecord, company_dir: Path, args: argparse.Namespace) -> CompanyExecutionResult:
    result = CompanyExecutionResult(
        company_id=company.company_id,
        company_name=company.name,
        company_document=company.document,
        status="pending",
        eligible=True,
    )
    current_year = datetime.now().year
    automation.runtime.logger.info(
        f"{company.name} [{format_document(company.document)}] | Abrindo PGDAS-D e DEFIS."
    )

    automation.open_pgdas_defis()
    result.flags["pgdas_opened"] = True
    frame = automation.wait_for_pgdas_frame()
    if frame is None:
        raise RuntimeError("Frame do PGDAS-D nao ficou disponivel para consulta de extratos.")

    consulta_url = urljoin(str(getattr(frame, "url", "") or ""), "Consulta")
    if not consulta_url.lower().endswith("/consulta"):
        consulta_url = "https://sinac.cav.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgdasd2018.app/Consulta"
    automation.navigate_scope(frame, consulta_url)
    result.flags["consultas_opened"] = True

    extratos: list[dict[str, Any]] = []
    for year in range(2018, current_year + 1):
        automation.runtime.logger.info(f"{company.name} | Consultando extratos do ano {year}.")
        automation.submit_consulta_year(frame, year)
        normalized_body = _normalize_portal_text(automation.collect_scope_text(frame))
        if "nao existem declaracoes transmitidas para o ano informado" in normalized_body:
            extratos.append(
                {
                    "kind": "extrato",
                    "year": year,
                    "downloaded": False,
                    "skipped": True,
                    "reason": "no_declarations",
                }
            )
            continue

        targets = automation.collect_extrato_targets(frame)
        if not targets:
            extratos.append(
                {
                    "kind": "extrato",
                    "year": year,
                    "downloaded": False,
                    "skipped": True,
                    "reason": "no_extrato_links",
                }
            )
            continue

        for target in targets:
            competencia = str(target.get("competencia") or "").strip()
            href = str(target.get("href") or "").strip()
            payload = {
                "kind": "extrato",
                "year": year,
                "competencia": competencia,
                "pa": str(target.get("pa") or "").strip(),
                "raw_text": str(target.get("raw_text") or "").strip(),
                "downloaded": False,
                "extrato_count": int(target.get("extrato_count") or 0),
            }
            if not href:
                payload["warning"] = "Mes sem link de extrato."
                extratos.append(payload)
                continue
            file_path = company_dir / f"Extrato do Simples - {competencia}.pdf"
            saved = automation.expect_download_href(
                frame,
                href,
                file_path,
                fallback_capture_pdf=True,
            )
            if saved is not None and saved.exists():
                payload["downloaded"] = True
                payload["path"] = str(saved)
                result.files.append(
                    {"kind": "extrato", "path": str(saved), "filename": saved.name}
                )
            extratos.append(payload)

    result.flags["extratos_count"] = sum(1 for item in extratos if item.get("downloaded"))
    result.records.extend(extratos)

    automation.runtime.logger.info(f"{company.name} | Abrindo DEFIS.")
    automation.navigate_scope(frame, DEFIS_ENTRY_URL)
    defis_frame = automation.wait_for_defis_frame(timeout_ms=30000) or frame
    result.flags["defis_opened"] = True

    defis_years = automation.collect_defis_year_options(defis_frame)
    result.flags["defis_year_options"] = len(defis_years)
    defis_files: list[dict[str, Any]] = []
    if defis_years:
        selected_year = defis_years[0]
        result.flags["defis_selected_year"] = selected_year
        automation.runtime.logger.info(
            f"{company.name} | Abrindo DEFIS a partir do primeiro ano disponivel: {selected_year}."
        )
        try:
            automation.open_defis_year(defis_frame, selected_year)
            automation.open_defis_print_page(defis_frame)
        except Exception as exc:
            defis_files.append(
                {
                    "kind": "defis",
                    "year": selected_year,
                    "recibo_downloaded": False,
                    "declaracao_downloaded": False,
                    "warning": str(exc),
                }
            )
        else:
            rows = automation.collect_defis_print_rows(defis_frame)
            if not rows:
                defis_files.append(
                    {
                        "kind": "defis",
                        "year": selected_year,
                        "recibo_downloaded": False,
                        "declaracao_downloaded": False,
                        "warning": "Nenhuma linha foi encontrada na tela de impressao da DEFIS.",
                    }
                )
            for row in rows:
                year = str(row.get("year") or "").strip()
                payload = {
                    "kind": "defis",
                    "year": year,
                    "recibo_downloaded": False,
                    "declaracao_downloaded": False,
                }
                recibo_path = company_dir / f"DEFIS - {year} - Recibo.pdf"
                declaracao_path = company_dir / f"DEFIS - {year} - Declaracao.pdf"
                saved_recibo = automation.expect_download_href(
                    defis_frame, str(row.get("recibo_href") or ""), recibo_path
                )
                saved_declaracao = automation.expect_download_href(
                    defis_frame, str(row.get("declaracao_href") or ""), declaracao_path
                )
                if saved_recibo is not None and saved_recibo.exists():
                    payload["recibo_downloaded"] = True
                    payload["recibo_path"] = str(saved_recibo)
                    result.files.append(
                        {"kind": "defis_recibo", "path": str(saved_recibo), "filename": saved_recibo.name}
                    )
                if saved_declaracao is not None and saved_declaracao.exists():
                    payload["declaracao_downloaded"] = True
                    payload["declaracao_path"] = str(saved_declaracao)
                    result.files.append(
                        {
                            "kind": "defis_declaracao",
                            "path": str(saved_declaracao),
                            "filename": saved_declaracao.name,
                        }
                    )
                payload["raw_text"] = str(row.get("raw_text") or "").strip()
                defis_files.append(payload)

    result.flags["defis_count"] = sum(
        1 for item in defis_files if item.get("recibo_downloaded") or item.get("declaracao_downloaded")
    )
    result.records.extend(defis_files)
    result.records.append(default_company_payload(company))
    automation.return_to_ecac_home()
    downloaded_any = any(item.get("downloaded") for item in extratos) or any(
        item.get("recibo_downloaded") or item.get("declaracao_downloaded")
        for item in defis_files
    )
    result.status = "success" if downloaded_any else "partial"
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
            self.runner.stop_requested = True

    def run(self) -> None:
        try:
            self.status.emit("Executando")
            runner = RobotRunner(technical_id=ROBOT_TECHNICAL_ID, display_name=ROBOT_DISPLAY_NAME, args=self.args, runtime_dir=SCRIPT_DIR, process_company=process_company)
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
        self.dashboard = DashboardClient(runtime_env, ROBOT_TECHNICAL_ID, ROBOT_DISPLAY_NAME)
        self.job = self.runtime.json_runtime.load_job()
        self.office_context = self.dashboard.resolve_office_context(self.job)
        self.worker: Optional[SimplesWorker] = None
        self.companies: list[CompanyRecord] = []
        self.filtered_companies: list[CompanyRecord] = []
        self.robot_dashboard_id: Optional[str] = None
        self.robot_heartbeat_timer = QTimer(self)
        self.robot_heartbeat_timer.setInterval(30000)
        self.robot_heartbeat_timer.timeout.connect(self._on_robot_heartbeat)
        self.setWindowTitle(ROBOT_DISPLAY_NAME)
        self.resize(1180, 760)
        self._build_ui()
        self.runtime.logger.bind_sink(self.append_log)
        self.robot_dashboard_id = self.dashboard.register_robot_presence(status="active")
        self.reload_companies()
        self.robot_heartbeat_timer.start()

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
        return argparse.Namespace(no_ui=False, job_mode=bool(self.job), headless=False, pfx_path="", pfx_password="", company_id="", company_name="", company_document="")

    def _build_manual_job(self) -> JobPayload:
        execution_id = f"manual_{uuid.uuid4().hex}"; company_ids = [c.company_id for c in self.companies if str(c.company_id).strip()]; company_rows = [c.to_dict() for c in self.companies]
        return JobPayload(job_id=execution_id, execution_request_id=execution_id, office_id=self.office_context.office_id, company_ids=company_ids, companies=company_rows, raw={"job_id": execution_id, "execution_request_id": execution_id, "office_id": self.office_context.office_id, "company_ids": company_ids, "companies": company_rows, "source": "manual_ui"})

    def start_worker(self) -> None:
        self.worker = SimplesWorker(self._build_args(), list(self.companies), self.job or self._build_manual_job())
        self.worker.log.connect(self.append_log); self.worker.status.connect(self.status_label.setText); self.worker.progress.connect(self._on_progress); self.worker.summary_ready.connect(self._on_summary); self.worker.error.connect(lambda message: QMessageBox.critical(self, "Execucao", message)); self.worker.finished.connect(lambda: (self.start_button.setEnabled(True), self.stop_button.setEnabled(False)))
        self.start_button.setEnabled(False); self.stop_button.setEnabled(True); self.worker.start()

    def stop_worker(self) -> None:
        if self.worker is not None:
            self.worker.request_stop()
            self.status_label.setText("Parando")

    def _on_progress(self, payload: object) -> None:
        if isinstance(payload, dict): self.progress_label.setText(f"{payload.get('current', 0)} / {payload.get('total', 0)}")

    def _on_summary(self, summary: object) -> None:
        if isinstance(summary, dict): self.status_label.setText(str(summary.get("status") or "Concluido"))

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
        cleanup_runtime_artifacts(self.runtime)
        super().closeEvent(event)


def _run_cli(args: argparse.Namespace) -> int:
    runner = RobotRunner(technical_id=ROBOT_TECHNICAL_ID, display_name=ROBOT_DISPLAY_NAME, args=args, runtime_dir=SCRIPT_DIR, process_company=process_company)
    summary = runner.run(); print(json.dumps(summary, ensure_ascii=False, indent=2)); return 0 if summary.get("status") in {"completed", "partial"} else 1


def main() -> int:
    args = parse_args()
    if args.no_ui: return _run_cli(args)
    if QApplication is None: raise RuntimeError("PySide6 nao esta disponivel para a interface grafica.")
    app = QApplication(sys.argv); runtime_env = build_runtime(ROBOT_TECHNICAL_ID, ROBOT_DISPLAY_NAME, runtime_dir=SCRIPT_DIR); window = MainWindow(runtime_env); window.show(); return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
