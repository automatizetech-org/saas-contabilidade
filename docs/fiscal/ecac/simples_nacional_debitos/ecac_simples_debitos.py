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
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

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

DEFAULT_TEST_CERT_PFX = (
    Path(__file__).resolve().parents[3]
    / "ecac"
    / "caixa postal"
    / "2026 - EG FLEURY ASSESSORIA E SERVICOS LTDA_37197978000103.pfx"
)
DEFAULT_TEST_CERT_PASSWORD = "12345678"

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


def _stack_tail(exc: BaseException) -> str:
    return "".join(traceback.format_exception_only(type(exc), exc)).strip()


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
                "display_name": self.display_name,
                "status": status,
                "last_heartbeat_at": utc_now_iso(),
                "segment_path": str((api_cfg or {}).get("segment_path") or "").strip() or None,
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

    def required_mailbox_notice_visible(self) -> bool:
        scopes = self._visible_modal_scopes() or list(self._iter_scopes())
        for scope in scopes:
            text = " ".join(self._body_text(scope).split()).lower()
            if "mensagens importantes" in text and "caixa postal" in text and (
                "nao lidas" in text or "não lidas" in text
            ):
                return True
        return False

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

    def mark_required_mailbox_messages_as_read(self, limit: int = 12) -> int:
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

        page_cycles = 0
        while locator is not None and opened < limit and page_cycles < 4:
            try:
                row_count = locator.count()
            except Exception:
                row_count = 0
            if row_count <= 0:
                break

            candidate_indexes: list[int] = []
            fallback_indexes: list[int] = []
            for index in range(row_count):
                row = locator.nth(index)
                try:
                    if not row.is_visible():
                        continue
                    raw_text = " ".join(row.inner_text(timeout=1500).split())
                except Exception:
                    continue
                if not raw_text or raw_text in seen_rows:
                    continue
                seen_rows.add(raw_text)
                if self._mailbox_row_requires_read(raw_text):
                    candidate_indexes.append(index)
                elif page_cycles == 0 and len(fallback_indexes) < 2:
                    fallback_indexes.append(index)

            target_indexes = candidate_indexes or fallback_indexes
            if not target_indexes:
                if not self._click_next_page():
                    break
                page_cycles += 1
                try:
                    self.page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass
                locator, _, _ = self._message_rows_locator()
                continue

            for index in target_indexes:
                if opened >= limit:
                    break
                locator_missing = False
                try:
                    row = locator.nth(index)
                    detail_text = self._open_row_detail(row)
                    if detail_text or self._is_message_detail_view():
                        opened += 1
                        try:
                            self.page.wait_for_timeout(1000)
                        except Exception:
                            time.sleep(1.0)
                finally:
                    self.close_blocking_modals()
                    self._return_to_message_list()
                    locator, _, _ = self._message_rows_locator()
                    locator_missing = locator is None
                if locator_missing:
                    break

            if opened >= limit:
                break
            if not self._click_next_page():
                break
            page_cycles += 1
            try:
                self.page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass
            locator, _, _ = self._message_rows_locator()

        return opened

    def resolve_required_mailbox_notice(self) -> bool:
        if not self.required_mailbox_notice_visible():
            return False
        self.runtime.logger.info("Modal de leitura obrigatoria da Caixa Postal detectado.")
        if not self._click_by_label(["Ir para a Caixa Postal", "Caixa Postal"], timeout_ms=6000):
            raise RuntimeError("O modal de leitura obrigatoria apareceu, mas o acesso para a Caixa Postal nao foi localizado.")
        ready = self._wait_until(
            lambda: self._message_rows_locator()[0] is not None or self._is_message_detail_view(),
            timeout_ms=20000,
            interval_ms=400,
        )
        if not ready:
            raise RuntimeError("A Caixa Postal nao ficou disponivel apos o modal de leitura obrigatoria.")
        opened = self.mark_required_mailbox_messages_as_read(limit=12)
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

    def captcha_visible(self) -> bool:
        try:
            if HCAPTCHA_ERROR_URL_TOKEN.lower() in self.page.url.lower():
                return True
        except Exception:
            pass
        markers = [
            "Por favor, tente novamente.",
            "Verificar",
            "Desafio hCaptcha",
            "hCaptcha",
        ]
        for scope in self._iter_scopes():
            text = self._body_text(scope)
            lowered = text.lower()
            if any(marker.lower() in lowered for marker in markers):
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

    def ensure_company_profile(self, company: CompanyRecord) -> tuple[bool, str]:
        self.ensure_logged_in()
        self.resolve_required_mailbox_notice()
        current_context = self._current_profile_context()
        if company.document and only_digits(company.document) and only_digits(company.document) in only_digits(current_context):
            return False, current_context
        if company.name and company.name.lower() in current_context.lower():
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
            lambda: self.page.locator("#frmApp").count() > 0 or "pgdasd2018.app" in self.page.content().lower(),
            timeout_ms=60000,
            interval_ms=500,
        )
        if not iframe_ready:
            raise RuntimeError("Iframe principal do PGDAS-D / DEFIS nao ficou disponivel.")
        src = ""
        try:
            src = str(self.page.locator("#frmApp").first.get_attribute("src") or "").strip()
        except Exception:
            pass
        return src

    def wait_for_pgdas_frame(self, timeout_ms: int = 120000):
        deadline = time.time() + (timeout_ms / 1000)
        while time.time() < deadline:
            for frame in self.page.frames:
                url = str(getattr(frame, "url", "") or "")
                if "pgdasd2018" in url.lower() or "simplesnacional" in url.lower():
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


def ensure_company_output_dir(runtime_env: RuntimeEnvironment, company: CompanyRecord) -> Path:
    output_root = runtime_env.paths.data_dir / "output"
    cleanup_old_outputs(output_root)
    day_dir = output_root / datetime.now().strftime("%Y-%m-%d")
    company_dir = day_dir / f"{slugify(company.name)}_{only_digits(company.document)}"
    company_dir.mkdir(parents=True, exist_ok=True)
    return company_dir


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
    if DEFAULT_TEST_CERT_PFX.exists():
        return import_pfx_and_get_metadata(DEFAULT_TEST_CERT_PFX, DEFAULT_TEST_CERT_PASSWORD)
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
            updated.eligible = updated.has_certificate_credentials and str(updated.auth_mode or "").strip().lower() == "certificate"
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
    if company.has_certificate_credentials and str(company.auth_mode or "").strip().lower() == "certificate":
        metadata = resolve_certificate_from_dashboard(runtime_env, [company])
        if metadata:
            return metadata
        raise RuntimeError(f"Certificado digital nao encontrado para {company.name}.")
    env_certificate = resolve_certificate_from_env()
    if env_certificate:
        return env_certificate
    if DEFAULT_TEST_CERT_PFX.exists():
        return import_pfx_and_get_metadata(DEFAULT_TEST_CERT_PFX, DEFAULT_TEST_CERT_PASSWORD)
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
                    company_dir = ensure_company_output_dir(self.runtime, company)
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

ROBOT_TECHNICAL_ID = "ecac_simples_debitos"
ROBOT_DISPLAY_NAME = "e-CAC - Simples Nacional - Consulta Debitos"

DEBITOS_LABELS = ["Consultar debitos", "Consultar Debitos", "Consultar/Gerar DAS", "Consultar Gerar DAS", "Debitos", "DAS Avulso"]


def parse_args() -> argparse.Namespace:
    return build_common_parser(ROBOT_DISPLAY_NAME).parse_args()


def _parse_currency(value: str) -> str:
    match = re.search(r"\d{1,3}(?:\.\d{3})*,\d{2}", value or "")
    return match.group(0) if match else ""


def _rows(scope) -> list[str]:
    rows: list[str] = []
    for locator in (scope.locator("tr"), scope.locator("[role='row']"), scope.locator("li"), scope.locator(".linha")):
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
            if re.search(r"\d{2}/\d{4}", text) and re.search(r"\d{2}/\d{2}/\d{4}", text):
                rows.append(text)
    return rows


def _parse_debito(company: CompanyRecord, text: str) -> dict[str, object]:
    values = re.findall(r"\d{1,3}(?:\.\d{3})*,\d{2}", text)
    periodo_match = re.search(r"\b(\d{2}/\d{4})\b", text)
    vencimento_match = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", text)
    parcelamento_match = re.search(r"(?:parcelamento|parc)\D+(\d+)", text, flags=re.IGNORECASE)
    numero_parcelamento = int(parcelamento_match.group(1)) if parcelamento_match else 0
    return {
        "company_id": company.company_id,
        "company_name": company.name,
        "company_document": company.document,
        "periodo_apuracao": periodo_match.group(1) if periodo_match else "",
        "data_vencimento": vencimento_match.group(1) if vencimento_match else "",
        "debito_declarado": _parse_currency(values[0] if len(values) > 0 else ""),
        "principal": _parse_currency(values[1] if len(values) > 1 else ""),
        "multa": _parse_currency(values[2] if len(values) > 2 else ""),
        "juros": _parse_currency(values[3] if len(values) > 3 else ""),
        "total": _parse_currency(values[4] if len(values) > 4 else ""),
        "numero_parcelamento": numero_parcelamento,
        "is_parcelado": numero_parcelamento > 0,
        "raw_text": text,
    }


def process_company(automation: SimplesEcacAutomation, company: CompanyRecord, company_dir: Path, args: argparse.Namespace) -> CompanyExecutionResult:
    del company_dir, args
    result = CompanyExecutionResult(company_id=company.company_id, company_name=company.name, company_document=company.document, status="pending", eligible=True)
    automation.open_pgdas_defis()
    result.flags["pgdas_opened"] = True
    frame = automation.wait_for_pgdas_ready()
    result.flags["debitos_opened"] = automation.click_anywhere(DEBITOS_LABELS, timeout_ms=12000)
    debitos: list[dict[str, object]] = []
    for row_text in _rows(frame):
        item = _parse_debito(company, row_text)
        if item["periodo_apuracao"] and int(item["numero_parcelamento"]) == 0:
            debitos.append(item)
    result.records.extend(debitos)
    result.records.append(default_company_payload(company))
    result.flags["debitos_count"] = len(debitos)
    result.status = "success" if debitos else "partial"
    if not debitos:
        result.warnings.append("Nenhuma linha de debito sem parcelamento foi identificada com os seletores genericos.")
    return result


try:
    from PySide6.QtCore import Qt, QThread, QTimer, Signal
    from PySide6.QtGui import QColor
    from PySide6.QtWidgets import QApplication, QAbstractItemView, QGroupBox, QHBoxLayout, QLabel, QLineEdit, QListWidget, QListWidgetItem, QMainWindow, QMessageBox, QPushButton, QTextEdit, QVBoxLayout, QWidget
except Exception:
    QApplication = None  # type: ignore[assignment]
    QThread = object  # type: ignore[misc,assignment]
    Signal = lambda *args, **kwargs: object()  # type: ignore[misc,assignment]


class SimplesWorker(QThread):  # type: ignore[misc]
    log = Signal(str); status = Signal(str); progress = Signal(object); summary_ready = Signal(object); error = Signal(str)
    def __init__(self, args: argparse.Namespace, companies: list[CompanyRecord], job: Optional[JobPayload]) -> None:
        super().__init__(); self.args = args; self.companies = companies; self.job = job; self.runner: Optional[RobotRunner] = None
    def request_stop(self) -> None:
        if self.runner is not None:
            self.runner.stop_requested = True
    def run(self) -> None:
        try:
            self.status.emit("Executando")
            runner = RobotRunner(technical_id=ROBOT_TECHNICAL_ID, display_name=ROBOT_DISPLAY_NAME, args=self.args, runtime_dir=SCRIPT_DIR, process_company=process_company)
            self.runner = runner
            runner.runtime.logger.bind_sink(self.log.emit); self.summary_ready.emit(runner.run()); self.status.emit("Concluido")
        except Exception as exc:
            self.error.emit(str(exc)); self.status.emit("Falha")
        finally:
            self.runner = None


class MainWindow(QMainWindow):
    def __init__(self, runtime_env: RuntimeEnvironment) -> None:
        super().__init__(); self.runtime = runtime_env; self.dashboard = DashboardClient(runtime_env, ROBOT_TECHNICAL_ID, ROBOT_DISPLAY_NAME); self.job = self.runtime.json_runtime.load_job(); self.office_context = self.dashboard.resolve_office_context(self.job); self.worker: Optional[SimplesWorker] = None; self.companies: list[CompanyRecord] = []; self.robot_dashboard_id: Optional[str] = None; self.robot_heartbeat_timer = QTimer(self); self.robot_heartbeat_timer.setInterval(30000); self.robot_heartbeat_timer.timeout.connect(self._on_robot_heartbeat); self.setWindowTitle(ROBOT_DISPLAY_NAME); self.resize(1080, 720); self._build_ui(); self.runtime.logger.bind_sink(self.append_log); self.reload_companies(); self.robot_dashboard_id = self.dashboard.register_robot_presence(status="active"); self.robot_heartbeat_timer.start()
    def _build_ui(self) -> None:
        central = QWidget(); self.setCentralWidget(central); layout = QVBoxLayout(central); layout.addWidget(QLabel(ROBOT_DISPLAY_NAME)); top = QHBoxLayout(); layout.addLayout(top)
        left = QGroupBox("Empresas"); left_l = QVBoxLayout(left); self.search_input = QLineEdit(); self.search_input.textChanged.connect(self._apply_company_filter); left_l.addWidget(self.search_input); self.company_list = QListWidget(); self.company_list.setSelectionMode(QAbstractItemView.NoSelection); left_l.addWidget(self.company_list); top.addWidget(left, 2)
        right = QVBoxLayout(); self.status_label = QLabel("Pronto"); self.progress_label = QLabel("0 / 0"); right.addWidget(QLabel("Status")); right.addWidget(self.status_label); right.addWidget(QLabel("Progresso")); right.addWidget(self.progress_label); controls = QHBoxLayout(); self.start_button = QPushButton("Iniciar"); self.start_button.clicked.connect(self.start_worker); self.stop_button = QPushButton("Parar"); self.stop_button.clicked.connect(self.stop_worker); self.stop_button.setEnabled(False); controls.addWidget(self.start_button); controls.addWidget(self.stop_button); top.addLayout(right, 1); right.addLayout(controls)
        self.log_panel = QTextEdit(); self.log_panel.setReadOnly(True); layout.addWidget(self.log_panel)
    def append_log(self, message: str) -> None: self.log_panel.append(message)
    def _fallback_manual_company(self) -> list[CompanyRecord]:
        try:
            certificate = ensure_default_certificate(self.runtime, []); company = build_company_from_certificate(certificate, source="ui_certificate_default"); return [company] if company else []
        except Exception: return []
    def reload_companies(self) -> None:
        self.job = self.runtime.json_runtime.load_job(); self.office_context = self.dashboard.resolve_office_context(self.job, force_refresh=True)
        try: self.companies = self.dashboard.load_companies(self.office_context, job=self.job, company_ids=self.job.company_ids or None) if self.job else self.dashboard.load_companies(self.office_context, job=None, company_ids=None)
        except Exception: self.companies = self._fallback_manual_company()
        self.progress_label.setText(f"0 / {len(self.companies)}"); self._apply_company_filter(); self.start_button.setEnabled(bool(self.companies))
    def _apply_company_filter(self) -> None:
        search = self.search_input.text().strip().lower(); self.company_list.clear()
        for company in self.companies:
            if search and search not in company.search_text: continue
            item = QListWidgetItem(f"{company.name}\n{format_document(company.document)}"); self.company_list.addItem(item)
    def start_worker(self) -> None:
        args = argparse.Namespace(no_ui=False, job_mode=bool(self.job), headless=False, pfx_path="", pfx_password="", company_id="", company_name="", company_document="")
        self.worker = SimplesWorker(args, list(self.companies), self.job); self.worker.log.connect(self.append_log); self.worker.status.connect(self.status_label.setText); self.worker.summary_ready.connect(lambda summary: self.status_label.setText(str(summary.get('status') if isinstance(summary, dict) else 'Concluido'))); self.worker.error.connect(lambda message: QMessageBox.critical(self, 'Execucao', message)); self.worker.finished.connect(lambda: (self.start_button.setEnabled(True), self.stop_button.setEnabled(False))); self.start_button.setEnabled(False); self.stop_button.setEnabled(True); self.worker.start()

    def stop_worker(self) -> None:
        if self.worker is not None:
            self.worker.request_stop()
            self.status_label.setText("Parando")

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


def _run_cli(args: argparse.Namespace) -> int:
    runner = RobotRunner(technical_id=ROBOT_TECHNICAL_ID, display_name=ROBOT_DISPLAY_NAME, args=args, runtime_dir=SCRIPT_DIR, process_company=process_company); summary = runner.run(); print(json.dumps(summary, ensure_ascii=False, indent=2)); return 0 if summary.get("status") in {"completed", "partial"} else 1


def main() -> int:
    args = parse_args();
    if args.no_ui: return _run_cli(args)
    if QApplication is None: raise RuntimeError("PySide6 nao esta disponivel para a interface grafica.")
    app = QApplication(sys.argv); runtime_env = build_runtime(ROBOT_TECHNICAL_ID, ROBOT_DISPLAY_NAME, runtime_dir=SCRIPT_DIR); window = MainWindow(runtime_env); window.show(); return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
