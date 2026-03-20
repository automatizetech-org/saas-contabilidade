from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    _ensure_parent(path)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(path)


@dataclass
class JsonRobotRuntime:
    technical_id: str
    display_name: str
    base_dir: Path
    last_status: str = "inactive"

    def __post_init__(self) -> None:
        self.json_dir = self.base_dir / "data" / "json"
        self.job_path = self.json_dir / "job.json"
        self.result_path = self.json_dir / "result.json"
        self.heartbeat_path = self.json_dir / "heartbeat.json"

    def register_robot(self) -> str:
        self.write_heartbeat(status="active")
        return self.technical_id

    def load_job(self) -> Optional[Dict[str, Any]]:
        if self.result_path.exists() or not self.job_path.exists():
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
        return payload

    def load_job_companies(self, job: Optional[Dict[str, Any]], company_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if not isinstance(job, dict):
            return []
        companies = job.get("companies")
        if not isinstance(companies, list):
            return []
        wanted = {str(company_id) for company_id in (company_ids or []) if str(company_id).strip()}
        rows: List[Dict[str, Any]] = []
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
        progress: Optional[Dict[str, Any]] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.last_status = status
        payload: Dict[str, Any] = {
            "robot_technical_id": self.technical_id,
            "display_name": self.display_name,
            "status": status,
            "updated_at": _utc_now_iso(),
            "current_job_id": current_job_id,
            "current_execution_request_id": current_execution_request_id,
            "message": message,
            "progress": progress or {},
        }
        if extra:
            payload.update(extra)
        _atomic_write_json(self.heartbeat_path, payload)

    def write_result(
        self,
        *,
        job: Optional[Dict[str, Any]],
        success: bool,
        error_message: Optional[str] = None,
        summary: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        company_results: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        execution_request_id = None
        job_id = None
        if isinstance(job, dict):
            execution_request_id = str(job.get("execution_request_id") or job.get("id") or "").strip() or None
            job_id = str(job.get("job_id") or job.get("id") or "").strip() or execution_request_id

        event_id = execution_request_id or str(uuid.uuid4())
        result_payload = {
            "event_id": event_id,
            "job_id": job_id or event_id,
            "execution_request_id": execution_request_id,
            "robot_technical_id": self.technical_id,
            "status": "completed" if success else "failed",
            "started_at": _utc_now_iso(),
            "finished_at": _utc_now_iso(),
            "error_message": error_message,
            "summary": summary or {},
            "company_results": company_results or [],
            "payload": payload or {},
        }
        _atomic_write_json(self.result_path, result_payload)
        self.write_heartbeat(
          status="active" if success else "inactive",
          current_job_id=None,
          current_execution_request_id=None,
          message="result_ready",
        )
