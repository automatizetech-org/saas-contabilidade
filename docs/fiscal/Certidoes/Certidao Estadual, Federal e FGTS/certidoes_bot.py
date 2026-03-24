import os, sys, json, re, unicodedata, base64, shutil, tempfile, uuid, socket
import math
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Tuple, Dict, List, Optional, Any, Callable
import subprocess, time


def _configure_qt_platform_plugin_path() -> None:
    base_candidates: List[Path] = []
    if getattr(sys, "frozen", False):
        base_candidates.append(Path(sys.executable).resolve().parent)
    base_candidates.append(Path(__file__).resolve().parent)

    os.environ.pop("QT_PLUGIN_PATH", None)

    for base in base_candidates:
        platforms_dir = base / "venv" / "Lib" / "site-packages" / "PySide6" / "plugins" / "platforms"
        if platforms_dir.exists():
            os.environ["QT_QPA_PLATFORM_PLUGIN_PATH"] = str(platforms_dir)
            return


_configure_qt_platform_plugin_path()


def _load_app_icon() -> "QIcon":
    icon = QIcon()
    candidates: List[str] = []
    for base in _bases():
        for rel in ("data/image/logo.png", "data/Image/logo.png", "data/IMAGE/logo.png", "image/logo.png", "logo.png"):
            cand = base / rel
            if cand.exists():
                candidates.append(str(cand))
                break
    if ICO_PATH and os.path.exists(ICO_PATH):
        candidates.append(ICO_PATH)

    for candidate in candidates:
        try:
            source = QPixmap(candidate)
            if source.isNull():
                continue
            built_icon = QIcon()
            for size in (16, 20, 24, 32, 40, 48, 64):
                built_icon.addPixmap(
                    source.scaled(size, size, Qt.KeepAspectRatio, Qt.SmoothTransformation)
                )
            if not built_icon.isNull():
                return built_icon
        except Exception:
            continue
    return icon

from PySide6.QtCore import Qt, Signal, QThread, QSize, QTimer, QPropertyAnimation, QEasingCurve
from PySide6.QtGui import QIcon, QPixmap, QPainter, QColor, QAction
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QPushButton, QPlainTextEdit,
    QScrollArea, QLineEdit, QHBoxLayout, QVBoxLayout, QFrame, QCheckBox,
    QFileDialog, QDialog, QDialogButtonBox, QMessageBox, QComboBox,
    QSpacerItem, QSizePolicy, QStackedWidget, QLayout, QSystemTrayIcon, QMenu, QStyle
)

from PIL import Image, ImageQt, ImageDraw, ImageFont, ImageFilter, ImageChops
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import PyPDF2
import requests

# =============================================================================
# Local paths
# =============================================================================
def _bases():
    b = []
    if hasattr(sys, "_MEIPASS"):
        b.append(Path(sys._MEIPASS))
    if getattr(sys, "frozen", False):
        b.append(Path(sys.executable).parent)
    b.append(Path(__file__).parent.resolve())
    seen, out = set(), []
    for x in b:
        sx = str(x)
        if sx not in seen:
            seen.add(sx)
            out.append(x)
    return out


def get_base_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


def get_internal_dir():
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return sys._MEIPASS  # type: ignore[attr-defined]
    return get_base_dir()


BASE_DIR = get_base_dir()
INTERNAL_DIR = get_internal_dir()
BASE_DIR_PATH = Path(BASE_DIR)


def _resolve_robots_base_env_dir() -> Optional[Path]:
    """Pasta ROBOS onde costuma estar venv\ e .env (não hardcodar utilizador Windows)."""
    raw = (os.environ.get("ROBOTS_ROOT_PATH") or "").strip().strip('"')
    if raw:
        p = Path(raw)
        if p.is_dir():
            return p.resolve()
    cur = Path(__file__).resolve().parent
    for _ in range(12):
        if (cur / "venv").is_dir():
            return cur.resolve()
        if cur.parent == cur:
            break
        cur = cur.parent
    legacy = Path(r"C:\Users\ROBO\Documents\ROBOS")
    if legacy.is_dir():
        return legacy.resolve()
    return None


ROBOTS_BASE_ENV_DIR = _resolve_robots_base_env_dir()

try:
    from dotenv import load_dotenv

    if ROBOTS_BASE_ENV_DIR is not None:
        env_robos = ROBOTS_BASE_ENV_DIR / ".env"
        if env_robos.exists():
            load_dotenv(env_robos)
        elif (ROBOTS_BASE_ENV_DIR / ".env.example").exists():
            load_dotenv(ROBOTS_BASE_ENV_DIR / ".env.example")

    env_path = BASE_DIR_PATH / ".env"
    if not env_path.exists():
        env_path = BASE_DIR_PATH / ".env.example"
    if env_path.exists():
        load_dotenv(env_path)

    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        if ROBOTS_BASE_ENV_DIR is None or exe_dir.resolve() != ROBOTS_BASE_ENV_DIR:
            load_dotenv(exe_dir / ".env")
            if not (exe_dir / ".env").exists():
                load_dotenv(exe_dir / ".env.example")
except ImportError:
    pass


def get_data_dir() -> Path:
    for base in _bases():
        for rel in ("data", "_internal/data"):
            cand = Path(base) / rel
            if cand.exists():
                return cand.resolve()
    return (Path(BASE_DIR) / "data").resolve()


DATA_DIR = get_data_dir()


def get_config_dir():
    base = get_base_dir()
    if getattr(sys, "frozen", False):
        return os.path.join(base, "_internal", "data", "json")
    return os.path.join(base, "data", "json")


CONFIG_DIR = get_config_dir()
CONFIG_PATH = os.path.join(CONFIG_DIR, "config.json")

# Monday.com API: token apenas no .env (arquivo à parte); board/item/coluna em config_certidoes.json
# .env (só o token): token monday = <seu_token>
# config_certidoes.json: monday_board_id, monday_item_id, monday_status_column_id (opcional; padrão "status")
# Coluna de status no board deve ter as opções "Pendente" e "Feito".
MONDAY_API_URL = "https://api.monday.com/v2"


def _load_env():
    """Carrega do .env (BASE_DIR) apenas o token da API Monday (token monday -> MONDAY_TOKEN)."""
    env_path = Path(BASE_DIR) / ".env"
    if not env_path.exists():
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip().lower().replace(" ", "_")
                val = val.strip().strip('"').strip("'")
                if not val:
                    continue
                if key == "token_monday":
                    os.environ["MONDAY_TOKEN"] = val
                    break
    except Exception:
        pass


AUTOMATION_NAME = "CertidoesBot"
PORTAL_TIMEOUT = 90000
CDP_PORT = 9222
CHROME_EXE = DATA_DIR / "Chrome" / "chrome.exe"
PROFILE_DIR = DATA_DIR / "chrome_cdp_profile"
# CDP / Chrome portátil:
# - Por padrão, usa o Chrome portátil em data/Chrome/chrome.exe e o perfil persistente em PROFILE_DIR.
# - Você pode sobrescrever o executável via env CERTIDOES_CHROME_EXE (útil para testes locais).
CHROME_CDP_PORT = int((os.environ.get("CERTIDOES_CDP_PORT") or str(CDP_PORT)).strip() or CDP_PORT)
ROBOT_TECHNICAL_ID = "certidoes_fiscal"
ROBOT_DISPLAY_NAME_DEFAULT = "Certidoes Fiscal"
ROBOT_SEGMENT_PATH_DEFAULT = "FISCAL/CERTIDOES"
AUTH_PASSWORD = "password"
AUTH_CERTIFICATE = "certificate"
CERTIDOES_DIRNAME = "Certidoes"

# ICO fallback
ICO_PATH = ""
ico_candidates = [
    DATA_DIR / "ico" / "app.ico",
    DATA_DIR / "ICO" / "app.ico",
    Path(BASE_DIR) / "data" / "ico" / "app.ico",
    Path(BASE_DIR) / "data" / "ICO" / "app.ico",
]
for base in _bases():
    ico_candidates.extend(
        [
            base / "data" / "ico" / "app.ico",
            base / "data" / "ICO" / "app.ico",
            base / "ico" / "app.ico",
            base / "ICO" / "app.ico",
            base / "app.ico",
        ]
    )
for cand in ico_candidates:
    try:
        if cand.exists():
            ICO_PATH = str(cand.resolve())
            break
    except Exception:
        continue

# =============================================================================
# Estilo de botões (mesmo padrão usado no bot principal)
# =============================================================================
def button_style(base: str, hover: str, pressed: str, text_color: str = "#E8F4FF") -> str:
    return f"""
    QPushButton {{
        font: 9pt 'Verdana';
        font-weight: bold;
        color: {text_color};
        padding: 9px 14px;
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
        padding-top: 11px;
        padding-bottom: 7px;
    }}
    """

# =============================================================================
# Supabase (service role para o robô)
# =============================================================================
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
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


def _get_license_supabase_client():
    """Cliente Supabase usado apenas para validar licença (projeto separado)."""
    url = (os.environ.get("LICENSE_SUPABASE_URL") or os.environ.get("SUPABASE_URL") or "").strip()
    anon = (os.environ.get("LICENSE_SUPABASE_ANON_KEY") or os.environ.get("SUPABASE_ANON_KEY") or "").strip()
    if not url or not anon:
        raise RuntimeError(
            "Supabase (licença) não configurado. Defina LICENSE_SUPABASE_URL e LICENSE_SUPABASE_ANON_KEY."
        )
    return create_client(url, anon)


def _get_supabase_client():
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError(
            "Supabase não configurado. Defina SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY no ambiente da VM."
        )
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


LICENSE_PATH = os.path.join(CONFIG_DIR, "license.json")


def _load_license_local() -> dict:
    if os.path.exists(LICENSE_PATH):
        try:
            with open(LICENSE_PATH, "r", encoding="utf-8") as f:
                return json.load(f) or {}
        except Exception:
            return {}
    return {}


def _save_license_local(data: dict) -> None:
    os.makedirs(CONFIG_DIR, exist_ok=True)
    with open(LICENSE_PATH, "w", encoding="utf-8") as f:
        json.dump(data or {}, f, ensure_ascii=False, indent=2)


def get_saved_key() -> str:
    return str(_load_license_local().get("license_key", "")).strip()


def persist_key(key: str, meta: dict | None = None) -> None:
    payload = _load_license_local()
    payload["license_key"] = (key or "").strip()
    if meta:
        payload.update(meta)
    _save_license_local(payload)


def check_license_with_supabase(key: str) -> tuple[bool, str, dict]:
    key = (key or "").strip()
    if not key:
        return (False, "Informe a chave de licença.", {})
    try:
        client = _get_license_supabase_client()
        res = client.rpc("verify_license", {"p_key": key}).execute()
        rows = res.data if hasattr(res, "data") else []
    except Exception as e:
        return (False, f"Falha ao contatar o servidor de licença: {e}", {})
    if not rows:
        return (False, "Chave inválida, expirada ou inativa.", {})
    row = rows[0] or {}
    meta = {"expires_at": row.get("expires_at"), "licensee": row.get("client_name") or ""}
    return (True, "Licença válida.", meta)


class LicenseDialog(QDialog):
    ACCENT = "#7C3AED"
    BG1 = "#0B1220"
    BG2 = "#111827"
    CARD = "#0F172A"
    STROKE = "rgba(255,255,255,0.08)"
    TEXT = "rgba(255,255,255,0.92)"
    MUTED = "rgba(255,255,255,0.65)"

    def __init__(self, parent=None, preset_key: str = ""):
        super().__init__(parent)
        self.setObjectName("LicenseDialog")
        self.setModal(True)
        self.setMinimumWidth(520)
        self.setWindowTitle("Ativação da Licença")
        try:
            self.setWindowIcon(QIcon(ICO_PATH))
        except Exception:
            pass
        self.setStyleSheet(f"""
        QDialog#LicenseDialog {{
            background: qlineargradient(x1:0,y1:0, x2:1,y2:1, stop:0 {self.BG1}, stop:1 {self.BG2});
        }}
        QLabel, QLineEdit, QPushButton {{
            color: {self.TEXT};
            font-family: "Segoe UI", "Inter", "Roboto", "Ubuntu", "Arial";
            font-size: 14px;
        }}
        QFrame#Card {{
            background: {self.CARD};
            border: 1px solid {self.STROKE};
            border-radius: 18px;
        }}
        QLabel#Title {{ font-size: 22px; font-weight: 700; color: {self.TEXT}; }}
        QLabel#Subtitle {{ font-size: 13px; color: {self.MUTED}; }}
        QLineEdit#KeyEdit {{
            background: #0B1220; border: 1px solid #243041; border-radius: 12px; padding: 12px 14px;
            selection-background-color: {self.ACCENT}; selection-color: white;
        }}
        QLineEdit#KeyEdit:focus {{ border: 1px solid {self.ACCENT}; box-shadow: 0 0 0 3px rgba(124,58,237,0.25); }}
        QPushButton#PrimaryButton {{ background: {self.ACCENT}; border: none; border-radius: 12px; padding: 10px 16px; font-weight: 600; color: white; }}
        QPushButton#PrimaryButton:hover {{ background: #6D28D9; }}
        QPushButton#PrimaryButton:disabled {{ background: #5B21B6; opacity: 0.6; }}
        QPushButton#GhostButton {{ background: transparent; border: 1px solid {self.STROKE}; border-radius: 12px; padding: 10px 16px; color: {self.MUTED}; }}
        QPushButton#GhostButton:hover {{ border: 1px solid #2A374B; color: {self.TEXT}; }}
        QPushButton#LinkButton {{ background: transparent; border: none; color: {self.ACCENT}; font-weight: 600; }}
        QPushButton#LinkButton:hover {{ text-decoration: underline; }}
        QLabel#Msg {{ color: {self.MUTED}; }}
        """)

        root = QVBoxLayout(self)
        root.setContentsMargins(20, 20, 20, 20)
        card = QFrame(self)
        card.setObjectName("Card")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(28, 28, 28, 28)
        card_layout.setSpacing(16)

        header = QHBoxLayout()
        header.setSpacing(14)
        logo_lbl = QLabel(card)
        try:
            logo_pix = QPixmap(ICO_PATH)
        except Exception:
            logo_pix = QPixmap()
        if logo_pix.isNull():
            logo_pix = QPixmap(64, 64)
            logo_pix.fill(Qt.transparent)
        logo_lbl.setPixmap(logo_pix.scaled(48, 48, Qt.KeepAspectRatio, Qt.SmoothTransformation))
        header.addWidget(logo_lbl, 0, Qt.AlignTop)

        title_box = QVBoxLayout()
        title = QLabel("Ative sua licença", card)
        title.setObjectName("Title")
        self.subtitle = QLabel("Digite sua chave para liberar todas as funcionalidades.", card)
        self.subtitle.setObjectName("Subtitle")
        self.subtitle.setWordWrap(True)
        title_box.addWidget(title)
        title_box.addWidget(self.subtitle)
        header.addLayout(title_box)
        header.addStretch(1)
        card_layout.addLayout(header)

        key_row = QHBoxLayout()
        key_row.setSpacing(10)
        self.edt = QLineEdit(card)
        self.edt.setObjectName("KeyEdit")
        self.edt.setPlaceholderText("XXXXX-XXXXX-XXXXX-XXXXX")
        if preset_key:
            self.edt.setText(preset_key)
        self.edt.returnPressed.connect(self._on_activate)
        paste_btn = QPushButton("Colar", card)
        paste_btn.setStyleSheet(button_style("#2980B9", "#2471A3", "#1F618D"))
        paste_btn.setObjectName("GhostButton")
        paste_btn.setCursor(Qt.PointingHandCursor)
        paste_btn.clicked.connect(self._paste_from_clipboard)
        key_row.addWidget(self.edt, 1)
        key_row.addWidget(paste_btn, 0)
        card_layout.addLayout(key_row)

        self.msg = QLabel("", card)
        self.msg.setObjectName("Msg")
        self.msg.setWordWrap(True)
        card_layout.addWidget(self.msg)
        card_layout.addItem(QSpacerItem(0, 8, QSizePolicy.Minimum, QSizePolicy.Expanding))

        btns_row = QHBoxLayout()
        btns_row.addStretch(1)
        self.btn_cancel = QPushButton("Sair", card)
        self.btn_cancel.setStyleSheet(button_style("#7F8C8D", "#95A5A6", "#707B7C"))
        self.btn_cancel.setObjectName("GhostButton")
        self.btn_cancel.setCursor(Qt.PointingHandCursor)
        self.btn_cancel.clicked.connect(self.reject)
        self.btn_ok = QPushButton("Ativar", card)
        self.btn_ok.setStyleSheet(button_style("#7C3AED", "#6D28D9", "#5B21B6"))
        self.btn_ok.setObjectName("PrimaryButton")
        self.btn_ok.setCursor(Qt.PointingHandCursor)
        self.btn_ok.clicked.connect(self._on_activate)
        btns_row.addWidget(self.btn_cancel)
        btns_row.addWidget(self.btn_ok)
        card_layout.addLayout(btns_row)

        help_row = QHBoxLayout()
        help_row.addStretch(1)
        help_lbl = QLabel(
            '<span style="color:rgba(255,255,255,0.55)">Precisa de ajuda?</span> '
            '<a style="color:#7C3AED; text-decoration:none;" href="https://wa.me/5562993626968"><b>Fale com o suporte</b></a>',
            card
        )
        help_lbl.setTextFormat(Qt.RichText)
        help_lbl.setOpenExternalLinks(True)
        help_row.addWidget(help_lbl, 0, Qt.AlignRight)
        card_layout.addLayout(help_row)
        root.addWidget(card)

        self.setWindowOpacity(0.0)
        QTimer.singleShot(0, self._animate_entrance)

    def set_revoked_mode(self, reason: str | None = None):
        base = "Sua licen‡a foi revogada. Entre em contato com o suporte para reativar."
        if reason:
            base = f"{base}\n{reason}"
        try:
            self.subtitle.setText(base)
        except Exception:
            pass
        try:
            self.msg.clear()
        except Exception:
            pass

    def _paste_from_clipboard(self):
        cb = QApplication.clipboard()
        if cb:
            self.edt.setText(cb.text().strip())

    def _animate_entrance(self):
        anim = QPropertyAnimation(self, b"windowOpacity", self)
        anim.setDuration(220)
        anim.setStartValue(0.0)
        anim.setEndValue(1.0)
        anim.setEasingCurve(QEasingCurve.OutCubic)
        anim.start(QPropertyAnimation.DeleteWhenStopped)

    def _on_activate(self):
        key = self.edt.text().strip()
        self._set_busy(True)
        ok, msg, meta = check_license_with_supabase(key)
        self._set_busy(False)
        self.msg.setText(msg or "")
        if ok:
            persist_key(key, meta)
            os.environ["AUTOMATIZE_VERIFIED"] = "1"
            self.accept()
            return
        low = (msg or "").lower()
        if "desativad" in low or "inativa" in low or "revog" in low:
            try:
                self.subtitle.setText("Sua licença foi revogada. Entre em contato com o suporte.")
            except Exception:
                pass

    def _set_busy(self, busy: bool):
        self.btn_ok.setEnabled(not busy)
        self.btn_cancel.setEnabled(not busy)
        self.edt.setEnabled(not busy)


class ConfirmDialog(QDialog):
    BG1 = LicenseDialog.BG1
    BG2 = LicenseDialog.BG2
    CARD = LicenseDialog.CARD
    STROKE = LicenseDialog.STROKE
    TEXT = LicenseDialog.TEXT
    MUTED = LicenseDialog.MUTED
    ACCENT = LicenseDialog.ACCENT

    def __init__(self, title: str, message: str, ok_text: str = "Confirmar", cancel_text: str = "Cancelar", parent=None):
        super().__init__(parent)
        self.setObjectName("ConfirmDialog")
        self.setModal(True)
        self.setMinimumWidth(420)
        self.setWindowTitle(title or "Confirma‡Æo")
        try:
            self.setWindowIcon(QIcon(ICO_PATH))
        except Exception:
            pass
        self.setStyleSheet(f"""
        QDialog#ConfirmDialog {{
            background: qlineargradient(x1:0,y1:0, x2:1,y2:1, stop:0 {self.BG1}, stop:1 {self.BG2});
        }}
        QLabel, QPushButton {{
            color: {self.TEXT};
            font-family: "Segoe UI", "Inter", "Roboto", "Ubuntu", "Arial";
            font-size: 14px;
        }}
        QFrame#Card {{
            background: {self.CARD};
            border: 1px solid {self.STROKE};
            border-radius: 16px;
        }}
        QLabel#Title {{ font-size: 18px; font-weight: 700; color: {self.TEXT}; }}
        QLabel#Msg {{ color: {self.MUTED}; }}
        QPushButton#PrimaryButton {{ background: {self.ACCENT}; border: none; border-radius: 12px; padding: 10px 16px; font-weight: 600; color: white; }}
        QPushButton#PrimaryButton:hover {{ background: #6D28D9; }}
        QPushButton#PrimaryButton:disabled {{ background: #5B21B6; opacity: 0.6; }}
        QPushButton#GhostButton {{ background: transparent; border: 1px solid {self.STROKE}; border-radius: 12px; padding: 10px 16px; color: {self.MUTED}; }}
        QPushButton#GhostButton:hover {{ border: 1px solid #2A374B; color: {self.TEXT}; }}
        """)

        root = QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        card = QFrame(self)
        card.setObjectName("Card")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(22, 22, 22, 22)
        card_layout.setSpacing(12)

        lbl_title = QLabel(title or "Confirma‡Æo", card)
        lbl_title.setObjectName("Title")
        lbl_msg = QLabel(message or "", card)
        lbl_msg.setObjectName("Msg")
        lbl_msg.setWordWrap(True)
        card_layout.addWidget(lbl_title)
        card_layout.addWidget(lbl_msg)
        card_layout.addItem(QSpacerItem(0, 6, QSizePolicy.Minimum, QSizePolicy.Expanding))

        btns_row = QHBoxLayout()
        btns_row.addStretch(1)
        btn_cancel = QPushButton(cancel_text or "Cancelar", card)
        btn_cancel.setStyleSheet(button_style("#7F8C8D", "#95A5A6", "#707B7C"))
        btn_cancel.setObjectName("GhostButton")
        btn_cancel.setCursor(Qt.PointingHandCursor)
        btn_cancel.clicked.connect(self.reject)
        btn_ok = QPushButton(ok_text or "Confirmar", card)
        btn_ok.setStyleSheet(button_style("#7C3AED", "#6D28D9", "#5B21B6"))
        btn_ok.setObjectName("PrimaryButton")
        btn_ok.setCursor(Qt.PointingHandCursor)
        btn_ok.clicked.connect(self.accept)
        btns_row.addWidget(btn_cancel)
        btns_row.addWidget(btn_ok)
        card_layout.addLayout(btns_row)

        root.addWidget(card)

        self.setWindowOpacity(0.0)
        QTimer.singleShot(0, self._animate_entrance)

    def _animate_entrance(self):
        anim = QPropertyAnimation(self, b"windowOpacity", self)
        anim.setDuration(180)
        anim.setStartValue(0.0)
        anim.setEndValue(1.0)
        anim.setEasingCurve(QEasingCurve.OutCubic)
        anim.start(QPropertyAnimation.DeleteWhenStopped)


def _warn_if_expiring_soon(parent=None):
    meta = _load_license_local()
    exp = str(meta.get("expires_at") or "").strip()
    if not exp:
        return
    try:
        dt = datetime.fromisoformat(exp.replace("Z", "+00:00"))
        now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.utcnow()
    except Exception:
        return
    delta = dt - now
    if delta.days <= 7:
        QMessageBox.warning(
            parent,
            "Licença",
            f"Sua licença expira em {delta.days} dia(s). Renove com o suporte."
        )


def ensure_license_valid(app) -> bool:
    preset_key = get_saved_key()
    initial_msg = ""
    revoked_msg = None
    if preset_key:
        ok, msg, meta = check_license_with_supabase(preset_key)
        if ok:
            persist_key(preset_key, meta)
            _warn_if_expiring_soon(app)
            return True
        initial_msg = msg or ""
        low = initial_msg.lower()
        if "desativad" in low or "inativa" in low or "revog" in low:
            revoked_msg = initial_msg
    while True:
        parent = app.activeWindow() if hasattr(app, "activeWindow") else None
        dlg = LicenseDialog(parent, preset_key=preset_key)
        if initial_msg:
            try:
                dlg.msg.setText(initial_msg)
            except Exception:
                pass
        if revoked_msg:
            try:
                dlg.set_revoked_mode(revoked_msg)
            except Exception:
                pass
        result = dlg.exec()
        if result == QDialog.Accepted:
            _warn_if_expiring_soon(app)
            return True
        # se cancelar, confirma se quer encerrar ou voltar a ativar
        confirm = ConfirmDialog(
            title="É necessário ativar a licença",
            message="Para continuar, você precisa ativar a licença.\nDeseja encerrar o aplicativo agora?",
            ok_text="Encerrar",
            cancel_text="Voltar e ativar",
            parent=parent,
        )
        if confirm.exec() == QDialog.Accepted:
            return False
        # se escolher voltar, reabre o diálogo de licença
# =============================================================================
# Robot infra helpers
# =============================================================================
def only_digits(text: str) -> str:
    return "".join(ch for ch in str(text or "") if ch.isdigit())


def _robot_log(message: str) -> None:
    try:
        print(message, file=sys.stderr)
    except Exception:
        pass


def get_robot_supabase(preferences: Optional[Dict[str, Any]] = None) -> Tuple[Optional[str], Optional[str]]:
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = _get_supabase_service_role_key()
    if url and key:
        return (url, key)
    return (None, None)


_robot_api_config: Optional[Dict[str, Any]] = None


def fetch_robot_config_from_api() -> Optional[Dict[str, Any]]:
    url_base = (os.environ.get("FOLDER_STRUCTURE_API_URL") or os.environ.get("SERVER_API_URL") or "").strip().rstrip("/")
    if not url_base:
        return None
    try:
        headers = {}
        if "ngrok" in url_base.lower():
            headers["ngrok-skip-browser-warning"] = "true"
        response = requests.get(
            f"{url_base}/api/robot-config",
            params={"technical_id": ROBOT_TECHNICAL_ID},
            headers=headers,
            timeout=15,
        )
        response.raise_for_status()
        return response.json()
    except Exception:
        return None


def get_robot_api_config() -> Optional[Dict[str, Any]]:
    global _robot_api_config
    if _robot_api_config is None:
        _robot_api_config = fetch_robot_config_from_api()
    return _robot_api_config


def get_resolved_output_base() -> Optional[Path]:
    cfg = get_robot_api_config()
    if cfg and (cfg.get("base_path") or "").strip():
        try:
            return Path((cfg.get("base_path") or "").strip())
        except Exception:
            return None
    base_env = os.environ.get("BASE_PATH", "").strip()
    if base_env:
        try:
            return Path(base_env)
        except Exception:
            return None
    return None


def load_companies_from_supabase(supabase_url: str, supabase_anon_key: str) -> List[Dict[str, str]]:
    if not (supabase_url and supabase_anon_key):
        return []
    try:
        client = create_client(supabase_url.strip(), supabase_anon_key.strip())
        res = (
            client.table("company_robot_config")
            .select("companies(id,name,document,auth_mode,cert_blob_b64,cert_password)")
            .eq("robot_technical_id", ROBOT_TECHNICAL_ID)
            .eq("enabled", True)
            .execute()
        )
        rows = getattr(res, "data", None) or []
        companies: List[Dict[str, str]] = []
        for row in rows:
            company = row.get("companies") or {}
            company_id = company.get("id")
            if not company_id:
                continue
            companies.append(
                {
                    "id": company_id,
                    "name": (company.get("name") or "").strip(),
                    "doc": only_digits(company.get("document") or ""),
                    "cnpj": only_digits(company.get("document") or ""),
                    "auth_mode": (company.get("auth_mode") or AUTH_PASSWORD).strip().lower(),
                    "cert_blob_b64": company.get("cert_blob_b64") or "",
                    "cert_password": company.get("cert_password") or "",
                }
            )
        companies.sort(key=lambda item: (item.get("name") or "").lower())
        return companies
    except Exception:
        return []


def load_companies_from_supabase_by_ids(
    supabase_url: str, supabase_anon_key: str, company_ids: List[str]
) -> List[Dict[str, str]]:
    if not (supabase_url and supabase_anon_key and company_ids):
        return []
    try:
        client = create_client(supabase_url.strip(), supabase_anon_key.strip())
        res = (
            client.table("companies")
            .select("id,name,document,auth_mode,cert_blob_b64,cert_password")
            .in_("id", company_ids)
            .eq("active", True)
            .execute()
        )
        rows = getattr(res, "data", None) or []
        companies: List[Dict[str, str]] = []
        for row in rows:
            company_id = row.get("id")
            if not company_id:
                continue
            companies.append(
                {
                    "id": company_id,
                    "name": (row.get("name") or "").strip(),
                    "doc": only_digits(row.get("document") or ""),
                    "cnpj": only_digits(row.get("document") or ""),
                    "auth_mode": (row.get("auth_mode") or AUTH_PASSWORD).strip().lower(),
                    "cert_blob_b64": row.get("cert_blob_b64") or "",
                    "cert_password": row.get("cert_password") or "",
                }
            )
        order_map = {company_id: idx for idx, company_id in enumerate(company_ids)}
        companies.sort(key=lambda item: order_map.get(item.get("id"), 10**9))
        return companies
    except Exception:
        return []


def fetch_robot_display_config(supabase_url: str, supabase_anon_key: str) -> Optional[Dict[str, Any]]:
    try:
        client = create_client(supabase_url.strip(), supabase_anon_key.strip())
        office_id = ""
        current_job = _current_json_job()
        if isinstance(current_job, dict):
            office_id = str(current_job.get("office_id") or "").strip()
        if not office_id:
            try:
                office_id = str((_bridge_get_office_context(client) or {}).get("office_id") or "").strip()
            except Exception:
                office_id = ""
        query = client.table("robot_display_config").select("*").eq("robot_technical_id", ROBOT_TECHNICAL_ID)
        if office_id:
            query = query.eq("office_id", office_id)
        res = query.limit(1).execute()
        rows = getattr(res, "data", None) or []
        return rows[0] if rows else None
    except Exception:
        return None


def register_robot(supabase_url: str, supabase_anon_key: str) -> Optional[str]:
    try:
        client = create_client(supabase_url.strip(), supabase_anon_key.strip())
        api_cfg = get_robot_api_config() or {}
        segment_path = (api_cfg.get("segment_path") or ROBOT_SEGMENT_PATH_DEFAULT).strip()
        res = client.table("robots").select("id").eq("technical_id", ROBOT_TECHNICAL_ID).execute()
        rows = getattr(res, "data", None) or []
        if rows:
            robot_id = rows[0]["id"]
            upd = client.table("robots").update(
                {
                    "status": "active",
                    "segment_path": segment_path,
                    "last_heartbeat_at": datetime.now(timezone.utc).isoformat(),
                }
            ).eq("id", robot_id).execute()
            upd_rows = getattr(upd, "data", None)
            if upd_rows is None:
                _robot_log("[Robô] Aviso: update em robots não retornou linhas. Verifique RLS/permissões.")
            return robot_id
        ins = (
            client.table("robots")
            .insert(
                {
                    "technical_id": ROBOT_TECHNICAL_ID,
                    "display_name": ROBOT_DISPLAY_NAME_DEFAULT,
                    "status": "active",
                    "segment_path": segment_path,
                    "last_heartbeat_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            .select("id")
            .execute()
        )
        data = getattr(ins, "data", None) or []
        if data:
            return data[0].get("id")
        _robot_log("[Robô] Insert em robots retornou vazio. Verifique RLS/permissões na tabela robots para a anon key.")
        return None
    except Exception as exc:
        _robot_log(f"[Robô] Falha ao registrar no dashboard (robots): {exc}")
        return None


def register_robot_compat(supabase_url: str, supabase_anon_key: str) -> Optional[str]:
    try:
        client = create_client(supabase_url.strip(), supabase_anon_key.strip())
        api_cfg = get_robot_api_config() or {}
        segment_path = (api_cfg.get("segment_path") or ROBOT_SEGMENT_PATH_DEFAULT).strip()
        res = client.table("robots").select("id").eq("technical_id", ROBOT_TECHNICAL_ID).execute()
        rows = getattr(res, "data", None) or []
        if rows:
            robot_id = rows[0]["id"]
            upd = client.table("robots").update(
                {
                    "status": "active",
                    "segment_path": segment_path,
                    "last_heartbeat_at": datetime.now(timezone.utc).isoformat(),
                }
            ).eq("id", robot_id).execute()
            upd_rows = getattr(upd, "data", None)
            if upd_rows is None:
                _robot_log("[Robô] Aviso: update em robots não retornou linhas. Verifique RLS/permissões.")
            return robot_id

        ins = client.table("robots").insert(
            {
                "technical_id": ROBOT_TECHNICAL_ID,
                "display_name": ROBOT_DISPLAY_NAME_DEFAULT,
                "status": "active",
                "segment_path": segment_path,
                "last_heartbeat_at": datetime.now(timezone.utc).isoformat(),
            }
        ).execute()
        ins_data = getattr(ins, "data", None)
        if ins_data is None:
            _robot_log("[Robô] Insert em robots não retornou data. Vou tentar reler pelo technical_id.")

        reread = client.table("robots").select("id").eq("technical_id", ROBOT_TECHNICAL_ID).execute()
        reread_rows = getattr(reread, "data", None) or []
        if reread_rows:
            return reread_rows[0].get("id")

        _robot_log("[Robô] Insert em robots não ficou visível. Verifique RLS/permissões na tabela robots para a anon key.")
        return None
    except Exception as exc:
        _robot_log(f"[Robô] Falha ao registrar no dashboard (robots): {exc}")
        return None


def fetch_robot_config(supabase_url: str, supabase_anon_key: str) -> Optional[Dict[str, Any]]:
    try:
        client = create_client(supabase_url.strip(), supabase_anon_key.strip())
        res = (
            client.table("robots")
            .select("segment_path")
            .eq("technical_id", ROBOT_TECHNICAL_ID)
            .limit(1)
            .execute()
        )
        rows = getattr(res, "data", None) or []
        if not rows:
            return None
        return {"segment_path": (rows[0].get("segment_path") or "").strip() or None}
    except Exception:
        return None


def update_robot_heartbeat(supabase_url: str, supabase_anon_key: str, robot_id: str) -> None:
    try:
        client = create_client(supabase_url.strip(), supabase_anon_key.strip())
        client.table("robots").update(
            {
                "last_heartbeat_at": datetime.now(timezone.utc).isoformat(),
            }
        ).eq("id", robot_id).execute()
    except Exception as exc:
        _robot_log(f"[Robô] Falha ao atualizar heartbeat: {exc}")
        pass


def update_robot_status(supabase_url: str, supabase_anon_key: str, robot_id: str, status: str) -> None:
    try:
        client = create_client(supabase_url.strip(), supabase_anon_key.strip())
        client.table("robots").update(
            {
                "status": status,
                "last_heartbeat_at": datetime.now(timezone.utc).isoformat(),
            }
        ).eq("id", robot_id).execute()
    except Exception as exc:
        _robot_log(f"[Robô] Falha ao atualizar status '{status}': {exc}")
        pass


def _get_active_schedule_rule_ids(client: Any) -> Optional[List[str]]:
    try:
        res = (
            client.table("schedule_rules")
            .select("id")
            .eq("status", "active")
            .eq("run_daily", True)
            .execute()
        )
        rows = getattr(res, "data", None) or []
        return [row["id"] for row in rows if row.get("id")]
    except Exception:
        return None


def claim_execution_request(
    supabase_url: str,
    supabase_anon_key: str,
    robot_id: str,
    log_callback: Optional[Callable[[str], None]] = None,
) -> Optional[Dict[str, Any]]:
    try:
        client = create_client(supabase_url.strip(), supabase_anon_key.strip())
        active_rule_ids = _get_active_schedule_rule_ids(client)
        try:
            rpc_response = client.rpc(
                "claim_next_execution_request",
                {
                    "p_robot_technical_id": ROBOT_TECHNICAL_ID,
                    "p_robot_id": robot_id,
                    "p_active_schedule_rule_ids": active_rule_ids,
                },
            ).execute()
            rpc_rows = getattr(rpc_response, "data", None) or []
            if rpc_rows:
                return rpc_rows[0]
        except Exception:
            pass
        res = (
            client.table("execution_requests")
            .select("*")
            .eq("status", "pending")
            .order("execution_order")
            .order("created_at")
            .limit(10)
            .execute()
        )
        rows = getattr(res, "data", None) or []
        for row in rows:
            schedule_rule_id = row.get("schedule_rule_id")
            if schedule_rule_id is not None and active_rule_ids:
                if schedule_rule_id not in active_rule_ids:
                    continue
            execution_mode = str(row.get("execution_mode") or "sequential").strip().lower()
            execution_group_id = row.get("execution_group_id")
            if execution_mode == "sequential" and execution_group_id:
                blockers = (
                    client.table("execution_requests")
                    .select("id, execution_order, created_at")
                    .eq("execution_group_id", execution_group_id)
                    .in_("status", ["pending", "running"])
                    .order("execution_order")
                    .order("created_at")
                    .execute()
                )
                blocker_rows = getattr(blockers, "data", None) or []
                if blocker_rows and blocker_rows[0].get("id") != row.get("id"):
                    continue
            tech_ids = row.get("robot_technical_ids") or []
            if "all" in tech_ids or ROBOT_TECHNICAL_ID in tech_ids:
                (
                    client.table("execution_requests")
                    .update(
                        {
                            "status": "running",
                            "robot_id": robot_id,
                            "claimed_at": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                    .eq("id", row["id"])
                    .eq("status", "pending")
                    .execute()
                )
                claimed = (
                    client.table("execution_requests")
                    .select("*")
                    .eq("id", row["id"])
                    .eq("status", "running")
                    .limit(1)
                    .execute()
                )
                claimed_rows = getattr(claimed, "data", None) or []
                if claimed_rows:
                    return claimed_rows[0]
        return None
    except Exception as exc:
        if log_callback:
            log_callback(f"[Robô] Erro ao buscar job da fila: {exc}")
        return None


def complete_execution_request(
    supabase_url: str,
    supabase_anon_key: str,
    request_id: str,
    success: bool,
    error_message: Optional[str] = None,
) -> None:
    try:
        client = create_client(supabase_url.strip(), supabase_anon_key.strip())
        client.table("execution_requests").update(
            {
                "status": "completed" if success else "failed",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "error_message": error_message,
            }
        ).eq("id", request_id).execute()
    except Exception:
        pass


def resolve_reports_root(config: Dict[str, Any]) -> Path:
    base = (
        config.get("_resolved_output_base")
        or config.get("reports_path")
        or BASE_DIR
    )
    base_path = Path(base)
    base_path.mkdir(parents=True, exist_ok=True)
    return base_path


def resolve_date_segments(config: Dict[str, Any], reference_dt: Optional[datetime] = None) -> List[str]:
    dt = reference_dt or datetime.now()
    rule = str((config or {}).get("_date_rule") or "").strip().lower()
    year = f"{dt.year}"
    month = f"{dt.month:02d}"
    day = f"{dt.day:02d}"
    if rule == "year":
        return [year]
    if rule == "year_month":
        return [year, month]
    if rule == "year_month_day":
        return [year, month, day]
    return []


def resolve_company_output_dir(config: Dict[str, Any], company_name: str, reference_dt: Optional[datetime] = None) -> Path:
    reports_root = resolve_reports_root(config)
    segment_slug = str((config.get("_segment_path") or ROBOT_SEGMENT_PATH_DEFAULT)).strip().replace("/", os.sep)
    safe_name = _sanitize_name(company_name)
    out_dir = reports_root / safe_name / segment_slug
    for part in resolve_date_segments(config, reference_dt):
        out_dir = out_dir / part
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def resolve_report_output_path(config: Dict[str, Any], reference_dt: Optional[datetime] = None) -> Path:
    reports_root = resolve_reports_root(config)
    segment_slug = str((config.get("_segment_path") or ROBOT_SEGMENT_PATH_DEFAULT)).strip().replace("/", os.sep)
    report_dir = reports_root / segment_slug
    for part in resolve_date_segments(config, reference_dt):
        report_dir = report_dir / part
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir / "Relatorio de Certidoes.pdf"


def ensure_report_in_vm_base(config: Dict[str, Any], pdf_path: Path, reference_dt: Optional[datetime] = None) -> Path:
    target_path = resolve_report_output_path(config, reference_dt)
    try:
        if pdf_path.resolve() == target_path.resolve():
            return target_path
    except Exception:
        if str(pdf_path) == str(target_path):
            return target_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(pdf_path), str(target_path))
    return target_path


def normalize_result_status(status_text: str) -> str:
    norm = _normalize_status_text(status_text)
    # Impedimento da Caixa tem prioridade: se a página pede consulta manual, é irregular (não negativa).
    if (
        "informacoes disponiveis nao sao suficientes" in norm
        and "comprovacao automatica da regularidade do empregador perante o fgts" in norm
    ) or "conectividade social" in norm:
        return "irregular"
    if "irregular" in norm:
        return "irregular"
    if "positiva com efeito de negativa" in norm or "positiva com efeitos de negativa" in norm:
        return "regular"
    if "regular" in norm and "irregular" not in norm:
        return "regular"
    if "negativa" in norm:
        return "negativa"
    if "positiva" in norm:
        return "positiva"
    return "irregular"


def sync_certidao_result(
    supabase_url: str,
    supabase_anon_key: str,
    company_id: str,
    tipo_certidao: str,
    status_text: str,
    pdf_path: Optional[Path],
    data_consulta: datetime,
) -> None:
    if not (supabase_url and supabase_anon_key and company_id and tipo_certidao):
        return
    file_value = str(pdf_path) if pdf_path else None
    periodo = data_consulta.strftime("%Y-%m")
    document_date = data_consulta.strftime("%Y-%m-%d")
    payload = {
        "company_id": company_id,
        "tipo_certidao": tipo_certidao,
        "status": normalize_result_status(status_text),
        "arquivo_pdf": file_value,
        "data_consulta": data_consulta.isoformat(),
        "periodo": periodo,
        "document_date": document_date,
        "robot_technical_id": ROBOT_TECHNICAL_ID,
    }
    try:
        base_url = supabase_url.strip().rstrip("/")
        anon_key = supabase_anon_key.strip()
        idem_key = f"{company_id}:{tipo_certidao}:{document_date}"
        headers = {
            "apikey": anon_key,
            "Authorization": f"Bearer {anon_key}",
            "Content-Type": "application/json",
        }
        try:
            requests.delete(
                f"{base_url}/rest/v1/sync_events",
                params={"idempotency_key": f"eq.{idem_key}"},
                headers={**headers, "Prefer": "return=minimal"},
                timeout=20,
            )
        except Exception:
            pass
        response = requests.post(
            f"{base_url}/rest/v1/sync_events",
            headers={**headers, "Prefer": "return=minimal"},
            json={
                "company_id": company_id,
                "tipo": "certidao_resultado",
                "payload": json.dumps(payload, ensure_ascii=False),
                "status": "sucesso",
                "idempotency_key": idem_key,
            },
            timeout=20,
        )
        response.raise_for_status()
    except Exception as exc:
        print(f"[Robô] Falha ao sincronizar certidão '{tipo_certidao}' da empresa {company_id}: {exc}", file=sys.stderr)


# =============================================================================
# UI helpers
# =============================================================================
def _format_cnpj(cnpj: str) -> str:
    digits = re.sub(r"\D", "", cnpj or "")
    if len(digits) == 14:
        return f"{digits[0:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:14]}"
    return cnpj or ""


class WatermarkLog(QFrame):
    def __init__(self, image_path: str, height=260):
        super().__init__()
        self.setStyleSheet("border:0.5px solid #3498db; border-radius:4px;")
        self.setFixedHeight(height)
        def _soften(img):
            if img.mode != "RGBA":
                img = img.convert("RGBA")
            w, h = img.size
            alpha = img.split()[-1].point(lambda p: int(p * 0.22))
            cx, cy = w / 2.0, h / 2.0
            half_x = max(1.0, cx)
            grad_x = Image.new("L", (w, 1), 0)
            gx = grad_x.load()
            for x in range(w):
                t = abs((x + 0.5 - cx) / half_x)
                t = min(1.0, t)
                fall = 1.0 - (t ** 1.8)
                gx[x, 0] = int(255 * fall)
            grad_x = grad_x.resize((w, h))
            alpha = ImageChops.multiply(alpha, grad_x)
            alpha = alpha.filter(ImageFilter.GaussianBlur(max(8, int(min(w, h) * 0.05))))
            img.putalpha(alpha)
            return img

        try:
            img = Image.open(image_path)
            img = _soften(img)
            self.pixmap = QPixmap.fromImage(ImageQt.ImageQt(img))
        except Exception:
            self.pixmap = QPixmap()
        self.text = QPlainTextEdit(self)
        self.text.setReadOnly(True)
        self.text.setStyleSheet(
            "QPlainTextEdit{background:transparent;color:#ECF0F1;font:10pt Verdana;padding:5px;}"
        )
        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.addWidget(self.text)

    def paintEvent(self, e):
        super().paintEvent(e)
        if getattr(self, "pixmap", None) and not self.pixmap.isNull():
            p = QPainter(self)
            sz = self.size()
            # aumenta levemente a logo de fundo do log
            target_w = int(sz.width() * 1.35)
            target_h = int(sz.height() * 1.35)
            scaled = self.pixmap.scaled(
                target_w, target_h, Qt.KeepAspectRatio, Qt.SmoothTransformation
            )
            x = (sz.width() - scaled.width()) // 2
            y = (sz.height() - scaled.height()) // 2
            p.drawPixmap(x, y, scaled)


class EmpresaItem(QWidget):
    def __init__(self, name: str):
        super().__init__()
        h = QHBoxLayout(self)
        h.setContentsMargins(2, 2, 2, 2)
        self.checkbox = QCheckBox(name)
        self.checkbox.setStyleSheet("QCheckBox{color:#ECF0F1;font:10pt Verdana;}")
        h.addWidget(self.checkbox)
        h.addStretch()


class ConfigDialog(QDialog):
    def __init__(self, cfg: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Caminhos Padrão")
        self.resize(700, 200)
        self.setStyleSheet(
            "background:#17202A; QLabel{color:#ECF0F1;font:10pt Verdana;} QWidget{color:#ECF0F1;}"
        )

        def row(lbl, key):
            h = QHBoxLayout()
            line = QLineEdit(cfg.get(key, ""))
            line.setStyleSheet("background:#34495E;color:#ECF0F1;border-radius:10px;padding:6px;")
            btn = QPushButton("...")
            btn.setStyleSheet(button_style("#2980B9", "#2471A3", "#1F618D"))
            btn.clicked.connect(lambda: self.browse(line))
            h.addWidget(QLabel(lbl))
            h.addWidget(line)
            h.addWidget(btn)
            return h, line

        v = QVBoxLayout(self)
        (h1, self.line_emp) = row("Pasta base das empresas:", "companies_path")
        v.addLayout(h1)
        (h2, self.line_rep) = row("Pasta para PDFs:", "reports_path")
        v.addLayout(h2)

        box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_ok = box.button(QDialogButtonBox.Ok)
        if btn_ok:
            btn_ok.setText("OK")
            btn_ok.setStyleSheet(button_style("#2ECC71", "#27AE60", "#1E9E55"))
        btn_cancel = box.button(QDialogButtonBox.Cancel)
        if btn_cancel:
            btn_cancel.setText("Cancelar")
            btn_cancel.setStyleSheet(button_style("#E74C3C", "#C0392B", "#A93226"))
        box.accepted.connect(self.accept)
        box.rejected.connect(self.reject)
        v.addWidget(box)

    def browse(self, line):
        p = QFileDialog.getExistingDirectory(self, "Selecione a pasta")
        if p:
            line.setText(p)


class ManageCompaniesDialog(QDialog):
    DIALOG_WIDTH = 560
    PAGE_1_SIZE = (520, 220)
    PAGE_3_SIZE = (520, 140)

    BTN_ADD_COLORS = ("#1ABC9C", "#16A085", "#149174")
    BTN_EDIT_COLORS = ("#9B59B6", "#8E44AD", "#7D3C98")
    BTN_DEL_COLORS = ("#E74C3C", "#C0392B", "#A93226")
    BTN_CONF_COLORS = ("#16A085", "#138D75", "#117A65")
    BTN_SAVE_COLORS = ("#2980B9", "#2471A3", "#1F618D")
    BTN_BACK_COLORS = ("#7F8C8D", "#95A5A6", "#707B7C")
    EDIT_BG = "background:#34495E;color:#ECF0F1;border-radius:10px;padding:6px;"

    def __init__(self, companies, save_cb, refresh_cb, log_cb, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Gerenciador de Empresas")
        self.setModal(True)
        self.setStyleSheet(
            "background:#17202A; QLabel{color:#ECF0F1;font:10pt Verdana;} QWidget{color:#ECF0F1;}"
        )

        self.companies = []
        for comp in companies:
            try:
                comp = dict(comp)
                comp["name"] = (comp.get("name", "") or "").upper()
                self.companies.append(comp)
            except Exception:
                continue
        self.save_cb = save_cb
        self.refresh_cb = refresh_cb
        self.log_cb = log_cb
        self._parent = parent
        self._sort_companies()

        self._base_size = QSize(self.DIALOG_WIDTH, self.PAGE_1_SIZE[1])

        main = QVBoxLayout(self)
        main.setContentsMargins(8, 8, 8, 8)
        main.setSpacing(6)
        main.setSizeConstraint(QLayout.SetMinimumSize)

        self.stack = QStackedWidget(self)
        main.addWidget(self.stack)

        menu = QWidget()
        v = QVBoxLayout(menu)
        v.setContentsMargins(10, 10, 10, 10)
        v.setSpacing(6)

        def mbtn(text, idx, colors):
            b = QPushButton(text)
            b.setStyleSheet(button_style(*colors))
            b.clicked.connect(lambda: self._switch(idx))
            v.addWidget(b)

        mbtn("➕ Adicionar Empresa", 1, self.BTN_ADD_COLORS)
        mbtn("✏️ Editar Empresa", 2, self.BTN_EDIT_COLORS)
        mbtn("🗑️ Excluir Empresa", 3, self.BTN_DEL_COLORS)

        btn_paths = QPushButton("📂 Caminhos Padrão")
        btn_paths.setStyleSheet(button_style(*self.BTN_CONF_COLORS))
        btn_paths.clicked.connect(self._open_config_from_parent)
        v.addSpacing(4)
        v.addWidget(btn_paths)
        self.stack.addWidget(menu)

        margins = (10, 10, 10, 10)
        spacing = 6

        addp = QWidget()
        va = QVBoxLayout(addp)
        va.setContentsMargins(*margins)
        va.setSpacing(spacing)

        va.addWidget(QLabel("Nome da empresa:"))
        self.add_nome = QLineEdit()
        self.add_nome.setPlaceholderText("Ex.: Empresa XPTO LTDA")
        self.add_nome.setStyleSheet(self.EDIT_BG)
        va.addWidget(self.add_nome)

        va.addWidget(QLabel("CNPJ:"))
        self.add_cnpj = QLineEdit()
        self.add_cnpj.setPlaceholderText("00.000.000/0000-00")
        self.add_cnpj.setInputMask("00.000.000/0000-00;_")
        self.add_cnpj.setStyleSheet(self.EDIT_BG)
        self.add_cnpj.editingFinished.connect(self._fetch_cnpj_on_blur)
        va.addWidget(self.add_cnpj)

        ha = QHBoxLayout()
        btn_add_save = QPushButton("Salvar")
        btn_add_save.setStyleSheet(button_style(*self.BTN_SAVE_COLORS))
        btn_add_save.clicked.connect(self._add_save)
        btn_add_back = QPushButton("↩ Voltar")
        btn_add_back.setStyleSheet(button_style(*self.BTN_BACK_COLORS))
        btn_add_back.clicked.connect(lambda: self._switch(0))
        ha.addWidget(btn_add_save)
        ha.addWidget(btn_add_back)
        va.addLayout(ha)

        self.stack.addWidget(addp)

        editp = QWidget()
        ve = QVBoxLayout(editp)
        ve.setContentsMargins(*margins)
        ve.setSpacing(spacing)

        ve.addWidget(QLabel("Selecione a empresa:"))
        self.edit_combo = QComboBox()
        self.edit_combo.setStyleSheet(self.EDIT_BG)
        self._reload_combo(self.edit_combo)
        ve.addWidget(self.edit_combo)

        ve.addWidget(QLabel("Novo nome:"))
        self.edit_nome = QLineEdit()
        self.edit_nome.setStyleSheet(self.EDIT_BG)
        ve.addWidget(self.edit_nome)

        ve.addWidget(QLabel("CNPJ:"))
        self.edit_cnpj = QLineEdit()
        self.edit_cnpj.setInputMask("00.000.000/0000-00;_")
        self.edit_cnpj.setStyleSheet(self.EDIT_BG)
        ve.addWidget(self.edit_cnpj)

        self.edit_combo.currentTextChanged.connect(self._populate_edit_fields)
        self._populate_edit_fields(self.edit_combo.currentText())

        he = QHBoxLayout()
        btn_edit_save = QPushButton("Salvar")
        btn_edit_save.setStyleSheet(button_style(*self.BTN_SAVE_COLORS))
        btn_edit_save.clicked.connect(self._edit_save)
        btn_edit_back = QPushButton("↩ Voltar")
        btn_edit_back.setStyleSheet(button_style(*self.BTN_BACK_COLORS))
        btn_edit_back.clicked.connect(lambda: self._switch(0))
        he.addWidget(btn_edit_save)
        he.addWidget(btn_edit_back)
        ve.addLayout(he)

        self.stack.addWidget(editp)

        delp = QWidget()
        vd = QVBoxLayout(delp)
        vd.setContentsMargins(*margins)
        vd.setSpacing(spacing)

        vd.addWidget(QLabel("Selecione a empresa:"))
        self.del_combo = QComboBox()
        self.del_combo.setStyleSheet(self.EDIT_BG)
        self._reload_combo(self.del_combo)
        vd.addWidget(self.del_combo)

        hd = QHBoxLayout()
        btn_del_do = QPushButton("Excluir")
        btn_del_do.setStyleSheet(button_style(*self.BTN_DEL_COLORS))
        btn_del_do.clicked.connect(self._del_do)
        btn_del_back = QPushButton("↩ Voltar")
        btn_del_back.setStyleSheet(button_style(*self.BTN_BACK_COLORS))
        btn_del_back.clicked.connect(lambda: self._switch(0))
        hd.addWidget(btn_del_do)
        hd.addWidget(btn_del_back)
        vd.addLayout(hd)

        self.stack.addWidget(delp)

        self._recalc_base_size()
        self._apply_size_for_current_page()
        self._center_on_parent()
        QTimer.singleShot(0, self._finalize_size_and_center)

    def showEvent(self, e):
        super().showEvent(e)
        self._finalize_size_and_center()

    def _finalize_size_and_center(self):
        self._recalc_base_size()
        self._apply_size_for_current_page()
        self._center_on_parent()

    def _recalc_base_size(self):
        self.setFixedWidth(self.DIALOG_WIDTH)
        base_h = self._calc_max_height()
        self._base_size = QSize(self.DIALOG_WIDTH, base_h)

    def _calc_max_height(self) -> int:
        cur = self.stack.currentIndex()
        max_h = 0
        self.setFixedWidth(self.DIALOG_WIDTH)
        for i in range(self.stack.count()):
            self.stack.setCurrentIndex(i)
            if self.layout():
                self.layout().activate()
            self.adjustSize()
            max_h = max(max_h, self.sizeHint().height())
        self.stack.setCurrentIndex(cur)
        return max_h

    def _apply_size(self, w: int, h: int):
        size = QSize(w, h)
        self.setMinimumSize(size)
        self.setMaximumSize(size)
        self.resize(size)

    def _apply_size_for_current_page(self):
        idx = self.stack.currentIndex()
        if idx == 1 and self.PAGE_1_SIZE:
            w, h = self.PAGE_1_SIZE
            self._apply_size(w, h)
        elif idx == 3 and self.PAGE_3_SIZE:
            w, h = self.PAGE_3_SIZE
            self._apply_size(w, h)
        else:
            self._apply_size(self._base_size.width(), self._base_size.height())

    def _center_on_parent(self):
        p = self.parent()
        if not p:
            return
        center = p.mapToGlobal(p.rect().center())
        g = self.frameGeometry()
        g.moveCenter(center)
        self.move(g.topLeft())

    def _open_config_from_parent(self):
        parent = getattr(self, "_parent", None)
        if parent and hasattr(parent, "_open_config_dialog"):
            try:
                parent._open_config_dialog()
            except Exception:
                pass

    def _switch(self, idx: int):
        self.stack.setCurrentIndex(idx)
        self._apply_size_for_current_page()
        self._center_on_parent()

    def _names(self):
        return [c.get("name", "") for c in self.companies]

    def _consulta_cnpj_api(self, cnpj: str):
        if not cnpj or len(cnpj) != 14:
            raise ValueError("CNPJ inválido (precisa ter 14 dígitos).")
        url = f"https://www.receitaws.com.br/v1/cnpj/{cnpj}"
        try:
            resp = requests.get(url, timeout=12)
        except Exception as e:
            raise RuntimeError(f"Falha ao consultar a API: {e}")
        if resp.status_code != 200:
            raise RuntimeError(f"Resposta inesperada da API (HTTP {resp.status_code}).")
        try:
            return resp.json()
        except Exception as e:
            raise RuntimeError(f"Não consegui ler a resposta da API: {e}")

    def _fetch_cnpj_on_blur(self):
        raw = self.add_cnpj.text().strip()
        cnpj = re.sub(r"\D", "", raw)
        if len(cnpj) != 14:
            return
        if self.add_nome.text().strip():
            return
        try:
            info = self._consulta_cnpj_api(cnpj)
            if (info or {}).get("status", "").upper() == "OK":
                nome = (info.get("nome") or "").strip()
                if nome:
                    self.add_nome.setText(nome)
        except Exception as e:
            if self.log_cb:
                self.log_cb(f"ℹ️ Não consegui preencher automaticamente o nome: {e}")

    def _sort_companies(self):
        try:
            def _key(comp):
                nm = (comp.get("name", "") or "").strip().lower()
                nm = "".join(ch for ch in unicodedata.normalize("NFKD", nm) if not unicodedata.combining(ch))
                return nm
            self.companies.sort(key=_key)
        except Exception:
            pass

    def _exists(self, name):
        return any(
            name.strip().lower() == (c.get("name", "").strip().lower())
            for c in self.companies
        )

    def _cnpj_exists(self, cnpj, ignore_name: str = ""):
        cnpj_digits = re.sub(r"\D", "", cnpj or "")
        for comp in self.companies:
            if ignore_name and comp.get("name", "") == ignore_name:
                continue
            cnpj_comp = re.sub(r"\D", "", comp.get("cnpj", "") or "")
            if cnpj_comp == cnpj_digits and cnpj_digits:
                return True
        return False

    def _persist(self):
        # Sincroniza lista interna com o parent, salva e atualiza UI
        self._sort_companies()
        try:
            if getattr(self, "_parent", None) is not None:
                self._parent.companies = [dict(c) for c in self.companies]
        except Exception:
            pass
        self.save_cb()
        self.refresh_cb()

    def _reload_combo(self, combo: QComboBox):
        self._sort_companies()
        combo.clear()
        combo.addItems(self._names())

    def _get_company(self, name):
        for c in self.companies:
            if c.get("name", "") == name:
                return c
        return None

    def _add_save(self):
        name = self.add_nome.text().strip()
        name_up = name.upper()
        raw_cnpj = self.add_cnpj.text().strip()
        cnpj = re.sub(r"\D", "", raw_cnpj)

        if not name:
            return QMessageBox.warning(self, "Aviso", "Informe um nome válido.")
        if not cnpj or len(cnpj) != 14:
            return QMessageBox.warning(self, "Aviso", "Informe um CNPJ válido (14 dígitos).")
        if self._exists(name_up):
            return QMessageBox.warning(self, "Aviso", "Já existe uma empresa com esse nome.")
        if self._cnpj_exists(cnpj):
            return QMessageBox.warning(self, "Aviso", "Já existe uma empresa com esse CNPJ cadastrado.")

        info = None
        try:
            info = self._consulta_cnpj_api(cnpj)
        except Exception as e:
            # Se a API rate-limitou ou falhou, segue adiante usando o que o usuário já preencheu
            if self.log_cb:
                self.log_cb(f"ℹ️ Validação online do CNPJ ignorada: {e}")

        status_api = (info or {}).get("status", "").upper()
        if info and status_api != "OK":
            return QMessageBox.warning(self, "Aviso", "CNPJ não encontrado na Receita. Informe um CNPJ válido.")

        nome_api = (info or {}).get("nome", "").strip()
        if not name and nome_api:
            self.add_nome.setText(nome_api)
            name_up = nome_api.upper()

        if self._exists(name_up):
            return QMessageBox.warning(self, "Aviso", "Já existe uma empresa com esse nome.")

        self.companies.append({"name": name_up or nome_api.upper(), "cnpj": cnpj})
        self._persist()
        if self.log_cb:
            self.log_cb(f"✅ Empresa cadastrada para processar: {name_up}")

        self._reload_combo(self.edit_combo)
        self._reload_combo(self.del_combo)

        self.add_nome.clear()
        self.add_cnpj.clear()
        QMessageBox.information(self, "OK", "Empresa adicionada com sucesso.")

    def _populate_edit_fields(self, name):
        c = self._get_company(name)
        if not c:
            self.edit_nome.setText("")
            self.edit_cnpj.setText("")
            return
        self.edit_nome.setText(c.get("name", ""))
        cnpj = c.get("cnpj", "")
        if len(cnpj) == 14:
            fmt = f"{cnpj[0:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"
        else:
            fmt = cnpj
        self.edit_cnpj.setText(fmt)

    def _edit_save(self):
        if not self.companies:
            return QMessageBox.information(self, "Editar Empresa", "Não há empresas para editar.")
        current = self.edit_combo.currentText()
        c = self._get_company(current)
        if not c:
            return QMessageBox.warning(self, "Aviso", "Empresa não encontrada.")

        new_name = self.edit_nome.text().strip()
        new_name_up = new_name.upper()
        raw_cnpj = self.edit_cnpj.text().strip()
        new_cnpj = re.sub(r"\D", "", raw_cnpj)

        if not new_name:
            return QMessageBox.warning(self, "Aviso", "Informe um novo nome válido.")
        if not new_cnpj or len(new_cnpj) != 14:
            return QMessageBox.warning(self, "Aviso", "Informe um CNPJ válido (14 dígitos).")
        if new_name_up != current and self._exists(new_name_up):
            return QMessageBox.warning(self, "Aviso", "Já existe uma empresa com esse nome.")
        if self._cnpj_exists(new_cnpj, ignore_name=current):
            return QMessageBox.warning(self, "Aviso", "Já existe uma empresa com esse CNPJ cadastrado.")

        old = c.get("name", "")
        c["name"] = new_name_up
        c["cnpj"] = new_cnpj

        self._persist()
        if self.log_cb:
            self.log_cb(f"✏️ Dados da empresa atualizados: {old} → {new_name_up}")

        self._reload_combo(self.edit_combo)
        self._reload_combo(self.del_combo)
        self.edit_combo.setCurrentText(new_name_up)
        QMessageBox.information(self, "OK", "Empresa atualizada com sucesso.")

    def _del_do(self):
        if not self.companies:
            return QMessageBox.information(self, "Excluir Empresa", "Não há empresas para excluir.")
        current = self.del_combo.currentText()
        if not current:
            return
        if QMessageBox.question(self, "Confirmar", f"Excluir a empresa “{current}”?") == QMessageBox.Yes:
            self.companies[:] = [c for c in self.companies if c.get("name", "") != current]
            self._persist()
            if self.log_cb:
                self.log_cb(f"🗑️ Empresa removida da lista: {current}")
            self._reload_combo(self.edit_combo)
            self._reload_combo(self.del_combo)
            QMessageBox.information(self, "OK", "Empresa excluída com sucesso.")
# =============================================================================
# Automação
# =============================================================================
def _sanitize_name(name: str) -> str:
    n = unicodedata.normalize("NFKD", name or "")
    n = "".join(ch for ch in n if not unicodedata.combining(ch))
    n = re.sub(r"[^A-Za-z0-9 _.-]", "", n).strip()
    return n or "EMPRESA"


def _read_pdf_text(path: Path) -> str:
    try:
        with open(path, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            text = ""
            for page in reader.pages[:2]:
                text += page.extract_text() or ""
        return text
    except Exception:
        return ""


def _classify_status_from_text(text: str) -> str:
    low = (text or "").lower()
    if (
        "nao consta debito" in low
        or "não consta débito" in low
        or "certidao negativa de debitos" in low
        or "certidão negativa de débitos" in low
        or "situação regular" in low
    ):
        return "Negativa"
    if "positiva com efeitos de negativa" in low or "positiva com efeito de negativa" in low:
        return "Positiva com efeito de negativa"
    if "certidão positiva" in low and "negativa" not in low:
        return "Positiva"
    return "Indefinido"


def _normalize_status_text(val: str) -> str:
    base = (val or "").lower()
    norm = unicodedata.normalize("NFKD", base)
    return "".join(ch for ch in norm if not unicodedata.combining(ch))


def _is_error_status(val: str) -> bool:
    norm = _normalize_status_text(val)
    if not norm:
        return True
    return any(k in norm for k in ["erro", "falha", "timeout", "tempo esgotado", "indispon", "nao processado", "nao consegui"])


def _is_fgts_irregular_message(val: str) -> bool:
    """True quando a página da Caixa indica impedimento/consulta manual (FGTS irregular)."""
    norm = _normalize_status_text(val)
    if not norm:
        return False
    return (
        "informacoes disponiveis nao sao suficientes" in norm
        and "comprovacao automatica da regularidade do empregador perante o fgts" in norm
    ) or "conectividade social" in norm


def _detail_has_error(detail: Dict[str, str]) -> bool:
    for key in ("Estadual", "Federal", "FGTS"):
        if _is_error_status(detail.get(key, "")):
            return True
    return False


class AutomationThread(QThread):
    log = Signal(str)
    finished = Signal(dict)

    def __init__(
        self,
        companies: List[dict],
        config: dict,
        hide_browser: bool,
        supabase_url: Optional[str] = None,
        supabase_anon_key: Optional[str] = None,
        job: Optional[Dict[str, Any]] = None,
        parent=None,
    ):
        super().__init__(parent)
        self.companies = companies
        self.config = config
        self.hide_browser = hide_browser
        self.supabase_url = (supabase_url or "").strip()
        self.supabase_anon_key = (supabase_anon_key or "").strip()
        self.job = job
        self.results: Dict[str, str] = {}
        self.results_details: Dict[str, Dict[str, str]] = {}
        self.started_at: Optional[datetime] = None
        self.finished_at: Optional[datetime] = None
        self._minimize_log_sent = False
        self._chrome_proc: Optional[subprocess.Popen] = None
        self._stop_requested = False

    def stop(self) -> None:
        # Parada cooperativa (evita deixar Chrome/processos/perfil órfãos).
        try:
            self._stop_requested = True
        except Exception:
            pass
        try:
            self.requestInterruption()
        except Exception:
            pass

    # ---------- Playwright helpers ----------
    def _minimize_browser_window(self, ctx, page, log_errors: bool = False, log_once: bool = True) -> bool:
        if not self.hide_browser or not ctx or not page:
            return False
        try:
            sess = ctx.new_cdp_session(page)
            info = sess.send("Browser.getWindowForTarget")
            win_id = info.get("windowId")
            if win_id is not None:
                sess.send(
                    "Browser.setWindowBounds",
                    {"windowId": win_id, "bounds": {"windowState": "minimized"}},
                )
                if log_once and not self._minimize_log_sent:
                    self._minimize_log_sent = True
                    self.log.emit("🙈 Navegador minimizado para ficar em segundo plano.")
                return True
        except Exception as e:
            if log_errors:
                self.log.emit(f"ℹ️ Não consegui minimizar automaticamente; a janela pode aparecer. Detalhe: {e}")
        return False

    def _get_primary_page(self, context, page=None):
        try:
            if page and not page.is_closed():
                return page
        except Exception:
            pass
        try:
            for pg in context.pages:
                if not pg.is_closed():
                    return pg
        except Exception:
            pass
        return context.new_page()

    def _ensure_single_tab(self, context):
        try:
            pages = [p for p in context.pages if not p.is_closed()]
            if not pages:
                return context.new_page()
            main = pages[0]
            for extra in pages[1:]:
                try:
                    extra.close()
                except Exception:
                    pass
            return main
        except Exception:
            return context.new_page()

    def _cleanup_profile_runtime_files(self, profile_dir: Path) -> None:
        runtime_names = (
            "DevToolsActivePort",
            "SingletonCookie",
            "SingletonLock",
            "SingletonSocket",
            "lockfile",
        )
        for name in runtime_names:
            for target in (profile_dir / name, profile_dir / "Default" / name):
                try:
                    if target.exists():
                        target.unlink()
                except Exception:
                    pass

    def _kill_stale_chrome_processes(self, chrome_exe: str, profile_dir: Path, port: int) -> None:
        chrome_hint = os.path.normcase(str(chrome_exe or "")).lower()
        profile_hint = os.path.normcase(str(profile_dir.resolve())).lower()
        ps = rf"""
$chromeExe = '{chrome_hint}'
$profileDir = '{profile_hint}'
$portArg = '--remote-debugging-port={int(port)}'
Get-CimInstance Win32_Process | Where-Object {{
    $_.Name -match 'chrome' -and $_.CommandLine
}} | ForEach-Object {{
    $cmd = ($_.CommandLine | Out-String).ToLower()
    if (($chromeExe -and $cmd.Contains($chromeExe)) -or ($profileDir -and $cmd.Contains($profileDir)) -or $cmd.Contains($portArg.ToLower())) {{
        try {{ Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop }} catch {{ }}
    }}
}}
"""
        try:
            subprocess.run(
                ["powershell", "-NoProfile", "-Command", ps],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
                timeout=8,
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except Exception:
            pass

    def _start_playwright_cdp(self, max_wait: int = 60):
        self.log.emit("🌐 🚀 Preparando ambiente de automação (Playwright + Chrome)...")

        # 1) Executável: obrigatoriamente o Chrome portátil em data/Chrome/chrome.exe
        # (com override opcional apenas para testes locais via CERTIDOES_CHROME_EXE).
        exe_override = (os.environ.get("CERTIDOES_CHROME_EXE") or "").strip()
        chrome_candidates: List[Path] = []
        if exe_override:
            chrome_candidates.append(Path(exe_override))
        chrome_candidates.extend(
            [
                CHROME_EXE if isinstance(CHROME_EXE, Path) else Path(CHROME_EXE),
                Path(INTERNAL_DIR) / "Chrome" / "chrome.exe",
            ]
        )
        exe = None
        for cand in chrome_candidates:
            try:
                if cand and cand.exists():
                    exe = str(cand)
                    break
            except Exception:
                continue
        if not exe:
            raise RuntimeError("Chrome portátil não encontrado em data/Chrome/chrome.exe.")

        # 2) Perfil: sempre o mesmo (persistente)
        profile_dir = PROFILE_DIR if isinstance(PROFILE_DIR, Path) else Path(PROFILE_DIR)
        try:
            profile_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        # 3) CDP: 1 Chrome só. Se já existir escutando na porta, só conecta.
        port = int(CHROME_CDP_PORT)
        cdp_url = f"http://127.0.0.1:{port}"

        def _cdp_ready() -> bool:
            try:
                r = requests.get(f"{cdp_url}/json/version", timeout=0.6)
                if r.status_code != 200:
                    return False
                try:
                    data = r.json() or {}
                except Exception:
                    data = {}
                return bool(data.get("webSocketDebuggerUrl"))
            except Exception:
                return False

        self._cleanup_profile_runtime_files(profile_dir)
        if not _cdp_ready():
            self._kill_stale_chrome_processes(exe, profile_dir, port)
            self._cleanup_profile_runtime_files(profile_dir)
            chrome_args = [
                f"--remote-debugging-port={port}",
                "--remote-debugging-address=127.0.0.1",
                f"--user-data-dir={str(profile_dir)}",
                "--no-first-run",
                "--no-default-browser-check",
                "--remote-allow-origins=*",
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
            try:
                self._chrome_proc = subprocess.Popen(
                    [exe, *chrome_args],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    stdin=subprocess.DEVNULL,
                    cwd=str(Path(exe).parent),
                )
            except Exception as e:
                raise RuntimeError(f"Falha ao iniciar o Chrome portátil ({exe}): {e}")

            deadline = time.monotonic() + max(1.0, float(max_wait))
            while time.monotonic() < deadline:
                try:
                    if self._chrome_proc and self._chrome_proc.poll() is not None:
                        break
                except Exception:
                    pass
                if _cdp_ready():
                    break
                try:
                    time.sleep(0.25)
                except Exception:
                    pass

            if not _cdp_ready():
                try:
                    if self._chrome_proc:
                        self._chrome_proc.terminate()
                except Exception:
                    pass
                raise TimeoutError(f"Timeout ao aguardar CDP em {cdp_url}.")

        pw = sync_playwright().start()
        browser = pw.chromium.connect_over_cdp(cdp_url, timeout=max(1, int(max_wait * 1000)))
        contexts = getattr(browser, "contexts", []) or []
        if not contexts:
            # alguns casos raros podem vir sem contexto inicial; tenta abrir um
            try:
                ctx = browser.new_context(ignore_https_errors=True)
            except Exception:
                ctx = None
            if not ctx:
                raise RuntimeError("Conectou via CDP, mas não conseguiu obter um contexto do navegador.")
        else:
            ctx = contexts[0]
            try:
                ctx.set_default_timeout(PORTAL_TIMEOUT)
            except Exception:
                pass
            try:
                ctx.set_default_navigation_timeout(PORTAL_TIMEOUT)
            except Exception:
                pass

        page = None
        try:
            pages = [p for p in ctx.pages if p and not p.is_closed()]
            if pages:
                page = pages[0]
        except Exception:
            page = None
        if not page:
            page = ctx.new_page()
        try:
            page = self._ensure_single_tab(ctx)
            page.goto("about:blank", wait_until="domcontentloaded", timeout=15000)
        except Exception:
            pass

        def minimize_if_needed(target_page=None, log_errors: bool = False):
            pg = target_page or page
            self._minimize_browser_window(ctx, pg, log_errors=log_errors, log_once=True)

        # primeira tentativa (janela inicial)
        try:
            time.sleep(0.5)
        except Exception:
            pass
        minimize_if_needed()

        # ao abrir novas páginas, tentar minimizar de novo (alguns fluxos criam tabs novas)
        try:
            def _on_new_page(pg):
                minimize_if_needed(pg)
            ctx.on("page", _on_new_page)
        except Exception:
            pass
        try:
            page.set_default_timeout(PORTAL_TIMEOUT)
            ctx.set_default_navigation_timeout(PORTAL_TIMEOUT)
        except Exception:
            pass
        self.log.emit("📗 📗 Playwright/Chrome inicializados com sucesso.")
        return pw, browser, ctx, page, self._chrome_proc

    def _download_pdf_viewer_by_text(self, pdf_page, dest_path: Path, label: str = "PDF", timeout_ms: int = 2200) -> bool:
        """
        Tenta baixar pelo botão semântico do visualizador (inclusive shadow-root do pdf-viewer)
        antes de recorrer ao clique por coordenadas.
        """
        baixar_exact_rx = re.compile(r"^\s*baixar\s*$", re.IGNORECASE)
        name_rx = re.compile(r"baixar|download|save|salvar", re.IGNORECASE)

        def _header_ci(headers: dict | None, key: str) -> str:
            if not headers:
                return ""
            lk = (key or "").lower()
            try:
                for hk, hv in headers.items():
                    if str(hk).lower() == lk:
                        return str(hv or "")
            except Exception:
                return ""
            return ""

        def _looks_like_pdf_response(resp) -> bool:
            try:
                status = int(resp.status or 0)
            except Exception:
                status = 0
            if status < 200 or status >= 400:
                return False

            try:
                url = str(resp.url or "").lower()
            except Exception:
                url = ""

            try:
                headers = resp.headers or {}
            except Exception:
                headers = {}

            ctype = _header_ci(headers, "content-type").lower()
            cdisp = _header_ci(headers, "content-disposition").lower()

            if "application/pdf" in ctype:
                return True
            if "attachment" in cdisp and (".pdf" in cdisp or "filename=" in cdisp):
                return True
            if ".pdf" in url:
                return True
            if any(tok in url for tok in ("download", "baixar", "save", "certidao", "relatorio", "report")):
                if "octet-stream" in ctype or "application/" in ctype or "attachment" in cdisp:
                    return True
            return False

        def _save_response_body(resp, source_desc: str) -> bool:
            try:
                data = resp.body()
                if not data:
                    return False
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                dest_path.write_bytes(data)
                self.log.emit(f"✅ {label} baixado por request/response em {source_desc} (url: {resp.url}).")
                return True
            except Exception:
                return False

        def _trigger_and_capture(action, source_desc: str) -> bool:
            try:
                with pdf_page.expect_download(timeout=timeout_ms) as dl_info:
                    action()
                download = dl_info.value
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                download.save_as(str(dest_path))
                self.log.emit(f"✅ {label} baixado pelo botão semântico em {source_desc}.")
                return True
            except Exception:
                pass

            try:
                with pdf_page.expect_response(lambda r: _looks_like_pdf_response(r), timeout=timeout_ms) as resp_info:
                    action()
                resp = resp_info.value
                if _save_response_body(resp, source_desc):
                    return True
            except Exception:
                pass

            return False

        try:
            viewport = pdf_page.viewport_size or {"width": 1366, "height": 768}
            pdf_page.mouse.move((viewport.get("width", 1366) or 1366) / 2, (viewport.get("height", 768) or 768) / 2)
            pdf_page.wait_for_timeout(120)
        except Exception:
            pass

        scopes = [("page", pdf_page)]
        try:
            for idx, fr in enumerate(pdf_page.frames):
                scopes.append((f"frame#{idx}", fr))
        except Exception:
            pass

        def _scope_locators(scope):
            return [
                ("role_button_baixar_exact", scope.get_by_role("button", name=baixar_exact_rx)),
                ("role_button_baixar_literal", scope.get_by_role("button", name="Baixar")),
                ("role_aria_baixar_exact", scope.locator("[role='button'][aria-label='Baixar'], [role='button'][aria-label='baixar']")),
                ("role_title_baixar_exact", scope.locator("[role='button'][title='Baixar'], [role='button'][title='baixar']")),
                ("role_button_name", scope.get_by_role("button", name=name_rx)),
                ("role_aria_label", scope.locator("[role='button'][aria-label*='Baixar' i], [role='button'][aria-label*='Download' i], [role='button'][aria-label*='Save' i], [role='button'][aria-label*='Salvar' i]")),
                ("role_title", scope.locator("[role='button'][title*='Baixar' i], [role='button'][title*='Download' i], [role='button'][title*='Save' i], [role='button'][title*='Salvar' i]")),
                ("cr_icon_button_id_save", scope.locator("cr-icon-button#save")),
                ("cr_icon_button_id_download", scope.locator("cr-icon-button#download")),
                ("save_id_role_button", scope.locator("#save[role='button']")),
                ("download_id_role_button", scope.locator("#download[role='button']")),
                ("save_id_button", scope.locator("#save")),
                ("download_id_button", scope.locator("#download")),
                ("text_baixar_exact", scope.get_by_text(baixar_exact_rx)),
                ("text_fallback", scope.get_by_text(name_rx)),
            ]

        for scope_name, scope in scopes:
            for desc, loc in _scope_locators(scope):
                try:
                    cnt = loc.count()
                except Exception:
                    cnt = 0
                if cnt <= 0:
                    continue

                for i in range(min(cnt, 5)):
                    btn = loc.nth(i)
                    try:
                        btn.scroll_into_view_if_needed()
                    except Exception:
                        pass

                    actions = (
                        lambda: btn.click(no_wait_after=True),
                        lambda: btn.click(force=True, no_wait_after=True),
                        lambda: btn.evaluate("el => el.click()"),
                    )

                    for action in actions:
                        if _trigger_and_capture(action, f"{scope_name}/{desc}"):
                            return True

        js_has_save_btn = """() => {
            const viewer = document.querySelector('pdf-viewer#viewer') || document.querySelector('pdf-viewer');
            if (!viewer || !viewer.shadowRoot) return false;
            const toolbar = viewer.shadowRoot.querySelector('viewer-toolbar#toolbar') || viewer.shadowRoot.querySelector('viewer-toolbar');
            if (!toolbar || !toolbar.shadowRoot) return false;
            const downloads = toolbar.shadowRoot.querySelector('viewer-download-controls#downloads') || toolbar.shadowRoot.querySelector('viewer-download-controls');
            if (!downloads || !downloads.shadowRoot) return false;
            return !!downloads.shadowRoot.querySelector('#save,#download,[id="save"],[id="download"]');
        }"""
        js_click_save_btn = """() => {
            const viewer = document.querySelector('pdf-viewer#viewer') || document.querySelector('pdf-viewer');
            if (!viewer || !viewer.shadowRoot) return false;
            const toolbar = viewer.shadowRoot.querySelector('viewer-toolbar#toolbar') || viewer.shadowRoot.querySelector('viewer-toolbar');
            if (!toolbar || !toolbar.shadowRoot) return false;
            const downloads = toolbar.shadowRoot.querySelector('viewer-download-controls#downloads') || toolbar.shadowRoot.querySelector('viewer-download-controls');
            if (!downloads || !downloads.shadowRoot) return false;
            const saveBtn = downloads.shadowRoot.querySelector('#save,#download,[id="save"],[id="download"]');
            if (!saveBtn) return false;
            saveBtn.click();
            return true;
        }"""

        for scope_name, scope in scopes:
            try:
                has_btn = bool(scope.evaluate(js_has_save_btn))
            except Exception:
                has_btn = False
            if not has_btn:
                continue

            try:
                def _click_shadow_save():
                    clicked = bool(scope.evaluate(js_click_save_btn))
                    if not clicked:
                        raise Exception("save_button_not_clicked")
                if _trigger_and_capture(_click_shadow_save, f"{scope_name}/shadow-save"):
                    return True
            except Exception:
                continue

        self.log.emit(f"ℹ️ Não encontrei/clickou botão semântico 'Baixar/Salvar' no viewer (escopos verificados: {len(scopes)}).")
        return False

    def _download_pdf_viewer(self, pdf_page, dest_path: Path, label: str = "PDF") -> bool:
        try:
            pdf_page.wait_for_load_state("load", timeout=PORTAL_TIMEOUT)
        except Exception:
            pass
        try:
            pdf_page.wait_for_timeout(450)
        except Exception:
            pass
        if not self.hide_browser:
            try:
                pdf_page.bring_to_front()
            except Exception:
                pass
        else:
            self._minimize_browser_window(pdf_page.context, pdf_page, log_once=True)

        try:
            if self._download_pdf_viewer_by_text(pdf_page, dest_path, label=label, timeout_ms=2200):
                self._minimize_browser_window(pdf_page.context, pdf_page, log_once=True)
                return True
        except Exception as e:
            self.log.emit(f"⚠️ Falha ao tentar o botão semântico de download do {label}: {e}")

        viewport = pdf_page.viewport_size or {"width": 1366, "height": 768}
        vw = viewport.get("width", 1366) or 1366
        vh = viewport.get("height", 768) or 768

        self.log.emit("ℹ️ Vou estimar onde clicar para baixar.")

        # Coordenada validada manualmente via Playwright no viewer do Chrome/Edge da SEFAZ GO:
        # o ícone de download fica aproximadamente 104px da borda direita no toolbar superior.
        toolbar_download_points = [
            (max(1, vw - 104), 28),
            (max(1, vw - 104), 30),
            (max(1, vw - 106), 28),
            (max(1, vw - 102), 28),
            (max(1, vw - 104), 26),
            (max(1, vw - 108), 28),
            (max(1, vw - 100), 28),
        ]

        # estimativa baseada em resolução (metade superior/direita)
        def build_coords(box=None):
            if box:
                x_left = box["x"] + box["width"] * 0.55
                x_right = box["x"] + box["width"] - 4
                y_top = box["y"] + box["height"] * 0.02
                y_bottom = box["y"] + box["height"] * 0.35
            else:
                x_left = vw * 0.55
                x_right = vw - 4
                y_top = vh * 0.02
                y_bottom = vh * 0.35
            width = max(1.0, x_right - x_left)
            height = max(1.0, y_bottom - y_top)
            side = min(max(40.0, min(width, height)), 140.0)
            area_x1 = max(1.0, x_right)
            area_x0 = max(0.0, area_x1 - side)
            area_y0 = max(0.0, y_top)
            area_y1 = min(vh, area_y0 + side, y_bottom)
            if area_y1 - area_y0 < 20:
                area_y1 = min(vh, area_y0 + max(side, 40.0))
            side = min(area_x1 - area_x0, area_y1 - area_y0)
            top_row_y = area_y0 + side * 0.12
            prim = [
                (area_x1 - 5, area_y0 + 5),
                (area_x1 - 12, area_y0 + 12),
                (area_x1 - 20, area_y0 + 8),
                (area_x1 - side * 0.25, top_row_y),
                (area_x1 - side * 0.12, top_row_y),
                (area_x1 - side * 0.05, top_row_y),
                (area_x1 - side * 0.30, area_y0 + side * 0.30),
            ]
            sweep_step = max(8, int(side / 10))
            grid = [
                (x, y)
                for y in range(int(area_y0), int(area_y1), sweep_step)
                for x in range(int(area_x0), int(area_x1), sweep_step)
            ]
            edge_x = area_x1 - max(4, sweep_step // 2)
            grid.extend(
                (edge_x, y)
                for y in range(int(area_y0), int(area_y1), max(6, sweep_step // 2))
            )
            if not grid:
                grid = [(area_x1 - side * 0.2, top_row_y)]
            return prim + grid

        candidates = toolbar_download_points + build_coords()

        print_deadzone = None
        try:
            print_btn = pdf_page.locator("button[aria-label*='Print' i], button[aria-label*='Imprimir' i]")
            if print_btn.count() > 0:
                bbox = print_btn.nth(0).bounding_box()
                if bbox:
                    margin = 16
                    print_deadzone = (
                        bbox.get("x", 0) - margin,
                        bbox.get("y", 0) - margin,
                        bbox.get("x", 0) + bbox.get("width", 0) + margin,
                        bbox.get("y", 0) + bbox.get("height", 0) + margin,
                    )
        except Exception:
            print_deadzone = None

        download_btns = pdf_page.locator("button[aria-label*='Download' i], button[aria-label*='Baixar' i]")
        try:
            dl_count = download_btns.count()
        except Exception:
            dl_count = 0
        for idx in range(dl_count):
            try:
                with pdf_page.expect_download(timeout=2500) as download_info:
                    download_btns.nth(idx).click()
                download = download_info.value
                self._minimize_browser_window(pdf_page.context, pdf_page, log_once=True)
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                download.save_as(str(dest_path))
                self.log.emit(f"✅ {label} baixado via botão de Download.")
                return True
            except Exception:
                continue

        points = []
        seen = set()
        for pt in candidates:
            try:
                key = (int(pt[0]), int(pt[1]))
            except Exception:
                continue
            if print_deadzone:
                x0, y0, x1, y1 = print_deadzone
                if x0 <= key[0] <= x1 and y0 <= key[1] <= y1:
                    continue
            if key in seen:
                continue
            seen.add(key)
            points.append((pt[0], pt[1]))

        spinner = ["⏳", "🔄", "🌀", "⌛"]
        attempts = 0
        download = None
        last_error = None

        try:
            pdf_page.mouse.move(vw / 2, vh / 2)
            pdf_page.wait_for_timeout(150)
        except Exception:
            pass

        for cx, cy in points:
            attempts += 1
            if attempts == 1 or attempts % 8 == 0:
                try:
                    ch = spinner[(attempts // 8) % len(spinner)]
                    self.log.emit(f"{ch} Baixando {label} (tentativa {attempts})")
                except Exception:
                    pass
            try:
                with pdf_page.expect_download(timeout=700) as dl_info:
                    pdf_page.mouse.click(cx, cy, button="left", click_count=1)
                download = dl_info.value
                self._minimize_browser_window(pdf_page.context, pdf_page)
                break
            except Exception as e:
                last_error = e
                try:
                    pdf_page.wait_for_timeout(60)
                except Exception:
                    pass

        if not download:
            self.log.emit(f"❌ Não consegui disparar download do {label} pelo visualizador. Último erro: {last_error}")
            return False

        try:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            download.save_as(str(dest_path))
            self.log.emit(f"✅ {label} salvo via visualizador em: {dest_path}")
            self._minimize_browser_window(pdf_page.context, pdf_page, log_once=True)
            return True
        except Exception as e:
            self.log.emit(f"⚠️ Baixei o {label}, mas não consegui salvar em '{dest_path}'. Detalhe: {e}")
            self._minimize_browser_window(pdf_page.context, pdf_page)
            return False

    # ---------- Certidão Estadual (GO) ----------
    def _certidao_estadual(self, context, cnpj: str, out_dir: Path, page=None) -> Optional[Path]:
        page = self._get_primary_page(context, page)
        page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        url = "https://www.sefaz.go.gov.br/certidao/emissao/"
        self.log.emit("🌐 Abrindo certidão estadual (SEFAZ GO)...")
        page.goto(url, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(800)
        try:
            page.locator("#Certidao\\.TipoDocumentoCNPJ").check()
        except Exception:
            pass
        page.locator("#Certidao\\.NumeroDocumentoCNPJ").fill(cnpj)
        pdf_bytes: List[bytes] = []
        dest_path = out_dir / "estadual_go.pdf"

        def on_resp(resp):
            ctype = resp.headers.get("content-type", "").lower()
            if "application/pdf" in ctype:
                try:
                    pdf_bytes.append(resp.body())
                except Exception:
                    pass

        page.on("response", on_resp)
        with page.expect_popup() as pop_info:
            page.locator('input[type=submit][value="Emitir"]').click()
        pop = pop_info.value
        pop.wait_for_load_state("domcontentloaded")
        if "Acesso Negado" in (pop.title() or ""):
            self.log.emit("⚠️ Acesso negado na SEFAZ GO.")
            return None

        # Captura PDF diretamente no popup (onde é servido)
        pop_pdf: List[bytes] = []
        def on_pop_resp(resp):
            ctype = resp.headers.get("content-type", "").lower()
            if "application/pdf" in ctype and "certidao.asp" in resp.url.lower():
                try:
                    pop_pdf.append(resp.body())
                except Exception:
                    pass
        pop.on("response", on_pop_resp)

        try:
            pop.wait_for_timeout(800)
        except Exception:
            pass

        confirm_btn = pop.locator("#Certidao\\.ConfirmaNomeContribuinteSim")
        confirm_needed = False
        try:
            confirm_needed = confirm_btn.count() > 0 and confirm_btn.first.is_visible(timeout=1200)
        except Exception:
            confirm_needed = False

        if confirm_needed:
            self.log.emit("ℹ️ SEFAZ GO pediu confirmação do contribuinte. Vou clicar em 'Sim'.")
            try:
                with pop.expect_response(
                    lambda r: "certidao.asp" in r.url.lower()
                    and "pdf" in r.headers.get("content-type", "").lower(),
                    timeout=15000,
                ) as resp_info:
                    confirm_btn.first.click(timeout=2000)
                resp = resp_info.value
                try:
                    pop_pdf.append(resp.body())
                except Exception:
                    pass
            except Exception:
                try:
                    confirm_btn.first.click(timeout=2000)
                except Exception:
                    self.log.emit("⚠️ Não consegui clicar no botão de confirmação da SEFAZ GO.")
        else:
            self.log.emit("ℹ️ A SEFAZ GO abriu direto no PDF; seguindo para o download.")
        pop.wait_for_timeout(4000)

        try:
            downloaded = self._download_pdf_viewer(pop, dest_path, label="certidão estadual")
            if downloaded:
                return dest_path
        except Exception as e:
            self.log.emit(f"⚠️ Falha ao tentar clicar para baixar a certidão estadual: {e}")

        # se veio PDF do popup, prioriza ele
        if pop_pdf:
            dest_path.write_bytes(pop_pdf[-1])
            self.log.emit(f"📄 Certidão estadual salva: {dest_path}")
            return dest_path

        # se não vier o PDF, salva o container inteiro (HTML) como fallback
        if pdf_bytes:
            dest_path.write_bytes(pdf_bytes[-1])
            self.log.emit(f"📄 Certidão estadual salva: {dest_path}")
            return dest_path
        try:
            container_html = pop.locator("#container").evaluate("el => el.outerHTML")
            fallback = out_dir / "estadual_go_container.html"
            fallback.write_text(container_html or "", encoding="utf-8")
            self.log.emit(f"💾 Container salvo em {fallback} (PDF não interceptado).")
        except Exception as e:
            self.log.emit(f"⚠️ Falha ao salvar container da certidão estadual: {e}")
        self.log.emit("⚠️ Não consegui baixar a certidão estadual.")
        return None

    # ---------- Certidão Federal (RFB) ----------
    def _certidao_federal(self, context, cnpj: str, out_dir: Path, page=None) -> Tuple[Optional[Path], Optional[str]]:
        def tentar(pg) -> Tuple[Optional[Path], Optional[str], bool]:
            pdf_bytes: List[bytes] = []
            alert_seen = False
            loader_selector = "div.backdrop div.loading-container"
            dest_path = out_dir / "federal.pdf"

            def on_resp(resp):
                ctype = resp.headers.get("content-type", "").lower()
                if "application/pdf" in ctype:
                    try:
                        pdf_bytes.append(resp.body())
                    except Exception:
                        pass

            pg.on("response", on_resp)
            url = "https://servicos.receitafederal.gov.br/servico/certidoes/#/home/cnpj"
            self.log.emit("🌐 Abrindo certidão federal (RFB)...")
            pg.goto(url, wait_until="domcontentloaded", timeout=120000)
            pg.wait_for_timeout(2000)
            try:
                pg.locator("button:has-text('Aceitar')").click(timeout=4000)
            except Exception:
                pass
            try:
                pg.fill('input[placeholder="Informe o CNPJ"]', cnpj)
            except Exception as e:
                return None, f"Erro ao preencher CNPJ: {e}", False

            try:
                pg.locator("button:has-text('Emitir Certidão')").click()
            except Exception as e:
                self.log.emit(f"⚠️ Falha ao clicar em Emitir Certidão: {e}")

            try:
                start_wait = time.time()
                while time.time() - start_wait < 30:
                    loader = pg.locator(loader_selector)
                    if loader.count() > 0 and loader.nth(0).is_visible():
                        pg.wait_for_timeout(250)
                        continue
                    break
            except Exception:
                pass

            start = time.time()
            last_msg = ""
            while time.time() - start < 60:
                try:
                    loader = pg.locator(loader_selector)
                    if loader.count() > 0 and loader.nth(0).is_visible():
                        pg.wait_for_timeout(250)
                        continue
                except Exception:
                    pass
                if pdf_bytes:
                    dest_path.write_bytes(pdf_bytes[-1])
                    return dest_path, None, False
                try:
                    msg_el = pg.locator("div.msg-resultado, #alert-content .description")
                    if msg_el.count() > 0:
                        msg_txt = (msg_el.inner_text() or "").strip()
                        if msg_txt and msg_txt != last_msg:
                            last_msg = msg_txt
                            self.log.emit(f"ℹ️ RFB retornou: {msg_txt}")
                        low = (msg_txt or "").lower()
                        if "temporariamente indispon" in low:
                            return None, "Erro: serviço indisponível (RFB)", True
                        if "não foi possível concluir" in low or "nao foi possivel concluir" in low:
                            alert_seen = True
                            return None, "Falha temporária na emissão", True
                        if "insuficientes" in low or "insuficiente" in low or "emitir a certidão pela internet" in low:
                            return None, "Possui pendências; verificar no e-CAC", False
                except Exception:
                    pass
                try:
                    link_download = pg.locator("a:has-text('download do documento PDF da certidão')")
                    if link_download.count() > 0 and link_download.nth(0).is_visible():
                        with pg.expect_download(timeout=15000) as dl_info:
                            link_download.nth(0).click()
                        download = dl_info.value
                        download.save_as(str(dest_path))
                        return dest_path, None, False
                except Exception:
                    pass

                try:
                    btn_modal = pg.locator("div.modal-content button.br-button.primary.btn-acao")
                    btn_emitir_nova = pg.locator("button.br-button.primary.btn-acao")
                    target_btn = None
                    if btn_modal.count() > 0 and btn_modal.nth(0).is_enabled():
                        target_btn = btn_modal.nth(0)
                    elif btn_emitir_nova.count() > 0 and btn_emitir_nova.nth(0).is_enabled():
                        target_btn = btn_emitir_nova.nth(0)
                    if target_btn:
                        try:
                            with pg.expect_download(timeout=15000) as dl_info:
                                target_btn.click()
                            download = dl_info.value
                            download.save_as(str(dest_path))
                            return dest_path, None, False
                        except Exception:
                            # Se não baixou, tentar extrair mensagem e encerrar de forma informativa
                            msg_txt = ""
                            try:
                                msg_el = pg.locator("div.msg-resultado, #alert-content .description")
                                if msg_el.count() > 0:
                                    msg_txt = (msg_el.inner_text() or "").strip()
                                    if msg_txt and msg_txt != last_msg:
                                        last_msg = msg_txt
                                        self.log.emit(f"ℹ️ RFB retornou: {msg_txt}")
                                    low = (msg_txt or "").lower()
                                    if "temporariamente indispon" in low:
                                        return None, "Erro: serviço indisponível (RFB)", True
                                    if "não foi possível concluir" in low or "nao foi possivel concluir" in low:
                                        return None, "Falha temporária na emissão", True
                                    if "insuficientes" in low or "insuficiente" in low or "emitir a certidão pela internet" in low:
                                        return None, "Possui pendências; verificar no e-CAC", False
                            except Exception:
                                pass
                            self.log.emit("ℹ️ RFB não liberou o download automático. Vou seguir com o status exibido em tela.")
                            if msg_txt:
                                return None, msg_txt, False
                except Exception:
                    pass

                pg.wait_for_timeout(2000)

            return None, "Não consegui baixar a certidão federal (timeout)", alert_seen

        for attempt in range(3):
            try:
                pg = page if attempt == 0 and page else context.new_page()
                out, status, alert_seen = tentar(pg)
                if out or (status and not alert_seen):
                    return out, status
                if alert_seen and attempt < 2:
                    self.log.emit(f"🚨 Repetindo a certidão federal (tentativa {attempt+2}/3) após aviso temporário da RFB.")
                    continue
                return out, status
            except Exception as e:
                last_err = f"Erro na tentativa {attempt+1}: {e}"
                self.log.emit(f"⚠️ {last_err}")
        return None, "Erro na certidão federal (tentativas esgotadas)"

    # ---------- Certidão FGTS (Caixa) ----------
    def _save_page_as_pdf(self, page, out_path: Path):
        try:
            page.emulate_media(media="screen")
        except Exception:
            pass
        try:
            pdf_bytes = page.pdf(format="A4", print_background=True)
            out_path.write_bytes(pdf_bytes)
            return True
        except Exception:
            try:
                png_path = out_path.with_suffix(".png")
                page.screenshot(path=str(png_path), full_page=True)
                img = Image.open(png_path).convert("RGB")
                img.save(out_path, "PDF", resolution=300.0)
                try:
                    png_path.unlink()
                except Exception:
                    pass
                return True
            except Exception:
                return False

    def _certidao_fgts(self, context, cnpj: str, out_dir: Path, page=None) -> Tuple[Optional[Path], Optional[str]]:
        page = self._get_primary_page(context, page)
        page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })

        def _read_fgts_page_text() -> str:
            try:
                return (page.locator("body").inner_text(timeout=3000) or "").strip()
            except Exception:
                return ""

        def _detect_fgts_terminal_status() -> Optional[str]:
            txt = _read_fgts_page_text()
            if not txt:
                return None
            norm = _normalize_status_text(txt)
            if "empregador nao cadastrado" in norm:
                self.log.emit("⚠️ Empregador não cadastrado no FGTS.")
                return "Empregador não cadastrado"
            if _is_fgts_irregular_message(txt):
                self.log.emit("⚠️ FGTS retornou aviso de impedimento/consulta manual. Marcando como irregular.")
                return "Irregular"
            return None

        url = "https://consulta-crf.caixa.gov.br/consultacrf/pages/consultaEmpregador.jsf"
        self.log.emit("🌐 Abrindo certidão FGTS (Caixa)...")
        page.goto(url, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(800)
        try:
            select = page.locator("#tbl select, select[id*='tipoInscricao']")
            if select.count() > 0:
                try:
                    select.select_option(label="CNPJ")
                except Exception:
                    try:
                        select.select_option(value="1")
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            page.fill("#mainForm\\:txtInscricao1", cnpj)
        except Exception:
            page.fill("input[id*='txtInscricao']", cnpj)
        page.locator("#mainForm\\:btnConsultar, button[name='mainForm:btnConsultar']").click()
        page.wait_for_timeout(2000)

        detected_status = _detect_fgts_terminal_status()
        if detected_status:
            return None, detected_status

        # verificar mensagem de empregador não cadastrado
        try:
            msg = page.locator("span.feedback-text")
            for _ in range(6):
                if msg.count() > 0:
                    txt = (msg.inner_text() or "").strip().lower()
                    if "empregador não cadastrado" in txt or "empregador nao cadastrado" in txt:
                        self.log.emit("⚠️ Empregador não cadastrado no FGTS.")
                        return None, "Empregador não cadastrado"
                page.wait_for_timeout(1000)
        except Exception:
            pass

        detected_status = _detect_fgts_terminal_status()
        if detected_status:
            return None, detected_status

        # seguir para emissão do CRF
        try:
            page.locator("#mainForm\\:j_id51").click()
            page.wait_for_timeout(1500)
        except Exception:
            pass
        detected_status = _detect_fgts_terminal_status()
        if detected_status:
            return None, detected_status
        try:
            page.locator("#mainForm\\:btnVisualizar").click()
            page.wait_for_timeout(1500)
        except Exception:
            self.log.emit("⚠️ Não consegui clicar em Visualizar antes de salvar a certidão FGTS.")

        out = out_dir / "fgts.pdf"
        ok = self._save_page_as_pdf(page, out)
        if ok:
            self.log.emit(f"📄 Certidão FGTS salva: {out}")
            return out, None
        self.log.emit("⚠️ Não consegui salvar a certidão FGTS.")
        return None, None

    def run(self):
        self.results = {}
        self.results_details = {}
        self.started_at = datetime.now()
        if not self.companies:
            self.log.emit("⚠️ Nenhuma empresa selecionada.")
            self.finished.emit({})
            return
        pw = browser = context = proc = base_page = None
        try:
            pw, browser, context, base_page, proc = self._start_playwright_cdp()
            try:
                if hasattr(context, "set_default_timeout"):
                    context.set_default_timeout(PORTAL_TIMEOUT)
            except Exception:
                pass
            try:
                if hasattr(context, "set_default_navigation_timeout"):
                    context.set_default_navigation_timeout(PORTAL_TIMEOUT)
            except Exception:
                pass
            try:
                if base_page:
                    base_page.set_viewport_size({"width": 1366, "height": 768})
            except Exception:
                pass

            def process_company(comp: dict, attempt: int, prev_detail: Optional[Dict[str, str]] = None) -> Optional[Dict[str, str]]:
                name = (comp.get("name", "") or "").strip() or "EMPRESA"
                name_disp = name.upper()
                company_id = (comp.get("id") or "").strip()
                cnpj_raw = only_digits(comp.get("doc") or comp.get("cnpj") or "")
                cnpj_fmt = _format_cnpj(cnpj_raw)
                if len(cnpj_raw) != 14:
                    self.log.emit(f"⚠️ CNPJ inválido para {name_disp}. Pulando.")
                    return None
                base_detail = prev_detail.copy() if isinstance(prev_detail, dict) else {}
                detail = {
                    "company_id": company_id,
                    "cnpj": cnpj_fmt,
                    "Estadual": base_detail.get("Estadual", "Não processado"),
                    "Federal": base_detail.get("Federal", "Não processado"),
                    "FGTS": base_detail.get("FGTS", "Não processado"),
                }
                consulta_at = datetime.now()
                out_dir = resolve_company_output_dir(self.config, name, consulta_at)
                suffix = f" (tentativa {attempt})" if attempt > 1 else ""
                self.log.emit("")  # separador visual
                self.log.emit(f"━━━━━━━━ EMPRESA: {name_disp} - {cnpj_fmt}{suffix} ━━━━━━━━")
                pdf_est = None
                pdf_fed = None
                pdf_fgts = None
                try:
                    current_page = self._ensure_single_tab(context)
                    # Estadual
                    if _is_error_status(detail.get("Estadual", "")):
                        try:
                            pdf_est = self._certidao_estadual(context, cnpj_raw, out_dir, page=current_page)
                            if pdf_est and pdf_est.exists():
                                status = _classify_status_from_text(_read_pdf_text(pdf_est))
                                detail["Estadual"] = status
                        except Exception as e:
                            detail["Estadual"] = f"Erro: {e}"
                            self.log.emit(f"⚠️ Erro na estadual ({name_disp}): {e}")
                    # Federal
                    if _is_error_status(detail.get("Federal", "")):
                        try:
                            current_page = self._ensure_single_tab(context)
                            pdf_fed, status_fed = self._certidao_federal(context, cnpj_raw, out_dir, page=current_page)
                            if pdf_fed and pdf_fed.exists():
                                status = _classify_status_from_text(_read_pdf_text(pdf_fed))
                                detail["Federal"] = status
                            elif status_fed:
                                detail["Federal"] = status_fed
                            else:
                                detail["Federal"] = "Erro: não processado"
                        except Exception as e:
                            detail["Federal"] = f"Erro: {e}"
                            self.log.emit(f"⚠️ Erro na federal ({name_disp}): {e}")
                    # FGTS
                    if _is_error_status(detail.get("FGTS", "")):
                        try:
                            current_page = self._ensure_single_tab(context)
                            pdf_fgts, status_fgts = self._certidao_fgts(context, cnpj_raw, out_dir, page=current_page)
                            if pdf_fgts and pdf_fgts.exists():
                                txt = _read_pdf_text(pdf_fgts)
                                if _is_fgts_irregular_message(txt):
                                    st = "Irregular"
                                else:
                                    st = "Regular" if "regularidade" in txt.lower() or "situação regular" in txt.lower() else "Indefinido"
                                detail["FGTS"] = st
                            elif status_fgts:
                                detail["FGTS"] = status_fgts
                            else:
                                detail["FGTS"] = "Erro: não processado"
                        except Exception as e:
                            detail["FGTS"] = f"Erro: {e}"
                            self.log.emit(f"⚠️ Erro na FGTS ({name_disp}): {e}")
                except PlaywrightTimeoutError as e:
                    detail["Estadual"] = f"Erro: tempo esgotado ({e})"
                    detail["Federal"] = f"Erro: tempo esgotado ({e})"
                    detail["FGTS"] = f"Erro: tempo esgotado ({e})"
                    self.log.emit(f"⏱️ Tempo esgotado para {name_disp}: {e}")
                except Exception as e:
                    detail["Estadual"] = f"Erro: {e}"
                    detail["Federal"] = f"Erro: {e}"
                    detail["FGTS"] = f"Erro: {e}"
                    self.log.emit(f"⚠️ Erro geral para {name_disp}: {e}")

                # garante apenas uma aba antes de seguir para a próxima empresa
                if company_id and (attempt >= max_attempts or not _detail_has_error(detail)):
                    sync_certidao_result(
                        self.supabase_url,
                        self.supabase_anon_key,
                        company_id,
                        "estadual_go",
                        detail.get("Estadual", ""),
                        pdf_est,
                        consulta_at,
                    )
                    sync_certidao_result(
                        self.supabase_url,
                        self.supabase_anon_key,
                        company_id,
                        "federal",
                        detail.get("Federal", ""),
                        pdf_fed,
                        consulta_at,
                    )
                    sync_certidao_result(
                        self.supabase_url,
                        self.supabase_anon_key,
                        company_id,
                        "fgts",
                        detail.get("FGTS", ""),
                        pdf_fgts,
                        consulta_at,
                    )
                try:
                    self._ensure_single_tab(context)
                except Exception:
                    pass

                self.results[name] = "Processado"
                self.results_details[name] = detail
                self.log.emit("━━━━━━━━──────────────────────────────────")
                return detail

            remaining = [(comp, None) for comp in self.companies]
            attempt = 1
            max_attempts = 3
            while remaining and attempt <= max_attempts:
                if self.isInterruptionRequested() or self._stop_requested:
                    self.log.emit("⛔ Interrupção solicitada. Encerrando com limpeza...")
                    break
                if attempt > 1:
                    self.log.emit(f"🔁 Tentativa {attempt} para {len(remaining)} empresa(s) com erro/pendência.")
                next_round = []
                for comp, prev_detail in remaining:
                    if self.isInterruptionRequested() or self._stop_requested:
                        self.log.emit("⛔ Interrupção solicitada. Parando a iteração...")
                        next_round = []
                        break
                    detail = process_company(comp, attempt, prev_detail)
                    if not detail:
                        continue
                    if attempt < max_attempts and _detail_has_error(detail):
                        next_round.append((comp, detail))
                        try:
                            disp = ((comp.get("name", "") or "EMPRESA").strip() or "EMPRESA").upper()
                        except Exception:
                            disp = "EMPRESA"
                        self.log.emit(f"↩️ {disp} voltará para nova tentativa ao final.")
                remaining = next_round
                attempt += 1
            if remaining:
                self.log.emit(f"⚠️ Permaneceram {len(remaining)} empresa(s) com erro após {attempt - 1} tentativa(s).")
        finally:
            try:
                if context:
                    context.close()
            except Exception:
                pass
            try:
                if browser:
                    browser.close()
            except Exception:
                pass
            try:
                if pw:
                    pw.stop()
            except Exception:
                pass
            try:
                if proc:
                    proc.terminate()
            except Exception:
                pass
            try:
                if proc and proc.poll() is None:
                    proc.kill()
            except Exception:
                pass
            try:
                profile_dir = PROFILE_DIR if isinstance(PROFILE_DIR, Path) else Path(PROFILE_DIR)
                self._cleanup_profile_runtime_files(profile_dir)
            except Exception:
                pass
            self._chrome_proc = None
        self.finished.emit(self.results)
        self.finished_at = datetime.now()
# =============================================================================
# MainWindow
# =============================================================================
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.base_dir = Path(__file__).parent.resolve()
        self.data_dir = self.base_dir / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)

        icon = None
        for b in _bases():
            for rel in ("data/ICO/app.ico", "data/ico/app.ico", "ICO/app.ico", "ico/app.ico", "app.ico"):
                p = b / rel
                if p.exists():
                    icon = str(p)
                    break
            if icon:
                break
        if icon:
            self.setWindowIcon(QIcon(icon))
        else:
            app_icon = _load_app_icon()
            if not app_icon.isNull():
                self.setWindowIcon(app_icon)

        self.json_dir = self.data_dir / "json"
        self.json_dir.mkdir(parents=True, exist_ok=True)
        self.config_file = self.json_dir / "config_certidoes.json"
        if self.config_file.exists():
            try:
                self.config = json.loads(self.config_file.read_text(encoding="utf-8") or "{}")
            except Exception:
                self.config = {}
        else:
            self.config = {}
        if "reports_path" not in self.config:
            self.config["reports_path"] = str(self.base_dir)
        if "companies_path" not in self.config:
            self.config["companies_path"] = str(self.base_dir)

        self.companies: List[Dict[str, str]] = []
        self.output_base: Optional[Path] = get_resolved_output_base()
        api_cfg = get_robot_api_config() or {}
        self._segment_path: str = (api_cfg.get("segment_path") or ROBOT_SEGMENT_PATH_DEFAULT).strip() or ROBOT_SEGMENT_PATH_DEFAULT
        self._date_rule: str = (api_cfg.get("date_rule") or "").strip()
        if self.output_base:
            self.config["_resolved_output_base"] = str(self.output_base)
            self.config["reports_path"] = str(self.output_base)
        self._robot_supabase_url: Optional[str] = None
        self._robot_supabase_key: Optional[str] = None
        self._robot_id: Optional[str] = None
        self._last_display_config_updated_at: str = ""
        self._active_job: Optional[Dict[str, Any]] = None

        self.schedule_timer = QTimer(self)
        self.schedule_timer.timeout.connect(self._on_schedule_tick)
        self.scheduled_start: Optional[datetime] = None
        self.schedule_pending = False

        self.heartbeat_timer = QTimer(self)
        self.heartbeat_timer.timeout.connect(self._on_robot_heartbeat)
        self.display_config_timer = QTimer(self)
        self.display_config_timer.timeout.connect(self._on_display_config_poll)
        self.job_poll_timer = QTimer(self)
        self.job_poll_timer.timeout.connect(self._on_robot_poll_job)

        self.thread: Optional[AutomationThread] = None
        self._build_ui()
        self._setup_tray_icon()
        self._init_robot_integration()

    # ---------- persistence ----------
    def _save_companies(self):
        return

    def _save_config(self):
        with open(self.config_file, "w", encoding="utf-8") as f:
            json.dump(self.config, f, indent=2, ensure_ascii=False)

    def _log(self, msg: str):
        try:
            self.log_message(msg)
        except Exception:
            pass

    def _init_robot_integration(self):
        url, key = get_robot_supabase()
        self._robot_supabase_url = url
        self._robot_supabase_key = key
        if self.output_base:
            self.config["_resolved_output_base"] = str(self.output_base)
        self.config["_segment_path"] = self._segment_path
        self.config["_date_rule"] = self._date_rule
        if url and key:
            self.companies = load_companies_from_supabase(url, key)
            self._sort_companies()
            self._robot_id = register_robot_compat(url, key)
            if not (get_robot_api_config() or {}).get("segment_path"):
                robot_cfg = fetch_robot_config(url, key)
                if robot_cfg and robot_cfg.get("segment_path"):
                    self._segment_path = robot_cfg["segment_path"]
                    self.config["_segment_path"] = self._segment_path
            if self._robot_id:
                self.heartbeat_timer.start(60000)
                self.display_config_timer.start(10000)
                self.job_poll_timer.start(10000)
                QTimer.singleShot(500, self._on_display_config_poll)
                QTimer.singleShot(1500, self._on_robot_poll_job)
                self._log("[Robô] Conectado ao dashboard. Status: ativo.")
            else:
                self._log("[Robô] Supabase encontrado, mas falhou ao registrar em robots.")
        else:
            self._log("[Robô] SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY não definidos.")
        self._reload_company_list()

    def _refresh_companies_from_supabase(self):
        if not self._robot_supabase_url or not self._robot_supabase_key:
            self.companies = []
            self._reload_company_list()
            return
        self.companies = load_companies_from_supabase(self._robot_supabase_url, self._robot_supabase_key)
        self._sort_companies()
        self._reload_company_list()

    def _on_robot_heartbeat(self):
        if self._robot_id and self._robot_supabase_url and self._robot_supabase_key:
            update_robot_heartbeat(self._robot_supabase_url, self._robot_supabase_key, self._robot_id)

    def _on_display_config_poll(self):
        if not self._robot_supabase_url or not self._robot_supabase_key or self.thread and self.thread.isRunning():
            return
        cfg = fetch_robot_display_config(self._robot_supabase_url, self._robot_supabase_key)
        if not cfg:
            return
        updated = (cfg.get("updated_at") or "").strip()
        if updated and updated == self._last_display_config_updated_at:
            return
        self._last_display_config_updated_at = updated
        company_ids = cfg.get("company_ids")
        if isinstance(company_ids, list):
            self.companies = load_companies_from_supabase_by_ids(
                self._robot_supabase_url, self._robot_supabase_key, company_ids
            )
        else:
            self.companies = load_companies_from_supabase(
                self._robot_supabase_url, self._robot_supabase_key
            )
        self._sort_companies()
        self._reload_company_list()

    def _on_robot_poll_job(self):
        if not self._robot_id or not self._robot_supabase_url or not self._robot_supabase_key:
            return
        if self.thread and self.thread.isRunning():
            return
        job = claim_execution_request(
            self._robot_supabase_url, self._robot_supabase_key, self._robot_id, log_callback=self._log
        )
        if job:
            self._log("[Robô] Job da fila iniciado.")
            self._run_job(job)

    def _run_job(self, job: Dict[str, Any]):
        company_ids = job.get("company_ids") or []
        if not company_ids:
            complete_execution_request(
                self._robot_supabase_url, self._robot_supabase_key, job["id"], False, "Nenhuma empresa no job"
            )
            return
        records = load_companies_from_supabase_by_ids(
            self._robot_supabase_url or "",
            self._robot_supabase_key or "",
            company_ids,
        )
        if not records:
            complete_execution_request(
                self._robot_supabase_url, self._robot_supabase_key, job["id"], False, "Nenhuma empresa encontrada"
            )
            self._log("[Robô] Nenhuma empresa retornada pelo dashboard para este job.")
            return
        self._active_job = job
        if self._robot_id and self._robot_supabase_url and self._robot_supabase_key:
            update_robot_status(self._robot_supabase_url, self._robot_supabase_key, self._robot_id, "processing")
        self.thread = AutomationThread(
            records,
            self.config,
            hide_browser=self.chk.isChecked(),
            supabase_url=self._robot_supabase_url,
            supabase_anon_key=self._robot_supabase_key,
            job=job,
            parent=self,
        )
        self.thread.log.connect(self.log_message)
        self.thread.finished.connect(self.on_automation_finished)
        self.thread.start()
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.btn_mgr.setEnabled(False)

    def _setup_tray_icon(self):
        self._tray_icon = QSystemTrayIcon(self)
        app_icon = _load_app_icon()
        if app_icon.isNull():
            app_icon = self.windowIcon()
        if app_icon.isNull():
            app_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_ComputerIcon)
        self._tray_icon.setIcon(app_icon)
        menu = QMenu()
        show_act = QAction("Abrir janela", self)
        show_act.triggered.connect(self._show_from_tray)
        menu.addAction(show_act)
        quit_act = QAction("Fechar robô", self)
        quit_act.triggered.connect(self._quit_from_tray)
        menu.addAction(quit_act)
        self._tray_icon.setContextMenu(menu)
        self._tray_icon.activated.connect(self._on_tray_activated)
        self._tray_icon.setToolTip("Certidões - Federal / Estadual GO / FGTS")

    def _show_from_tray(self):
        self.showNormal()
        self.raise_()
        self.activateWindow()
        if self._robot_id and self._robot_supabase_url and self._robot_supabase_key:
            current_status = "processing" if self.thread and self.thread.isRunning() else "active"
            update_robot_status(self._robot_supabase_url, self._robot_supabase_key, self._robot_id, current_status)
        if not self.heartbeat_timer.isActive():
            self.heartbeat_timer.start(60000)
        if not self.display_config_timer.isActive():
            self.display_config_timer.start(10000)
        if not self.job_poll_timer.isActive():
            self.job_poll_timer.start(10000)

    def _on_tray_activated(self, reason: int):
        if reason == QSystemTrayIcon.DoubleClick:
            self._show_from_tray()

    def _quit_from_tray(self):
        if self.thread and self.thread.isRunning():
            try:
                if hasattr(self.thread, "stop"):
                    self.thread.stop()
            except Exception:
                pass
            self.thread.wait(15000)
            if self.thread.isRunning():
                self.thread.terminate()
                self.thread.wait(2000)
        if self._robot_id and self._robot_supabase_url and self._robot_supabase_key:
            update_robot_status(self._robot_supabase_url, self._robot_supabase_key, self._robot_id, "inactive")
        self.heartbeat_timer.stop()
        self.display_config_timer.stop()
        self.job_poll_timer.stop()
        self._tray_icon.hide()
        QApplication.instance().quit()

    # ---------- ui ----------
    def _build_ui(self):
        self.setWindowTitle("Consulta de Certidões (Federal/Estadual/FGTS)")
        self.resize(1100, 750)

        central = QWidget()
        main = QVBoxLayout(central)
        main.setContentsMargins(8, 8, 8, 8)
        main.setSpacing(6)

        top = QFrame()
        top.setStyleSheet("background:#2C3E50;border-radius:5px;")
        h_top = QHBoxLayout(top)
        lbl = QLabel("Consulta de Certidões (Federal / Estadual / FGTS)")
        lbl.setStyleSheet("color:#ECF0F1;font:12pt Verdana;font-weight:bold;")
        h_top.addWidget(lbl, alignment=Qt.AlignCenter)
        main.addWidget(top)

        bar2 = QHBoxLayout()
        self.edit_search = QLineEdit()
        self.edit_search.setPlaceholderText("🔍 Pesquisar empresas")
        self.edit_search.setStyleSheet("background:#34495E;color:#ECF0F1;border-radius:10px;padding:6px;")
        self.edit_search.textChanged.connect(self._apply_company_filter)
        bar2.addWidget(self.edit_search)

        self.btn_select_all = QPushButton("Selecionar tudo")
        self._style_button(self.btn_select_all, "#2980B9", "#2471A3", "#1F618D")
        self.btn_select_all.clicked.connect(self._select_all_companies)
        bar2.addWidget(self.btn_select_all)

        self.btn_clear_sel = QPushButton("Limpar seleção")
        self._style_button(self.btn_clear_sel, "#7F8C8D", "#707B7C", "#616A6B")
        self.btn_clear_sel.clicked.connect(self._clear_all_companies)
        bar2.addWidget(self.btn_clear_sel)

        self.btn_mgr = QPushButton("Atualizar Empresas")
        self._style_button(self.btn_mgr, "#8E44AD", "#7D3C98", "#6C3483")
        self.btn_mgr.clicked.connect(self.open_companies_manager)
        bar2.addWidget(self.btn_mgr)
        main.addLayout(bar2)

        bar_sched = QHBoxLayout()
        self.chk_schedule = QCheckBox("⏳ Agendar início")
        self.chk_schedule.setStyleSheet("color:#ECF0F1;font:10pt Verdana;")
        self.chk_schedule.toggled.connect(self._on_toggle_schedule)
        bar_sched.addWidget(self.chk_schedule)

        self.edit_sched_date = QLineEdit()
        self.edit_sched_date.setInputMask("00/00/0000;_")
        self.edit_sched_date.setPlaceholderText("DD/MM/AAAA")
        self.edit_sched_date.setEnabled(False)
        self.edit_sched_date.setStyleSheet("background:#34495E;color:#ECF0F1;border-radius:10px;padding:6px;")
        bar_sched.addWidget(self.edit_sched_date)

        self.edit_sched_time = QLineEdit()
        self.edit_sched_time.setInputMask("00:00;_")
        self.edit_sched_time.setPlaceholderText("HH:MM")
        self.edit_sched_time.setEnabled(False)
        self.edit_sched_time.setStyleSheet("background:#34495E;color:#ECF0F1;border-radius:10px;padding:6px;")
        bar_sched.addWidget(self.edit_sched_time)

        self.lbl_sched_countdown = QLabel("⏳ Aguardando")
        self.lbl_sched_countdown.setStyleSheet("color:#BDC3C7;font:9pt Verdana;")
        bar_sched.addWidget(self.lbl_sched_countdown)
        bar_sched.addStretch()
        main.addLayout(bar_sched)
        self._init_schedule_defaults()

        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        container = QWidget()
        self.v_empresas = QVBoxLayout(container)
        self.v_empresas.setContentsMargins(2, 2, 2, 2)
        self.v_empresas.setSpacing(2)
        self.company_items = []
        self._reload_company_list()
        self.v_empresas.addStretch()
        self.scroll.setWidget(container)
        self.scroll.setStyleSheet(
            "QScrollArea{background:#1C2833;border:1px solid #34495E;border-radius:10px;}"
        )
        main.addWidget(self.scroll, stretch=1)

        img = None
        for b in _bases():
            for rel in (
                "data/IMAGE/logo 2.png",
                "data/Image/logo 2.png",
                "data/image/logo 2.png",
                "IMAGE/logo 2.png",
                "Image/logo 2.png",
                "image/logo 2.png",
                "data/IMAGE/logo.png",
                "data/Image/logo.png",
                "data/image/logo.png",
                "IMAGE/logo.png",
                "Image/logo.png",
                "image/logo.png",
            ):
                p = Path(b) / rel
                if p.exists():
                    img = str(p)
                    break
            if img:
                break
        self.log_frame = WatermarkLog(img or "", height=240)
        main.addWidget(self.log_frame, stretch=0)

        self.chk = QCheckBox()
        self.chk.setStyleSheet("color:#ECF0F1;font:10pt Verdana;")
        self.chk.setChecked(False)
        self._update_chk_text(False)
        self.chk.toggled.connect(self._on_toggle_browser)
        main.addWidget(self.chk)

        btns = QHBoxLayout()
        self.btn_start = QPushButton("🚀 Iniciar")
        self._style_button(self.btn_start, "#2980B9", "#2471A3", "#1F618D")
        self.btn_start.clicked.connect(self.start_automation)

        self.btn_stop = QPushButton("⛔ Parar")
        self._style_button(self.btn_stop, "#C0392B", "#A93226", "#922B21")
        self.btn_stop.clicked.connect(self.stop_automation)
        self.btn_stop.setEnabled(False)

        self.btn_clear_log = QPushButton("🧹 Limpar Log")
        self._style_button(self.btn_clear_log, "#1ABC9C", "#17A589", "#148F77")
        self.btn_clear_log.clicked.connect(lambda: self.log_frame.text.clear())

        btns.addWidget(self.btn_start)
        btns.addWidget(self.btn_stop)
        btns.addWidget(self.btn_clear_log)
        main.addLayout(btns)

        footer = QLabel("© Automatize Tech")
        footer.setAlignment(Qt.AlignCenter)
        footer.setStyleSheet("color:#95A5A6;font:8pt Verdana;margin-top:4px;")
        main.addWidget(footer)

        self.setCentralWidget(central)
        self.setStyleSheet("background:#17202A; QWidget{color:#ECF0F1;}")

    # ---------- helpers ----------
    def _reload_company_list(self):
        self._sort_companies()
        for i in reversed(range(self.v_empresas.count())):
            item = self.v_empresas.itemAt(i)
            w = item.widget()
            if isinstance(w, EmpresaItem):
                w.setParent(None)
                self.v_empresas.removeWidget(w)
        self.company_items = []
        for comp in self.companies:
            name = comp.get("name", "")
            item = EmpresaItem(name)
            self.company_items.append((comp, item))
            self.v_empresas.insertWidget(self.v_empresas.count() - 1, item)
        self._apply_company_filter(self.edit_search.text())

    def _sort_companies(self):
        try:
            def _key(comp):
                nm = (comp.get("name", "") or "").strip().lower()
                nm = "".join(ch for ch in unicodedata.normalize("NFKD", nm) if not unicodedata.combining(ch))
                return nm
            self.companies.sort(key=_key)
        except Exception:
            pass

    def _style_button(self, btn: QPushButton, base: str, hover: str, pressed: str, text_color: str = "#E8F4FF"):
        btn.setStyleSheet(button_style(base, hover, pressed, text_color=text_color))

    def _generate_pdf_report(self, details: Dict[str, Dict[str, str]], started_at: Optional[datetime], finished_at: Optional[datetime]) -> Optional[str]:
        try:
            if not finished_at:
                finished_at = datetime.now()
            pdf_path = resolve_report_output_path(self.config, finished_at)

            # A4 em ~300dpi para melhor qualidade
            width, height = 2480, 3508
            margin = 140
            base_block_h = 280
            gap = 50
            total_empresas = len(details)

            def status_color(val: str):
                norm = _normalize_status_text(val)
                if any(k in norm for k in ["negativa", "regular", "valida", "nao consta debito", "nao consta debito"]):
                    return (57, 181, 74)
                if _is_error_status(val):
                    return (218, 68, 83)
                if any(k in norm for k in ["com efeito", "positiva com", "com efeito de negativa"]):
                    return (242, 201, 76)
                if any(k in norm for k in ["positiva", "pend", "inativa", "revog", "debito"]):
                    return (218, 68, 83)
                return (86, 101, 115)

            def is_debt(val: str) -> bool:
                norm = _normalize_status_text(val)
                return any(k in norm for k in ["positiva", "pend", "inativa", "revog", "debito"])

            debitos_count = 0
            erros_count = 0
            for detail in details.values():
                for st in (detail.get("Estadual"), detail.get("Federal"), detail.get("FGTS")):
                    if is_debt(st or ""):
                        debitos_count += 1
                    if _is_error_status(st or ""):
                        erros_count += 1

            def load_font(size: int, bold: bool = False):
                try:
                    name = "segoeuib.ttf" if bold else "segoeui.ttf"
                    return ImageFont.truetype(str(Path(os.getenv("WINDIR", "C:/Windows")) / "Fonts" / name), size)
                except Exception:
                    return ImageFont.load_default()

            def draw_gradient(img):
                top = (245, 249, 255)
                bottom = (224, 236, 255)
                for y in range(height):
                    ratio = y / float(height)
                    r = int(top[0] * (1 - ratio) + bottom[0] * ratio)
                    g = int(top[1] * (1 - ratio) + bottom[1] * ratio)
                    b = int(top[2] * (1 - ratio) + bottom[2] * ratio)
                    ImageDraw.Draw(img).line([(0, y), (width, y)], fill=(r, g, b))

            def load_icon():
                try:
                    img = Image.open(ICO_PATH).convert("RGBA")
                    return img.resize((140, 140), Image.LANCZOS)
                except Exception:
                    return None

            def load_logo_bg():
                for rel in ("data/image/logo.png", "data/Image/logo.png", "data/IMAGE/logo.png", "image/logo.png", "logo.png"):
                    p = self.base_dir / rel
                    if p.exists():
                        try:
                            return Image.open(p).convert("RGBA")
                        except Exception:
                            continue
                return None

            bg_logo = load_logo_bg()

            def apply_background_logo(img):
                if not bg_logo:
                    return
                w, h = img.size
                # slightly bigger watermark on the PDF background
                scale = min(w * 2.7 / bg_logo.width, h * 2.7 / bg_logo.height, 3.0)
                if scale <= 0:
                    return
                lw = max(1, int(bg_logo.width * scale))
                lh = max(1, int(bg_logo.height * scale))
                logo = bg_logo.resize((lw, lh), Image.LANCZOS)
                if logo.mode != "RGBA":
                    logo = logo.convert("RGBA")
                alpha = logo.split()[-1].point(lambda a: int(a * 0.7))
                cx, cy = lw / 2.0, lh / 2.0
                max_r = math.hypot(lw, lh) / 2.0 if lw and lh else 1.0
                inner_r = max_r * 0.82
                outer_r = max_r * 0.97
                mask = Image.new("L", (lw, lh), 0)
                pix = mask.load()
                for y in range(lh):
                    dy = (y + 0.5) - cy
                    for x in range(lw):
                        r = math.hypot((x + 0.5) - cx, dy)
                        if r <= inner_r:
                            val = 255
                        elif r >= outer_r:
                            val = 0
                        else:
                            t = (r - inner_r) / (outer_r - inner_r)
                            val = int(255 * (1 - t * t))
                        pix[x, y] = val
                # gradiente horizontal suave para evitar linhas
                grad_x = Image.new("L", (lw, 1), 0)
                gx = grad_x.load()
                edge_start = 0.64
                edge_end = 0.95
                half = max(1.0, cx)
                for x in range(lw):
                    t = abs((x + 0.5 - cx) / half)
                    t = min(1.0, t)
                    if t <= edge_start:
                        fall = 1.0
                    elif t >= edge_end:
                        fall = 0.0
                    else:
                        tt = (t - edge_start) / (edge_end - edge_start)
                        fall = 1.0 - (tt ** 1.4)
                    gx[x, 0] = int(255 * fall)
                grad_x = grad_x.resize((lw, lh))
                # gradiente vertical para suavizar topo/baixo
                grad_y = Image.new("L", (1, lh), 0)
                gy = grad_y.load()
                edge_start_y = 0.6
                edge_end_y = 0.94
                for y in range(lh):
                    t = abs((y + 0.5 - cy) / half)
                    t = min(1.0, t)
                    if t <= edge_start_y:
                        fall = 1.0
                    elif t >= edge_end_y:
                        fall = 0.0
                    else:
                        tt = (t - edge_start_y) / (edge_end_y - edge_start_y)
                        fall = 1.0 - (tt ** 1.4)
                    gy[0, y] = int(255 * fall)
                grad_y = grad_y.resize((lw, lh))
                alpha = ImageChops.multiply(alpha, mask)
                alpha = ImageChops.multiply(alpha, grad_x)
                alpha = ImageChops.multiply(alpha, grad_y)
                alpha = alpha.filter(ImageFilter.GaussianBlur(max(10, int(min(lw, lh) * 0.03))))
                logo.putalpha(alpha)
                overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
                overlay.paste(logo, ((w - lw) // 2, (h - lh) // 2), logo)
                base = img if img.mode == "RGBA" else img.convert("RGBA")
                composed = Image.alpha_composite(base, overlay)
                img.paste(composed)

            def new_canvas():
                img = Image.new("RGBA", (width, height), (255, 255, 255, 255))
                draw_gradient(img)
                apply_background_logo(img)
                return img, ImageDraw.Draw(img)

            pages = []
            cur, d = new_canvas()

            icon = load_icon()
            y = margin
            header_x = margin
            if icon:
                cur.paste(icon, (header_x, y), icon)
                header_x += 180
            title_font = load_font(80, True)
            sub_font = load_font(40, False)
            d.text((header_x, y + 10), "Relatório de Certidões", fill=(31, 41, 55), font=title_font)
            y += 80
            if started_at or finished_at:
                period = []
                if started_at:
                    period.append(f"Início: {started_at.strftime('%d/%m/%Y %H:%M')}")
                if finished_at:
                    period.append(f"Fim: {finished_at.strftime('%d/%m/%Y %H:%M')}")
                d.text((header_x, y + 20), "   •   ".join(period), fill=(90, 102, 122), font=sub_font)
            y += 120
            # resumo estatístico
            stats_font = load_font(46, True)
            # carrega ícones das métricas
            def load_icon_path(fname: str):
                for rel in (f"data/image/{fname}", f"data/Image/{fname}", f"data/IMAGE/{fname}", fname):
                    p = self.base_dir / rel
                    if p.exists():
                        return p
                return None

            casa_icon = load_icon_path("casa.png")
            moedas_icon = load_icon_path("moedas.png")
            erro_icon = load_icon_path("erro.png")

            stats_items = [
                ("Empresas", casa_icon, str(total_empresas), (52, 120, 246)),
                ("Débitos", moedas_icon, str(debitos_count), (244, 167, 66)),
                ("Erros", erro_icon, str(erros_count), (231, 76, 60)),
            ]
            stat_x = header_x
            stat_y = y
            stat_gap = 32
            stat_w = 460
            stat_h = 150
            for label, icon_path, value, color in stats_items:
                d.rounded_rectangle(
                    [(stat_x, stat_y), (stat_x + stat_w, stat_y + stat_h)],
                    radius=24,
                    fill=(color[0], color[1], color[2], 220),
                )
                # ícone opcional
                icon_size = 70
                if icon_path:
                    try:
                        icon_img = Image.open(icon_path).convert("RGBA").resize((icon_size, icon_size), Image.LANCZOS)
                        cur.paste(icon_img, (stat_x + 24, stat_y + 18), icon_img)
                        text_x = stat_x + 24 + icon_size + 12
                    except Exception:
                        text_x = stat_x + 24
                else:
                    text_x = stat_x + 24
                d.text((text_x, stat_y + 18), label, fill=(255, 255, 255), font=stats_font)
                val_font = load_font(60, True)
                val_bbox = d.textbbox((0, 0), value, font=val_font)
                val_w = val_bbox[2] - val_bbox[0]
                val_h = val_bbox[3] - val_bbox[1]
                val_x = stat_x + (stat_w - val_w) / 2
                val_y = stat_y + (stat_h - val_h) / 2 + 4
                d.text((val_x, val_y), value, fill=(255, 255, 255), font=val_font)
                stat_x += stat_w + stat_gap
            y += stat_h + 60

            def add_page():
                nonlocal cur, d, y
                pages.append(cur.convert("RGB"))
                cur, d = new_canvas()
                y = margin

            card_font = load_font(58, True)
            status_font = load_font(40, True)

            def measure_text(txt: str, font=status_font) -> Tuple[int, int]:
                bbox = d.textbbox((0, 0), txt, font=font)
                return bbox[2] - bbox[0], bbox[3] - bbox[1]

            def wrap_text_to_width(text: str, font, max_width: int) -> List[str]:
                """Quebra a linha respeitando a largura máxima do chip."""
                def line_width(t: str) -> int:
                    return measure_text(t, font)[0]

                if line_width(text) <= max_width:
                    return [text]

                words = text.split()
                lines: List[str] = []
                current = ""
                for word in words:
                    candidate = (current + " " + word).strip()
                    if current and line_width(candidate) > max_width:
                        lines.append(current)
                        current = word
                    else:
                        current = candidate
                if current:
                    lines.append(current)

                wrapped: List[str] = []
                for line in lines:
                    if line_width(line) <= max_width:
                        wrapped.append(line)
                        continue
                    # força quebra em palavras longas sem espaços
                    chunk = ""
                    for ch in line:
                        cand = chunk + ch
                        if chunk and line_width(cand) > max_width:
                            wrapped.append(chunk)
                            chunk = ch
                        else:
                            chunk = cand
                    if chunk:
                        wrapped.append(chunk)
                return wrapped or [text]

            for name in sorted(details.keys(), key=lambda n: n.lower()):
                detail = details.get(name, {})
                statuses = {
                    "Estadual": detail.get("Estadual", "Não processado"),
                    "Federal": detail.get("Federal", "Não processado"),
                    "FGTS": detail.get("FGTS", "Não processado"),
                }
                # calcula altura com base no texto real dos chips
                chips_info = []
                max_chip_inner_w = max(200, width - (margin * 2) - 140)
                line_height = measure_text("Ag", status_font)[1]
                line_gap = 6
                for label, val in [
                    ("Estadual", statuses.get("Estadual", "Não processado")),
                    ("Federal", statuses.get("Federal", "Não processado")),
                    ("FGTS", statuses.get("FGTS", "Não processado")),
                ]:
                    raw_text = f"{label}: {val}"
                    lines = wrap_text_to_width(raw_text, status_font, max_chip_inner_w)
                    max_line_w = max((measure_text(line, status_font)[0] for line in lines), default=0)
                    chip_w = min(max_chip_inner_w, max_line_w) + 60
                    chip_h = (len(lines) * line_height) + 30 + max(0, (len(lines) - 1) * line_gap)
                    chips_info.append((lines, chip_w, chip_h, label, val))
                header_h = status_font.getbbox("Ag")[3] - status_font.getbbox("Ag")[1] + 140
                chips_total = sum(ch[2] + 20 for ch in chips_info)
                block_h = max(base_block_h, header_h + chips_total + 40)
                if y + block_h + gap > height - margin:
                    add_page()
                card_top = y
                card_bottom = y + block_h
                shadow_offset = 8
                # sombra suave
                d.rounded_rectangle(
                    [(margin + shadow_offset, card_top + shadow_offset), (width - margin + shadow_offset, card_bottom + shadow_offset)],
                    radius=28,
                    fill=(210, 219, 230),
                )
                # card
                d.rounded_rectangle(
                    [(margin, card_top), (width - margin, card_bottom)],
                    radius=28,
                    fill=(255, 255, 255),
                    outline=(216, 225, 235),
                    width=3,
                )
                detail = details.get(name, {})
                name_line = name
                if isinstance(detail, dict) and detail.get("cnpj"):
                    name_line = f"{name} - {detail.get('cnpj')}"
                # ícone de prédio opcional antes do nome
                text_x = margin + 30
                predio_icon = None
                for rel in ("data/image/predio.png", "data/Image/predio.png", "data/IMAGE/predio.png", "predio.png"):
                    p = self.base_dir / rel
                    if p.exists():
                        predio_icon = p
                        break
                if predio_icon:
                    try:
                        pred_img = Image.open(predio_icon).convert("RGBA").resize((64, 64), Image.LANCZOS)
                        cur.paste(pred_img, (text_x, card_top + 26), pred_img)
                        text_x += 64 + 14
                    except Exception:
                        pass
                d.text((text_x, card_top + 26), name_line, fill=(31, 41, 55), font=card_font)
                chip_y = card_top + header_h
                chip_x = margin + 30
                for lines, chip_w, chip_h, label, val in chips_info:
                    color = status_color(val)
                    if chip_x + chip_w + margin > width:
                        chip_x = margin + 30
                        chip_y += chip_h + 20
                    d.rounded_rectangle(
                        [(chip_x, chip_y), (chip_x + chip_w, chip_y + chip_h)],
                        radius=20,
                        fill=(color[0], color[1], color[2], 220),
                        outline=None,
                    )
                    text_y = chip_y + 14
                    for line in lines:
                        d.text((chip_x + 24, text_y), line, fill=(255, 255, 255), font=status_font)
                        text_y += line_height + line_gap
                    chip_y += chip_h + 20
                y = card_bottom + gap

            if cur:
                pages.append(cur.convert("RGB"))

            if not pages:
                return None
            first, *rest = pages
            first.save(
                pdf_path,
                "PDF",
                resolution=300.0,
                save_all=True,
                append_images=rest,
            )
            return str(pdf_path)
        except Exception:
            return None

    def _apply_company_filter(self, text: str):
        text = (text or "").strip().lower()
        for comp, item in self.company_items:
            name = comp.get("name", "").lower()
            show = (not text) or (text in name)
            item.setVisible(show)

    def _select_all_companies(self):
        for _, item in self.company_items:
            if item.isVisible():
                item.checkbox.setChecked(True)

    def _clear_all_companies(self):
        for _, item in self.company_items:
            item.checkbox.setChecked(False)

    def _update_chk_text(self, checked: bool):
        if checked:
            self.chk.setText("🗕 Iniciar navegador minimizado")
        else:
            self.chk.setText("🖥️ Iniciar navegador visível")

    def _on_toggle_browser(self, checked: bool):
        self._update_chk_text(checked)

    def log_message(self, msg: str):
        self.log_frame.text.appendPlainText(msg)
        QApplication.processEvents()

    # ---------- scheduling ----------
    def _init_schedule_defaults(self):
        dt = datetime.now() + timedelta(minutes=2)
        try:
            self.edit_sched_date.setText(dt.strftime("%d/%m/%Y"))
            self.edit_sched_time.setText(dt.strftime("%H:%M"))
        except Exception:
            pass
        self.lbl_sched_countdown.clear()
        self.edit_sched_date.setEnabled(False)
        self.edit_sched_time.setEnabled(False)
        self.schedule_pending = False
        self.scheduled_start = None

    def _reset_schedule(self, keep_date: bool = False):
        self.schedule_timer.stop()
        self.scheduled_start = None
        self.schedule_pending = False
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        if not keep_date:
            try:
                self.edit_sched_date.clear()
                self.edit_sched_time.clear()
            except Exception:
                pass
        self.lbl_sched_countdown.clear()
        try:
            self.chk_schedule.setChecked(False)
        except Exception:
            pass
        self.edit_sched_date.setEnabled(False)
        self.edit_sched_time.setEnabled(False)

    def _on_toggle_schedule(self, checked: bool):
        if not checked:
            self._reset_schedule(keep_date=True)
            return
        self._init_schedule_defaults()
        self.edit_sched_date.setEnabled(True)
        self.edit_sched_time.setEnabled(True)

    def _parse_scheduled_dt(self) -> Optional[datetime]:
        try:
            date_txt = (self.edit_sched_date.text() or "").strip()
            time_txt = (self.edit_sched_time.text() or "").strip()
            if not date_txt or not time_txt:
                return None
            dt = datetime.strptime(f"{date_txt} {time_txt}", "%d/%m/%Y %H:%M")
            return dt
        except Exception:
            return None

    def _update_schedule_countdown(self):
        if not self.schedule_pending or not self.scheduled_start:
            self.schedule_timer.stop()
            self.lbl_sched_countdown.clear()
            return
        now = datetime.now()
        delta = (self.scheduled_start - now).total_seconds()
        if delta <= 0:
            self.schedule_timer.stop()
            self.lbl_sched_countdown.setText("Iniciando...")
            self.schedule_pending = False
            self.scheduled_start = None
            self._start_automation_now()
            return
        horas = int(delta // 3600)
        minutos = int((delta % 3600) // 60)
        segundos = int(delta % 60)
        self.lbl_sched_countdown.setText(f"Início em {horas:02d}:{minutos:02d}:{segundos:02d}")

    def _on_schedule_tick(self):
        self._update_schedule_countdown()

    # ---------- dialogs ----------
    def open_companies_manager(self):
        self._refresh_companies_from_supabase()
        self.log_message("🔄 Empresas recarregadas do dashboard.")

    def _open_config_dialog(self):
        dlg = ConfigDialog(self.config, self)
        if dlg.exec() == QDialog.Accepted:
            self.config["companies_path"] = dlg.line_emp.text().strip()
            self.config["reports_path"] = dlg.line_rep.text().strip() or self.config.get("reports_path", "")
            self._save_config()
            self.log_message(f"📂 Pasta das empresas: {self.config['companies_path']}")
            self.log_message(f"📁 Pasta dos PDFs: {self.config['reports_path']}")

    # ---------- automation controls ----------
    def _start_automation_now(self):
        was_scheduled = self.schedule_pending
        self.schedule_timer.stop()
        self.schedule_pending = False
        self.scheduled_start = None
        if self.thread and self.thread.isRunning():
            QMessageBox.warning(self, "Atenção", "Já existe uma automação em execução.")
            return
        selected = [comp for comp, item in self.company_items if item.checkbox.isChecked()]
        if not selected:
            QMessageBox.warning(self, "Atenção", "Selecione ao menos uma empresa.")
            if was_scheduled:
                self.btn_start.setEnabled(True)
                self.btn_stop.setEnabled(False)
                self.btn_mgr.setEnabled(True)
                self.lbl_sched_countdown.setText("⏳ Aguardando")
            return
        companies_path = (self.config.get("companies_path") or "").strip()
        if not companies_path:
            QMessageBox.warning(self, "Atenção", "Defina a pasta base das empresas em Caminhos Padrão.")
            self._open_config_dialog()
            if was_scheduled:
                self.btn_start.setEnabled(True)
                self.btn_stop.setEnabled(False)
                self.btn_mgr.setEnabled(True)
                self.lbl_sched_countdown.setText("⏳ Aguardando")
            return
        if not os.path.isdir(companies_path):
            QMessageBox.warning(self, "Atenção", "O caminho configurado para a pasta base das empresas não existe.")
            if was_scheduled:
                self.btn_start.setEnabled(True)
                self.btn_stop.setEnabled(False)
                self.btn_mgr.setEnabled(True)
                self.lbl_sched_countdown.setText("⏳ Aguardando")
            return
        self._active_job = None
        if self._robot_id and self._robot_supabase_url and self._robot_supabase_key:
            update_robot_status(self._robot_supabase_url, self._robot_supabase_key, self._robot_id, "processing")
        self.thread = AutomationThread(
            selected,
            self.config,
            hide_browser=self.chk.isChecked(),
            supabase_url=self._robot_supabase_url,
            supabase_anon_key=self._robot_supabase_key,
            parent=self,
        )
        self.thread.log.connect(self.log_message)
        self.thread.finished.connect(self.on_automation_finished)
        self.thread.start()
        self.log_message("🚀 Automação iniciada.")
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.btn_mgr.setEnabled(False)

    def start_automation(self):
        if self.thread and self.thread.isRunning():
            QMessageBox.warning(self, "Atenção", "Já existe uma automação em execução.")
            return
        if self.schedule_pending:
            QMessageBox.information(self, "Agendamento", "Já existe um agendamento em espera.")
            return
        if self.chk_schedule.isChecked():
            dt = self._parse_scheduled_dt()
            if not dt:
                QMessageBox.warning(self, "Atenção", "Preencha a data e hora para o agendamento.")
                return
            now = datetime.now()
            if dt <= now:
                QMessageBox.warning(self, "Atenção", "Escolha um horário futuro para iniciar a automação.")
                return
            self.scheduled_start = dt
            self.schedule_pending = True
            self._update_schedule_countdown()
            self.schedule_timer.start(1000)
            self.btn_start.setEnabled(False)
            self.btn_stop.setEnabled(True)
            self.btn_mgr.setEnabled(False)
            self.log_message(f"⏳ Execução agendada para {dt.strftime('%d/%m/%Y %H:%M')}.")
            return
        self._start_automation_now()

    def stop_automation(self):
        if self.schedule_pending and (not self.thread or not self.thread.isRunning()):
            self.log_message("⛔ Agendamento cancelado.")
            self._reset_schedule(keep_date=True)
            self.btn_mgr.setEnabled(True)
            return
        stopped_job = self._active_job
        if self.thread and self.thread.isRunning():
            try:
                if hasattr(self.thread, "stop"):
                    self.thread.stop()
            except Exception:
                pass
            self.thread.wait(15000)
            if self.thread.isRunning():
                self.thread.terminate()
                self.thread.wait(2000)
        if stopped_job and self._robot_supabase_url and self._robot_supabase_key:
            complete_execution_request(
                self._robot_supabase_url,
                self._robot_supabase_key,
                stopped_job["id"],
                False,
                "Execução interrompida manualmente",
            )
        self._active_job = None
        self.on_automation_finished({})
        self.log_message("⛔ Automação interrompida manualmente.")

    def on_automation_finished(self, results):
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.btn_mgr.setEnabled(True)
        details = getattr(self.thread, "results_details", None) if self.thread else None
        started = getattr(self.thread, "started_at", None) if self.thread else None
        finished = getattr(self.thread, "finished_at", datetime.now()) if self.thread else datetime.now()
        try:
            self.schedule_timer.stop()
            self.schedule_pending = False
            self.scheduled_start = None
            if self.chk_schedule.isChecked():
                self.lbl_sched_countdown.setText("⏳ Aguardando")
            if details:
                pdf_path = self._generate_pdf_report(details, started, finished)
                if pdf_path:
                    pdf_vm_base = ensure_report_in_vm_base(self.config, Path(pdf_path), finished)
                    self.log_message("📄 Relatório PDF gerado com sucesso:")
                    self.log_message(f"    {pdf_path}")
                    if str(pdf_vm_base) != str(pdf_path):
                        self.log_message("📁 Cópia do relatório salva na pasta base da VM:")
                        self.log_message(f"    {pdf_vm_base}")
        except Exception as e:
            self.log_message(f"⚠️ Não consegui gerar o relatório PDF: {e}")
        # Sincronizar status e atualização no Monday.com
        if self._robot_id and self._robot_supabase_url and self._robot_supabase_key:
            update_robot_status(self._robot_supabase_url, self._robot_supabase_key, self._robot_id, "active")
        if self._active_job and self._robot_supabase_url and self._robot_supabase_key:
            complete_execution_request(
                self._robot_supabase_url,
                self._robot_supabase_key,
                self._active_job["id"],
                True,
                None,
            )
            self._active_job = None

    def closeEvent(self, event) -> None:
        event.ignore()
        self.hide()
        if not self._tray_icon.icon().isNull():
            self._tray_icon.show()

    def _sync_to_monday(self, details: Dict[str, Dict[str, str]]) -> None:
        """Atualiza a tarefa no Monday: status (Feito/Pendente) e atualização (observação por empresa)."""
        _load_env()
        token = (os.environ.get("MONDAY_TOKEN") or "").strip()
        item_id_raw = self.config.get("monday_item_id")
        board_id_raw = self.config.get("monday_board_id")
        status_column_id = (self.config.get("monday_status_column_id") or "status").strip()
        if not token:
            self.log_message("⚠️ Monday: token não configurado (use arquivo .env com 'token monday = ...').")
            return
        if not item_id_raw:
            self.log_message("⚠️ Monday: monday_item_id não configurado em config_certidoes.json.")
            return
        if not board_id_raw:
            self.log_message("⚠️ Monday: monday_board_id não configurado em config_certidoes.json.")
            return
        try:
            board_id = int(board_id_raw) if str(board_id_raw).isdigit() else board_id_raw
            item_id = int(item_id_raw) if str(item_id_raw).isdigit() else item_id_raw
        except (TypeError, ValueError):
            board_id, item_id = board_id_raw, item_id_raw

        def is_debt(val: str) -> bool:
            norm = _normalize_status_text(val)
            return any(k in norm for k in ["positiva", "pend", "inativa", "revog", "debito"])

        pending_by_empresa: Dict[str, List[str]] = {}
        for name, detail in details.items():
            pendentes: List[str] = []
            for k in ("Estadual", "Federal", "FGTS"):
                v = detail.get(k, "")
                if _is_error_status(v) or is_debt(v):
                    pendentes.append(f"{k}: {v}")
            if pendentes:
                pending_by_empresa[name] = pendentes

        if pending_by_empresa:
            status_label = "Pendente"
            parts = ["Certidões pendentes por empresa:\n"]
            for empresa in sorted(pending_by_empresa.keys(), key=lambda n: n.lower()):
                parts.append(f"**{empresa}**")
                parts.extend(pending_by_empresa[empresa])
                parts.append("")
            body = "\n".join(parts).strip()
        else:
            status_label = "Feito"
            body = "Todas as certidões conferidas sem pendências."

        headers = {"Authorization": token, "Content-Type": "application/json"}

        # Alterar status da coluna
        try:
            change_mutation = (
                "mutation ChangeColumnValue($board_id: ID!, $item_id: ID!, $column_id: String!, $value: JSON!) {"
                " change_column_value(board_id: $board_id, item_id: $item_id, column_id: $column_id, value: $value) { id } }"
            )
            r = requests.post(
                MONDAY_API_URL,
                json={"query": change_mutation, "variables": {"board_id": board_id, "item_id": item_id, "column_id": status_column_id, "value": json.dumps({"label": status_label})}},
                headers=headers,
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
            if data.get("errors"):
                self.log_message(f"⚠️ Monday (status): {data.get('errors')}")
            else:
                self.log_message(f"✅ Monday: status alterado para '{status_label}'.")
        except Exception as e:
            self.log_message(f"⚠️ Monday (status): {e}")

        # Criar atualização (observação) no item — aparece em "Escreva uma atualização..." e dispara email
        try:
            create_update_mutation = (
                "mutation CreateUpdate($item_id: ID!, $body: String!) {"
                " create_update(item_id: $item_id, body: $body) { id } }"
            )
            r = requests.post(
                MONDAY_API_URL,
                json={"query": create_update_mutation, "variables": {"item_id": item_id, "body": body}},
                headers=headers,
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
            if data.get("errors"):
                self.log_message(f"⚠️ Monday (atualização): {data.get('errors')}")
            else:
                self.log_message("✅ Monday: atualização publicada (email enviado pelo Monday).")
        except Exception as e:
            self.log_message(f"⚠️ Monday (atualização): {e}")


# =============================================================================
# main
# =============================================================================
if __name__ == "__main__":
    app = QApplication(sys.argv)
    try:
        app_icon = _load_app_icon()
        if not app_icon.isNull():
            app.setWindowIcon(app_icon)
    except Exception:
        pass
    if not ensure_license_valid(app):
        sys.exit(0)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())
