import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from playwright.sync_api import sync_playwright


def get_runtime_dirs() -> tuple[Path, Path]:
    if getattr(sys, "frozen", False):
        app_dir = Path(sys.executable).resolve().parent
        resource_dir = Path(getattr(sys, "_MEIPASS", app_dir / "_internal")).resolve()
        return app_dir, resource_dir
    source_dir = Path(__file__).resolve().parent
    return source_dir, source_dir


BASE_DIR, RESOURCE_DIR = get_runtime_dirs()
PROFILE_DIR = (RESOURCE_DIR if getattr(sys, "frozen", False) else BASE_DIR) / "chrome_profile"
CERTIFICATES_PATH = BASE_DIR / "certificates.json"
DEFAULT_CDP_PORT = 9333
DEFAULT_TIMEOUT_MS = 90000


def get_chrome_candidates() -> list[Path]:
    candidates = [
        RESOURCE_DIR / "Chrome" / "chrome.exe",
        RESOURCE_DIR / "_internal" / "Chrome" / "chrome.exe",
        BASE_DIR / "_internal" / "Chrome" / "chrome.exe",
        BASE_DIR / "Chrome" / "chrome.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",
        Path(os.environ.get("PROGRAMFILES", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",
        Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
    ]
    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = str(candidate.resolve(strict=False)).lower()
        if normalized not in seen:
            seen.add(normalized)
            unique.append(candidate)
    return unique


def resolve_chrome_exe() -> Path:
    for candidate in get_chrome_candidates():
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Chrome nao encontrado. Caminhos verificados: "
        + " | ".join(str(candidate) for candidate in get_chrome_candidates())
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Login no e-CAC via Chrome externo + CDP.")
    parser.add_argument("--pfx-path", help="Caminho do arquivo .pfx")
    parser.add_argument("--pfx-password", help="Senha do .pfx")
    parser.add_argument("--cert-subject", help="Trecho do subject para localizar o certificado correto")
    parser.add_argument("--cert-issuer", default="", help="Trecho opcional do issuer esperado")
    parser.add_argument("--cdp-port", type=int, default=DEFAULT_CDP_PORT, help="Porta do CDP")
    parser.add_argument("--timeout-ms", type=int, default=DEFAULT_TIMEOUT_MS, help="Timeout padrao")
    parser.add_argument("--gui", action="store_true", help="Forca abertura da interface grafica")
    return parser.parse_args()


def run_powershell(script: str, timeout_ms: int = 60000) -> str:
    # No Windows (principalmente em .exe), o PowerShell pode abrir uma janela que atrapalha.
    # Rodamos com WindowStyle Hidden e também sem criar console.
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
        check=False,
        timeout=max(1, timeout_ms // 1000),
        creationflags=creationflags,
    )

    out = (result.stdout or "").strip()
    err = (result.stderr or "").strip()
    if result.returncode != 0:
        # Evita estourar a tela com o script inteiro (subprocess.CalledProcessError imprime o comando).
        tail_err = err[-1500:] if err else ""
        tail_out = out[-1500:] if out else ""
        msg = f"PowerShell retornou codigo {result.returncode}."
        if tail_err:
            msg += f"\nErro:\n{tail_err}"
        elif tail_out:
            msg += f"\nSaida:\n{tail_out}"
        raise RuntimeError(msg)

    return out


def ensure_dependencies() -> None:
    if shutil.which("powershell") is None:
        raise RuntimeError("PowerShell nao encontrado no PATH.")


def ensure_pfx_imported(pfx_path: Path, pfx_password: str, cert_subject: str, cert_issuer: str) -> None:
    if not pfx_path.exists():
        raise FileNotFoundError(f"PFX nao encontrado: {pfx_path}")

    script = textwrap.dedent(
        f"""
        $path = '{pfx_path}'
        $password = ConvertTo-SecureString '{pfx_password}' -AsPlainText -Force
        $existing = Get-ChildItem Cert:\\CurrentUser\\My |
            Where-Object {{ $_.Subject -like '*{cert_subject}*' }}
        if (-not $existing) {{
            Import-PfxCertificate -FilePath $path -CertStoreLocation Cert:\\CurrentUser\\My -Password $password | Out-Null
        }}
        $cert = Get-ChildItem Cert:\\CurrentUser\\My |
            Where-Object {{ $_.Subject -like '*{cert_subject}*' }} |
            Select-Object -First 1 Subject, Thumbprint, Issuer
        if (-not $cert) {{
            throw 'Certificado alvo nao encontrado no store CurrentUser\\My.'
        }}
        $cert | ConvertTo-Json -Compress
        """
    )
    cert = json.loads(run_powershell(script))
    if cert_issuer and cert_issuer not in cert["Issuer"]:
        raise RuntimeError(f"Issuer inesperado: {cert['Issuer']}")


def import_pfx_and_get_metadata(pfx_path: Path, pfx_password: str) -> dict:
    if not pfx_path.exists():
        raise FileNotFoundError(f"PFX nao encontrado: {pfx_path}")

    script = textwrap.dedent(
        f"""
        $path = '{pfx_path}'
        $password = ConvertTo-SecureString '{pfx_password}' -AsPlainText -Force
        $cert = Import-PfxCertificate -FilePath $path -CertStoreLocation Cert:\\CurrentUser\\My -Password $password
        if (-not $cert) {{
            throw 'Falha ao importar o certificado.'
        }}
        $selected = $cert | Select-Object -First 1 Subject, Thumbprint, Issuer
        $selected | ConvertTo-Json -Compress
        """
    )
    return json.loads(run_powershell(script))


def ensure_certificate_in_store(cert_subject: str, cert_issuer: str = "") -> None:
    script = textwrap.dedent(
        f"""
        $cert = Get-ChildItem Cert:\\CurrentUser\\My |
            Where-Object {{ $_.Subject -like '*{cert_subject}*' }} |
            Select-Object -First 1 Subject, Thumbprint, Issuer
        if (-not $cert) {{
            throw 'Certificado selecionado nao esta no store CurrentUser\\My.'
        }}
        $cert | ConvertTo-Json -Compress
        """
    )
    cert = json.loads(run_powershell(script))
    if cert_issuer and cert_issuer not in cert["Issuer"]:
        raise RuntimeError(f"Issuer inesperado: {cert['Issuer']}")


def build_selector_candidates(cert_subject: str) -> list[str]:
    candidates: list[str] = []
    raw = (cert_subject or "").strip()
    if raw:
        candidates.append(raw)

    match = re.search(r"CN=([^,]+)", raw, flags=re.IGNORECASE)
    if match:
        cn_value = match.group(1).strip()
        if cn_value and cn_value not in candidates:
            candidates.append(cn_value)

    for item in list(candidates):
        normalized = item.replace("CN=", "").strip()
        if normalized and normalized not in candidates:
            candidates.append(normalized)
        company_only = normalized.split(":")[0].strip()
        if company_only and company_only not in candidates:
            candidates.append(company_only)

    return candidates


def kill_automation_chrome(profile_dir: Path, chrome_exe: Path) -> None:
    script = textwrap.dedent(
        f"""
        $chromeExe = '{chrome_exe}'.ToLower()
        $profileDir = '{profile_dir}'.ToLower()
        $procs = Get-CimInstance Win32_Process -Filter "name = 'chrome.exe'"
        foreach ($proc in $procs) {{
            $cmd = [string]$proc.CommandLine
            if ($cmd.ToLower().Contains($chromeExe) -or $cmd.ToLower().Contains($profileDir)) {{
                try {{ Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop }} catch {{}}
            }}
        }}
        """
    )
    run_powershell(script, timeout_ms=20000)


def reset_profile_dir(profile_dir: Path) -> None:
    if profile_dir.exists():
        shutil.rmtree(profile_dir, ignore_errors=True)
    profile_dir.mkdir(parents=True, exist_ok=True)


def wait_for_cdp_port(port: int, chrome_proc: subprocess.Popen | None = None, timeout_seconds: int = 40) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if chrome_proc is not None:
            return_code = chrome_proc.poll()
            if return_code is not None:
                raise RuntimeError(f"Chrome externo encerrou antes do CDP subir. Codigo de saida: {return_code}.")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.25)
    raise RuntimeError(f"Chrome com CDP nao respondeu na porta {port}.")


def start_playwright_browser(cdp_port: int):
    """
    No .exe, iniciar Chrome externo via CDP tende a falhar silenciosamente (Chrome fecha antes do CDP subir).
    Para alinhar com o bot de Certidoes, usamos launch_persistent_context (Playwright gerencia o processo)
    e deixamos fallback para Chrome/Edge do sistema (quando disponivel).

    Observacao: nao criamos/gerenciamos pasta de browsers do Playwright aqui; o objetivo e nao gerar
    a pasta "ms-playwright" ao lado do executavel.
    """
    profile_dir = PROFILE_DIR
    try:
        profile_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    exe: str | None = None
    try:
        exe = str(resolve_chrome_exe())
    except Exception:
        exe = None

    playwright = sync_playwright().start()
    try:
        kwargs: dict = {
            "headless": False,
            "ignore_https_errors": True,
            "args": [
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
        }
        if exe:
            kwargs["executable_path"] = exe
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            **kwargs,
        )
        browser = context.browser if hasattr(context, "browser") else None
        page = context.pages[0] if context.pages else context.new_page()
        return playwright, browser, context, page, None
    except Exception as exc:
        try:
            playwright.stop()
        except Exception:
            pass
        hint = (
            "Falha ao iniciar o navegador via Playwright. "
            "Se estiver rodando via .exe, verifique se existe Chrome/Edge instalado."
        )
        raise RuntimeError(f"{hint}\nDetalhe: {exc}") from exc


def select_certificate_dialog(cert_subject: str) -> None:
    selector_candidates = build_selector_candidates(cert_subject)
    powershell_targets = "@(" + ", ".join("'" + c.replace("'", "''") + "'" for c in selector_candidates) + ")"
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
        $deadline = (Get-Date).AddSeconds(35)
        $root = [System.Windows.Automation.AutomationElement]::RootElement
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

        function Get-CertificateGrid($dialog) {{
            $elements = $dialog.FindAll(
                [System.Windows.Automation.TreeScope]::Descendants,
                [System.Windows.Automation.Condition]::TrueCondition
            )
            foreach ($element in $elements) {{
                $controlType = $element.Current.ControlType.ProgrammaticName
                if ($controlType -eq 'ControlType.DataGrid') {{
                    return $element
                }}
            }}
            return $null
        }}

        function Get-RowData($gridPattern, [int]$rowIndex) {{
            $values = @()
            $firstCell = $null
            for ($column = 0; $column -lt $gridPattern.Current.ColumnCount; $column++) {{
                try {{
                    $cell = $gridPattern.GetItem($rowIndex, $column)
                    if (-not $firstCell) {{ $firstCell = $cell }}
                    $values += ([string]$cell.Current.Name)
                }} catch {{
                    $values += ''
                }}
            }}
            return [PSCustomObject]@{{
                FirstCell = $firstCell
                Values = $values
                Text = ($values -join ' | ')
            }}
        }}

        function Get-OkButton($dialog) {{
            $okCondition = New-Object System.Windows.Automation.PropertyCondition(
                [System.Windows.Automation.AutomationElement]::NameProperty,
                'OK'
            )
            $button = $dialog.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $okCondition)
            if ($button) {{
                return $button
            }}
            $windows = $root.FindAll([System.Windows.Automation.TreeScope]::Children, [System.Windows.Automation.Condition]::TrueCondition)
            for ($i = 0; $i -lt $windows.Count; $i++) {{
                $window = $windows.Item($i)
                if ([string]$window.Current.Name -eq $dialogTitle) {{
                    $button = $window.FindFirst([System.Windows.Automation.TreeScope]::Descendants, $okCondition)
                    if ($button) {{
                        return $button
                    }}
                }}
            }}
            return $null
        }}

        function Try-InvokeElement($element) {{
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
                    if ($programmatic -eq 'InvokePatternIdentifiers.Pattern') {{
                        $invoke = $element.GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern)
                        $invoke.Invoke()
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

        function Wait-DialogClosed() {{
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
                Start-Sleep -Milliseconds 150
            }}
            return $false
        }}

        function Get-FocusedCertificateName() {{
            try {{
                $focused = [System.Windows.Automation.AutomationElement]::FocusedElement
                if ($focused) {{
                    return [string]$focused.Current.Name
                }}
            }} catch {{}}
            return ''
        }}

        function Try-DirectUiAutomation($dialog) {{
            $grid = Get-CertificateGrid $dialog
            if (-not $grid) {{
                return $false
            }}

            $okButton = Get-OkButton $dialog
            if (-not $okButton) {{
                return $false
            }}

            try {{
                $gridPattern = $grid.GetCurrentPattern([System.Windows.Automation.GridPattern]::Pattern)
            }} catch {{
                return $false
            }}

            $matchedRow = $null
            for ($row = 0; $row -lt $gridPattern.Current.RowCount; $row++) {{
                try {{
                    $rowData = Get-RowData $gridPattern $row
                    if (Matches-Target $rowData.Text $targets) {{
                        $matchedRow = $rowData
                        break
                    }}
                }} catch {{}}
            }}

            if (-not $matchedRow -or -not $matchedRow.FirstCell) {{
                return $false
            }}

            if (-not (Try-InvokeElement $matchedRow.FirstCell)) {{
                return $false
            }}
            Start-Sleep -Milliseconds 250

            $focusedName = Get-FocusedCertificateName
            if (-not (Matches-Target $focusedName $targets)) {{
                return $false
            }}

            if (Wait-DialogClosed) {{
                return $true
            }}
            return $false
        }}

        function Find-ChromeWindow($root) {{
            # Evita depender de um titulo exato (muda por idioma, por site, por perfil, por Edge, etc.)
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

        function Fallback-GlobalSelection() {{
            $chromeWindow = Find-ChromeWindow $root
            if (-not $chromeWindow) {{
                throw 'Nao encontrei uma janela do Chrome/Edge para focar. Deixe o navegador visivel e tente novamente.'
            }}

            $chromeHandle = [IntPtr]$chromeWindow.Current.NativeWindowHandle
            if ($chromeHandle -eq [IntPtr]::Zero) {{
                throw 'A janela principal do Chrome nao possui handle valido.'
            }}

            $previousWindow = [NativeCertUi]::GetForegroundWindow()
            $previousCursor = New-Object POINT
            [NativeCertUi]::GetCursorPos([ref]$previousCursor) | Out-Null

            try {{
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

                $wsh = New-Object -ComObject WScript.Shell

                # Normaliza a navegacao para o topo da lista antes de procurar o alvo.
                $wsh.SendKeys('{{HOME}}')
                Start-Sleep -Milliseconds 220

                for ($step = 0; $step -lt 40; $step++) {{
                    $focused = [System.Windows.Automation.AutomationElement]::FocusedElement
                    if ($focused) {{
                        $focusedName = [string]$focused.Current.Name
                        if (Matches-Target $focusedName $targets) {{
                            $wsh.SendKeys('~')
                            if (Wait-DialogClosed) {{
                                return $true
                            }}
                            throw ('O seletor permaneceu aberto apos confirmar o certificado: ' + $focusedName)
                        }}
                    }}
                    if ($step -lt 39) {{
                        $wsh.SendKeys('{{DOWN}}')
                        Start-Sleep -Milliseconds 180
                    }}
                }}

                throw 'Nao foi possivel alcancar o certificado alvo navegando pela lista nativa.'
            }} finally {{
                [NativeCertUi]::SetCursorPos($previousCursor.X, $previousCursor.Y) | Out-Null
                if ($previousWindow -ne [IntPtr]::Zero) {{
                    [NativeCertUi]::SetForegroundWindow($previousWindow) | Out-Null
                }}
            }}
        }}

        if (Fallback-GlobalSelection) {{
            exit 0
        }}

        exit 0

        """
    )
    run_powershell(script, timeout_ms=45000)


def wait_for_certificate_dialog(timeout_seconds: int = 10) -> bool:
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


def authenticated_page_matches(candidate_page, cert_subject: str) -> bool:
    try:
        current_url = candidate_page.url
        body_text = candidate_page.locator("body").inner_text(timeout=5000)
    except Exception:
        return False

    if "/ecac/" not in current_url and "Titular (Acesso GOV.BR por Certificado):" not in body_text:
        return False

    for candidate in build_selector_candidates(cert_subject):
        short_name = candidate.split(",")[0].replace("CN=", "").strip()
        if short_name and short_name in body_text:
            return True
    return False


def click_access_gov_br(page, timeout_ms: int) -> None:
    access_button = page.get_by_role("button", name="Acesso Gov BR")
    deadline = time.time() + (timeout_ms / 1000)
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            access_button.wait_for(state="visible", timeout=5000)
        except Exception as exc:
            last_error = exc
            try:
                page.reload(wait_until="domcontentloaded")
            except Exception:
                pass
            time.sleep(0.5)
            continue

        previous_url = page.url
        clicked = False
        for mode in ("normal", "forced", "dom", "js"):
            try:
                if mode == "normal":
                    access_button.click(timeout=5000)
                elif mode == "forced":
                    access_button.click(timeout=5000, force=True)
                elif mode == "dom":
                    access_button.dispatch_event("click")
                else:
                    page.evaluate(
                        """
                        () => {
                          const candidates = Array.from(document.querySelectorAll('button, input[type="button"], input[type="submit"]'));
                          const target = candidates.find((element) => (element.innerText || element.value || '').includes('Acesso Gov BR'));
                          if (!target) {
                            throw new Error('Botao Acesso Gov BR nao encontrado no DOM.');
                          }
                          target.click();
                        }
                        """
                    )
                clicked = True
            except Exception as exc:
                last_error = exc
                continue

            navigation_deadline = time.time() + 8
            while time.time() < navigation_deadline:
                current_url = page.url
                if "sso.acesso.gov.br" in current_url or current_url != previous_url:
                    return
                try:
                    body_text = page.locator("body").inner_text(timeout=1000)
                    if "Seu certificado digital" in body_text or "Identifique-se no gov.br com:" in body_text:
                        return
                except Exception:
                    pass
                time.sleep(0.25)

            if clicked:
                break

        try:
            page.reload(wait_until="domcontentloaded")
        except Exception:
            pass
        time.sleep(0.75)

    if last_error:
        raise RuntimeError(f"Falha ao acionar o botao Acesso Gov BR. Ultimo erro: {last_error}")
    raise RuntimeError("Falha ao acionar o botao Acesso Gov BR.")


def login_ecac(page, cert_subject: str, timeout_ms: int) -> None:
    page.set_default_timeout(timeout_ms)
    page.context.set_default_navigation_timeout(timeout_ms)
    page.goto("https://cav.receita.fazenda.gov.br/autenticacao/login", wait_until="domcontentloaded")

    for candidate_page in page.context.pages:
        if authenticated_page_matches(candidate_page, cert_subject):
            return

    click_access_gov_br(page, timeout_ms)
    cert_button = None

    wait_deadline = time.time() + (timeout_ms / 1000)
    while time.time() < wait_deadline:
        for candidate_page in page.context.pages:
            if authenticated_page_matches(candidate_page, cert_subject):
                return
        try:
            possible_button = page.get_by_role("button", name="Seu certificado digital", exact=True)
            if possible_button.is_visible(timeout=1000):
                cert_button = possible_button
                break
        except Exception:
            pass
        time.sleep(0.5)

    if cert_button is None:
        raise RuntimeError("Nem a tela do gov.br nem o e-CAC autenticado apareceram dentro do tempo limite.")

    while True:
        cert_button.click(timeout=10000, no_wait_after=True)
        if wait_for_certificate_dialog(timeout_seconds=10):
            break
        page.reload(wait_until="domcontentloaded")
        cert_button = page.get_by_role("button", name="Seu certificado digital", exact=True)
        cert_button.wait_for(timeout=timeout_ms)

    select_certificate_dialog(cert_subject)
    time.sleep(2.0)

    deadline = time.time() + (timeout_ms / 1000)
    while time.time() < deadline:
        for candidate in page.context.pages:
            if authenticated_page_matches(candidate, cert_subject):
                return
        time.sleep(1.0)

    raise RuntimeError("O e-CAC nao confirmou o titular esperado dentro do tempo limite.")


def execute_login(cert_subject: str, cert_issuer: str, timeout_ms: int) -> None:
    ensure_dependencies()
    ensure_certificate_in_store(cert_subject, cert_issuer)

    playwright = browser = context = page = chrome_proc = None
    try:
        playwright, browser, context, page, chrome_proc = start_playwright_browser(DEFAULT_CDP_PORT)
        login_ecac(page, cert_subject, timeout_ms)
    finally:
        if context is not None:
            try:
                context.close()
            except Exception:
                pass
        if browser is not None:
            try:
                browser.close()
            except Exception:
                pass
        if playwright is not None:
            try:
                playwright.stop()
            except Exception:
                pass
        if chrome_proc is not None:
            try:
                chrome_proc.terminate()
            except Exception:
                pass


def load_certificates() -> list[dict]:
    if not CERTIFICATES_PATH.exists():
        return []
    try:
        return json.loads(CERTIFICATES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []


def save_certificates(certificates: list[dict]) -> None:
    CERTIFICATES_PATH.write_text(
        json.dumps(certificates, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def open_gui() -> None:
    root = tk.Tk()
    root.title("eCAC Login")
    root.geometry("760x520")

    name_var = tk.StringVar()
    pfx_var = tk.StringVar()
    password_var = tk.StringVar()
    status_var = tk.StringVar(value="Pronto.")
    certificates = load_certificates()
    selected_index = {"value": None}

    def refresh_tree() -> None:
        tree.delete(*tree.get_children())
        for idx, item in enumerate(certificates):
            tree.insert(
                "",
                "end",
                iid=str(idx),
                values=(
                    item.get("name", item.get("alias", "")),
                    item.get("subject", ""),
                    item.get("thumbprint", ""),
                ),
            )

    def browse_pfx() -> None:
        path = filedialog.askopenfilename(
            title="Selecionar certificado PFX",
            filetypes=[("Certificado PFX", "*.pfx"), ("Todos os arquivos", "*.*")],
        )
        if path:
            pfx_var.set(path)

    def on_select(_: object) -> None:
        focus = tree.focus()
        selected_index["value"] = int(focus) if focus else None

    def register_certificate() -> None:
        name = name_var.get().strip()
        pfx_path = Path(pfx_var.get().strip())
        password = password_var.get()
        if not name or not pfx_path or not password:
            messagebox.showerror("Cadastro", "Preencha nome, arquivo PFX e senha.")
            return
        try:
            status_var.set("Importando certificado...")
            root.update_idletasks()
            meta = import_pfx_and_get_metadata(pfx_path, password)
            entry = {
                "name": name,
                "subject": meta["Subject"],
                "issuer": meta["Issuer"],
                "thumbprint": meta["Thumbprint"],
            }
            certificates[:] = [c for c in certificates if c.get("thumbprint") != entry["thumbprint"]]
            certificates.append(entry)
            save_certificates(certificates)
            refresh_tree()
            name_var.set("")
            pfx_var.set("")
            password_var.set("")
            status_var.set("Certificado cadastrado com sucesso.")
        except Exception as exc:
            messagebox.showerror("Cadastro", str(exc))
            status_var.set("Falha ao cadastrar certificado.")

    def run_selected() -> None:
        idx = selected_index["value"]
        if idx is None or idx >= len(certificates):
            messagebox.showerror("Execucao", "Selecione um certificado na lista.")
            return
        cert = certificates[idx]

        def worker() -> None:
            try:
                display_name = cert.get("name", cert.get("alias", cert["subject"]))
                status_var.set(f"Executando com {display_name}...")
                execute_login(cert["subject"], cert.get("issuer", ""), DEFAULT_TIMEOUT_MS)
                root.after(0, lambda: messagebox.showinfo("Execucao", "Acesso confirmado no e-CAC."))
                root.after(0, lambda: status_var.set("Execucao concluida com sucesso."))
            except Exception as exc:
                root.after(0, lambda: messagebox.showerror("Execucao", str(exc)))
                root.after(0, lambda: status_var.set("Falha na execucao."))

        threading.Thread(target=worker, daemon=True).start()

    form = ttk.LabelFrame(root, text="Cadastrar certificado")
    form.pack(fill="x", padx=12, pady=12)

    ttk.Label(form, text="Nome").grid(row=0, column=0, padx=8, pady=8, sticky="w")
    ttk.Entry(form, textvariable=name_var, width=28).grid(row=0, column=1, padx=8, pady=8, sticky="ew")
    ttk.Label(form, text="Arquivo PFX").grid(row=1, column=0, padx=8, pady=8, sticky="w")
    ttk.Entry(form, textvariable=pfx_var, width=52).grid(row=1, column=1, padx=8, pady=8, sticky="ew")
    ttk.Button(form, text="Explorar", command=browse_pfx).grid(row=1, column=2, padx=8, pady=8)
    ttk.Label(form, text="Senha").grid(row=2, column=0, padx=8, pady=8, sticky="w")
    ttk.Entry(form, textvariable=password_var, show="*", width=28).grid(row=2, column=1, padx=8, pady=8, sticky="w")
    ttk.Button(form, text="Cadastrar", command=register_certificate).grid(row=2, column=2, padx=8, pady=8)
    form.columnconfigure(1, weight=1)

    list_frame = ttk.LabelFrame(root, text="Certificados cadastrados")
    list_frame.pack(fill="both", expand=True, padx=12, pady=(0, 12))

    tree = ttk.Treeview(list_frame, columns=("name", "subject", "thumbprint"), show="headings", height=12)
    tree.heading("name", text="Nome")
    tree.heading("subject", text="Subject")
    tree.heading("thumbprint", text="Thumbprint")
    tree.column("name", width=160, anchor="w")
    tree.column("subject", width=360, anchor="w")
    tree.column("thumbprint", width=180, anchor="w")
    tree.pack(fill="both", expand=True, padx=8, pady=8)
    tree.bind("<<TreeviewSelect>>", on_select)

    bottom = ttk.Frame(root)
    bottom.pack(fill="x", padx=12, pady=(0, 12))
    ttk.Button(bottom, text="Executar certificado selecionado", command=run_selected).pack(side="left")
    ttk.Label(bottom, textvariable=status_var).pack(side="left", padx=12)

    refresh_tree()
    root.mainloop()


def main() -> None:
    args = parse_args()
    if args.gui or (not args.pfx_path and not args.pfx_password and not args.cert_subject):
        open_gui()
        return

    if not args.pfx_path or not args.pfx_password or not args.cert_subject:
        raise RuntimeError("No modo CLI, informe --pfx-path, --pfx-password e --cert-subject.")

    ensure_dependencies()
    pfx_path = Path(args.pfx_path).expanduser().resolve()
    ensure_pfx_imported(pfx_path, args.pfx_password, args.cert_subject, args.cert_issuer)
    execute_login(args.cert_subject, args.cert_issuer, args.timeout_ms)
    print("Acesso confirmado no e-CAC com o certificado selecionado.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
