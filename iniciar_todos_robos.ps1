#Requires -Version 5.1
<#
.SYNOPSIS
  Inicia todos os bot.py sob esta pasta (arvore), usando apenas venv\ na raiz.
  Instala dependencias em falta (requirements.txt + pacotes inferidos dos imports).
  Opcional: $env:ROBOTS_MAX_BOTS = numero maximo de robos (1-64, default 32).
  $env:ROBOS_FORCE_SYNC = '1' forca pip -r + playwright mesmo sem alteracoes nos requirements.
  $env:ROBOS_UPGRADE_PIP = '1' para tambem atualizar o pip em sync completo (por defeito nao atualiza).
  $env:ROBOS_VERBOSE_PIP = '1' para listar cada requirements e saida pip mais falada.
#>
$ErrorActionPreference = 'Continue'

# Consola em UTF-8 (chcp 65001) para acentos portugueses no Write-Host
try {
    chcp 65001 | Out-Null
    $enc = [System.Text.Encoding]::UTF8
    [Console]::OutputEncoding = $enc
    [Console]::InputEncoding = $enc
    $OutputEncoding = $enc
} catch { }

$Root = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$Root = [System.IO.Path]::GetFullPath($Root)
$VenvPy = Join-Path $Root 'venv\Scripts\python.exe'
$ReqRoot = Join-Path $Root 'requirements.txt'
$script:DepsFingerprintPath = Join-Path $Root '.robos_sync_fp.txt'
$script:PythonModuleSpecCache = @{}
$script:VenvStdlibTopNames = $null

$script:PyReservedWords = @(
    'False', 'None', 'True', 'and', 'as', 'assert', 'async', 'await', 'break', 'class', 'continue',
    'def', 'del', 'elif', 'else', 'except', 'finally', 'for', 'from', 'global', 'if', 'import', 'in',
    'is', 'lambda', 'nonlocal', 'not', 'or', 'pass', 'raise', 'return', 'try', 'while', 'with', 'yield'
)

function Test-IsValidTopLevelModuleName {
    param([string] $n)
    if ([string]::IsNullOrWhiteSpace($n)) { return $false }
    if ($n -notmatch '^[A-Za-z_][A-Za-z0-9_]*$') { return $false }
    if ($n -match '^__') { return $false }
    if ($script:PyReservedWords -contains $n) { return $false }
    return $true
}

function Test-SafePipInstallSpec {
    param([string] $line)
    $t = $line.Trim()
    if (-not $t) { return $false }
    if ($t.StartsWith('#')) { return $false }
    $firstTok = ($t -split '\s+', 2)[0]
    if ($script:PyReservedWords -contains $firstTok) { return $false }
    if ($firstTok -eq 'import' -or $firstTok -eq 'from') { return $false }
    return $true
}

function Write-RobosHr {
    Write-Host ('=' * 72) -ForegroundColor DarkGray
}

function Write-RobosSection {
    param([string] $Title)
    Write-Host ''
    Write-RobosHr
    Write-Host "  $Title" -ForegroundColor Cyan
    Write-RobosHr
}

function Invoke-Pip {
    param([string[]] $PipArgs)
    if (-not $PipArgs -or $PipArgs.Count -eq 0) { return $false }
    $pipArgsEff = [System.Collections.Generic.List[string]]::new()
    if ($env:ROBOS_VERBOSE_PIP -ne '1') {
        $hasQ = $false
        foreach ($a in $PipArgs) { if ($a -eq '-q' -or $a -eq '--quiet') { $hasQ = $true } }
        if (-not $hasQ -and ($PipArgs[0] -eq 'install')) { [void]$pipArgsEff.Add('-q') }
    }
    foreach ($a in $PipArgs) { [void]$pipArgsEff.Add($a) }
    $null = & $VenvPy -m pip @($pipArgsEff.ToArray()) 2>&1
    $ex = $LASTEXITCODE
    if ($null -eq $ex) { $ex = 0 }
    return ($ex -eq 0)
}

function Get-VenvStdlibTopLevelNames {
    param([string] $PythonExe)
    if ($null -ne $script:VenvStdlibTopNames) { return $script:VenvStdlibTopNames }
    $set = New-Object 'System.Collections.Generic.HashSet[string]'
    $code = "import sys; print('|'.join(sorted(x for x in sys.stdlib_module_names if x.isidentifier())))"
    $out = & $PythonExe -c $code 2>$null
    $ex = $LASTEXITCODE
    if ($null -eq $ex) { $ex = 0 }
    if ($ex -eq 0 -and $out) {
        foreach ($t in ($out.ToString() -split '\|' | ForEach-Object { $_.Trim() } | Where-Object { $_ })) {
            [void]$set.Add($t)
        }
    }
    if ($set.Count -eq 0) {
        foreach ($x in @(
                'sys', 'os', 'io', 're', 'json', 'time', 'math', 'socket', 'threading', 'subprocess',
                'pathlib', 'datetime', 'typing', 'collections', 'urllib', 'asyncio', 'tempfile', 'shutil',
                'hashlib', 'base64', 'zipfile', 'uuid', 'platform', 'signal', 'atexit', 'traceback',
                'unicodedata', 'html', 'decimal', 'dataclasses', 'ctypes', 'copy', 'enum', 'functools',
                'itertools', 'logging', 'operator', 'pkgutil', 'string', 'struct', 'warnings', 'weakref',
                'contextlib', 'abc', 'inspect', 'textwrap', 'calendar', 'email', 'queue', 'select', 'ssl'
            )) {
            [void]$set.Add($x)
        }
    }
    $script:VenvStdlibTopNames = $set
    return $set
}

function Test-VenvImportModule {
    param([string] $ImportName)
    $first = ($ImportName -split '\.')[0].Trim()
    if (-not (Test-IsValidTopLevelModuleName $first)) { return $true }
    if ($script:PythonModuleSpecCache.ContainsKey($first)) {
        return [bool]$script:PythonModuleSpecCache[$first]
    }
    if ($first -notmatch '^[A-Za-z_][A-Za-z0-9_]*$') {
        $script:PythonModuleSpecCache[$first] = $true
        return $true
    }
    $code = "import importlib; importlib.import_module('$first')"
    $null = & $VenvPy -c $code 2>&1
    $ex = $LASTEXITCODE
    if ($null -eq $ex) { $ex = 0 }
    $ok = ($ex -eq 0)
    $script:PythonModuleSpecCache[$first] = $ok
    return $ok
}

function Get-AllRequirementsPathsSorted {
    $rxSkip = [regex]'\\(venv|\.venv)(\\|$)|\\site-packages\\|\\node_modules\\|\\\.git\\|\\__pycache__\\'
    $set = New-Object 'System.Collections.Generic.HashSet[string]'
    if (Test-Path -LiteralPath $ReqRoot) {
        try {
            $nr = [System.IO.Path]::GetFullPath($ReqRoot)
            if (-not $rxSkip.IsMatch($nr)) { [void]$set.Add($nr) }
        }
        catch { }
    }
    try {
        Get-ChildItem -LiteralPath $Root -Recurse -Force -Filter 'requirements.txt' -ErrorAction SilentlyContinue | ForEach-Object {
            $full = $_.FullName
            if ($rxSkip.IsMatch($full)) { return }
            try {
                $norm = [System.IO.Path]::GetFullPath($full)
            }
            catch { $norm = $full }
            [void]$set.Add($norm)
        }
    }
    catch { }
    return [string[]]($set | Sort-Object)
}

function Get-DepsFingerprint {
    $parts = New-Object System.Collections.Generic.List[string]
    if (Test-Path -LiteralPath $VenvPy) {
        try {
            $vi = Get-Item -LiteralPath $VenvPy
            [void]$parts.Add(('VENVPY:{0}:{1}' -f $vi.LastWriteTimeUtc.Ticks, $vi.Length))
        }
        catch { }
    }
    foreach ($rp in (Get-AllRequirementsPathsSorted)) {
        if (Test-Path -LiteralPath $rp) {
            try {
                $ri = Get-Item -LiteralPath $rp
                [void]$parts.Add(('REQ:{0}|{1}|{2}' -f $rp, $ri.LastWriteTimeUtc.Ticks, $ri.Length))
            }
            catch { }
        }
    }
    $raw = $parts -join "`n"
    if (-not $raw) { $raw = 'NO_REQ' }
    $sha = [System.Security.Cryptography.SHA256]::Create()
    $bytes = [Text.Encoding]::UTF8.GetBytes($raw)
    $hash = $sha.ComputeHash($bytes)
    return ([BitConverter]::ToString($hash) -replace '-', '').ToLowerInvariant()
}

function Test-DepsFingerprintUnchanged {
    if ($env:ROBOS_FORCE_SYNC -eq '1') { return $false }
    if (-not (Test-Path -LiteralPath $script:DepsFingerprintPath)) { return $false }
    try {
        $saved = ([IO.File]::ReadAllText($script:DepsFingerprintPath)).Trim()
        $now = (Get-DepsFingerprint).Trim()
        return ($saved -eq $now -and $saved.Length -gt 0)
    }
    catch { return $false }
}

function Save-DepsFingerprint {
    try {
        [IO.File]::WriteAllText($script:DepsFingerprintPath, (Get-DepsFingerprint), [Text.UTF8Encoding]::new($false))
    }
    catch { }
}

function Get-BootstrapPython {
    $candidates = @(
        @{ Exe = 'py'; Args = @('-3.11', '-c', 'import sys; print(sys.executable)') },
        @{ Exe = 'py'; Args = @('-3.10', '-c', 'import sys; print(sys.executable)') },
        @{ Exe = 'py'; Args = @('-3', '-c', 'import sys; print(sys.executable)') },
        @{ Exe = 'python'; Args = @('-c', 'import sys; print(sys.executable)') }
    )
    foreach ($c in $candidates) {
        try {
            $out = & $c.Exe @($c.Args) 2>$null | Select-Object -First 1
            $t = if ($out) { $out.ToString().Trim() } else { '' }
            if ($t -and (Test-Path -LiteralPath $t)) { return $t }
        }
        catch { }
    }
    $locals = @(
        "$env:LocalAppData\Programs\Python\Python311\python.exe",
        "$env:LocalAppData\Programs\Python\Python312\python.exe",
        "$env:LocalAppData\Programs\Python\Python313\python.exe",
        "$env:LocalAppData\Programs\Python\Python314\python.exe",
        "$env:LocalAppData\Programs\Python\Python310\python.exe",
        "${env:ProgramFiles}\Python311\python.exe",
        "${env:ProgramFiles}\Python312\python.exe"
    )
    foreach ($p in $locals) {
        if ($p -and (Test-Path -LiteralPath $p)) { return $p }
    }
    return $null
}

function Ensure-Venv {
    if (Test-Path -LiteralPath $VenvPy) { return }
    $boot = Get-BootstrapPython
    if (-not $boot) {
        Write-Error 'Nao encontrei Python 3.x no PATH para criar venv\ na raiz ROBOS.'
        exit 1
    }
    Write-Host "Criar venv com: $boot"
    $venvDir = Join-Path $Root 'venv'
    $pr = Start-Process -FilePath $boot -ArgumentList @('-m', 'venv', $venvDir) -Wait -PassThru -NoNewWindow
    if ($pr.ExitCode -ne 0 -or -not (Test-Path -LiteralPath $VenvPy)) {
        Write-Error 'Falha ao criar venv.'
        exit 1
    }
}

function Get-PipNameMap {
    return @{
        'PIL'                 = 'Pillow'
        'cv2'                 = 'opencv-python'
        'bs4'                 = 'beautifulsoup4'
        'yaml'                = 'pyyaml'
        'dotenv'              = 'python-dotenv'
        'dateutil'            = 'python-dateutil'
        'google.protobuf'     = 'protobuf'
        'OpenSSL'             = 'pyopenssl'
        'Crypto'              = 'pycryptodome'
        'Cryptodome'          = 'pycryptodome'
        'sklearn'             = 'scikit-learn'
        'magic'               = 'python-magic-bin'
        'win32api'            = 'pywin32'
        'win32con'            = 'pywin32'
        'win32gui'            = 'pywin32'
        'requests'            = 'requests'
        'httpx'               = 'httpx'
        'playwright'          = 'playwright'
        'supabase'            = 'supabase'
        'pyautogui'           = 'pyautogui'
        'reportlab'           = 'reportlab'
        'PyPDF2'              = 'PyPDF2'
        'PySide6'             = 'PySide6'
        'postgrest'           = 'postgrest'
    }
}

function Get-ImportNamesFromFile {
    param([string] $Path)
    $names = New-Object 'System.Collections.Generic.HashSet[string]'
    try {
        $lines = Get-Content -LiteralPath $Path -ErrorAction Stop
    }
    catch { return $names }
    foreach ($line in $lines) {
        $t = $line.TrimStart()
        if ($t.StartsWith('#')) { continue }
        if ($t -match '^\s*from\s+\.') { continue }
        if ($t -match '^\s*from\s+([a-zA-Z_][a-zA-Z0-9_]*)') {
            $fm = $matches[1]
            if (-not (Test-IsValidTopLevelModuleName $fm)) { continue }
            [void]$names.Add($fm)
            continue
        }
        if ($t -match '^\s*import\s+(.+)') {
            $rest = $matches[1].Trim()
            if (-not $rest) { continue }
            $parts = $rest -split ','
            foreach ($p in $parts) {
                $chunk = ($p.Trim() -split '\s+')[0]
                if (-not $chunk) { continue }
                if (Test-IsValidTopLevelModuleName $chunk) {
                    [void]$names.Add($chunk)
                }
            }
        }
    }
    return $names
}

function Install-DetectedModules {
    param(
        [System.Collections.Generic.HashSet[string]] $Names,
        [hashtable] $PipMap
    )
    $missing = New-Object System.Collections.Generic.List[string]
    $stdlib = Get-VenvStdlibTopLevelNames $VenvPy
    foreach ($n in $Names) {
        if (-not (Test-IsValidTopLevelModuleName $n)) { continue }
        if ($stdlib.Contains($n)) { continue }
        if (Test-VenvImportModule $n) { continue }
        $pip = $PipMap[$n]
        if (-not $pip) { $pip = $n }
        if (-not (Test-SafePipInstallSpec $pip)) { continue }
        [void]$missing.Add("${n} -> $pip")
        if ($env:ROBOS_VERBOSE_PIP -eq '1') {
            Write-Host "  [pip] Falta import '$n' - a instalar: $pip" -ForegroundColor Yellow
        }
        [void](Invoke-Pip @('install', $pip))
        if ($script:PythonModuleSpecCache.ContainsKey($n)) { $null = $script:PythonModuleSpecCache.Remove($n) }
        $script:PythonModuleSpecCache[$n] = (Test-VenvImportModule $n)
    }
    if ($missing.Count -eq 0) {
        Write-Host '  Nenhum modulo em falta no venv (import OK nos bot.py).' -ForegroundColor DarkGreen
    }
    elseif ($env:ROBOS_VERBOSE_PIP -ne '1') {
        Write-Host "  Instalados / corrigidos: $($missing.Count) pacote(s)." -ForegroundColor Green
        foreach ($line in $missing) { Write-Host "    - $line" -ForegroundColor Gray }
    }
}

function Sync-AllRequirements {
    $reqPaths = @(Get-AllRequirementsPathsSorted)
    if ($env:ROBOS_VERBOSE_PIP -eq '1') {
        Write-Host "  Lista de requirements ($($reqPaths.Count)):"
        foreach ($rp in $reqPaths) { Write-Host "    - $rp" }
    }
    else {
        $msg = '  pip install -r em {0} ficheiros requirements. Saida reduzida - defina ROBOS_VERBOSE_PIP=1 para mais detalhe.' -f $reqPaths.Count
        Write-Host $msg -ForegroundColor DarkGray
    }
    if ($env:ROBOS_UPGRADE_PIP -eq '1') {
        [void](Invoke-Pip @('install', '--upgrade', 'pip'))
    }
    if (Test-Path -LiteralPath $ReqRoot) {
        $ok = Invoke-Pip @('install', '-r', $ReqRoot)
        if (-not $ok) {
            Write-Host '[INFO] pip -r raiz incompleto; linha a linha...'
            Get-Content -LiteralPath $ReqRoot | ForEach-Object {
                $line = $_.Trim()
                if (-not $line) { return }
                if ($line.StartsWith('#')) { return }
                if ($line.StartsWith('-r')) { return }
                if (-not (Test-SafePipInstallSpec $line)) { return }
                [void](Invoke-Pip @('install', $line))
            }
        }
    }
    $rxSkip = [regex]'\\(venv|\.venv)(\\|$)|\\site-packages\\|\\node_modules\\|\\\.git\\|\\__pycache__\\'
    $seenReq = New-Object 'System.Collections.Generic.HashSet[string]'
    try {
        Get-ChildItem -LiteralPath $Root -Recurse -Force -Filter 'requirements.txt' -ErrorAction SilentlyContinue | ForEach-Object {
            $full = $_.FullName
            if ($rxSkip.IsMatch($full)) { return }
            try {
                $norm = [System.IO.Path]::GetFullPath($full)
            }
            catch { $norm = $full }
            if ($seenReq.Contains($norm)) { return }
            [void]$seenReq.Add($norm)
            if ($env:ROBOS_VERBOSE_PIP -eq '1') {
                Write-Host "    - $norm"
            }
            $ok2 = Invoke-Pip @('install', '-r', $norm)
            if (-not $ok2) {
                Get-Content -LiteralPath $norm | ForEach-Object {
                    $line = $_.Trim()
                    if (-not $line -or $line.StartsWith('#')) { return }
                    if ($line.StartsWith('-r')) { return }
                    if (-not (Test-SafePipInstallSpec $line)) { return }
                    [void](Invoke-Pip @('install', $line))
                }
            }
        }
    }
    catch { }
    $baseMap = @(
        @('requests', 'requests'),
        @('PIL', 'Pillow'),
        @('PySide6', 'PySide6'),
        @('playwright', 'playwright'),
        @('supabase', 'supabase'),
        @('dotenv', 'python-dotenv'),
        @('pyautogui', 'pyautogui'),
        @('reportlab', 'reportlab'),
        @('PyPDF2', 'PyPDF2'),
        @('httpx', 'httpx')
    )
    foreach ($pair in $baseMap) {
        if (-not (Test-VenvImportModule $pair[0])) {
            if ($env:ROBOS_VERBOSE_PIP -eq '1') {
                Write-Host "  [pip] Pacote base em falta: $($pair[1]) (import $($pair[0]))" -ForegroundColor Yellow
            }
            [void](Invoke-Pip @('install', $pair[1]))
            if ($script:PythonModuleSpecCache.ContainsKey($pair[0])) { $null = $script:PythonModuleSpecCache.Remove($pair[0]) }
            $script:PythonModuleSpecCache[$pair[0]] = (Test-VenvImportModule $pair[0])
        }
    }
    $script:PythonModuleSpecCache = @{}
}

function Get-BotPaths {
    $max = 32
    if ($env:ROBOTS_MAX_BOTS) {
        try { $max = [int]$env:ROBOTS_MAX_BOTS } catch { $max = 32 }
    }
    if ($max -lt 1) { $max = 1 }
    if ($max -gt 64) { $max = 64 }

    $rxSkip = [regex]'\\(venv|\.venv)(\\|$)|\\site-packages\\|\\node_modules\\|\\\.git\\|\\__pycache__\\|\\\.vscode\\|\\\.idea\\'
    $visitedReal = New-Object 'System.Collections.Generic.HashSet[string]'
    $seenBot = New-Object 'System.Collections.Generic.HashSet[string]'
    $out = New-Object System.Collections.Generic.List[string]

    if (-not (Test-Path -LiteralPath $Root)) { return [string[]]@() }

    $stack = New-Object System.Collections.Generic.Stack[string]
    $stack.Push($Root)

    while ($stack.Count -gt 0) {
        $dir = $stack.Pop()
        try {
            $di = Get-Item -LiteralPath $dir -ErrorAction Stop
            $real = $di.FullName
        }
        catch { continue }

        try { $norm = [System.IO.Path]::GetFullPath($real) }
        catch { $norm = $real }

        if ($visitedReal.Contains($norm)) { continue }
        [void]$visitedReal.Add($norm)

        try {
            $children = Get-ChildItem -LiteralPath $dir -Force -ErrorAction Stop
        }
        catch { continue }

        foreach ($c in $children) {
            if (-not $c.PSIsContainer) { continue }
            $n = $c.Name
            $ln = $n.ToLowerInvariant()
            if ($ln -eq 'venv' -or $ln -eq '.venv') { continue }
            if ($ln.StartsWith('.')) { continue }
            if ($ln -eq '__pycache__' -or $ln -eq 'node_modules' -or $ln -eq 'site-packages') { continue }
            if ($ln -eq '.git' -or $ln -eq '.idea' -or $ln -eq '.vscode') { continue }
            if ($ln -eq 'dist' -or $ln -eq 'build' -or $ln -eq '.tox') { continue }
            $fullChild = $c.FullName
            if ($rxSkip.IsMatch($fullChild)) { continue }
            $stack.Push($fullChild)
        }

        $botPath = Join-Path $dir 'bot.py'
        if (Test-Path -LiteralPath $botPath -PathType Leaf) {
            if ($rxSkip.IsMatch($botPath)) { continue }
            try {
                $br = [System.IO.Path]::GetFullPath((Get-Item -LiteralPath $botPath).FullName)
            }
            catch { continue }
            if ($seenBot.Contains($br)) { continue }
            [void]$seenBot.Add($br)
            $out.Add($botPath)
            if ($out.Count -ge $max) {
                Write-Warning "Limite ROBOTS_MAX_BOTS=$max atingido."
                break
            }
        }
    }

    return [string[]]($out.ToArray())
}

# --- main ---
Write-RobosSection 'ROBOS - Launcher'
Write-Host "  Pasta raiz : $Root"
Write-Host "  Venv Python: $VenvPy"
Write-Host ''
Write-Host '  Nota: cada robô abre a sua própria janela CMD - mensagens [SUPABASE], [GOIANIA], etc. aparecem lá, não nesta consola.' -ForegroundColor DarkGray
Write-Host '        ROBOS_VERBOSE_PIP=1 mostra mais detalhe de pip e requirements.' -ForegroundColor DarkGray

if (-not (Test-Path -LiteralPath $ReqRoot)) {
    Write-Warning "Sem requirements.txt na raiz; continuo apenas com imports e requirements nas subpastas."
}

Ensure-Venv

Write-RobosSection 'Dependências no venv (requirements + pacotes base)'
$depsSkipHeavy = Test-DepsFingerprintUnchanged
if ($depsSkipHeavy) {
    Write-Host '  Cache: venv + ficheiros requirements inalterados - sem pip -r nem Playwright nesta execução.' -ForegroundColor Green
    Write-Host '  Forçar sync completo: defina ROBOS_FORCE_SYNC=1 antes de correr o script.' -ForegroundColor DarkGray
}
else {
    Sync-AllRequirements
    Save-DepsFingerprint
}

$allImports = New-Object 'System.Collections.Generic.HashSet[string]'
$bots = Get-BotPaths
foreach ($bp in $bots) {
    $set = Get-ImportNamesFromFile -Path $bp
    foreach ($x in $set) { [void]$allImports.Add($x) }
}

Write-RobosSection 'Imports nos bot.py (teste real: import no venv)'
Write-Host '  Só corre pip install se importlib.import_module falhar neste venv.' -ForegroundColor DarkGray
$pipMap = Get-PipNameMap
Install-DetectedModules -Names $allImports -PipMap $pipMap

if (-not $depsSkipHeavy) {
    Write-RobosSection 'Playwright Chromium'
    $pwErr = Join-Path $env:TEMP ('robos_playwright_stderr_{0}.txt' -f [Guid]::NewGuid().ToString('n'))
    try {
        $pw = Start-Process -FilePath $VenvPy -ArgumentList @('-m', 'playwright', 'install', 'chromium') -Wait -PassThru -NoNewWindow -RedirectStandardError $pwErr
        if ($pw.ExitCode -ne 0) {
            Write-Host '  (exit code nao zero - browsers podem ja existir; robos Playwright costumam funcionar.)' -ForegroundColor DarkYellow
            if ($env:ROBOS_VERBOSE_PIP -eq '1' -and (Test-Path -LiteralPath $pwErr)) {
                Get-Content -LiteralPath $pwErr -ErrorAction SilentlyContinue | Select-Object -First 15 | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
            }
        }
        else {
            Write-Host '  Chromium OK.' -ForegroundColor DarkGreen
        }
    }
    finally {
        Remove-Item -LiteralPath $pwErr -Force -ErrorAction SilentlyContinue
    }
}

$lockPath = Join-Path $Root '_robos_a_iniciar.lock'
if (Test-Path -LiteralPath $lockPath) {
    Write-Warning "Ja existe lock: $lockPath (outro launcher ou lock antigo). Remova o ficheiro e tente de novo."
    exit 1
}
try {
    "$(Get-Date)" | Out-File -LiteralPath $lockPath -Encoding utf8
}
catch {
    Write-Warning 'Nao foi possivel criar lock; continuo mesmo assim.'
}

Write-RobosSection 'Arranque dos robôs (uma janela por robô)'
$count = 0
try {
    foreach ($bot in $bots) {
        if ([string]::IsNullOrWhiteSpace($bot)) { continue }
        $dir = Split-Path -Parent $bot
        $name = Split-Path -Leaf $dir
        Write-Host ''
        Write-Host ('  === ' + $name + ' ===') -ForegroundColor Green
        Write-Host "      Pasta : $dir"
        Write-Host "      Script: $bot"
        Write-Host ('      ' + ('-' * 64)) -ForegroundColor DarkGray
        # Start-Process: consola NOVA por robô. UseShellExecute=$false herdava a consola do launcher
        # e misturava prints dos bots com Read-Host / resumo desta janela.
        $qpy = '"' + $VenvPy + '"'
        $qbot = '"' + $bot + '"'
        $cmdLine = 'set "ROBOT_SCRIPT_DIR=' + $dir + '" && set "ROBOTS_ROOT_PATH=' + $Root + '" && ' + $qpy + ' ' + $qbot
        $shell = if ($env:ComSpec) { $env:ComSpec } else { "$env:SystemRoot\System32\cmd.exe" }
        Start-Process -FilePath $shell -WorkingDirectory $dir -ArgumentList @('/k', $cmdLine) -WindowStyle Normal
        $count++
        Write-Host "      Janela CMD iniciada (logs deste robô aparecem nessa janela)." -ForegroundColor DarkGray
    }
}
finally {
    Remove-Item -LiteralPath $lockPath -Force -ErrorAction SilentlyContinue
}

Write-Host ''
Write-RobosSection 'Resumo'
if ($count -eq 0) {
    Write-Host "  Nenhum bot.py encontrado sob $Root"
}
else {
    Write-Host "  $count robô(s) iniciado(s) em janelas separadas."
}
Write-Host ''
Read-Host 'Enter para fechar esta consola do launcher'
