from __future__ import annotations

import argparse
import base64
import csv
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from cryptography.hazmat.primitives.serialization import pkcs12


def only_digits(value: str) -> str:
    return re.sub(r"\D+", "", value or "")


def extract_cnpj_from_text(text: str) -> str | None:
    # 1) tenta padrão de CNPJ com pontuação (##.###.###/####-##)
    m_fmt = re.findall(r"(?<!\d)\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}(?!\d)", text)
    if m_fmt:
        return only_digits(m_fmt[-1])

    # 2) tenta 14 dígitos contíguos (com borda) no texto original
    m_raw = re.findall(r"(?<!\d)\d{14}(?!\d)", text)
    if m_raw:
        return m_raw[-1]

    # 3) fallback: separa tokens por não-dígito e procura token exatamente com 14 dígitos
    tokens = re.split(r"\D+", text)
    tokens = [t for t in tokens if t]
    for t in reversed(tokens):
        if len(t) == 14 and t.isdigit():
            return t

    return None


def sql_escape_literal(value: str) -> str:
    # SQL single-quoted literal escape
    return value.replace("'", "''")


def try_extract_password_from_filename(name: str) -> str | None:
    # Heurística: procura padrões comuns tipo "senha 1234", "senha-xxxx", "SENHAxxxx"
    raw = name
    # remove extensão
    if raw.lower().endswith(".pfx"):
        raw = raw[:-4]

    # padrões explícitos
    patterns = [
        r"senha[\s:_-]*([^\s]+)$",
        r"senha[\s:_-]*([^\s]+)[\s_-]",
        r"\(senha[\s:_-]*([^)]+)\)",
        r"\[senha[\s:_-]*([^\]]+)\]",
        r"pwd[\s:_-]*([^\s]+)$",
        r"pass[\s:_-]*([^\s]+)$",
    ]
    for pat in patterns:
        m = re.search(pat, raw, flags=re.IGNORECASE)
        if m:
            pwd = str(m.group(1)).strip()
            # limpa aspas/trailling pontuação comum
            pwd = pwd.strip(" .-_—–()[]{}\"“”'`")
            if pwd:
                return pwd

    # fallback: procura token após a palavra "senha" em qualquer lugar
    m2 = re.search(r"senha[\s:_-]*([0-9A-Za-z@#$%&!]+)", raw, flags=re.IGNORECASE)
    if m2:
        pwd = m2.group(1).strip()
        pwd = pwd.strip(" .-_—–()[]{}\"“”'`")
        return pwd or None

    return None


def build_password_candidates(c: "CertCandidate") -> list[bytes | None]:
    """
    Monta uma lista pequena (mas efetiva) de senhas candidatas para abrir PFX.
    Cobre casos comuns: sem senha, senha vazia, senha no nome, tokens numéricos no caminho,
    CNPJ e variações, e alguns defaults frequentes.
    """
    ordered: list[bytes | None] = [None, b""]

    candidates: list[str] = []

    # extração direta do filename
    if c.password and c.password.strip():
        candidates.append(c.password.strip())

    def add_token(tok: str) -> None:
        t = (tok or "").strip().strip(" .-_—–()[]{}\"“”'`")
        if not t:
            return
        # evita tokens enormes (base64 etc.)
        if len(t) > 32:
            return
        candidates.append(t)

    # tokens numéricos no filename/caminho (muito comum ser 6-14 dígitos)
    for txt in (c.file_path.name, str(c.file_path.parent), str(c.file_path)):
        for token in re.findall(r"\d{6,14}", txt):
            add_token(token)

    # tokens alfanuméricos típicos tipo "lm100491", "Data1234", etc.
    for txt in (c.file_path.name, str(c.file_path.parent)):
        for token in re.findall(r"[A-Za-z]{1,6}\d{3,12}", txt):
            add_token(token)
            add_token(token.lower())
            add_token(token.upper())

    # tokens após separadores comuns (" - ", "_" etc.)
    base = c.file_path.name
    if base.lower().endswith(".pfx"):
        base = base[:-4]
    for token in re.split(r"[ _\-]+", base):
        if 4 <= len(token) <= 20 and re.search(r"[0-9]", token):
            add_token(token)
            add_token(token.lower())
            add_token(token.upper())

    # CNPJ e variações
    if c.cnpj and len(c.cnpj) == 14 and c.cnpj.isdigit():
        candidates.append(c.cnpj)
        candidates.append(c.cnpj[:8])
        candidates.append(c.cnpj[-8:])

    # defaults comuns
    candidates.extend(
        [
            "1234",
            "123456",
            "12345678",
            "123456789",
            "senha",
            "SENHA",
            "senha123",
            "SENHA123",
        ]
    )

    # tenta achar senha em arquivos próximos (ex.: "senha.txt", "certificado.txt")
    sidecar_dirs = [c.file_path.parent, c.file_path.parent.parent]
    sidecar_patterns = [
        re.compile(r"senha[\s:=_-]*([0-9A-Za-z@#$%&!._-]{3,32})", flags=re.IGNORECASE),
        re.compile(r"password[\s:=_-]*([0-9A-Za-z@#$%&!._-]{3,32})", flags=re.IGNORECASE),
        re.compile(r"pwd[\s:=_-]*([0-9A-Za-z@#$%&!._-]{3,32})", flags=re.IGNORECASE),
    ]
    for d in sidecar_dirs:
        try:
            if not d.exists() or not d.is_dir():
                continue
        except OSError:
            continue
        try:
            for p in d.iterdir():
                if not p.is_file():
                    continue
                if p.suffix.lower() not in {".txt", ".md", ".csv", ".log", ".json"}:
                    continue
                # prioriza arquivos com "senha" no nome
                if "senha" not in p.name.lower() and "pass" not in p.name.lower() and "pwd" not in p.name.lower():
                    continue
                try:
                    if p.stat().st_size > 200_000:
                        continue
                    content = p.read_text(encoding="utf-8", errors="ignore")
                except OSError:
                    continue
                for rx in sidecar_patterns:
                    for m in rx.findall(content):
                        add_token(str(m))
        except OSError:
            continue

    # dedupe preservando ordem e limita tentativas
    seen: set[str] = set()
    for s in candidates:
        s2 = (s or "").strip()
        if not s2 or s2 in seen:
            continue
        seen.add(s2)
        ordered.append(s2.encode("utf-8"))

    return ordered[:30]


@dataclass(frozen=True)
class CertCandidate:
    company_folder: str
    file_path: Path
    cnpj: str | None
    password: str | None
    mtime: float


def iter_pfx_files(root: Path) -> Iterable[Path]:
    yield from root.rglob("*.pfx")


def pick_latest_by_cnpj(candidates: list[CertCandidate]) -> dict[str, CertCandidate]:
    by_cnpj: dict[str, CertCandidate] = {}
    for c in candidates:
        if not c.cnpj:
            continue
        prev = by_cnpj.get(c.cnpj)
        if not prev or c.mtime > prev.mtime:
            by_cnpj[c.cnpj] = c
    return by_cnpj


def build_update_sql(c: CertCandidate, *, cert_blob_b64: str) -> str:
    pwd_sql: str | None = None
    if c.password and c.password.strip():
        pwd_sql = sql_escape_literal(c.password.strip())
    cert_password_sql = "null" if pwd_sql is None else f"'{pwd_sql}'"
    # Mantém cert_valid_until = null (não dá pra calcular sem ler/parsear o PFX).
    # auth_mode certificate garante que a UI marque como cadastrado.
    return (
        "update public.companies set "
        "auth_mode = 'certificate', "
        f"cert_blob_b64 = '{cert_blob_b64}', "
        f"cert_password = {cert_password_sql}, "
        "cert_valid_until = null "
        "where regexp_replace(coalesce(document, ''), '\\\\D', '', 'g') = "
        f"'{c.cnpj}';\n"
    )

def build_validity_only_update_sql(c: CertCandidate, *, cert_valid_until_sql: str) -> str:
    return (
        "update public.companies set "
        f"cert_valid_until = {cert_valid_until_sql} "
        "where regexp_replace(coalesce(document, ''), '\\\\D', '', 'g') = "
        f"'{c.cnpj}' "
        "and cert_valid_until is null;\n"
    )


def split_sql_into_parts(sql_text: str, out_dir: Path, *, base_name: str, max_chars: int) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    lines = sql_text.splitlines(True)

    header: list[str] = []
    body: list[str] = []
    seen_begin = False
    for ln in lines:
        if not seen_begin:
            header.append(ln)
            if ln.strip().lower() == "begin;":
                seen_begin = True
        else:
            body.append(ln)

    while body and body[-1].strip().lower() in {"commit;", "end;"}:
        body.pop()

    stmts = [ln for ln in body if ln.strip()]

    def write_part(idx: int, chunk: str) -> Path:
        out: list[str] = []
        for ln in header:
            if ln.strip().lower() == "begin;":
                break
            out.append(ln)
        out.append("begin;\n")
        out.append(chunk)
        if not chunk.endswith("\n"):
            out.append("\n")
        out.append("commit;\n")
        p = out_dir / f"{base_name}.part{idx:03d}.sql"
        p.write_text("".join(out), encoding="utf-8")
        return p

    parts: list[Path] = []
    idx = 1
    chunk = ""
    for st in stmts:
        if chunk and len(chunk) + len(st) > max_chars:
            parts.append(write_part(idx, chunk))
            idx += 1
            chunk = ""
        chunk += st
        if not chunk.endswith("\n"):
            chunk += "\n"
        if len(chunk) >= max_chars:
            parts.append(write_part(idx, chunk))
            idx += 1
            chunk = ""

    if chunk:
        parts.append(write_part(idx, chunk))

    return parts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=str,
        default=r"C:\Users\Victor\Downloads\EMPRESAS - DOCUMENTOS CADASTRAIS",
        help="Diretório raiz onde estão as pastas das empresas.",
    )
    parser.add_argument(
        "--out-sql",
        type=str,
        default="supabase/import_sefaz_xml_certificates.from_disk.sql",
        help="Arquivo SQL de saída (relativo ao repo).",
    )
    parser.add_argument(
        "--out-parts-dir",
        type=str,
        default="supabase/segmented_from_disk_90k",
        help="Diretório para os SQLs segmentados (relativo ao repo).",
    )
    parser.add_argument(
        "--report-csv",
        type=str,
        default="supabase/import_sefaz_xml_certificates.from_disk.report.csv",
        help="CSV de conferência (relativo ao repo).",
    )
    parser.add_argument(
        "--max-chars",
        type=int,
        default=90_000,
        help="Tamanho máximo aproximado (em caracteres) por parte do SQL segmentado.",
    )
    parser.add_argument(
        "--with-validity",
        action="store_true",
        help="Quando possível, extrai validade do PFX e preenche cert_valid_until.",
    )
    parser.add_argument(
        "--validity-only",
        action="store_true",
        help="Gera SQL que atualiza somente cert_valid_until (sem mexer no blob/senha), apenas quando cert_valid_until estiver null.",
    )
    parser.add_argument(
        "--only-cnpjs-file",
        type=str,
        default="",
        help="Se informado, processa apenas os CNPJs listados (um por linha).",
    )
    parser.add_argument(
        "--try-passwords-1-to-8",
        action="store_true",
        help="Inclui tentativas de senha simples '1'..'8' ao extrair validade (útil quando a senha é bem curta).",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    root = Path(args.root)
    out_sql = repo_root / args.out_sql
    out_parts = repo_root / args.out_parts_dir
    out_csv = repo_root / args.report_csv

    if not root.exists():
        raise SystemExit(f"Diretório não encontrado: {root}")

    # Se o usuário passou filtro de CNPJs, prepara também tokens (8 primeiros / 8 últimos) para casar com nomes/pastas.
    only_cnpjs: set[str] = set()
    token_to_full: dict[str, set[str]] = {}
    if args.only_cnpjs_file:
        p = Path(args.only_cnpjs_file)
        if not p.is_absolute():
            p = repo_root / p
        for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines():
            s = re.sub(r"\D", "", (ln or "").strip())
            if len(s) == 14:
                only_cnpjs.add(s)
        for cnpj in sorted(only_cnpjs):
            for tok in (cnpj, cnpj[:8], cnpj[-8:]):
                token_to_full.setdefault(tok, set()).add(cnpj)

    candidates: list[CertCandidate] = []
    for pfx in iter_pfx_files(root):
        try:
            stat = pfx.stat()
        except OSError:
            continue
        rel = pfx.relative_to(root)
        folder = str(rel.parts[0]) if rel.parts else ""
        cnpj = extract_cnpj_from_text(str(rel))
        # Se estamos filtrando por CNPJs e o arquivo contém apenas a base (8 primeiros)
        # ou está com CNPJ diferente do que queremos cadastrar (matriz vs filial),
        # tenta mapear pelo token e sobrescreve para o CNPJ alvo quando o match for único.
        if token_to_full and (not cnpj or (only_cnpjs and cnpj not in only_cnpjs)):
            digits = re.sub(r"\D", "", str(rel))
            hits: set[str] = set()
            for tok, fulls in token_to_full.items():
                if tok and tok in digits:
                    hits |= fulls
            if len(hits) == 1:
                cnpj = next(iter(hits))
        pwd = try_extract_password_from_filename(pfx.name)
        candidates.append(
            CertCandidate(
                company_folder=folder,
                file_path=pfx,
                cnpj=cnpj,
                password=pwd,
                mtime=stat.st_mtime,
            )
        )

    picked = pick_latest_by_cnpj(candidates)
    if only_cnpjs:
        picked = {k: v for k, v in picked.items() if k in only_cnpjs}

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sql_lines: list[str] = []
    sql_lines.append(f"-- Gerado do disco em {now}\n")
    sql_lines.append(f"-- Root: {root}\n")
    sql_lines.append("begin;\n")

    report_rows: list[dict[str, str]] = []
    updates_written = 0
    for cnpj, c in sorted(picked.items(), key=lambda kv: kv[0]):
        raw = c.file_path.read_bytes()
        b64 = base64.b64encode(raw).decode("ascii")
        cert_valid_until_sql = "null"
        validity_error = ""
        validity_iso = ""
        if args.with_validity:
            pw_candidates = build_password_candidates(c)
            if args.try_passwords_1_to_8:
                for i in range(1, 9):
                    pw_candidates.append(str(i).encode("utf-8"))

            loaded = False
            for pw in pw_candidates:
                try:
                    key, cert, addl = pkcs12.load_key_and_certificates(raw, pw)
                    if cert is None:
                        continue
                    not_after = getattr(cert, "not_valid_after_utc", None) or cert.not_valid_after
                    cert_valid_until_sql = f"'{not_after.date().isoformat()}T00:00:00Z'::timestamptz"
                    validity_iso = not_after.date().isoformat()
                    loaded = True
                    break
                except Exception as e:  # noqa: BLE001
                    validity_error = str(e)
                    continue
            if not loaded and validity_error:
                # mantém null
                pass

        if args.validity_only:
            # Só escreve updates quando conseguimos extrair validade.
            if cert_valid_until_sql != "null":
                sql_lines.append(build_validity_only_update_sql(c, cert_valid_until_sql=cert_valid_until_sql))
                updates_written += 1
        else:
            sql_lines.append(
                build_update_sql(c, cert_blob_b64=b64).replace(
                    "cert_valid_until = null", f"cert_valid_until = {cert_valid_until_sql}"
                )
            )
            updates_written += 1
        report_rows.append(
            {
                "cnpj": cnpj,
                "company_folder": c.company_folder,
                "relative_path": str(c.file_path.relative_to(root)),
                "password_extracted": c.password or "",
                "validity_extracted": validity_iso,
                "validity_error": validity_error[:200],
                "mtime_iso": datetime.fromtimestamp(c.mtime).isoformat(timespec="seconds"),
                "size_bytes": str(len(raw)),
            }
        )

    sql_lines.append("commit;\n")

    out_sql.parent.mkdir(parents=True, exist_ok=True)
    out_sql.write_text("".join(sql_lines), encoding="utf-8")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "cnpj",
                "company_folder",
                "relative_path",
                "password_extracted",
                "validity_extracted",
                "validity_error",
                "mtime_iso",
                "size_bytes",
            ],
        )
        writer.writeheader()
        writer.writerows(report_rows)

    base_name = out_sql.stem
    parts = split_sql_into_parts(out_sql.read_text(encoding="utf-8"), out_parts, base_name=base_name, max_chars=args.max_chars)

    print(f"Found {len(candidates)} .pfx files; picked latest for {len(picked)} CNPJs.")
    print(f"Wrote {updates_written} updates to {out_sql}")
    print(f"Wrote report to {out_csv}")
    print(f"Segmented into {len(parts)} parts at {out_parts}")


if __name__ == "__main__":
    main()

