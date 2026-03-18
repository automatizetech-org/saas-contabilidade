from __future__ import annotations

import argparse
import base64
import csv
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography import x509


def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s


def tokens(s: str) -> set[str]:
    t = {x for x in norm(s).split() if len(x) >= 3}
    # remove tokens muito genéricos
    stop = {"ltda", "eireli", "me", "epp", "comercio", "servicos", "industria", "importacao", "exportacao", "de", "da", "do", "dos", "das"}
    return {x for x in t if x not in stop}


@dataclass(frozen=True)
class Wanted:
    cnpj: str
    name: str


@dataclass
class MatchResult:
    cnpj: str
    name: str
    matched_folder: str
    pfx_path: str
    picked_strategy: str
    password_found: str
    cert_cnpj_extracted: str
    validity_date: str
    error: str


def try_extract_password_from_filename(name: str) -> str | None:
    raw = name
    if raw.lower().endswith(".pfx"):
        raw = raw[:-4]
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
            pwd = str(m.group(1)).strip().strip(" .-_—–()[]{}\"“”'`")
            if pwd:
                return pwd
    m2 = re.search(r"senha[\s:_-]*([0-9A-Za-z@#$%&!._-]+)", raw, flags=re.IGNORECASE)
    if m2:
        pwd = m2.group(1).strip().strip(" .-_—–()[]{}\"“”'`")
        return pwd or None
    return None


def build_password_candidates(pfx_path: Path, cnpj: str, extracted: str | None, include_1_to_8: bool) -> list[tuple[bytes | None, str]]:
    """
    Retorna lista de (password_bytes_or_none, password_string_for_db).
    """
    out: list[tuple[bytes | None, str]] = [(None, ""), (b"", "")]

    def add(s: str) -> None:
        ss = (s or "").strip().strip(" .-_—–()[]{}\"“”'`")
        if not ss:
            return
        b = ss.encode("utf-8")
        if any(b == x[0] for x in out if x[0] is not None):
            return
        out.append((b, ss))

    if extracted:
        add(extracted)

    # tokens numéricos e alfanuméricos no nome/caminho
    base = pfx_path.name[:-4] if pfx_path.name.lower().endswith(".pfx") else pfx_path.name
    for txt in (base, str(pfx_path.parent), str(pfx_path)):
        for tok in re.findall(r"\d{6,14}", txt):
            add(tok)
        for tok in re.findall(r"[A-Za-z]{1,8}\d{3,12}", txt):
            add(tok)
            add(tok.lower())
            add(tok.upper())

    # CNPJ e variações
    if cnpj.isdigit() and len(cnpj) == 14:
        add(cnpj)
        add(cnpj[:8])
        add(cnpj[-8:])

    # defaults
    for d in ("1234", "123456", "12345678", "senha", "SENHA", "senha123", "SENHA123"):
        add(d)

    # senhas simples 1..8
    if include_1_to_8:
        for i in range(1, 9):
            add(str(i))

    # arquivos laterais com "senha"
    sidecar_dirs = [pfx_path.parent, pfx_path.parent.parent]
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
                        add(str(m))
        except OSError:
            continue

    return out


def pick_best_folder(root: Path, wanted: Wanted) -> Path | None:
    """
    Faz match do nome do wanted com as pastas no root.
    Retorna a pasta com maior score (interseção de tokens), ou None se score baixo.
    """
    w_tokens = tokens(wanted.name)
    if not w_tokens:
        return None

    best: tuple[int, Path] | None = None
    for p in root.iterdir():
        if not p.is_dir():
            continue
        p_tokens = tokens(p.name)
        score = len(w_tokens & p_tokens)
        if score <= 0:
            continue
        if best is None or score > best[0]:
            best = (score, p)
    if best is None:
        return None
    # exige pelo menos 2 tokens em comum para evitar falso positivo
    if best[0] < 2:
        return None
    return best[1]


def pick_latest_pfx(company_dir: Path) -> Path | None:
    pfxs = list(company_dir.rglob("*.pfx"))
    if not pfxs:
        return None
    pfxs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return pfxs[0]


def update_sql_for_company(*, cnpj: str, cert_blob_b64: str, cert_password_sql: str, cert_valid_until_sql: str) -> str:
    return (
        "update public.companies set "
        "auth_mode = 'certificate', "
        f"cert_blob_b64 = '{cert_blob_b64}', "
        f"cert_password = {cert_password_sql}, "
        f"cert_valid_until = {cert_valid_until_sql} "
        "where regexp_replace(coalesce(document, ''), '\\\\D', '', 'g') = "
        f"'{cnpj}';\n"
    )


def sql_escape_literal(value: str) -> str:
    return value.replace("'", "''")


def extract_cnpj_from_cert(cert: x509.Certificate) -> str:
    """
    Tenta extrair o CNPJ do certificado (ICP-Brasil) de formas comuns:
    - otherName OID 2.16.76.1.3.3 (CNPJ)
    - qualquer sequência de 14 dígitos no subject/SAN (fallback)
    Retorna 14 dígitos ou "".
    """
    # 1) otherName OID 2.16.76.1.3.3
    try:
        oid_cnpj = x509.ObjectIdentifier("2.16.76.1.3.3")
        try:
            san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        except x509.ExtensionNotFound:
            san = None
        if san is not None:
            for gn in san:
                if isinstance(gn, x509.OtherName) and gn.type_id == oid_cnpj:
                    # value costuma ser DER; tenta extrair 14 dígitos do blob
                    digits = re.sub(r"\D", "", gn.value.decode("latin-1", errors="ignore"))
                    m = re.search(r"(\d{14})", digits)
                    if m:
                        return m.group(1)
    except Exception:
        pass

    # 2) fallback: varre subject e SAN string por 14 dígitos
    blobs: list[str] = []
    try:
        blobs.append(cert.subject.rfc4514_string())
    except Exception:
        pass
    try:
        blobs.append(str(cert.subject))
    except Exception:
        pass
    try:
        san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        blobs.append(str(san))
    except Exception:
        pass

    for b in blobs:
        d = re.sub(r"\D", "", b)
        m = re.search(r"(\d{14})", d)
        if m:
            return m.group(1)
    return ""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=str,
        default=r"C:\Users\Victor\Downloads\EMPRESAS - DOCUMENTOS CADASTRAIS",
        help="Diretório raiz onde estão as pastas das empresas.",
    )
    parser.add_argument(
        "--wanted-csv",
        type=str,
        default="supabase/missing_cert_companies.csv",
        help="CSV com cnpj,name (relativo ao repo).",
    )
    parser.add_argument(
        "--out-sql",
        type=str,
        default="supabase/import_sefaz_xml_certificates.missing_by_folder.sql",
        help="Arquivo SQL de saída (relativo ao repo).",
    )
    parser.add_argument(
        "--out-parts-dir",
        type=str,
        default="supabase/segmented_missing_by_folder_90k",
        help="Diretório para SQLs segmentados (relativo ao repo).",
    )
    parser.add_argument(
        "--report-csv",
        type=str,
        default="supabase/import_sefaz_xml_certificates.missing_by_folder.report.csv",
        help="CSV de relatório (relativo ao repo).",
    )
    parser.add_argument("--max-chars", type=int, default=90_000)
    parser.add_argument("--try-passwords-1-to-8", action="store_true")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    root = Path(args.root)
    wanted_csv = repo_root / args.wanted_csv
    out_sql = repo_root / args.out_sql
    out_parts = repo_root / args.out_parts_dir
    out_report = repo_root / args.report_csv

    wanted: list[Wanted] = []
    with wanted_csv.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            cnpj = re.sub(r"\D", "", (row.get("cnpj") or "").strip())
            name = (row.get("name") or "").strip()
            if len(cnpj) == 14 and name:
                wanted.append(Wanted(cnpj=cnpj, name=name))

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sql_lines: list[str] = [f"-- Gerado do disco em {now}\n", f"-- Root: {root}\n", "begin;\n"]
    report: list[MatchResult] = []

    for w in wanted:
        folder = pick_best_folder(root, w)
        if folder is None:
            report.append(
                MatchResult(
                    cnpj=w.cnpj,
                    name=w.name,
                    matched_folder="",
                    pfx_path="",
                    picked_strategy="no_folder_match",
                    password_found="",
                    cert_cnpj_extracted="",
                    validity_date="",
                    error="folder_match_not_found_or_low_score",
                )
            )
            continue

        pfx = pick_latest_pfx(folder)
        if pfx is None:
            report.append(
                MatchResult(
                    cnpj=w.cnpj,
                    name=w.name,
                    matched_folder=folder.name,
                    pfx_path="",
                    picked_strategy="folder_matched_no_pfx",
                    password_found="",
                    cert_cnpj_extracted="",
                    validity_date="",
                    error="no_pfx_found_in_folder",
                )
            )
            continue

        extracted_pwd = try_extract_password_from_filename(pfx.name)
        pw_candidates = build_password_candidates(pfx, w.cnpj, extracted_pwd, args.try_passwords_1_to_8)

        raw = pfx.read_bytes()
        cert = None
        chosen_pwd_str = ""
        last_err = ""
        for pw_bytes, pw_str in pw_candidates:
            try:
                key, cert, addl = pkcs12.load_key_and_certificates(raw, pw_bytes)
                if cert is not None:
                    chosen_pwd_str = pw_str
                    break
            except Exception as e:  # noqa: BLE001
                last_err = str(e)
                cert = None

        cert_valid_until_sql = "null"
        validity_date = ""
        cert_cnpj_extracted = ""
        if cert is not None:
            cert_cnpj_extracted = extract_cnpj_from_cert(cert)
            if cert_cnpj_extracted and cert_cnpj_extracted != w.cnpj:
                # NÃO gera SQL para não vincular certificado errado
                report.append(
                    MatchResult(
                        cnpj=w.cnpj,
                        name=w.name,
                        matched_folder=folder.name,
                        pfx_path=str(pfx.relative_to(root)),
                        picked_strategy="folder_best_token_overlap_latest_pfx",
                        password_found=chosen_pwd_str,
                        cert_cnpj_extracted=cert_cnpj_extracted,
                        validity_date="",
                        error="cnpj_mismatch_certificate_subject",
                    )
                )
                continue

            not_after = getattr(cert, "not_valid_after_utc", None) or cert.not_valid_after
            validity_date = not_after.date().isoformat()
            cert_valid_until_sql = f"'{validity_date}T00:00:00Z'::timestamptz"

        b64 = base64.b64encode(raw).decode("ascii")
        cert_password_sql = "null"
        if chosen_pwd_str:
            cert_password_sql = f"'{sql_escape_literal(chosen_pwd_str)}'"

        sql_lines.append(
            update_sql_for_company(
                cnpj=w.cnpj,
                cert_blob_b64=b64,
                cert_password_sql=cert_password_sql,
                cert_valid_until_sql=cert_valid_until_sql,
            )
        )

        report.append(
            MatchResult(
                cnpj=w.cnpj,
                name=w.name,
                matched_folder=folder.name,
                pfx_path=str(pfx.relative_to(root)),
                picked_strategy="folder_best_token_overlap_latest_pfx",
                password_found=chosen_pwd_str,
                cert_cnpj_extracted=cert_cnpj_extracted,
                validity_date=validity_date,
                error="" if cert is not None else f"pfx_open_failed:{last_err}",
            )
        )

    sql_lines.append("commit;\n")
    out_sql.parent.mkdir(parents=True, exist_ok=True)
    out_sql.write_text("".join(sql_lines), encoding="utf-8")

    # segmenta
    out_parts.mkdir(parents=True, exist_ok=True)
    text = out_sql.read_text(encoding="utf-8")
    header, body = [], []
    seen_begin = False
    for ln in text.splitlines(True):
        if not seen_begin:
            header.append(ln)
            if ln.strip().lower() == "begin;":
                seen_begin = True
        else:
            body.append(ln)
    while body and body[-1].strip().lower() in {"commit;", "end;"}:
        body.pop()
    stmts = [ln for ln in body if ln.strip()]

    parts: list[Path] = []
    idx = 1
    chunk = ""
    base_name = out_sql.stem

    def write_part(i: int, ch: str) -> Path:
        out: list[str] = []
        for ln in header:
            if ln.strip().lower() == "begin;":
                break
            out.append(ln)
        out.append("begin;\n")
        out.append(ch if ch.endswith("\n") else ch + "\n")
        out.append("commit;\n")
        p = out_parts / f"{base_name}.part{i:03d}.sql"
        p.write_text("".join(out), encoding="utf-8")
        return p

    for st in stmts:
        if chunk and len(chunk) + len(st) > args.max_chars:
            parts.append(write_part(idx, chunk))
            idx += 1
            chunk = ""
        chunk += st
        if not chunk.endswith("\n"):
            chunk += "\n"
    if chunk:
        parts.append(write_part(idx, chunk))

    # report
    out_report.parent.mkdir(parents=True, exist_ok=True)
    with out_report.open("w", newline="", encoding="utf-8") as f:
        wtr = csv.DictWriter(
            f,
            fieldnames=[
                "cnpj",
                "name",
                "matched_folder",
                "pfx_path",
                "picked_strategy",
                "password_found",
                "cert_cnpj_extracted",
                "validity_date",
                "error",
            ],
        )
        wtr.writeheader()
        for r in report:
            wtr.writerow(r.__dict__)

    print(f"Wanted: {len(wanted)}")
    print(f"Wrote SQL: {out_sql}")
    print(f"Segmented parts: {len(parts)} -> {out_parts}")
    print(f"Wrote report: {out_report}")


if __name__ == "__main__":
    main()

