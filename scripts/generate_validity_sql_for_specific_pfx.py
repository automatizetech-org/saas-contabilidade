from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from cryptography.hazmat.primitives.serialization import pkcs12


@dataclass(frozen=True)
class Item:
    cnpj: str
    relative_pfx_path: str
    password: str


ITEMS: list[Item] = [
    Item(
        cnpj="49822184000164",
        relative_pfx_path=r"INLUZ ARTIGOS DE ILUMINACAO\10 CERTIFICADO DIGITAL\PJ\2025 - INLUZ ARTIGOS DE ILUMINACAO LTDA_49822184000164.pfx",
        password="INLUZ@2025",
    ),
    Item(
        cnpj="11938121000103",
        relative_pfx_path=r"NUTRATUS INDUSTRIA E COMERCIO DE COSMETICOS E ALIMENTOS\10 CERTIFICADO DIGITAL\PJ\2026 - 173903855_NUTRATUS_INDUSTRIA_E_COMERCIO_DE_COSMETICOS_E_ALI_11938121000103.pfx",
        password="050325",
    ),
    Item(
        cnpj="46672831000100",
        relative_pfx_path=r"2V EMPREENDIMENTOS\10 CERTIFICADO DIGITAL\PJ\antigos\2V EMPREENDIMENTOS NEGOCIOS E SERVICOS LTDA46672831000100.pfx",
        password="Vv011012",
    ),
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=str,
        default=r"C:\Users\Victor\Downloads\EMPRESAS - DOCUMENTOS CADASTRAIS",
        help="Diretório raiz onde estão as pastas das empresas.",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="supabase/segmented_validity_only_manual_90k/import_sefaz_xml_certificates.validity_only.manual.part001.sql",
        help="Arquivo SQL de saída (relativo ao repo).",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    root = Path(args.root)
    out_path = repo_root / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sql: list[str] = []
    sql.append(f"-- Gerado em {now}\n")
    sql.append(f"-- Root: {root}\n")
    sql.append("begin;\n")

    for it in ITEMS:
        pfx_path = root / it.relative_pfx_path
        raw = pfx_path.read_bytes()
        pw = it.password or ""
        pw_text_variants: list[str] = []
        for v in (
            pw,
            pw.strip(),
            pw.lower(),
            pw.upper(),
            f"{pw} ",
            f" {pw}",
        ):
            if v and v not in pw_text_variants:
                pw_text_variants.append(v)

        pw_bytes_variants: list[bytes | None] = [None, b""]
        seen_b: set[bytes] = set()
        for v in pw_text_variants:
            for enc in ("utf-8", "latin-1"):
                b = v.encode(enc)
                if b and b not in seen_b:
                    seen_b.add(b)
                    pw_bytes_variants.append(b)

        last_err: Exception | None = None
        cert = None
        # tenta no arquivo exato e também em outros .pfx da mesma pasta (às vezes tem mais de um)
        candidate_files: list[Path] = []
        candidate_files.append(pfx_path)
        try:
            for p in pfx_path.parent.glob("*.pfx"):
                if p not in candidate_files:
                    candidate_files.append(p)
        except OSError:
            pass
        # e também procura no diretório da empresa (recursivo), priorizando os mais recentes
        try:
            company_dir = root / Path(it.relative_pfx_path).parts[0]
            all_pfx = list(company_dir.rglob("*.pfx"))
            all_pfx.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            for p in all_pfx[:25]:
                if p not in candidate_files:
                    candidate_files.append(p)
        except Exception:  # noqa: BLE001
            pass

        chosen_path: Path | None = None
        for fp in candidate_files:
            raw_fp = fp.read_bytes()
            for b in pw_bytes_variants:
                try:
                    key, cert, addl = pkcs12.load_key_and_certificates(raw_fp, b)
                    if cert is not None:
                        raw = raw_fp
                        chosen_path = fp
                        break
                except Exception as e:  # noqa: BLE001
                    last_err = e
                    cert = None
            if cert is not None:
                break

        if cert is None:
            raise SystemExit(f"Falha ao abrir PFX (senha?): {pfx_path} ({last_err})")
        if cert is None:
            raise SystemExit(f"Certificado não encontrado dentro do PFX: {pfx_path}")
        not_after = getattr(cert, "not_valid_after_utc", None) or cert.not_valid_after
        valid_sql = f"'{not_after.date().isoformat()}T00:00:00Z'::timestamptz"
        sql.append(
            "update public.companies set "
            f"cert_valid_until = {valid_sql} "
            "where regexp_replace(coalesce(document, ''), '\\\\D', '', 'g') = "
            f"'{it.cnpj}' "
            "and cert_valid_until is null;\n"
        )

    sql.append("commit;\n")
    out_path.write_text("".join(sql), encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()

