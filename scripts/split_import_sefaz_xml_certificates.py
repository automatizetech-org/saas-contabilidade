from __future__ import annotations

import argparse
from pathlib import Path


def split_sql_file(
    src_path: Path,
    out_dir: Path,
    *,
    max_chars_per_part: int = 220_000,
) -> list[Path]:
    text = src_path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines(True)  # keep line endings

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

    # drop trailing commit/end from the original so we can wrap each part
    while body and body[-1].strip().lower() in {"commit;", "end;"}:
        body.pop()

    # In this generated file each statement is on its own line (the huge b64 stays in one line).
    stmts = [ln for ln in body if ln.strip()]

    out_dir.mkdir(parents=True, exist_ok=True)

    def write_part(idx: int, chunk: str) -> Path:
        out: list[str] = []

        # Keep only the comment header (everything before the original begin;)
        for ln in header:
            if ln.strip().lower() == "begin;":
                break
            out.append(ln)

        out.append("begin;\n")
        out.append(chunk)
        if not chunk.endswith("\n"):
            out.append("\n")
        out.append("commit;\n")

        part_path = out_dir / f"{src_path.stem}.part{idx:03d}.sql"
        part_path.write_text("".join(out), encoding="utf-8")
        return part_path

    parts: list[Path] = []
    idx = 1
    chunk = ""

    for st in stmts:
        # If a single statement exceeds the max, we still write it alone.
        if chunk and (len(chunk) + len(st) > max_chars_per_part):
            parts.append(write_part(idx, chunk))
            idx += 1
            chunk = ""

        chunk += st

        if not chunk.endswith("\n"):
            chunk += "\n"

        if len(chunk) >= max_chars_per_part:
            parts.append(write_part(idx, chunk))
            idx += 1
            chunk = ""

    if chunk:
        parts.append(write_part(idx, chunk))

    return parts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max-chars",
        type=int,
        default=90_000,
        help="Tamanho máximo aproximado (em caracteres) por arquivo gerado.",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default="supabase/segmented",
        help="Diretório de saída (relativo à raiz do repo).",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    src = repo_root / "supabase" / "import_sefaz_xml_certificates.sql"
    out_dir = repo_root / args.out_dir

    parts = split_sql_file(src, out_dir, max_chars_per_part=args.max_chars)
    print(f"Created {len(parts)} files in {out_dir}")
    for p in parts[:5]:
        print("-", p.name, p.stat().st_size, "bytes")
    if len(parts) > 5:
        print("...")
        for p in parts[-3:]:
            print("-", p.name, p.stat().st_size, "bytes")


if __name__ == "__main__":
    main()

