"""NUCC Taxonomy Code Set loader for the nppes-sqlite project.

Downloads or loads the official NUCC Healthcare Provider Taxonomy Code Set
(10-character codes) into a reference table.

Usage:
    # Download latest and load
    python -m nppes.taxonomy --db nppes.db --download

    # Load from local CSV
    python -m nppes.taxonomy --db nppes.db --csv /path/to/nucc_taxonomy_251.csv

The taxonomy_codes table gives human-readable descriptions you can JOIN
against your taxonomies.code column from NPI data.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
import tempfile
import urllib.request
from pathlib import Path
from typing import Optional

from .schema import get_db, init_db

DEFAULT_TAXONOMY_URL = "https://nucc.org/images/stories/CSV/nucc_taxonomy_251.csv"
BATCH_SIZE = 5000


def download_taxonomy_csv(url: str = DEFAULT_TAXONOMY_URL, dest: Optional[Path] = None) -> Path:
    """Download the NUCC taxonomy CSV to a temp file or specified path."""
    if dest is None:
        tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        dest = Path(tmp.name)
        tmp.close()

    print(f"Downloading taxonomy codes from {url} ...")
    with urllib.request.urlopen(url, timeout=60) as response:
        data = response.read()
    dest.write_bytes(data)
    print(f"Saved to {dest} ({len(data):,} bytes)")
    return dest


def load_taxonomy_codes(
    conn: sqlite3.Connection,
    csv_path: Path,
    batch_size: int = BATCH_SIZE,
) -> int:
    """Stream-load the NUCC taxonomy CSV into taxonomy_codes table.

    CSV columns (as of v25.1):
        Code, Grouping, Classification, Specialization, Definition, Notes,
        Display Name, Section
    """
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    print(f"[taxonomy] loading from {csv_path}")

    # Ensure table exists (idempotent)
    init_db(conn)

    total = 0
    batch = []

    with csv_path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader, None)

        if not header or "Code" not in header[0]:
            # Try to be forgiving if someone passes a file without header
            pass

        for row in reader:
            if not row or not row[0].strip():
                continue

            # Pad row in case of future format changes
            row = (row + [""] * 8)[:8]

            code, grouping, classification, specialization, definition, notes, display_name, section = row

            batch.append((
                code.strip(),
                grouping.strip() or None,
                classification.strip() or None,
                specialization.strip() or None,
                definition.strip() or None,
                notes.strip() or None,
                display_name.strip() or None,
                section.strip() or None,
            ))

            if len(batch) >= batch_size:
                _insert_batch(conn, batch)
                total += len(batch)
                batch = []
                if total % (batch_size * 4) == 0:
                    print(f"  ... {total:,} codes")

    if batch:
        _insert_batch(conn, batch)
        total += len(batch)

    conn.commit()
    print(f"[taxonomy] done: {total:,} taxonomy codes loaded/updated")
    return total


def _insert_batch(conn: sqlite3.Connection, batch: list[tuple]) -> None:
    sql = """
        INSERT INTO taxonomy_codes
            (code, grouping, classification, specialization, definition, notes, display_name, section)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
            grouping = excluded.grouping,
            classification = excluded.classification,
            specialization = excluded.specialization,
            definition = excluded.definition,
            notes = excluded.notes,
            display_name = excluded.display_name,
            section = excluded.section
    """
    conn.executemany(sql, batch)


def get_latest_taxonomy_url() -> str:
    """Return the current default URL (update when new version is released)."""
    return DEFAULT_TAXONOMY_URL


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        prog="nppes-taxonomy",
        description="Load NUCC Healthcare Provider Taxonomy Code Set into nppes.db"
    )
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--csv", help="Path to local nucc_taxonomy_*.csv file")
    parser.add_argument("--download", action="store_true", help="Download latest CSV from NUCC")
    parser.add_argument("--url", help="Override download URL")
    parser.add_argument("--batch", type=int, default=BATCH_SIZE, help="Batch size for inserts")
    args = parser.parse_args(argv)

    db_path = Path(args.db).expanduser().resolve()
    conn = get_db(db_path)

    if args.csv:
        csv_path = Path(args.csv).expanduser().resolve()
    elif args.download or not args.csv:
        url = args.url or get_latest_taxonomy_url()
        csv_path = download_taxonomy_csv(url)
    else:
        parser.error("Provide either --csv <path> or --download")

    try:
        load_taxonomy_codes(conn, csv_path, batch_size=args.batch)
    finally:
        # Clean up temp file if we downloaded
        if args.download and csv_path and str(csv_path).startswith(tempfile.gettempdir()):
            try:
                csv_path.unlink()
            except Exception:
                pass

    # Show a quick sample
    sample = conn.execute(
        "SELECT code, grouping, classification, specialization, display_name "
        "FROM taxonomy_codes ORDER BY code LIMIT 3"
    ).fetchall()
    print("\nSample entries:")
    for row in sample:
        print(" ", dict(row))

    conn.close()


if __name__ == "__main__":
    main()
