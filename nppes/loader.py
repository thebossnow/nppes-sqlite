"""Streaming normalized loader for CMS NPPES NPI CSVs.

Usage (weekly delta or full):
    python -m nppes.loader \\
        --db /data/nppes/nppes.db \\
        --main /path/to/npidata_pfile_....csv \\
        --pl /path/to/pl_pfile_....csv \\
        --endpoint /path/to/endpoint_pfile_....csv \\
        --othername /path/to/othername_pfile_....csv

The loader is designed for huge files: streams row-by-row, never loads full CSV
into memory. Batches inserts in transactions.
"""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from .schema import get_db, init_db

# ---------------------------------------------------------------------------
# Column mapping (main npidata file)
# We map the important scalar fields. Everything else is either dropped
# (rarely used) or extracted into child tables (tax/identifiers).
# ---------------------------------------------------------------------------

MAIN_FIELD_MAP: Dict[str, str] = {
    "NPI": "npi",
    "Entity Type Code": "entity_type",
    "Replacement NPI": "replacement_npi",
    "Employer Identification Number (EIN)": "ein",
    "Provider Organization Name (Legal Business Name)": "org_name",
    "Provider Last Name (Legal Name)": "last_name",
    "Provider First Name": "first_name",
    "Provider Middle Name": "middle_name",
    "Provider Name Prefix Text": "name_prefix",
    "Provider Name Suffix Text": "name_suffix",
    "Provider Credential Text": "credential",
    "Provider Sex Code": "sex",
    "Is Sole Proprietor": "sole_proprietor",
    "Is Organization Subpart": "is_organization_subpart",
    "Parent Organization LBN": "parent_org_lbn",
    "Parent Organization TIN": "parent_org_tin",
    "Provider Enumeration Date": "enumeration_date",
    "Last Update Date": "last_update_date",
    "NPI Deactivation Reason Code": "deactivation_reason",
    "NPI Deactivation Date": "deactivation_date",
    "NPI Reactivation Date": "reactivation_date",
    # Mailing
    "Provider First Line Business Mailing Address": "mailing_line1",
    "Provider Second Line Business Mailing Address": "mailing_line2",
    "Provider Business Mailing Address City Name": "mailing_city",
    "Provider Business Mailing Address State Name": "mailing_state",
    "Provider Business Mailing Address Postal Code": "mailing_postal",
    "Provider Business Mailing Address Country Code (If outside U.S.)": "mailing_country",
    "Provider Business Mailing Address Telephone Number": "mailing_phone",
    "Provider Business Mailing Address Fax Number": "mailing_fax",
    # Primary practice location
    "Provider First Line Business Practice Location Address": "location_line1",
    "Provider Second Line Business Practice Location Address": "location_line2",
    "Provider Business Practice Location Address City Name": "location_city",
    "Provider Business Practice Location Address State Name": "location_state",
    "Provider Business Practice Location Address Postal Code": "location_postal",
    "Provider Business Practice Location Address Country Code (If outside U.S.)": "location_country",
    "Provider Business Practice Location Address Telephone Number": "location_phone",
    "Provider Business Practice Location Address Fax Number": "location_fax",
    # Authorized official
    "Authorized Official Last Name": "auth_official_last_name",
    "Authorized Official First Name": "auth_official_first_name",
    "Authorized Official Middle Name": "auth_official_middle_name",
    "Authorized Official Title or Position": "auth_official_title",
    "Authorized Official Telephone Number": "auth_official_phone",
    "Authorized Official Name Prefix Text": "auth_official_prefix",
    "Authorized Official Name Suffix Text": "auth_official_suffix",
    "Authorized Official Credential Text": "auth_official_credential",
    # Misc
    "Certification Date": "certification_date",
}

# Repeating group detectors (suffix _N)
TAXONOMY_BASES = [
    "Healthcare Provider Taxonomy Code",
    "Provider License Number",
    "Provider License Number State Code",
    "Healthcare Provider Primary Taxonomy Switch",
    "Healthcare Provider Taxonomy Group",
]

IDENTIFIER_BASES = [
    "Other Provider Identifier",
    "Other Provider Identifier Type Code",
    "Other Provider Identifier State",
    "Other Provider Identifier Issuer",
]

# For practice locations (pl_pfile) - column names use dashes and slight variations
PL_MAP = {
    "NPI": "npi",
    "Provider Secondary Practice Location Address- Address Line 1": "line1",
    "Provider Secondary Practice Location Address-  Address Line 2": "line2",
    "Provider Secondary Practice Location Address - City Name": "city",
    "Provider Secondary Practice Location Address - State Name": "state",
    "Provider Secondary Practice Location Address - Postal Code": "postal",
    "Provider Secondary Practice Location Address - Country Code (If outside U.S.)": "country",
    "Provider Secondary Practice Location Address - Telephone Number": "phone",
    "Provider Secondary Practice Location Address - Telephone Extension": "phone_ext",
    "Provider Practice Location Address - Fax Number": "fax",
}

ENDPOINT_MAP = {
    "NPI": "npi",
    "Endpoint Type": "type",
    "Endpoint Type Description": "type_description",
    "Endpoint": "endpoint",
    "Affiliation": "affiliation",
    "Endpoint Description": "description",  # we will alias in insert
    "Affiliation Legal Business Name": "affiliation_lbn",
    "Use Code": "use_code",
    "Use Description": "use_description",
    "Content Type": "content_type",
    "Content Description": "content_description",
    "Affiliation Address Line One": "affiliation_line1",
    "Affiliation Address Line Two": "affiliation_line2",
    "Affiliation Address City": "affiliation_city",
    "Affiliation Address State": "affiliation_state",
    "Affiliation Address Country": "affiliation_country",
    "Affiliation Address Postal Code": "affiliation_postal",
}

OTHERNAME_MAP = {
    "NPI": "npi",
    "Provider Other Organization Name": "name",
    "Provider Other Organization Name Type Code": "type_code",
    "Created Date": "created_date",
}


def normalize_date(d: Optional[str]) -> Optional[str]:
    """Convert MM/DD/YYYY or other common formats to YYYY-MM-DD."""
    if not d:
        return None
    d = d.strip()
    if not d:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(d, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return d  # leave as-is if unknown


def _derive_status(row: Dict[str, Any]) -> str:
    if row.get("deactivation_date"):
        return "deactivated"
    return "active"


def _get_idx(header: List[str], name: str) -> Optional[int]:
    try:
        return header.index(name)
    except ValueError:
        return None


def _build_repeater_index(header: List[str], bases: List[str]) -> Dict[str, Dict[int, int]]:
    """Return {base_name: {suffix_num: col_index}} for _1, _2, ... groups."""
    pat = re.compile(r"^(.*)_(\d+)$")
    groups: Dict[str, Dict[int, int]] = {b: {} for b in bases}
    for i, h in enumerate(header):
        m = pat.match(h)
        if not m:
            continue
        base, num_s = m.group(1), m.group(2)
        if base in groups:
            try:
                num = int(num_s)
                groups[base][num] = i
            except ValueError:
                pass
    return groups


def _extract_taxonomies(row: List[str], header: List[str]) -> List[Dict[str, Any]]:
    """Extract all non-empty taxonomy rows for this NPI."""
    groups = _build_repeater_index(header, TAXONOMY_BASES)
    max_n = max((max(g.keys()) if g else 0) for g in groups.values()) or 15
    out: List[Dict[str, Any]] = []
    for n in range(1, max_n + 1):
        code_idx = groups.get("Healthcare Provider Taxonomy Code", {}).get(n)
        if code_idx is None:
            continue
        code = row[code_idx].strip() if code_idx < len(row) else ""
        if not code:
            continue
        lic_idx = groups.get("Provider License Number", {}).get(n)
        lic_state_idx = groups.get("Provider License Number State Code", {}).get(n)
        prim_idx = groups.get("Healthcare Provider Primary Taxonomy Switch", {}).get(n)
        grp_idx = groups.get("Healthcare Provider Taxonomy Group", {}).get(n)

        out.append({
            "code": code,
            "license": row[lic_idx].strip() if lic_idx is not None and lic_idx < len(row) else None,
            "license_state": row[lic_state_idx].strip() if lic_state_idx is not None and lic_state_idx < len(row) else None,
            "is_primary": row[prim_idx].strip() if prim_idx is not None and prim_idx < len(row) else None,
            "taxonomy_group": row[grp_idx].strip() if grp_idx is not None and grp_idx < len(row) else None,
        })
    return out


def _extract_identifiers(row: List[str], header: List[str]) -> List[Dict[str, Any]]:
    groups = _build_repeater_index(header, IDENTIFIER_BASES)
    max_n = max((max(g.keys()) if g else 0) for g in groups.values()) or 50
    out: List[Dict[str, Any]] = []
    for n in range(1, max_n + 1):
        ident_idx = groups.get("Other Provider Identifier", {}).get(n)
        if ident_idx is None:
            continue
        ident = row[ident_idx].strip() if ident_idx < len(row) else ""
        if not ident:
            continue
        t_idx = groups.get("Other Provider Identifier Type Code", {}).get(n)
        s_idx = groups.get("Other Provider Identifier State", {}).get(n)
        i_idx = groups.get("Other Provider Identifier Issuer", {}).get(n)

        out.append({
            "identifier": ident,
            "type_code": row[t_idx].strip() if t_idx is not None and t_idx < len(row) else None,
            "state": row[s_idx].strip() if s_idx is not None and s_idx < len(row) else None,
            "issuer": row[i_idx].strip() if i_idx is not None and i_idx < len(row) else None,
        })
    return out


def iter_main_rows(path: Path) -> Iterator[Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]]:
    """Yield (provider_dict, tax_list, ident_list) for every row in the main file."""
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)
        idx_cache: Dict[str, Optional[int]] = {k: _get_idx(header, k) for k in MAIN_FIELD_MAP}

        tax_bases = _build_repeater_index(header, TAXONOMY_BASES)
        ident_bases = _build_repeater_index(header, IDENTIFIER_BASES)

        for row in reader:
            if not row or not row[0].strip():
                continue
            prov: Dict[str, Any] = {}
            for csv_name, db_name in MAIN_FIELD_MAP.items():
                i = idx_cache.get(csv_name)
                val = row[i].strip() if (i is not None and i < len(row)) else ""
                if db_name.endswith(("_date", "enumeration_date", "last_update_date")):
                    val = normalize_date(val) or ""
                prov[db_name] = val or None

            # Derived
            prov["status"] = _derive_status(prov)

            # NPI is required
            npi = prov.get("npi")
            if not npi:
                continue

            taxes = _extract_taxonomies(row, header)
            idents = _extract_identifiers(row, header)

            yield prov, taxes, idents


def iter_pl_rows(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)
        idx = {k: _get_idx(header, k) for k in PL_MAP}
        for row in reader:
            if not row or not row[0].strip():
                continue
            rec = {}
            for csv_name, db_name in PL_MAP.items():
                i = idx.get(csv_name)
                rec[db_name] = row[i].strip() if (i is not None and i < len(row)) else None
            if rec.get("npi"):
                yield rec


def iter_endpoint_rows(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)
        idx = {k: _get_idx(header, k) for k in ENDPOINT_MAP}
        for row in reader:
            if not row or not row[0].strip():
                continue
            rec: Dict[str, Any] = {}
            for csv_name, db_name in ENDPOINT_MAP.items():
                i = idx.get(csv_name)
                val = row[i].strip() if (i is not None and i < len(row)) else None
                rec[db_name] = val
            # normalize some names used in insert
            rec["description"] = rec.pop("description", None) or rec.get("description")
            if rec.get("npi"):
                yield rec


def iter_othername_rows(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)
        idx = {k: _get_idx(header, k) for k in OTHERNAME_MAP}
        for row in reader:
            if not row or not row[0].strip():
                continue
            rec = {}
            for csv_name, db_name in OTHERNAME_MAP.items():
                i = idx.get(csv_name)
                rec[db_name] = row[i].strip() if (i is not None and i < len(row)) else None
            if rec.get("npi"):
                yield rec


# ---------------------------------------------------------------------------
# DB writes (batched, upsert)
# ---------------------------------------------------------------------------

BATCH_SIZE = 10000


def _upsert_providers(conn: sqlite3.Connection, providers: List[Dict[str, Any]]) -> int:
    if not providers:
        return 0
    cols = list(providers[0].keys())
    # Always include db_loaded_at
    if "db_loaded_at" not in cols:
        cols.append("db_loaded_at")
        for p in providers:
            p["db_loaded_at"] = datetime.now().astimezone().replace(microsecond=0).isoformat()

    placeholders = ", ".join("?" for _ in cols)
    col_list = ", ".join(cols)
    update_list = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "npi")

    sql = f"""
        INSERT INTO providers ({col_list})
        VALUES ({placeholders})
        ON CONFLICT(npi) DO UPDATE SET {update_list}
    """
    # Order values
    data = [[p.get(c) for c in cols] for p in providers]
    conn.executemany(sql, data)
    return len(providers)


def _insert_taxonomies(conn: sqlite3.Connection, rows: List[Tuple[str, Dict[str, Any]]]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT OR IGNORE INTO taxonomies (npi, code, license, license_state, is_primary, taxonomy_group)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    data = [(npi, t["code"], t.get("license"), t.get("license_state"),
             t.get("is_primary"), t.get("taxonomy_group")) for npi, t in rows]
    conn.executemany(sql, data)
    return len(data)


def _insert_identifiers(conn: sqlite3.Connection, rows: List[Tuple[str, Dict[str, Any]]]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT OR IGNORE INTO other_identifiers (npi, identifier, type_code, state, issuer)
        VALUES (?, ?, ?, ?, ?)
    """
    data = [(npi, i["identifier"], i.get("type_code"), i.get("state"), i.get("issuer"))
            for npi, i in rows]
    conn.executemany(sql, data)
    return len(data)


def _insert_practice_locations(conn: sqlite3.Connection, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT OR IGNORE INTO practice_locations
        (npi, line1, line2, city, state, postal, country, phone, phone_ext, fax)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    data = [(r["npi"], r.get("line1"), r.get("line2"), r.get("city"), r.get("state"),
             r.get("postal"), r.get("country"), r.get("phone"), r.get("phone_ext"), r.get("fax"))
            for r in rows]
    conn.executemany(sql, data)
    return len(data)


def _insert_endpoints(conn: sqlite3.Connection, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT OR IGNORE INTO endpoints
        (npi, type, type_description, endpoint, affiliation, use_code, use_description,
         content_type, content_description, affiliation_lbn,
         affiliation_line1, affiliation_line2, affiliation_city, affiliation_state,
         affiliation_postal, affiliation_country)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    data = [(r["npi"], r.get("type"), r.get("type_description"), r.get("endpoint"),
             r.get("affiliation"), r.get("use_code"), r.get("use_description"),
             r.get("content_type"), r.get("content_description"), r.get("affiliation_lbn"),
             r.get("affiliation_line1"), r.get("affiliation_line2"), r.get("affiliation_city"),
             r.get("affiliation_state"), r.get("affiliation_postal"), r.get("affiliation_country"))
            for r in rows]
    conn.executemany(sql, data)
    return len(data)


def _insert_other_names(conn: sqlite3.Connection, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT OR IGNORE INTO other_names (npi, name, type_code, created_date)
        VALUES (?, ?, ?, ?)
    """
    data = [(r["npi"], r.get("name"), r.get("type_code"), r.get("created_date")) for r in rows]
    conn.executemany(sql, data)
    return len(data)


def load_main(conn: sqlite3.Connection, path: Path, batch_size: int = BATCH_SIZE) -> Tuple[int, int, int]:
    """Load/upsert the main provider file + its embedded tax + idents."""
    print(f"[main] streaming {path}")
    total_p = total_t = total_i = 0
    batch_p: List[Dict[str, Any]] = []
    batch_t: List[Tuple[str, Dict[str, Any]]] = []
    batch_i: List[Tuple[str, Dict[str, Any]]] = []

    for prov, taxes, idents in iter_main_rows(path):
        batch_p.append(prov)
        npi = prov["npi"]
        for t in taxes:
            batch_t.append((npi, t))
        for ident in idents:
            batch_i.append((npi, ident))

        if len(batch_p) >= batch_size:
            conn.execute("BEGIN")
            total_p += _upsert_providers(conn, batch_p)
            total_t += _insert_taxonomies(conn, batch_t)
            total_i += _insert_identifiers(conn, batch_i)
            conn.commit()
            print(f"  ... {total_p:,} providers, {total_t:,} taxonomies, {total_i:,} identifiers")
            batch_p.clear()
            batch_t.clear()
            batch_i.clear()

    if batch_p:
        conn.execute("BEGIN")
        total_p += _upsert_providers(conn, batch_p)
        total_t += _insert_taxonomies(conn, batch_t)
        total_i += _insert_identifiers(conn, batch_i)
        conn.commit()

    print(f"[main] done: {total_p:,} providers | {total_t:,} tax | {total_i:,} idents")
    return total_p, total_t, total_i


def load_reference(conn: sqlite3.Connection, path: Path, kind: str, batch_size: int = BATCH_SIZE) -> int:
    print(f"[{kind}] streaming {path}")
    total = 0
    batch: List[Dict[str, Any]] = []
    it = {
        "pl": iter_pl_rows,
        "endpoint": iter_endpoint_rows,
        "othername": iter_othername_rows,
    }[kind](path)

    inserter = {
        "pl": _insert_practice_locations,
        "endpoint": _insert_endpoints,
        "othername": _insert_other_names,
    }[kind]

    for rec in it:
        batch.append(rec)
        if len(batch) >= batch_size:
            conn.execute("BEGIN")
            total += inserter(conn, batch)
            conn.commit()
            batch.clear()
            if total % (batch_size * 4) == 0:
                print(f"  ... {total:,} {kind} rows")

    if batch:
        conn.execute("BEGIN")
        total += inserter(conn, batch)
        conn.commit()

    print(f"[{kind}] done: {total:,} rows")
    return total


def load_all(
    db_path: Path,
    main_csv: Optional[Path] = None,
    pl_csv: Optional[Path] = None,
    endpoint_csv: Optional[Path] = None,
    othername_csv: Optional[Path] = None,
    batch_size: int = BATCH_SIZE,
) -> None:
    conn = get_db(db_path)
    init_db(conn)

    if main_csv:
        load_main(conn, main_csv, batch_size=batch_size)
    if pl_csv:
        load_reference(conn, pl_csv, "pl", batch_size)
    if endpoint_csv:
        load_reference(conn, endpoint_csv, "endpoint", batch_size)
    if othername_csv:
        load_reference(conn, othername_csv, "othername", batch_size)

    # Vacuum is expensive on huge DBs; comment or run manually when desired
    # conn.execute("VACUUM")
    conn.close()
    print(f"\n✓ Load complete. DB at {db_path}")


def main(argv: Optional[List[str]] = None) -> None:
    p = argparse.ArgumentParser(
        prog="nppes-loader",
        description="Streaming normalized loader for CMS NPPES data into SQLite"
    )
    p.add_argument("--db", required=True, help="Path to target SQLite DB (created if needed)")
    p.add_argument("--main", help="Path to npidata_pfile_*.csv (main wide file)")
    p.add_argument("--pl", help="Path to pl_pfile_*.csv (practice locations)")
    p.add_argument("--endpoint", help="Path to endpoint_pfile_*.csv")
    p.add_argument("--othername", help="Path to othername_pfile_*.csv")
    p.add_argument("--batch", type=int, default=BATCH_SIZE, help="Rows per transaction (5k-50k recommended)")
    p.add_argument("--delta", action="store_true", help="Hint that this is an incremental load (for logs only)")
    args = p.parse_args(argv)

    dbp = Path(args.db).expanduser().resolve()
    mainp = Path(args.main).expanduser().resolve() if args.main else None
    plp = Path(args.pl).expanduser().resolve() if args.pl else None
    endp = Path(args.endpoint).expanduser().resolve() if args.endpoint else None
    onp = Path(args.othername).expanduser().resolve() if args.othername else None

    if not any([mainp, plp, endp, onp]):
        p.error("Provide at least one of --main / --pl / --endpoint / --othername")

    load_all(
        db_path=dbp,
        main_csv=mainp,
        pl_csv=plp,
        endpoint_csv=endp,
        othername_csv=onp,
        batch_size=args.batch,
    )


if __name__ == "__main__":
    main()
