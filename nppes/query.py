"""Thin query layer for the normalized NPI DB.

Intended to be imported by your apps:

    from nppes.query import NPIQuery
    q = NPIQuery("/data/nppes/nppes.db")
    prov = q.get_by_npi("1234567893")
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from .schema import get_db


def _luhn_check(npi: str) -> bool:
    """NPI check-digit validation (Luhn with 80840 prefix)."""
    if not npi or not npi.isdigit() or len(npi) != 10:
        return False
    digits = [int(d) for d in "80840" + npi[:-1]]
    # Double every second digit from right (standard Luhn on the 14-digit string)
    for i in range(len(digits) - 2, -1, -2):
        digits[i] *= 2
        if digits[i] > 9:
            digits[i] -= 9
    total = sum(digits)
    check = (10 - (total % 10)) % 10
    return check == int(npi[-1])


@dataclass
class Provider:
    npi: str
    entity_type: Optional[str] = None
    org_name: Optional[str] = None
    last_name: Optional[str] = None
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    credential: Optional[str] = None
    location_state: Optional[str] = None
    location_postal: Optional[str] = None
    last_update_date: Optional[str] = None
    deactivation_date: Optional[str] = None
    # add more as needed from SELECT


class NPIQuery:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.conn: sqlite3.Connection = get_db(self.db_path)
        # ensure schema exists but do not clobber data
        self.conn.executescript("PRAGMA foreign_keys = ON;")

    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------ single
    def get_by_npi(self, npi: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM providers WHERE npi = ?", (npi,)
        ).fetchone()
        if not row:
            return None
        prov = dict(row)

        prov["taxonomies"] = [
            dict(r) for r in self.conn.execute(
                "SELECT code, license, license_state, is_primary, taxonomy_group FROM taxonomies WHERE npi = ? ORDER BY is_primary DESC, code",
                (npi,)
            )
        ]
        prov["other_identifiers"] = [
            dict(r) for r in self.conn.execute(
                "SELECT identifier, type_code, state, issuer FROM other_identifiers WHERE npi = ?",
                (npi,)
            )
        ]
        prov["practice_locations"] = [
            dict(r) for r in self.conn.execute(
                "SELECT line1, line2, city, state, postal, country, phone, fax FROM practice_locations WHERE npi = ?",
                (npi,)
            )
        ]
        prov["endpoints"] = [
            dict(r) for r in self.conn.execute(
                "SELECT type, endpoint, use_code, use_description, content_type FROM endpoints WHERE npi = ?",
                (npi,)
            )
        ]
        return prov

    # ------------------------------------------------------------------ search
    def search(
        self,
        *,
        state: Optional[str] = None,
        postal_prefix: Optional[str] = None,
        taxonomy_code: Optional[str] = None,
        name_like: Optional[str] = None,
        active_only: bool = True,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Flexible geographic + taxonomy search.

        Example: search(state="CA", taxonomy_code="1223G0001X", limit=50)
        """
        where = []
        params: List[Any] = []

        if active_only:
            where.append("(p.deactivation_date IS NULL OR p.deactivation_date = '')")

        if state:
            where.append("p.location_state = ?")
            params.append(state.upper())

        if postal_prefix:
            where.append("p.location_postal LIKE ?")
            params.append(postal_prefix + "%")

        if name_like:
            where.append("(p.org_name LIKE ? OR p.last_name LIKE ? OR p.first_name LIKE ?)")
            like = "%" + name_like + "%"
            params.extend([like, like, like])

        # taxonomy filter requires join
        join_tax = ""
        if taxonomy_code:
            join_tax = "JOIN taxonomies t ON t.npi = p.npi"
            where.append("t.code = ?")
            params.append(taxonomy_code)

        sql = f"""
            SELECT DISTINCT p.*
            FROM providers p
            {join_tax}
            {"WHERE " + " AND ".join(where) if where else ""}
            ORDER BY p.last_update_date DESC NULLS LAST, p.npi
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])

        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def search_by_taxonomy(self, code: str, *, state: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        return self.search(taxonomy_code=code, state=state, limit=limit)

    def search_by_postal(self, postal_prefix: str, *, state: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        return self.search(postal_prefix=postal_prefix, state=state, limit=limit)

    # ------------------------------------------------------------------ validate
    def validate_npi(self, npi: str, *, check_exists: bool = True) -> bool:
        """Return True if the NPI passes Luhn and (optionally) exists in DB."""
        if not _luhn_check(npi):
            return False
        if not check_exists:
            return True
        row = self.conn.execute("SELECT 1 FROM providers WHERE npi = ?", (npi,)).fetchone()
        return row is not None

    def npi_exists(self, npi: str) -> bool:
        return self.conn.execute("SELECT 1 FROM providers WHERE npi = ?", (npi,)).fetchone() is not None

    # ------------------------------------------------------------------ stats
    def stats(self) -> Dict[str, int]:
        out = {}
        for tbl in ("providers", "taxonomies", "other_identifiers", "practice_locations", "endpoints", "other_names"):
            out[tbl] = self.conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        out["active_providers"] = self.conn.execute(
            "SELECT COUNT(*) FROM providers WHERE deactivation_date IS NULL OR deactivation_date = ''"
        ).fetchone()[0]
        return out


# Convenience
def open_query(db_path: str | Path) -> NPIQuery:
    return NPIQuery(db_path)
