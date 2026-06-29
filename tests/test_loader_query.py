import pytest
from pathlib import Path
from nppes.loader import load_all
from nppes.query import NPIQuery, _luhn_check
from nppes.schema import get_db


def test_load_and_query_basic(empty_db, sample_main_csv, sample_pl_csv, sample_endpoint_csv, sample_othername_csv, sample_taxonomy_csv):
    load_all(
        db_path=empty_db,
        main_csv=sample_main_csv,
        pl_csv=sample_pl_csv,
        endpoint_csv=sample_endpoint_csv,
        othername_csv=sample_othername_csv,
        batch_size=100,
    )

    # Load taxonomy reference separately
    from nppes.taxonomy import load_taxonomy_codes
    conn = get_db(empty_db)
    load_taxonomy_codes(conn, sample_taxonomy_csv)
    conn.close()

    q = NPIQuery(empty_db)

    # Stats
    st = q.stats()
    assert st["providers"] == 2
    assert st["taxonomies"] >= 2
    assert st["practice_locations"] == 1
    assert st["endpoints"] == 1
    assert st["other_names"] == 1
    assert st["taxonomy_codes"] == 4

    # Get full record
    jane = q.get_by_npi("1234567895")
    assert jane is not None
    assert jane["last_name"] == "Doe"
    assert jane["location_state"] == "MA"
    assert len(jane["taxonomies"]) == 2
    assert jane["taxonomies"][0]["is_primary"] == "Y"
    assert len(jane["practice_locations"]) == 1
    assert len(jane["endpoints"]) == 1

    # Taxonomy reference lookup + enrichment
    tax = q.get_taxonomy("1223G0001X")
    assert tax is not None
    assert tax.get("classification") == "Dentist"
    assert tax.get("specialization") == "General Practice"

    enriched = q.enrich_with_taxonomy_details(jane)
    assert "taxonomy_details" in enriched
    assert len(enriched["taxonomy_details"]) >= 1  # only codes present in the small fixture are enriched

    # Search
    ma_dentists = q.search(state="MA", taxonomy_code="1223G0001X")
    assert len(ma_dentists) == 1
    assert ma_dentists[0]["npi"] == "1234567895"

    # Luhn + existence
    assert q.validate_npi("1234567895") is True
    assert q.validate_npi("1234567890") is False  # bad check digit or not present
    assert _luhn_check("1234567895") is True

    q.close()


def test_luhn():
    # Known good NPI format check (real check digit example)
    assert _luhn_check("1234567895")
    assert not _luhn_check("1234567890")
    assert not _luhn_check("12345")


def test_taxonomy_reference_load(empty_db, sample_taxonomy_csv):
    from nppes.taxonomy import load_taxonomy_codes
    conn = get_db(empty_db)
    n = load_taxonomy_codes(conn, sample_taxonomy_csv)
    conn.close()

    q = NPIQuery(empty_db)
    assert q.stats()["taxonomy_codes"] == 4

    details = q.get_taxonomy("1223G0001X")
    assert details["classification"] == "Dentist"
    assert "General Practice" in (details.get("specialization") or "")

    # ON CONFLICT upsert test: load again (reopen conn)
    conn2 = get_db(empty_db)
    n2 = load_taxonomy_codes(conn2, sample_taxonomy_csv)
    conn2.close()
    assert n2 == 4
    q.close()
