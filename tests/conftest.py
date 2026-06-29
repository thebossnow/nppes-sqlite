import pytest
import tempfile
import csv
from pathlib import Path
from nppes.schema import init_db, get_db


@pytest.fixture
def empty_db(tmp_path):
    db = tmp_path / "test.db"
    conn = get_db(db)
    init_db(conn)
    conn.close()
    return db


def _write_csv(path: Path, header: list[str], rows: list[list[str]]):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


@pytest.fixture
def sample_main_csv(tmp_path):
    """Tiny but realistic main CSV (2 rows, with repeating groups)."""
    p = tmp_path / "npidata_sample.csv"
    header = [
        "NPI", "Entity Type Code", "Employer Identification Number (EIN)",
        "Provider Organization Name (Legal Business Name)", "Provider Last Name (Legal Name)",
        "Provider First Name", "Provider Credential Text", "Provider Sex Code",
        "Provider First Line Business Mailing Address", "Provider Business Mailing Address City Name",
        "Provider Business Mailing Address State Name", "Provider Business Mailing Address Postal Code",
        "Provider First Line Business Practice Location Address", "Provider Business Practice Location Address City Name",
        "Provider Business Practice Location Address State Name", "Provider Business Practice Location Address Postal Code",
        "Provider Enumeration Date", "Last Update Date",
        "Healthcare Provider Taxonomy Code_1", "Provider License Number_1", "Provider License Number State Code_1",
        "Healthcare Provider Primary Taxonomy Switch_1",
        "Healthcare Provider Taxonomy Code_2", "Provider License Number_2", "Provider License Number State Code_2",
        "Healthcare Provider Primary Taxonomy Switch_2",
        "Other Provider Identifier_1", "Other Provider Identifier Type Code_1", "Other Provider Identifier State_1", "Other Provider Identifier Issuer_1",
        "Is Sole Proprietor"
    ]
    rows = [
        # Type 1 dentist
        ["1234567895", "1", "", "", "Doe", "Jane", "DDS", "F",
         "123 Main St", "Boston", "MA", "02101",
         "456 Dental Way", "Boston", "MA", "02101",
         "01/15/2020", "06/01/2026",
         "1223G0001X", "12345", "MA", "Y",
         "1223S0112X", "67890", "MA", "N",
         "1234567", "01", "MA", "MA Dental Board",
         "N"],
        # Type 2 org
        ["9876543219", "2", "123456789", "Acme Health LLC", "", "", "", "",
         "100 Corporate Dr", "Cambridge", "MA", "02139",
         "200 Clinic Ave", "Cambridge", "MA", "02139",
         "03/01/2018", "05/15/2026",
         "261QF0400X", "", "", "Y",
         "", "", "", "",
         "", "", "", "",
         "N"],
    ]
    _write_csv(p, header, rows)
    return p


@pytest.fixture
def sample_pl_csv(tmp_path):
    p = tmp_path / "pl_sample.csv"
    header = [
        "NPI",
        "Provider Secondary Practice Location Address- Address Line 1",
        "Provider Secondary Practice Location Address - City Name",
        "Provider Secondary Practice Location Address - State Name",
        "Provider Secondary Practice Location Address - Postal Code",
    ]
    rows = [
        ["1234567895", "789 Side St", "Brookline", "MA", "02445"],
    ]
    _write_csv(p, header, rows)
    return p


@pytest.fixture
def sample_endpoint_csv(tmp_path):
    p = tmp_path / "endpoint_sample.csv"
    header = ["NPI", "Endpoint Type", "Endpoint", "Use Code", "Content Type"]
    rows = [
        ["1234567895", "DIRECT", "jane.doe@direct.example.org", "R", "XHTML"],
    ]
    _write_csv(p, header, rows)
    return p


@pytest.fixture
def sample_othername_csv(tmp_path):
    p = tmp_path / "othername_sample.csv"
    header = ["NPI", "Provider Other Organization Name", "Provider Other Organization Name Type Code"]
    rows = [
        ["9876543219", "Acme Urgent Care", "3"],
    ]
    _write_csv(p, header, rows)
    return p
