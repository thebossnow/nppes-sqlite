#!/usr/bin/env python3
"""Minimal example of using the query layer after ingest."""

from nppes.query import NPIQuery

DB = "nppes.db"   # or /data/nppes/nppes.db

with NPIQuery(DB) as q:
    print("DB stats:", q.stats())

    # 1. Direct lookup
    npi = "1234567895"
    rec = q.get_by_npi(npi)
    if rec:
        print(f"\nNPI {npi}:")
        print("  Name:", rec.get("first_name"), rec.get("last_name"))
        print("  Location:", rec.get("location_city"), rec.get("location_state"))
        print("  Taxonomies:", [t["code"] for t in rec.get("taxonomies", [])])
        print("  Endpoints:", len(rec.get("endpoints", [])))

    # 2. Search for providers
    dentists = q.search_by_taxonomy("1223G0001X", state="MA", limit=5)
    print(f"\nFound {len(dentists)} dentists in MA with code 1223G0001X (sample)")

    # 3. NPI check (Luhn + DB)
    print("Valid format + present?", q.validate_npi("1234567893"))
    print("Valid format + present?", q.validate_npi("9999999999"))
