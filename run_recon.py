# run_recon.py
from recon.db import duckdb_session
from recon.service import ReconService

BUSINESS_DATE = "2025-11-14"

def main():
    with duckdb_session() as con:
        recon = ReconService(con)

        recon.bootstrap()
        recon.build_hashes(BUSINESS_DATE)
        recon.find_bad_keys()

        missing = recon.fetch_missing_counts(BUSINESS_DATE)
        summary = recon.fetch_summary(BUSINESS_DATE)
        bad_keys = recon.fetch_bad_keys_page()

        print("Missing:", missing)
        print("Summary:", summary)
        print("Bad keys:", bad_keys[:5])

if __name__ == "__main__":
    main()
