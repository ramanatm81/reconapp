# recon/service.py
from pathlib import Path

SQL_DIR = Path(__file__).parent / "sql"


def run_sql(con, filename, params=None):
    sql = (SQL_DIR / filename).read_text()
    if params:
        con.execute(sql, params)
    else:
        con.execute(sql)


class ReconService:
    def __init__(self, con):
        self.con = con

    def bootstrap(self):
        run_sql(self.con, "normalize.sql")

    def build_hashes(self, business_date):
        run_sql(self.con, "hashes.sql", [business_date, business_date])

    def find_bad_keys(self):
        run_sql(self.con, "bad_keys.sql")

    def fetch_missing_counts(self, business_date):
        sql = """
        SELECT
          (SELECT COUNT(*) FROM (
             SELECT ticker FROM src_norm WHERE business_date = ?
             EXCEPT
             SELECT ticker FROM tgt_norm WHERE business_date = ?
          )) AS missing_in_target,
          (SELECT COUNT(*) FROM (
             SELECT ticker FROM tgt_norm WHERE business_date = ?
             EXCEPT
             SELECT ticker FROM src_norm WHERE business_date = ?
          )) AS missing_in_source
        """
        return self.con.execute(
            sql, [business_date, business_date, business_date, business_date]
        ).fetchone()

    def fetch_summary(self, business_date):
        sql = """
        SELECT
          COUNT(*) AS total_rows,
          COUNT(bk.ticker) AS mismatched_rows
        FROM src_norm s
        LEFT JOIN bad_keys bk USING (ticker)
        WHERE s.business_date = ?
        """
        return self.con.execute(sql, [business_date]).fetchone()

    def fetch_bad_keys_page(self, limit=50, offset=0):
        sql = """
        SELECT ticker
        FROM bad_keys
        ORDER BY ticker
        LIMIT ? OFFSET ?
        """
        return self.con.execute(sql, [limit, offset]).fetchall()
