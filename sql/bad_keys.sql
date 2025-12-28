CREATE OR REPLACE TEMP VIEW bad_keys AS
SELECT
  COALESCE(a.ticker, b.ticker) AS ticker
FROM src_hash a
FULL OUTER JOIN tgt_hash b USING (ticker)
WHERE a.row_hash IS DISTINCT FROM b.row_hash;
