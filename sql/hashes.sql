CREATE OR REPLACE TEMP VIEW src_hash AS
SELECT
  ticker,
  hash(concat_ws('|', col, col2, col4, json(nested_json))) AS row_hash
FROM src_norm
WHERE business_date = ?;

CREATE OR REPLACE TEMP VIEW tgt_hash AS
SELECT
  ticker,
  hash(concat_ws('|', col, col2, col4, json(nested_json))) AS row_hash
FROM tgt_norm
WHERE business_date = ?;
