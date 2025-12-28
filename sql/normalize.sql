CREATE OR REPLACE TEMP VIEW src_norm AS
SELECT
  ticker,
  business_date,
  col,
  col2,
  col4,
  json(COALESCE(nested, '{}')) AS nested_json
FROM src;

CREATE OR REPLACE TEMP VIEW tgt_norm AS
SELECT
  ticker,
  business_date,
  col,
  col2,
  col4,
  json(COALESCE(nested, '{}')) AS nested_json
FROM tgt;
