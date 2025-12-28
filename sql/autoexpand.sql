cols = con.execute("""
SELECT name
FROM pragma_table_info('src')
WHERE name NOT IN ('ticker', 'business_date')
ORDER BY cid
""").fetchall()

exprs = []
for (c,) in cols:
    if c == "nested":
        exprs.append(f"json(COALESCE({c}, '{{}}'::JSON))")
    else:
        exprs.append(c)

concat_expr = ", ".join(exprs)

sql = f"""
SELECT
  ticker,
  hash(concat_ws('|', {concat_expr})) AS row_hash
FROM src
WHERE business_date = ?
"""
