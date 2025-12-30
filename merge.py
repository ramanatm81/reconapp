result = {
    'col1': [1, 2],
    'col2': [3, 4],
    'col3': [5, 6],
    'col4': [7, 8]
}

rows = []

rows.extend(
    dict(zip(result.keys(), values))
    for values in zip(*result.values())
)

import pandas as pd
df = pd.DataFrame(rows)
print(df)