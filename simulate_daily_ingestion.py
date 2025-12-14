import pandas as pd
import random
from datetime import datetime

characters = [
    "Jon Snow", "Walter White", "Tony Stark", "Arya Stark",
    "Sherlock Holmes", "Thomas Shelby", "Michael Scofield"
]

departments = ["Engineering", "Data", "Sales", "Marketing"]
sources = ["Movie", "TV Series"]

rows = []
base_id = int(datetime.now().strftime("%d%H%M"))

for i in range(200):
    rows.append([
        base_id + i,
        random.choice(characters),
        random.randint(18, 60),
        random.randint(30000, 150000),
        random.choice(departments),
        random.choice(sources)
    ])

rows.append([
    base_id + 999,
    "Prem",
    20,
    9000000,
    "Engineering",
    "Real"
])

df = pd.DataFrame(
    rows,
    columns=["id", "name", "age", "salary", "department", "source"]
)

file_name = f"data/daily_data_{datetime.now().strftime('%Y_%m_%d')}.csv"
df.to_csv(file_name, index=False)

print(f"Daily ingestion completed → {file_name}")
print(f"Rows ingested today: {len(df)}")
