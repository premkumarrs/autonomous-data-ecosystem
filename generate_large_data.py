import pandas as pd
import random

characters = [
    "Jon Snow", "Daenerys Targaryen", "Tyrion Lannister", "Arya Stark",
    "Walter White", "Jesse Pinkman", "Saul Goodman",
    "Tony Stark", "Bruce Wayne", "Peter Parker",
    "Harry Potter", "Hermione Granger", "Ron Weasley",
    "Geralt of Rivia", "Yennefer", "Ciri",
    "Naruto Uzumaki", "Sasuke Uchiha", "Kakashi Hatake",
    "Michael Scofield", "Sherlock Holmes", "John Watson",
    "Thomas Shelby", "Arthur Shelby"
]

departments = ["Engineering", "Marketing", "Sales", "Finance", "Operations", "Data"]
sources = ["Movie", "TV Series"]

rows = []

for i in range(1, 1001):
    name = random.choice(characters)
    age = random.randint(18, 60)
    salary = random.randint(30000, 150000)
    department = random.choice(departments)
    source = random.choice(sources)

    rows.append([i, name, age, salary, department, source])

rows.append([1001, "Prem", 20, 9000000, "Engineering", "Real"])

df = pd.DataFrame(
    rows,
    columns=["id", "name", "age", "salary", "department", "source"]
)

df.to_csv("data/large_sample_data.csv", index=False)

print("Large dataset generated successfully!")
print("Rows:", len(df))
print("Saved to: data/large_sample_data.csv")
