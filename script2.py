import json
import re
import psycopg2
with open('task1_d.json', 'r') as f:
    raw = f.read()
fixed_data = re.sub(r':([a-zA-Z_]+)=>', r'"\1":', raw)
data = json.loads(fixed_data)
conn = psycopg2.connect(
    dbname = 'postgres',
    user = 'postgres',
    password = 'postgres',
    host = 'localhost'
)
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS books;")
cur.execute("DROP TABLE IF EXISTS summary_table;")

cur.execute("""
CREATE TABLE books(
    id NUMERIC(25, 0) PRIMARY KEY,
    title TEXT,
    author TEXT, 
    genre TEXT, 
    publisher TEXT,
    year INT,
    price TEXT)
""")
for b in data:
    cur.execute("INSERT INTO books VALUES(%s, %s, %s, %s, %s, %s, %s)",
                (b['id'], b['title'], b['author'], b['genre'], b['publisher'], b['year'], b['price']))
sql = """
CREATE TABLE summary_table AS SELECT year as publication_year, 
COUNT(*) AS book_count,
ROUND(AVG(CASE
              WHEN price LIKE '€%' THEN CAST(REPLACE(price, '€', '') AS NUMERIC)*1.2
              WHEN price LIKE '$%' THEN CAST(REPLACE(price, '$', '') AS NUMERIC)
                ELSE null
    END), 2) AS avg_price
    FROM books 
    GROUP BY year 
    ORDER BY year;"""
cur.execute(sql)
conn.commit()
cur.execute("SELECT COUNT(*) FROM books")
print("books:", cur.fetchone()[0])
cur.execute("SELECT COUNT(*) FROM summary_table")
print("summary:", cur.fetchone()[0])
cur.execute("SELECT * FROM summary_table")
for row in cur.fetchall():
    print(row)
