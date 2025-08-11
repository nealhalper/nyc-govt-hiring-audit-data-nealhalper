import duckdb

conn = duckdb.connect(database="C:/Users/corne/Documents/sample_ducklake/lake.duckdb")

conn.execute("INSTALL 'ducklake'")
conn.execute("LOAD 'ducklake'")

conn.execute("ATTACH 'ducklake:C:/Users/corne/Documents/sample_ducklake/catalog.duckdb' AS my_lake (DATA_PATH 'C:/Users/corne/Documents/sample_ducklake/data')")
conn.execute("USE my_lake")

conn.execute("CREATE TABLE IF NOT EXISTS canada_testkit AS SELECT * FROM 'canada_testkit_damage.csv'")

conn.close()