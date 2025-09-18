import marimo

__generated_with = "0.15.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import clickhouse_connect

    password = ""
    engine = clickhouse_connect.get_client(
        host="",
        user="",
        secure=True,
        port=8443,
        password=password,
    )
    return (engine,)


@app.cell
def _(engine):
    tables = []
    result = engine.query("SHOW TABLES IN sentinelone")
    with result.rows_stream as s:
        for table in s.gen:
            tables.append(table[0])
    return (tables,)


@app.cell
def _(engine, tables):
    for tbl in tables[0:2]:
        query = f"SELECT * FROM sentinelone.{tbl} FORMAT CSVWithNames"
        stream = engine.raw_stream(query=query)  # fmt already in the SQL
        with open(f"sampledata/{tbl}.csv", "wb") as f:
            for chunk in stream:
                f.write(chunk)
    return


if __name__ == "__main__":
    app.run()
