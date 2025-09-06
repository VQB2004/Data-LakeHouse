import trino

# Tạo connection đến Trino
def get_trino_connection():
    conn = trino.dbapi.connect(
        host="localhost",   # hoặc IP/hostname của container Trino
        port=8080,
        user="admin",
        catalog="iceberg",  # catalog Iceberg
    )

    return conn


def trino_query(sql):

    conn = get_trino_connection()
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


