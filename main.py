import psycopg2


def create_db(cur):
    # Создаем таблицу "clients"
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(255) NOT NULL,
            last_name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL
        );
    """)

    # Создаем таблицу "phones"
    cur.execute("""
        CREATE TABLE IF NOT EXISTS phones (
            id SERIAL PRIMARY KEY,
            client_id INTEGER NOT NULL REFERENCES clients(id),
            phone_number VARCHAR(255) NOT NULL
        );
    """)


def add_client(cur, first_name, last_name, email, phones=None):
    cur.execute(
        "INSERT INTO clients (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
        (first_name, last_name, email)
    )
    client_id = cur.fetchone()[0]
    if phones:
        for phone in phones:
            cur.execute(
                "INSERT INTO phones (client_id, phone_number) VALUES (%s, %s)",
                (client_id, phone)
            )


def add_phone(cur, client_id, phone):
    cur.execute(
        "INSERT INTO phones (client_id, phone_number) VALUES (%s, %s)",
        (client_id, phone)
    )


def change_client(cur, client_id, first_name=None, last_name=None, email=None, phones=None):
    set_clause = []
    values = []
    if first_name is not None:
        set_clause.append("first_name = %s")
        values.append(first_name)
    if last_name is not None:
        set_clause.append("last_name = %s")
        values.append(last_name)
    if email is not None:
        set_clause.append("email = %s")
        values.append(email)
    if set_clause:
        set_clause = ", ".join(set_clause)
        values.append(client_id)
        cur.execute(f"UPDATE clients SET {set_clause} WHERE id = %s", values)

    if phones is not None:
        cur.execute("DELETE FROM phones WHERE client_id = %s", (client_id,))
        for phone in phones:
            cur.execute("INSERT INTO phones (client_id, phone_number) VALUES (%s, %s)", (client_id, phone))


def delete_phone(cur, client_id, phone):
    cur.execute("DELETE FROM phones WHERE client_id = %s AND phone_number = %s", (client_id, phone))


def delete_client(cur, client_id):
    cur.execute("DELETE FROM clients WHERE id = %s", (client_id,))


def find_client(cur, first_name=None, last_name=None, email=None, phone=None):
    conditions = []
    values = []
    if first_name is not None:
        conditions.append("first_name = %s")
        values.append(first_name)
    if last_name is not None:
        conditions.append("last_name = %s")
        values.append(last_name)
    if email is not None:
        conditions.append("email = %s")
        values.append(email)
    if phone is not None:
        conditions.append("phones.phone_number = %s")
        values.append(phone)

    if not conditions:
        cur.execute("""
            SELECT clients.id, first_name, last_name, email, array_agg(phone_number) AS phones
            FROM clients LEFT JOIN phones ON clients.id = phones.client_id
            GROUP BY clients.id
        """)
    else:
        conditions = " AND ".join(conditions)
        query = f"""
            SELECT clients.id, first_name, last_name, email, array_agg(phone_number) AS phones
            FROM clients LEFT JOIN phones ON clients.id = phones.client_id
            WHERE {conditions}
            GROUP BY clients.id
        """
        cur.execute(query, values)

    rows = cur.fetchall()
    clients = []
    for row in rows:
        client = {
            "id": row[0],
            "first_name": row[1],
            "last_name": row[2],
            "email": row[3],
            "phones": row[4] or []
        }
        clients.append(client)

    return clients


with psycopg2.connect(host='localhost', database="clients_db7", user="postgres", password="pgAdminpass") as conn:
    # Создание таблиц
    create_db(conn.cursor())
    # Добавление клиента
    add_client(conn.cursor(), "Андрей", "Петров", "abc123@yandex.ru", phones=["555-1234", "555-5678"])
    # Поиск клиента
    client = find_client(conn.cursor(), "Андрей", "Петров", "abc123@yandex.ru", "555-1234")
    # Получение client_id
    client_id = client[0]['id']
    # Добавление телефона клиента
    add_phone(conn.cursor(), client_id, "543-2112")
    # Изменение данных клиента
    change_client(conn.cursor(), client_id, phones=['123-4567'])
    # Удаление номера телефона клиента
    delete_phone(conn.cursor(), client_id, '555-1234')
    # Удаление клиента
    delete_client(conn.cursor(), client_id)

conn.close()
