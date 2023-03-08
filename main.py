import configparser
import psycopg2


def get_config(config_file: str) -> configparser.ConfigParser:
    """
    Функция читает конфигурационный файл и возвращает объект ConfigParser
    :param config_file: путь до .ini файла с параметрами(пароль и т.д.):
    :return: config
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


def get_client_id(search_term: str) -> str:
    """
    Функция возвращает идентификатор клиента
    :param search_term: данные клиента по которым искать в БД - имя, фамилия, email или номер телефона
    :return: client_id - идентификатор клиента
    """
    return find_client(search_term)[0][0]


def connect_to_db(config: configparser.ConfigParser) -> psycopg2.connect:
    """
    Функция соединения с БД, возвращает объект - соединение
    :return: conn
    """
    conn = psycopg2.connect(
        host=config['postgresql']['host'],
        database=config['postgresql']['database'],
        user=config['postgresql']['user'],
        password=config['postgresql']['password']
    )
    return conn


def close_db_connection(conn: psycopg2.connect):
    """
    Функция закрывает соединение с БД
    :param conn: объект соединение который надо закрыть
    """
    conn.close()


def execute_query(query: str, params=None, fetchall=False):
    from pathlib import Path
    """
    Функция создаёт соединение, делает запрос к БД, делает коммит, закрывает соединение и возвращает результат
    :param query: sql запрос к базе данных
    :param params: параметры запроса
    :param fetchall: ...
    :return: ...
    """
    conn = connect_to_db(get_config(f'{str(Path.home())}/config.ini'))
    cursor = conn.cursor()
    cursor.execute(query, params)
    conn.commit()
    if fetchall:
        result = cursor.fetchall()
    else:
        result = None
    close_db_connection(conn)
    return result


def create_table_clients():
    """
    Функция создаёт таблицу clients
    """
    query = """
        CREATE TABLE IF NOT EXISTS clients (
            client_id SERIAL PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone TEXT
        );
    """
    execute_query(query)


def add_client(first_name: str, last_name: str, email: str, phone: str):
    """
    Функция добавляет запись о клиенте в БД если его ещё в ней нет или пишет что клиент уже есть в БД
    :param first_name: ...
    :param last_name: ...
    :param email: ...
    :param phone: ...
    """
    query = """
        INSERT INTO clients (first_name, last_name, email, phone)
        VALUES(%s, %s, %s, %s)
    """
    try:
        execute_query(query, (first_name, last_name, email, phone))
    except psycopg2.IntegrityError:
        print("Клиент с таким email уже существует.")


def add_phone(client_id: str, phone: str):
    """
    Функция добавляет номер телефона клиента
    :param client_id: ...
    :param phone: ...
    """
    query = """
        UPDATE clients SET phone=%s
        WHERE client_id=%s
    """
    execute_query(query, (phone, client_id))


def update_client(client_id: str, first_name=None, last_name=None, email=None, phone=None):
    """
    Функция изменяет/обновляет запись о клиенте
    :param client_id: ...
    :param first_name: ...
    :param last_name: ...
    :param email: ...
    :param phone: ...
    """
    query = """
        UPDATE clients SET first_name=%s, last_name=%s, email=%s, phone=%s
        WHERE client_id=%s
    """
    execute_query(query, (first_name, last_name, email, phone, client_id))


def delete_phone(client_id: str):
    """
    Функция удаляет номер телефона клиента
    :param client_id: ...
    """
    query = """
        UPDATE clients SET phone=NULL
        WHERE client_id=%s
    """
    execute_query(query, (client_id,))


def delete_client(client_id: str):
    """
    Функция удаляет клиента из БД
    :param client_id: ...
    """
    query = """
        DELETE FROM clients
        WHERE client_id=%s
    """
    execute_query(query, (client_id,))


def find_client(search_term: str) -> list:
    """
    Функция ищет клиента в БД
    :param search_term: имя, фамилия, email или телефон клиента
    :return: result - список клиентов
    """
    query = """
        SELECT * FROM clients
        WHERE first_name=%s OR last_name=%s OR email=%s OR phone=%s
    """
    result = execute_query(query, (search_term, search_term, search_term, search_term), fetchall=True)
    return result


def main():
    # создаём таблицу clients
    create_table_clients()
    # добавляем клиента
    add_client('Иванов', 'Иван', 'ivanov@yandex.ru', '')
    # получаем идентификатор клиента
    client_id = get_client_id('ivanov@yandex.ru')
    # добавляем номер телефона клиента
    add_phone(client_id, '+7 999-987-65-43')
    # удаляем номер телефона клиента
    delete_phone(client_id)
    # изменяем запись клиента
    update_client(client_id, 'Иванов', 'Иван', 'ivan.ivanov@yandex.ru', '+7 901-234-56-781')
    # удаляем клиента
    delete_client(client_id)


if __name__ == "__main__":
    main()
