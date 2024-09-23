import sqlite3
from threading import Thread
import requests
from time import perf_counter

from typing import List, Tuple

DB_NAME = 'database.db'
TABLE_NAME = 'characters'
INSERT_PERSON_COMAND = f"INSERT INTO {TABLE_NAME} VALUES (?, ?, ?, ?);"
URL: str = 'https://www.swapi.tech/api/people/{}'
#      https://www.swapi.tech/api/people/1

with sqlite3.connect(DB_NAME) as conn:
    cursor = conn.cursor()
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME}(
            id INTEGER,
            name TEXT,
            birth_year TEXT,
            gender TEXT
        );
        """
    )
    conn.commit()


def get_info(num: int) -> Tuple[int, str, str, str]:
    response: requests.Response = requests.get(URL.format(num))
    if response.status_code != 200:
        return num, '', '', ''

    data: dict = response.json()
    return num, data['result']['properties']['name'], data['result']['properties']['birth_year'], data['result']['properties']['gender']


def fetch_and_save(num: int):
    with sqlite3.connect(DB_NAME) as conn2:
        cursor2: sqlite3.Cursor = conn2.cursor()
        cursor2.execute(INSERT_PERSON_COMAND, get_info(num))


def fetch_naive(count: int) -> None:
    for i in range(1, count + 1):
        fetch_and_save(i)


def fetch_threads(count: int) -> None:
    threads: List[Thread] = []
    for i in range(1, count + 1):
        thread: Thread = Thread(target=fetch_and_save, args=(i, ))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    COUNT: int = 20

    time: float = perf_counter()
    fetch_naive(COUNT)
    print('Naive time', perf_counter() - time)

    time: float = perf_counter()
    fetch_threads(COUNT)
    print('Threads time', perf_counter() - time)
