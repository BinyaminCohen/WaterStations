import logging
import sqlite3
import socket
import datetime
from concurrent.futures import ThreadPoolExecutor

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def create_db_and_table():
    with sqlite3.connect('data.SQLite') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS station_status (
                station_id INTEGER,
                last_date TEXT,
                alarm1 INTEGER,
                alarm2 INTEGER,
                PRIMARY KEY(station_id)
            );
        ''')


def validate_client_data(station_id, alarm1, alarm2):
    try:
        _, alarm1, alarm2 = int(station_id), int(alarm1), int(alarm2)
        if alarm1 in (0, 1) or alarm2 in (0, 1):
            return True
    except ValueError:
        return False


def handle_client(connection, address):
    with connection:
        try:
            data_station = connection.recv(1024).decode('utf-8')
            logging.info(f"Data received from {address}: {data_station}")
            station_id, alarm1, alarm2 = data_station.split()

            if validate_client_data(station_id, alarm1, alarm2):
                last_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
                with sqlite3.connect('data.SQLite') as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT OR REPLACE INTO station_status (station_id, last_date, alarm1, alarm2)
                        VALUES (?, ?, ?, ?);
                    ''', (station_id, last_date, alarm1, alarm2))
            else:
                logging.warning(f"Invalid data format received from {address}.")
        except Exception as e:
            logging.error(f"Error handling data from {address}: {e}")


def start_server():
    create_db_and_table()
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('127.0.0.1', 12345))
    server_socket.listen()
    logging.info("Server started. Listening for connections...")

    with ThreadPoolExecutor(max_workers=10) as executor:
        try:
            while True:
                conn, addr = server_socket.accept()
                logging.info(f"Connection from {addr}")
                executor.submit(handle_client, conn, addr)
        except KeyboardInterrupt:
            logging.info("Server shutting down...")
        finally:
            server_socket.close()


if __name__ == '__main__':
    start_server()
