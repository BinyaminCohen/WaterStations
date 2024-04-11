import logging
import socket
import time

WAIT_SECONDS = 60  # Global variable to adjust wait time

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_water_station_status():
    try:
        with open('status.txt', 'r') as file:
            data_station = file.read().splitlines()
        # Validate data format: station_id, alarm1, alarm2
        if len(data_station) != 3 or not all(s.isdigit() for s in data_station):
            raise ValueError("Invalid data format in status.txt")
        return data_station
    except Exception as e:
        logging.error(f"Error reading or validating status: {e}")
        return None


def send_data_to_server(data):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect(('127.0.0.1', 12345))
            client_socket.sendall(data.encode('utf-8'))
            # Optionally wait for a response from the server
            response = client_socket.recv(1024).decode('utf-8')
            logging.info(f"Server response: {response}")
    except Exception as e:
        logging.error(f"Error sending data to server: {e}")


def main():
    while True:
        start_time = time.time()
        station_data = read_water_station_status()
        if station_data:
            data_string = ' '.join(station_data)
            send_data_to_server(data_string)
        else:
            logging.warning("No valid data to send. Skipping this cycle.")

        # Calculate actual sleep time, considering processing time
        sleep_time = max(0, WAIT_SECONDS - int(time.time() - start_time))
        time.sleep(sleep_time)


if __name__ == '__main__':
    main()
