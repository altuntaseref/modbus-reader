import pika
import yaml
import json
import threading
import time
import os
from datetime import datetime
from ftplib import FTP
from flask import Flask, request, jsonify
from modbus_client import ModbusClient
from data_processor import process_data
import csv

app = Flask(__name__)

# Global değişkenler
process_thread = None
running = False
rabbitmq_channel = None


# RabbitMQ bağlantı bilgilerini yaml dosyasından okuma
def _load_rabbitmq_config():
    try:
        with open('rabbitmq_config.yaml', 'r') as file:
            rabbitmq_config = yaml.safe_load(file)
            print("RabbitMQ configuration loaded from YAML file.")
            return rabbitmq_config
    except Exception as e:
        print(f"Failed to load RabbitMQ configuration file: {e}")
        return {}


# RabbitMQ bağlantısı kurma
def connect_to_rabbitmq(config):
    global rabbitmq_channel
    try:
        # RabbitMQ bağlantısı oluştur
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=config['rabbitmq']['host'],  # rabbitmq ana bilgisayar adı
                port=config['rabbitmq'].get('port', 5672),  # rabbitmq portu, varsayılan 5672
                virtual_host=config['rabbitmq'].get('virtual_host', '/'),
                credentials=pika.PlainCredentials(
                    config['rabbitmq'].get('username', 'guest'),
                    config['rabbitmq'].get('password', 'guest')
                )
            )
        )
        rabbitmq_channel = connection.channel()
        rabbitmq_channel.queue_declare(queue=config['rabbitmq']['queue_name'], durable=config['rabbitmq'].get('durable', True))
        rabbitmq_channel.queue_declare(queue='error_queue', durable=True)  # Error kuyruğunu da tanımlıyoruz
        print("Connected to RabbitMQ.")
    except Exception as e:
        print(f"Error connecting to RabbitMQ: {e}")



# RabbitMQ'ya hata mesajı gönderme fonksiyonu
def send_error_to_rabbitmq(error_message, error_details, slave_id=None):
    if rabbitmq_channel is not None:
        try:
            # Eğer slave_id varsa, slave_id'ye karşılık gelen brand'i alıyoruz
            brand = slave_brand_map.get(slave_id, "Unknown Brand")  # Eğer slave_id bulunmazsa "Unknown Brand" dönecek
            error_data = {
                'error_message': error_message,
                'error_details': error_details,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'slave_id': slave_id,
                'brand': brand  # Brand bilgisini de ekliyoruz
            }
            message = json.dumps(error_data)

            rabbitmq_channel.basic_publish(
                exchange='',
                routing_key='error_queue',  # Hata kuyruğu
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Mesajı kalıcı hale getiriyoruz
                )
            )
            print(f"Error sent to RabbitMQ: {error_message}")
        except Exception as e:
            print(f"Error sending error to RabbitMQ: {e}")
    else:
        print("RabbitMQ connection is not established.")



# Modbus okuma ve CSV yazma işlemleri
import pika
import yaml
import json
import threading
import time
import os
from datetime import datetime
from ftplib import FTP
from flask import Flask, request, jsonify
from modbus_client import ModbusClient
from data_processor import process_data
import csv

app = Flask(__name__)

# Global değişkenler
process_thread = None
running = False
rabbitmq_channel = None

# slave_brand_map'i global olarak tanımlıyoruz
slave_brand_map = {}

# RabbitMQ bağlantı bilgilerini yaml dosyasından okuma
def _load_rabbitmq_config():
    try:
        with open('rabbitmq_config.yaml', 'r') as file:
            rabbitmq_config = yaml.safe_load(file)
            print("RabbitMQ configuration loaded from YAML file.")
            return rabbitmq_config
    except Exception as e:
        print(f"Failed to load RabbitMQ configuration file: {e}")
        return {}


# RabbitMQ bağlantısı kurma
def connect_to_rabbitmq(config):
    global rabbitmq_channel
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=config['rabbitmq']['host'],
                port=config['rabbitmq'].get('port', 5672),
                virtual_host=config['rabbitmq'].get('virtual_host', '/'),
                credentials=pika.PlainCredentials(
                    config['rabbitmq'].get('username', 'guest'),
                    config['rabbitmq'].get('password', 'guest')
                )
            )
        )
        rabbitmq_channel = connection.channel()
        rabbitmq_channel.queue_declare(queue=config['rabbitmq']['queue_name'], durable=config['rabbitmq'].get('durable', True))
        print("Connected to RabbitMQ.")
    except Exception as e:
        print(f"Error connecting to RabbitMQ: {e}")


# RabbitMQ'ya hata mesajı gönderme fonksiyonu
def send_error_to_rabbitmq(error_message, error_details, slave_id=None):
    global slave_brand_map  # global olarak slave_brand_map'e erişim sağlıyoruz
    if rabbitmq_channel is not None:
        try:
            # Eğer slave_id varsa, slave_id'ye karşılık gelen brand'i alıyoruz
            brand = slave_brand_map.get(slave_id, "Unknown Brand")  # Eğer slave_id bulunmazsa "Unknown Brand" dönecek
            error_data = {
                'error_message': error_message,
                'error_details': error_details,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'slave_id': slave_id,
                'brand': brand  # Brand bilgisini de ekliyoruz
            }
            message = json.dumps(error_data)

            rabbitmq_channel.basic_publish(
                exchange='',
                routing_key='error_queue',  # Hata kuyruğu
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Mesajı kalıcı hale getiriyoruz
                )
            )
            print(f"Error sent to RabbitMQ: {error_message}")
        except Exception as e:
            print(f"Error sending error to RabbitMQ: {e}")
    else:
        print("RabbitMQ connection is not established.")


# Modbus okuma ve CSV yazma işlemleri
def process_data_from_json(config):
    global running, slave_brand_map
    modbus_config = config['modbus_config']
    ftp_config = config['ftp_config']  # FTP yapılandırması
    register_configs = config['register_configs']
    intervals = config['intervals']

    fieldnames = ['Date', 'Slave_ID', 'Brand']
    all_fieldnames = set()

    # Tüm register isimlerini ekliyoruz
    for brand, brand_config in register_configs.items():
        for reg in brand_config['registers']:
            all_fieldnames.add(reg['name'])

    # Fieldnames'e tüm register isimlerini ekliyoruz
    fieldnames += list(all_fieldnames)
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # slave_brand_map'i burada oluşturuyoruz
    slave_brand_map = {}
    for brand, brand_config in register_configs.items():
        slave_id = brand_config['slaveId']
        slave_brand_map[slave_id] = brand

    data_rows = []  # CSV'ye yazılacak veriler
    last_csv_write_time = time.time()

    client_map = {}  # Slave_id'ye göre client bağlantıları
    last_read_times = {brand: 0 for brand in register_configs}  # Her brand için son okuma zamanı

    print("Processing started...")

    try:
        while running:
            current_time = time.time()

            connection_failed = {slave_id: False for slave_id in slave_brand_map}

            for brand, brand_config in register_configs.items():
                slave_id = brand_config['slaveId']
                read_interval = brand_config['read_interval']  # Brand'a özel okuma süresi
                registers_config = brand_config['registers']

                if slave_id not in client_map:
                    client_map[slave_id] = ModbusClient(modbus_config['host'], modbus_config['port'], slave_id)

                client = client_map[slave_id]

                try:
                    if client.connect():
                        if current_time - last_read_times[brand] >= read_interval:
                            print(f"Reading data from Slave ID {slave_id} ({brand})")
                            row = {
                                'Date': datetime.now().strftime('%d%m%y%H%M%S'),
                                'Slave_ID': slave_id,
                                'Brand': brand
                            }
                            for reg in registers_config:
                                registers = client.read_registers(reg['address'], reg['count'])
                                if registers is None:
                                    formatted_value = "NULL"
                                else:
                                    value = process_data(registers, reg['type'])
                                    formatted_value = f"{value:.6f}".replace('.', ',') if isinstance(value, (int, float)) else str(value)
                                row[reg['name']] = formatted_value

                            data_rows.append(row)
                            last_read_times[brand] = current_time

                    else:
                        # Hata durumunda RabbitMQ'ya mesaj gönder
                        error_message = f"Connection failed to Modbus Slave ID {slave_id} ({brand})"
                        error_details = f"Failed to connect to Modbus slave {slave_id} ({brand}) at {datetime.now().strftime('%d%m%y%H%M%S')}"
                        send_error_to_rabbitmq(error_message, error_details, slave_id=slave_id)

                except Exception as e:
                    # Hata mesajını RabbitMQ'ya gönder
                    error_message = f"Error with Modbus register for Slave ID {slave_id} ({brand})"
                    error_details = f"Error with register {reg['name']} at {datetime.now().strftime('%d%m%y%H%M%S')}: {str(e)}"
                    send_error_to_rabbitmq(error_message, error_details, slave_id=slave_id)
                    connection_failed[slave_id] = True

            # FTP hatalarını kontrol et
            if current_time - last_csv_write_time >= intervals['csv_write_interval']:
                if data_rows:
                    current_datetime = datetime.now()
                    date_str = current_datetime.strftime('%d%m%y%H%M%S')
                    csv_filename = f'data_{date_str}.csv'
                    csv_file = os.path.join(output_dir, csv_filename)

                    try:
                        with open(csv_file, mode='w', newline='', encoding='utf-8-sig') as file:
                            writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=';', quoting=csv.QUOTE_MINIMAL)
                            writer.writeheader()

                            for row in data_rows:
                                for field in fieldnames:
                                    if field not in row:
                                        row[field] = "NULL"

                            writer.writerows(data_rows)

                        print(f"CSV file {csv_filename} written.")

                        # FTP'ye gönder
                        send_file_to_ftp(ftp_config, csv_file)

                        # RabbitMQ'ya veri gönder
                        send_data_to_rabbitmq(data_rows)

                    except Exception as e:
                        # FTP hatasında RabbitMQ'ya gönder
                        error_message = f"FTP upload failed for file {csv_filename}"
                        error_details = f"FTP upload failed for {csv_filename} at {datetime.now().strftime('%d%m%y%H%M%S')}: {str(e)}"
                        send_error_to_rabbitmq(error_message, error_details, slave_id=None)

                    data_rows.clear()
                    last_csv_write_time = current_time

            time.sleep(1)  # Ana döngüde her saniye bekleyelim

    except KeyboardInterrupt:
        print("Process interrupted.")
    except Exception as e:
        print(f"Error: {e}")



# RabbitMQ'ya veri gönderme fonksiyonu
def send_data_to_rabbitmq(data_rows):
    if rabbitmq_channel is not None:
        for row in data_rows:
            try:
                message = json.dumps(row)
                rabbitmq_channel.basic_publish(
                    exchange='',
                    routing_key='my_queue',
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                    )
                )
                print("Data sent to RabbitMQ queue.")
            except Exception as e:
                print(f"Error sending data to RabbitMQ: {e}")
    else:
        print("RabbitMQ connection is not established.")


# FTP'ye dosya gönderme fonksiyonu
def send_file_to_ftp(ftp_config, csv_file_path):
    ftp_host = ftp_config['host']
    ftp_port = ftp_config.get('port', 21)
    ftp_user = ftp_config['username']
    ftp_password = ftp_config['password']
    ftp_directory = ftp_config.get('directory', '/')
    try:
        with FTP() as ftp:
            ftp.connect(ftp_host, ftp_port)
            ftp.login(ftp_user, ftp_password)
            ftp.cwd(ftp_directory)
            with open(csv_file_path, 'rb') as file:
                ftp.storbinary(f'STOR {os.path.basename(csv_file_path)}', file)
            print(f"{csv_file_path} successfully sent to FTP server.")
    except FTP.all_errors as e:
        # FTP hatasında RabbitMQ'ya gönder
        error_message = f"FTP upload failed for file {csv_file_path}"
        error_details = f"FTP upload failed for {csv_file_path} at {datetime.now().strftime('%d%m%y%H%M%S')}: {str(e)}"
        send_error_to_rabbitmq(error_message, error_details)


def stop_process():
    global running
    running = False


@app.route('/process_data', methods=['POST'])
def start_process():
    global process_thread, running

    if running:
        return jsonify({"message": "Process already running"}), 400

    try:
        config = request.get_json()
        if not config:
            return jsonify({"error": "Invalid JSON data"}), 400

        running = True
        process_thread = threading.Thread(target=process_data_from_json, args=(config,))
        process_thread.start()

        print("Process started...")

        return jsonify({"message": "Process started"}), 200

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


@app.route('/stop', methods=['POST'])
def stop_process_api():
    global running

    if not running:
        return jsonify({"message": "No process is running"}), 400

    stop_process()
    process_thread.join()
    return jsonify({"message": "Process stopped successfully"}), 200


if __name__ == '__main__':
    rabbitmq_config = _load_rabbitmq_config()
    connect_to_rabbitmq(rabbitmq_config)

    app.run(debug=True, host='0.0.0.0', port=5000)
