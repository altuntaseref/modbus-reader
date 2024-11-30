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
        print("Connected to RabbitMQ.")
    except Exception as e:
        print(f"Error connecting to RabbitMQ: {e}")


# Modbus okuma ve CSV yazma işlemleri
def process_data_from_json(config):
    global running
    modbus_config = config['modbus_config']
    ftp_config = config['ftp_config']  # FTP yapılandırması
    register_configs = config['register_configs']
    intervals = config['intervals']

    # İlk başta tüm register isimlerini içerecek fieldnames listesi oluşturuluyor
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

            # Bağlantı hatası durumu
            connection_failed = {slave_id: False for slave_id in slave_brand_map}

            # Her bir slave için verileri okuma işlemi
            for brand, brand_config in register_configs.items():
                slave_id = brand_config['slaveId']
                read_interval = brand_config['read_interval']  # Brand'a özel okuma süresi
                registers_config = brand_config['registers']

                # Bağlantıyı sadece bir kez yapıyoruz, her markada farklı bağlantı oluşturuluyor
                if slave_id not in client_map:
                    client_map[slave_id] = ModbusClient(modbus_config['host'], modbus_config['port'], slave_id)

                client = client_map[slave_id]

                try:
                    # Modbus verisini okuma işlemi
                    if client.connect():
                        # Her slave için okuma zamanını kontrol ediyoruz
                        if current_time - last_read_times[brand] >= read_interval:
                            print(f"Reading data from Slave ID {slave_id} ({brand})")

                            # Slave ve Brand'e özel veri satırı oluştur
                            row = {
                                'Date': datetime.now().strftime('%d%m%y%H%M%S'),
                                'Slave_ID': slave_id,
                                'Brand': brand
                            }

                            # Modbus register'larını oku
                            for reg in registers_config:
                                registers = client.read_registers(reg['address'], reg['count'])
                                if registers is None:
                                    formatted_value = "NULL"
                                else:
                                    scaling = reg.get('scaling', 1)
                                    multiplier = reg.get('multiplier', 1)
                                    value = process_data(registers, reg['type'], scaling, multiplier)

                                    if value is None:
                                        formatted_value = "NULL"
                                    elif isinstance(value, (int, float)):
                                        formatted_value = f"{value:.6f}".replace('.', ',')
                                    elif isinstance(value, str):
                                        try:
                                            formatted_value = f"{float(value):.6f}".replace('.', ',')
                                        except ValueError:
                                            formatted_value = value
                                    else:
                                        formatted_value = str(value)

                                row[reg['name']] = formatted_value

                            data_rows.append(row)  # Bu satırı verilerimize ekle
                            last_read_times[brand] = current_time  # Son okuma zamanını güncelle
                        else:
                            # Okuma için belirlenen aralık gelmediyse bekle
                            continue
                    else:
                        # Bağlantı sağlanamazsa "NULL" yazılması için işaretle
                        connection_failed[slave_id] = True

                except Exception as e:
                    print(f"Error with register {reg['name']}: {e}")
                    connection_failed[slave_id] = True

            # Eğer bağlantı hatası varsa, yalnızca bağlantı kopan slave'lerin verilerini NULL ile yazacağız
            for brand, brand_config in register_configs.items():
                slave_id = brand_config['slaveId']
                if connection_failed[slave_id]:
                    # Bağlantı hatası durumunda "NULL" yaz
                    row = {
                        'Date': datetime.now().strftime('%d%m%y%H%M%S'),
                        'Slave_ID': slave_id,
                        'Brand': brand
                    }
                    for reg in brand_config['registers']:
                        row[reg['name']] = "NULL"

                    data_rows.append(row)

            # CSV'yi belirli bir aralıkla yazma
            if current_time - last_csv_write_time >= intervals['csv_write_interval']:
                if data_rows:  # Veriler varsa CSV'yi yaz
                    current_datetime = datetime.now()
                    date_str = current_datetime.strftime('%d%m%y%H%M%S')
                    csv_filename = f'data_{date_str}.csv'
                    csv_file = os.path.join(output_dir, csv_filename)

                    with open(csv_file, mode='w', newline='', encoding='utf-8-sig') as file:
                        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=';', quoting=csv.QUOTE_MINIMAL)
                        writer.writeheader()

                        # Her satırda eksik register'lar varsa NULL ile doldur
                        for row in data_rows:
                            # Eksik register'lar için "NULL" yazalım
                            for field in fieldnames:
                                if field not in row:
                                    row[field] = "NULL"

                        writer.writerows(data_rows)

                    print(f"CSV dosyası {csv_filename} yazıldı.")

                    # CSV dosyasını FTP'ye gönder
                    send_file_to_ftp(ftp_config, csv_file)

                    # RabbitMQ'ya veri gönder
                    send_data_to_rabbitmq(data_rows)

                    data_rows.clear()  # Verileri temizleyelim
                    last_csv_write_time = current_time  # Son yazma zamanını güncelle

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
                message = json.dumps(row)  # Veriyi JSON formatında mesaj haline getiriyoruz
                rabbitmq_channel.basic_publish(
                    exchange='',
                    routing_key='my_queue',
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Mesajı kalıcı hale getiriyoruz
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
        print(f"FTP upload error: {e}")


def stop_process():
    global running
    running = False


@app.route('/process_data', methods=['POST'])
def start_process():
    global process_thread, running

    if running:
        return jsonify({"message": "Process already running"}), 400

    try:
        config = request.get_json()  # Gelen JSON verisini alıyoruz
        if not config:
            return jsonify({"error": "Geçersiz JSON verisi"}), 400

        running = True
        process_thread = threading.Thread(target=process_data_from_json, args=(config,))
        process_thread.start()  # Yeni bir thread başlatarak işlem arka planda devam eder

        print("Process started...")

        return jsonify({"message": "Process başlatıldı"}), 200

    except Exception as e:
        return jsonify({"error": f"Bir hata oluştu: {str(e)}"}), 500


@app.route('/stop', methods=['POST'])
def stop_process_api():
    """API endpoint: /stopPROCESS -> Çalışan süreci durdur."""
    global running

    if not running:
        return jsonify({"message": "No process is running"}), 400

    stop_process()  # Süreci durdur
    process_thread.join()  # Thread'in bitmesini bekleyelim
    return jsonify({"message": "Process stopped successfully"}), 200


if __name__ == '__main__':
    # RabbitMQ bağlantısını kur
    rabbitmq_config = _load_rabbitmq_config()
    connect_to_rabbitmq(rabbitmq_config)

    app.run(debug=True, host='0.0.0.0', port=5000)
