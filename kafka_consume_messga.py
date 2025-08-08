import paho.mqtt.client as mqtt
import json
from datetime import datetime
import time
import os
from kafka import KafkaConsumer
from multiprocessing import Process, Manager

from evn import MQTT_BROKER, MQTT_PORT, KAFKA_BROKER, KAFKA_TOPIC, KAFKA_TOPIC_COMSUMER
from mqtt_builder import mqtt_enddevice, mqtt_button, mqtt_basic, mqtt_newdevice

from db import SQLiteDeviceLineData

class mqtt2kafka:
    def __init__(self):
        self.l2s_deviceName = Manager().dict()
        self.s2l_deviceName = Manager().dict()
        self.kafka_message = Manager().dict()

        self.db_path = "data.db"
        self.db_handler = SQLiteDeviceLineData(self.db_path)

        self.mqtt_enddevice = mqtt_enddevice(self.db_path, self.l2s_deviceName, self.s2l_deviceName)
        self.mqtt_newdevice = mqtt_newdevice(self.db_path, self.l2s_deviceName, self.s2l_deviceName)
        self.mqtt_button = mqtt_button(self.l2s_deviceName, self.s2l_deviceName, self.kafka_message)
        

    def process_kafka_v1(self):
        mqtt_client = mqtt_basic(broker=MQTT_BROKER, port=MQTT_PORT)
        mqtt_client.connect()
        mqtt_client.client.loop_start()

        consumer1 = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="kafka-to-zigbee-bridgev19",
            # consumer_timeout_ms=100
        )
        while True:
            for message in consumer1:
                try:
                    data = json.loads(message.value.decode("utf-8"))
                    print(f"📥 Nhận từ Kafka: {data}")  # Debug dữ liệu Kafka
                except Exception as e:
                    print("🔴 Lỗi giải mã JSON từ thông điệp Kafka:", e)
                    continue

                try:
                    machine_code = data.get("machine_code")
                    if not machine_code:
                        print("⚠️ Thiếu machine_code")
                        continue
                    self.kafka_message[machine_code] = data

                    # update db button box
                    # if self.db_handler.machine_code_exists(machine_code):
                    #     print(f"⚠️ Machine {machine_code} đã có, sẽ cập nhật.")
                    #     self.db_handler.upsert_machine_data(machine_code, line1=data["line1"], line2=data["line2"])
                    # else:
                    #     print(f"➕ Machine {machine_code} chưa có, ghi mới.")
                    #     self.db_handler.upsert_machine_data(machine_code, line1=data["line1"], line2=data["line2"])
                                    
                    line2 = data["line2"]

                    if "-" in line2:
                        if self.db_handler.machine_code_exists(machine_code):
                            print(f"⚠️ Machine {machine_code} đã có, sẽ cập nhật line2.")
                        else:
                            print(f"➕ Machine {machine_code} chưa có, sẽ thêm mới line2.")

                        # Chỉ ghi DB khi line2 hợp lệ
                        self.db_handler.upsert_machine_data(machine_code, line2=data["line2"])
                    else:
                        print(f"⚠️ line2 không hợp lệ, dùng mặc định: {line2}")


                    device_id = self.s2l_deviceName.get(str(machine_code).lower())
                    print(f"📋 s2l_deviceName: {dict(self.s2l_deviceName)}")  # Debug ánh xạ
                    if not device_id:
                        print(f"❌ Không tìm thấy thiết bị với địa chỉ ngắn {machine_code}")
                        continue

                    print(f"✅ Kafka → Zigbee: {machine_code} ➝ {device_id}")

                    mqtt_topic = f"zigbee2mqtt/{device_id}/set"
                    attributes = [
                        ("line2", "line2"),
                        ("alarm", "alarm"),
                        ("count_up", "countUp"),
                        ("count_down", "countDown"),
                        ("line1", "line1")
                        
                    ]

                    for kafka_key, zigbee_key in attributes:
                        if kafka_key in data:
                            value = data[kafka_key]
                            payload = {zigbee_key: value}
                            print(f"📤 Gửi đến {mqtt_topic}: {json.dumps(payload)}")
                            mqtt_client.client.publish(mqtt_topic, json.dumps(payload))
                            time.sleep(0.1)

                    # payload = {"line2": data["line2"],
                    #            "alarm": data["alarm"],
                    #            "countUp": data["count_up"],
                    #            "countDown": data["count_down"],
                    #            "line1": data["line1"]}
                    # print(f"📤 Gửi đến {mqtt_topic}: {json.dumps(payload)}")
                    # mqtt_client.client.publish(mqtt_topic, json.dumps(payload))
                    # print(json.dumps(payload))

                    # time.sleep(0.1)  



                except Exception as e:
                    print(f"🔴 Lỗi xử lý thông điệp Kafka: {e}")
                    import traceback
                    traceback.print_exc()  # In stack trace để debug

    def process_kafka_v2(self):
        mqtt_client = mqtt_basic(broker=MQTT_BROKER, port=MQTT_PORT)
        mqtt_client.connect()
        mqtt_client.client.loop_start()

        consumer2 = KafkaConsumer(
            KAFKA_TOPIC_COMSUMER,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="kafka-to-zigbee-bridgev19",
            # consumer_timeout_ms=100
        )

        while True:
            for message in consumer2:
                try:
                    data = json.loads(message.value.decode("utf-8"))
                except Exception as e:
                    print("🔴 Lỗi giải mã JSON từ thông điệp Kafka:", e)
                    continue

                try:
                    machine_codes = data.get("machine_code")
                    if not machine_codes:
                        print("⚠️ Thiếu machine_code")
                        continue
                    for machine_code in machine_codes:
                        code = machine_code["code"]
                        value = machine_code["value"]

                        device_id = self.s2l_deviceName.get(str(code).lower(), code)
                        if not device_id:
                            print(f"❌ Không tìm thấy thiết bị với địa chỉ ngắn {code}")
                            continue

                        print(f"✅ Kafka → Zigbee: {code} ➝ {device_id}")

                        mqtt_topic = f"zigbee2mqtt/{device_id}/set"

                        payload = {"countUp": value}
                        print(f"📤 Gửi đến {mqtt_topic}: {json.dumps(payload)}")
                        mqtt_client.client.publish(mqtt_topic, json.dumps(payload))

                        # attributes = [("count_up", "countUp")]

                        # for kafka_key, zigbee_key in attributes:
                        #     if kafka_key in data:
                        #         payload = {zigbee_key: data[kafka_key]}
                        #         print(f"📤 Gửi đến {mqtt_topic}: {json.dumps(payload)}")
                        #         mqtt_client.client.publish(mqtt_topic, json.dumps(payload))
                        #         time.sleep(0.05)

                except Exception as e:
                    print("🔴 Lỗi xử lý thông điệp Kafka:", e)

    def run(self):
        self.mqtt_enddevice.connect()
        self.mqtt_button.connect()
        self.mqtt_newdevice.connect()

        process1 = Process(target=self.process_kafka_v1)
        process1.start()
        print(f"🔁 Lắng nghe topic Kafka `{KAFKA_TOPIC}`...")

        process2 = Process(target=self.process_kafka_v2)
        process2.start()
        print(f"🔁 Lắng nghe topic Kafka `{KAFKA_TOPIC_COMSUMER}`...")

        process1.join()
        process2.join()

        self.mqtt_enddevice.client.loop_stop()
        self.mqtt_button.client.loop_stop()
        self.mqtt_newdevice.loop_stop()

if __name__ == "__main__":
    mqtt = mqtt2kafka()
    mqtt.run()
    