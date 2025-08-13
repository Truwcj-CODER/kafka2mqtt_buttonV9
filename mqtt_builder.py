import paho.mqtt.client as mqtt
import json
import os
import time
from kafka import KafkaProducer
from evn import MQTT_BROKER, MQTT_PORT, MQTT_TOPIC, MQTT_DEVICE_LIST_TOPIC, KAFKA_BROKER, KAFKA_TOPIC_PRODUCER,MQTT_NEW_DEVICE_TOPIC
from db import SQLiteDeviceLineData

class mqtt_basic:
    def __init__(self, broker=MQTT_BROKER, port=MQTT_PORT, topic=MQTT_TOPIC):
        self.broker = broker
        self.port = port
        self.topic = topic
        self.client = mqtt.Client()
        self.client.on_connect = None
        self.client.on_message = None

    def connect(self):
        self.client.connect(self.broker, self.port, 60)
        self.client.subscribe(self.topic)
        self.client.loop_start()

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()


class mqtt_newdevice:
    def __init__(self, db_path, l2s_deviceName, s2l_deviceName, broker=MQTT_BROKER, port=MQTT_PORT, topic=MQTT_NEW_DEVICE_TOPIC):
        self.broker = broker
        self.port = port
        self.topic = topic
        self.l2s_deviceName = l2s_deviceName
        self.s2l_deviceName = s2l_deviceName
        self.db_path = db_path
        
        self.client = mqtt.Client(userdata={'l2s': self.l2s_deviceName, 's2l': self.s2l_deviceName})
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def connect(self):
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        print(f"✅ Connected to MQTT broker {self.broker}:{self.port} | topic: {self.topic}")
        self.client.subscribe(self.topic)
        
    def on_message(self, client, userdata, msg):
        l2s_deviceName = userdata['l2s']
        s2l_deviceName = userdata['s2l']
        db_handler = SQLiteDeviceLineData(db_path=self.db_path)
           
        try:
    
            dev = json.loads(msg.payload.decode())
            print("📥 Nhận thiết bị mới từ MQTT:", dev)
            
            data = dev["data"]
            long_addr = data.get("ieee_address")
            short = l2s_deviceName[long_addr]

            print(f"📲 Thiết bị mới: {short} ({long_addr})")
            time.sleep(0.05)

            # 🔁 Lấy dữ liệu line2 từ DB (nếu có)
            device_data = db_handler.get_device_data(short)
            if device_data:
                line2 = device_data.get("line2")
                mqtt_topic = f"zigbee2mqtt/{long_addr}/set"

                
                # Gửi line2
                payload_line2 = {"line2": line2}
                print(f"📤 Gửi đến {mqtt_topic}: {payload_line2}")
                self.client.publish(mqtt_topic, json.dumps(payload_line2))
            db_handler.close()

        except Exception as e:
            print("🔴 Lỗi xử lý message từ MQTT:", e)

class mqtt_enddevice:
    def __init__(self, db_path, l2s_deviceName, s2l_deviceName, broker=MQTT_BROKER, port=MQTT_PORT, topic=MQTT_DEVICE_LIST_TOPIC):
        self.broker = broker
        self.port = port
        self.topic = topic
        self.l2s_deviceName = l2s_deviceName
        self.s2l_deviceName = s2l_deviceName
        self.db_path = db_path
        
        self.client = mqtt.Client(userdata={'l2s': self.l2s_deviceName, 's2l': self.s2l_deviceName})
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def connect(self):
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        print(f"✅ Connected to MQTT broker {self.broker}:{self.port} | topic: {self.topic}")
        self.client.subscribe(self.topic)
        self.client.publish("zigbee2mqtt/bridge/request/device", "")

    def on_message(self, client, userdata, msg):
        l2s_deviceName = userdata['l2s']
        s2l_deviceName = userdata['s2l']
        db_handler = SQLiteDeviceLineData(db_path=self.db_path)

        try:
            devices = json.loads(msg.payload.decode())

            for dev in devices:
                if dev.get("type") == "EndDevice":
                    name = dev.get("friendly_name", dev.get("ieee_address", "unknown"))
                    net = dev.get("network_address")
                    short = format(net, "04x") if net is not None else "unknown"

                    l2s_deviceName[name] = short
                    s2l_deviceName[short] = name

                    print(f"📲 New endevice: {short} ({name})")

        except Exception as e:
            print("🔴 Erorr processing message từ MQTT:", e)


class mqtt_button:
    def __init__(self,  l2s_deviceName, s2l_deviceName, kafka_message, broker = MQTT_BROKER, port = MQTT_PORT, topic = MQTT_TOPIC):
        self.broker = broker
        self.port = port
        self.topic = topic
        self.l2s_deviceName = l2s_deviceName
        self.s2l_deviceName = s2l_deviceName
        self.kafka_message = kafka_message
        self.kafka_producer = self.init_kafka_producer()
        self.message_id = 0
        self.prev_counts = {} 
         
        self.client = mqtt.Client(userdata={'l2s': self.l2s_deviceName, 's2l': self.s2l_deviceName, 'kafka_message': self.kafka_message,'kafka_producer': self.kafka_producer})
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def connect(self):
        self.client.connect(self.broker, self.port, 60)
        # self.client.subscribe(MQTT_TOPIC)
        self.client.loop_start()
        # self.client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        print(f"Connected to MQTT broker {self.broker}:{self.port} toppic: {self.topic}")
        # client.subscribe(self.topic)
        self.client.subscribe(self.topic)

    @staticmethod
    def safe_int(value):
        if value is None or value == "":
            return 0
        try:
            return int(value)
        except Exception:
            return 0

    def on_message(self, client, userdata, msg):
        l2s_deviceName = userdata['l2s']
        kafka_message = userdata['kafka_message']
        try:
            data = json.loads(msg.payload.decode())
            device_id = msg.topic.split("/")[-1]
            
            if device_id not in l2s_deviceName:
                return

            print(f"📩 Device ID: {device_id} | Payload: {data}")

            short_addr = l2s_deviceName[device_id]

            
            curr_up = self.safe_int(data.get("countUp"))
            curr_down = self.safe_int(data.get("countDown"))
            curr_line2 = data.get("line2","")
            
            if len(curr_line2) > 21:
                print(f"⚠️ line2 dài {len(curr_line2)} ký tự, cắt để giới hạn ngưỡng.")
                curr_line2 = curr_line2[:21]
                data["line2"] = curr_line2
                


            if curr_up is None or curr_down is None:
                print("⚠️ countUp or countDown empty or invalid, skip")
                return

            # Lấy dữ liệu cũ (nếu có)
            prev = self.prev_counts.get(device_id, {"countUp": curr_up, "countDown": curr_down, "line2": curr_line2})
            prev_up = prev["countUp"]
            prev_down = prev["countDown"]
            prev_line2 = prev.get("line2", "")

            kafka_raw = kafka_message[short_addr]
            if not kafka_raw:
                return
            

            kafka_line2 = kafka_raw["data"].get("line2", "")
            if len(kafka_line2) > 21:
                kafka_line2 = kafka_line2[:21]
                kafka_raw["data"]["line2"] = kafka_line2
             
            kafka_line2 = kafka_raw["data"]["line2"]
            kafka_countUp = self.safe_int(kafka_raw["data"]["count_up"])
            kafka_countDown = self.safe_int(kafka_raw["data"]["count_down"])

            if not kafka_raw["load_all"]:
                if kafka_line2 == curr_line2 and kafka_countUp == curr_up and kafka_countDown == curr_down:
                    kafka_raw["load_all"] = True
                    print(f"✅ Data Update Full {device_id}")
                    # Cập nhật lại trạng thái mới
                    self.prev_counts[device_id] = {"countUp": curr_up, "countDown": curr_down, "line2": curr_line2}
                    print(f"📊 Updated counts for {device_id}: {self.prev_counts}")

                    kafka_message[short_addr] = kafka_raw  # Cập nhật lại dữ liệu đã tải đầy đủ

                    return                                                                                                             
                else:
                    print(f"⏳ Chưa đủ dữ liệu cho {device_id}, chờ thêm...")
                    return
                
            if curr_line2 == prev_line2 :
                if curr_up > prev_up:
                    print(f"⬆️   UP: {prev_up} → {curr_up}")                                                                                                                                 
                    self.send_kafka(l2s_deviceName[device_id], 1)  # UP
                elif curr_down > prev_down:
                    print(f"⬆️   DOWN: {prev_down} → {curr_down}")
                    self.send_kafka(l2s_deviceName[device_id], 0)  # DOWN
                else:
                    print("🔁 No change countUp/countDown")

            # Cập nhật lại trạng thái mới
            self.prev_counts[device_id] = {"countUp": curr_up, "countDown": curr_down, "line2": curr_line2}
            # print(f"📊 Updated counts for {device_id}: {self.prev_counts}")
                

        except Exception as e:
            print("🔴 Error on button press:", e)

    def init_kafka_producer(self):
        kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all"
        )
        print("✅ Kafka producer initialized.")
        return kafka_producer

    def send_kafka(self, device_id, status):
        kafka_message = {
            "id": self.message_id,
            "manufactor_id": 1,
            "machine_code": device_id,
            "status": status,
            "created_at": int(time.time()*1000),
            "__deleted": False
        }
        self.kafka_producer.send(KAFKA_TOPIC_PRODUCER, value=kafka_message, key=str(self.message_id).encode("utf-8"))
        print(f"📤 Kafka ({'UP' if status == 1 else 'DOWN'}):", json.dumps(kafka_message, indent=2))  
        self.message_id += 1