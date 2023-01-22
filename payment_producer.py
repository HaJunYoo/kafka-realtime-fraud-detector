from kafka import KafkaProducer
import datetime
import pytz
import time
import random
import json

TOPIC_NAME = "payments"
brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
producer = KafkaProducer(
    bootstrap_servers=brokers,
    api_version=(0, 11, 5)
)


# time date를 만들어주는 함수
def get_time_date():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())  # datetime을 뽑아옴
    kst_now = utc_now.astimezone(pytz.timezone("Asia/Seoul"))  # 한국 시간으로 바꿈
    d = kst_now.strftime("%m/%d/%Y")
    t = kst_now.strftime("%H:%M:%S")
    return d, t


# payment를 생성해주는 함수
def generate_payment_data():
    payment_type = random.choice(["VISA", "MASTERCARD", "BITCOIN"])
    amount = random.randint(0, 100)
    to = random.choice(["me", "mom", "dad", "friend", "stranger"])
    return payment_type, amount, to


# python dictionary 형태로 date, payment 정보를 만들어서 producer에 보내줌
# data를 python object -> json 형태로 바꿔서 보내준다
while True:
    d, t = get_time_date()
    payment_type, amount, to = generate_payment_data()
    new_data = {
        "DATE": d,
        "TIME": t,
        "PAYMENT_TYPE": payment_type,
        "AMOUNT": amount,
        "TO": to,
    }

    producer.send(TOPIC_NAME, json.dumps(new_data).encode("utf-8"))
    print(new_data)
    time.sleep(1)
