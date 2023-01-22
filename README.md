# kafka-realtime-fraud-detector
python-kafka를 이용한 dummy fraud 데이터 탐지 코드입니다

kafka와 kafdrop을 이용한 python-kafka 비정상 데이터 탐지 코드입니다.

1. payment_producer에서 payment 데이터가 들어와서 fraud detector로부터 2가지 갈래길로 나누어지게 된다
2. 두가지 갈래로 나누어진 흐름은 legit_processor와 fraud_processor로 나누어지게 됩니다.

### payment_producer 만들기

time, datetime, pytz 라이브러리 : timezone을 표기하기 위해 사용됨

- kafdrop을 이용해서 topic을 하나 만들자
    - topic 이름은 payment이다
    
    ![스크린샷 2023-01-22 오전 10.37.54.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/39969a22-a29c-4a74-8e16-a33885ef2ba6/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-01-22_%E1%84%8B%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%AB_10.37.54.png)
    

1. payment_producer.py
    
    ```python
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
    ```
    

### fraud detector 만들기

payment_producer에서 들어온 payment 데이터가 사기 데이터인지를 분류해주는 모듈

두가지 갈래로 나누어진 흐름은 legit_processor와 fraud_processor로 나누어지게 됩니다.

payment producer가 만들어낸 메세지를 소비할 consumer가 필요하다

같은 payment 토픽에 있는 메세지를 소비하는 consumer 작성

is_suspicious 함수 : 만약 메세지가 payment type이 bitcoin이면 fraud로 간주한다

cosumer가 메세지를 받아와서 payment 타입을 체크한 후

토픽 타입을 legit(정상)와 fraud(비정상)로 분류해준다

그런 후 분류된 토픽을 통해 모듈 내 새로 만든 kafka producer에 보내준다

토픽 생성

- partition : 2, replication factor : 1 로 아래 2개의 topic을 kafdrop을 사용하여 생성
- fraud_payments
- legit_payments
    
    ![스크린샷 2023-01-22 오전 11.08.31.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/1c0a1a85-4de9-47a8-83b4-d75a83fe8a4d/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-01-22_%E1%84%8B%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%AB_11.08.31.png)
    

파이썬 파일

- fraud_detector.py
    
    ```python
    from kafka import KafkaConsumer, KafkaProducer
    import json
    
    PAYMENT_TOPIC = "payments"
    FRAUD_TOPIC = "fraud_payments"
    LEGIT_TOPIC = "legit_payments"
    
    brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
    
    consumer = KafkaConsumer(
        PAYMENT_TOPIC,
        bootstrap_servers=brokers,
        api_version=(0, 11, 5)
    )
    
    producer = KafkaProducer(
        bootstrap_servers=brokers,
        api_version=(0, 11, 5)
    )
    
    def is_suspicious(transactions):
        # and transactions["TO"] == "stranger"
        if transactions["PAYMENT_TYPE"] == "BITCOIN":
            return True
        return False
    
    for message in consumer:
        msg = json.loads(message.value.decode())
        topic = FRAUD_TOPIC if is_suspicious(msg) else LEGIT_TOPIC
        producer.send(topic, json.dumps(msg).encode("utf-8"))
        print(topic, is_suspicious(msg), msg["PAYMENT_TYPE"])
    ```
    

### legit_processor와 fraud_processor

위의 2개의 분류 처리기는 consumer과 유사한 프로세스를 가진다

producer에서 각각 토픽으로 분류된 메세지를 consumer가 메세지 소비하는 형식이다

**fraud_processor.py**

```python
from kafka import KafkaConsumer
import json

FRAUD_TOPIC = "fraud_payments"
brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
consumer = KafkaConsumer(
    FRAUD_TOPIC,
    bootstrap_servers=brokers,
    api_version=(0, 11, 5)
)

for message in consumer:
    msg = json.loads(message.value.decode())
    to = msg["TO"]
    amount = msg["AMOUNT"]
    if msg["TO"] == "stranger":
        print(f"[ALERT] fraud detected payment to: {to} - {amount}")
    else:
        print(f"[PROCESSING BITCOIN] payment to: {to} - {amount}")
```

**legit_processor.py**

```python
from kafka import KafkaConsumer
import json

LEGIT_TOPIC = "legit_payments"
brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
consumer = KafkaConsumer(
    LEGIT_TOPIC,
    bootstrap_servers=brokers,
    api_version=(0, 11, 5)
)

for message in consumer:
    msg = json.loads(message.value.decode())
    to = msg["TO"]
    amount = msg["AMOUNT"]

    if msg["PAYMENT_TYPE"] == "VISA":
        print(f"[VISA] payment to: {to} - {amount}")
    elif msg["PAYMENT_TYPE"] == "MASTERCARD":
        print(f"[MASTERCARD] payment to: {to} - {amount}")
    else:
        print("[ALERT] unable to process payments")
```

### 파일 실행 순서

1. legit and fraud processor.py
2. fraud detector.py
3. payment producer.py

- 실행 결과
    
    ![스크린샷 2023-01-22 오전 11.50.50.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/243d6349-62b8-49be-bd9a-c0296dd8c05d/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-01-22_%E1%84%8B%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%AB_11.50.50.png)
    
    ![스크린샷 2023-01-22 오전 11.51.03.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/ff660a31-d646-4a19-b56e-3f1796d00f8f/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-01-22_%E1%84%8B%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%AB_11.51.03.png)
    
    ![스크린샷 2023-01-22 오전 11.51.16.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/057fe7d5-03d6-4962-b4f5-7f5ba4444f62/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-01-22_%E1%84%8B%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%AB_11.51.16.png)
    
    ![스크린샷 2023-01-22 오전 11.51.25.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/108c8839-c28a-44bc-8203-cc8827dc3444/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-01-22_%E1%84%8B%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%AB_11.51.25.png)
