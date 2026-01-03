# Kafka Event Producer Script

Kafka 토픽에 Impression 및 Click 이벤트를 발행하는 Python 스크립트입니다.

## 기능

- 초당 약 5건의 이벤트 발행 (설정 가능)
- Impression 이벤트와 Click 이벤트 랜덤 생성
- 여러 파트너, 그룹, 제품 정보 생성
- 부정클릭 시나리오 포함 (10회 초과 클릭)
- Kafka 브로커와의 안정적인 통신

## 설치

```bash
# Python 3.7 이상 필요
pip3 install --break-system-packages -r requirements.txt
```

또는 confluent-kafka 직접 설치:
```bash
pip3 install --break-system-packages confluent-kafka==2.3.0
```

## 사용법

### 기본 실행

```bash
python kafka_event_producer.py
```

### 옵션 설정

```bash
# Kafka 브로커 주소 변경
python kafka_event_producer.py --bootstrap-servers localhost:9092

# 실행 시간 변경 (분 단위)
python kafka_event_producer.py --duration 30

# 초당 이벤트 수 변경
python kafka_event_producer.py --rate 10.0

# 토픽 이름 변경
python kafka_event_producer.py --impression-topic my-impression-topic --click-topic my-click-topic
```

### 전체 옵션

```bash
python kafka_event_producer.py \
    --bootstrap-servers localhost:30092 \
    --duration 10 \
    --rate 5.0 \
    --impression-topic ad-impressed \
    --click-topic ad-clicked
```

## 이벤트 형식

### Impression Event

```json
{
    "groupId": 1,
    "partnerId": 100,
    "productId": 1,
    "displayTarget": "WEB",
    "displayId": "display-1",
    "amount": 1000,
    "timestamp": "2024-01-01T12:00:00"
}
```

### Click Event

```json
{
    "groupId": 1,
    "partnerId": 100,
    "productId": 1,
    "displayTarget": "WEB",
    "displayId": "display-1",
    "amount": 1000,
    "timestamp": "2024-01-01T12:00:00"
}
```

## 부정클릭 시나리오

스크립트는 자동으로 3개의 부정클릭 시나리오를 생성합니다:
- 동일한 `partnerId`, `displayTarget`, `displayId`, `productId` 조합으로
- 1분 내 10회를 초과하는 클릭 이벤트 발생
- 전체 클릭 이벤트의 약 20%가 부정클릭으로 생성됩니다

## 출력 예시

```
Starting event producer...
Duration: 10 minutes
Events per second: 5.0
Total events: ~3000
--------------------------------------------------------------------------------
Created fraudulent scenario: Partner=100, Display=WEB/display-1, Product=1
Created fraudulent scenario: Partner=200, Display=MOBILE/display-5, Product=2
Created fraudulent scenario: Partner=300, Display=APP/display-3, Product=3
--------------------------------------------------------------------------------
[Impression] Partner: 100, Group: 1, Display: WEB/display-1, Amount: 5000
[Normal] [Click] Partner: 200, Group: 2, Display: MOBILE/display-2, Product: 1, Amount: 3000
[FRAUDULENT] [Click] Partner: 100, Group: 1, Display: WEB/display-1, Product: 1, Amount: 2000
...
```

## 중지

`Ctrl+C`로 스크립트를 중지할 수 있습니다. 중지 시 통계 정보가 출력됩니다.

