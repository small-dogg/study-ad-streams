# Study Ad Streams

Kafka Streams를 활용한 스트림 처리 애플리케이션

## 기술 스택

- **Framework**: Spring Boot 3.2.0
- **Language**: Java 17
- **Stream Processing**: Kafka Streams 3.6.0
- **Architecture**: DDD (Domain-Driven Design), EDA (Event-Driven Architecture)

## 주요 기능

- 메시지 소비 및 스트림 처리
- State Store 기반 Tumbling Window 집계
- 집계된 데이터를 신규 이벤트로 발행

## 프로젝트 구조

```
src/main/java/com/study/adstreams/
├── domain/                    # 도메인 계층 (DDD)
│   ├── event/                # 도메인 이벤트
│   └── service/              # 도메인 서비스
├── application/              # 애플리케이션 계층 (DDD)
│   └── service/              # 애플리케이션 서비스
└── infrastructure/           # 인프라스트럭처 계층 (DDD)
    ├── config/               # 설정
    ├── stream/               # Kafka Streams 파이프라인
    └── messaging/            # 메시징 구현
```

## 실행 방법

### 사전 요구사항

- Java 17 이상
- Maven 3.6 이상
- Kafka Broker (localhost:9092)

### 빌드 및 실행

```bash
# 빌드
mvn clean package

# 실행
mvn spring-boot:run
```

### 설정

`application.yml` 파일에서 Kafka 설정을 변경할 수 있습니다:

- `spring.kafka.bootstrap-servers`: Kafka Broker 주소
- `spring.kafka.streams.input-topic`: 입력 토픽 이름
- `spring.kafka.streams.output-topic`: 출력 토픽 이름

## 개발 가이드

### DDD 구조

- **Domain Layer**: 비즈니스 로직과 도메인 모델
- **Application Layer**: 유스케이스 조정 및 애플리케이션 서비스
- **Infrastructure Layer**: Kafka Streams, 메시징 등 기술적 구현

### EDA 패턴

- 도메인 이벤트를 통한 비동기 통신
- 이벤트 발행 및 구독을 통한 느슨한 결합

## TODO

- [ ] State Store 기반 Tumbling Window 집계 로직 구현
- [ ] 도메인 모델 정의
- [ ] 집계 서비스 구현
- [ ] 이벤트 발행 구현

