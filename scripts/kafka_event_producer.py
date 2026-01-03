#!/usr/bin/env python3
"""
Kafka Event Producer Script
초당 약 5건의 Impression/Click 이벤트를 발행합니다.
부정클릭 시나리오도 포함합니다.
"""

import json
import random
import time
import uuid
from datetime import datetime
from typing import Dict
from confluent_kafka import Producer
from confluent_kafka import KafkaError


class EventProducer:
    def __init__(self, bootstrap_servers: str = "localhost:30092"):
        """
        Kafka Producer 초기화
        
        Args:
            bootstrap_servers: Kafka 브로커 주소
        """
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all',
            'enable.idempotence': True,
            'retries': 3,
            'max.in.flight.requests.per.connection': 1,
            'compression.type': 'snappy'
        })
        
        # 파트너 및 그룹 정보
        self.partners = [100, 200, 300, 400, 500]  # 파트너 ID 목록
        self.groups = [1, 2, 3, 4, 5]  # 그룹 ID 목록
        self.products = [1, 2, 3, 4, 5, 10, 20, 30]  # 제품 ID 목록
        self.display_targets = ["WEB", "MOBILE", "APP"]
        self.display_ids = [f"display-{i}" for i in range(1, 11)]  # display-1 ~ display-10
        
        # 부정클릭 시나리오를 위한 추적
        self.fraudulent_scenarios = []  # [(partner_id, display_target, display_id, product_id), ...]
        
    def generate_impression_event(self) -> Dict:
        """Impression 이벤트 생성"""
        partner_id = random.choice(self.partners)
        group_id = random.choice(self.groups)
        product_id = random.choice(self.products)
        display_target = random.choice(self.display_targets)
        display_id = random.choice(self.display_ids)
        amount = random.randint(100, 10000)  # 100 ~ 10000
        
        return {
            "groupId": group_id,
            "partnerId": partner_id,
            "productId": product_id,
            "displayTarget": display_target,
            "displayId": display_id,
            "amount": amount,
            "timestamp": datetime.now().isoformat()
        }
    
    def generate_click_event(self, is_fraudulent: bool = False, 
                            partner_id: int = None,
                            display_target: str = None,
                            display_id: str = None,
                            product_id: int = None) -> Dict:
        """Click 이벤트 생성"""
        if is_fraudulent and partner_id and display_target and display_id and product_id:
            # 부정클릭 시나리오: 동일한 키로 반복 생성
            group_id = random.choice(self.groups)
            amount = random.randint(100, 10000)
        else:
            # 정상 클릭
            partner_id = random.choice(self.partners)
            group_id = random.choice(self.groups)
            product_id = random.choice(self.products)
            display_target = random.choice(self.display_targets)
            display_id = random.choice(self.display_ids)
            amount = random.randint(100, 10000)
        
        return {
            "groupId": group_id,
            "partnerId": partner_id,
            "productId": product_id,
            "displayTarget": display_target,
            "displayId": display_id,
            "amount": amount,
            "timestamp": datetime.now().isoformat()
        }
    
    def create_fraudulent_scenario(self) -> tuple:
        """부정클릭 시나리오 생성 (10회 초과 클릭)"""
        partner_id = random.choice(self.partners)
        display_target = random.choice(self.display_targets)
        display_id = random.choice(self.display_ids)
        product_id = random.choice(self.products)
        
        scenario = (partner_id, display_target, display_id, product_id)
        self.fraudulent_scenarios.append(scenario)
        return scenario
    
    def delivery_callback(self, err, msg):
        """메시지 전달 콜백"""
        if err is not None:
            print(f"Message delivery failed: {err}")
    
    def send_impression(self, topic: str = "ad-impressed"):
        """Impression 이벤트 발행"""
        event = self.generate_impression_event()
        event_id = str(uuid.uuid4())
        
        try:
            value = json.dumps(event, default=str).encode('utf-8')
            self.producer.produce(
                topic,
                key=event_id.encode('utf-8'),
                value=value,
                callback=self.delivery_callback
            )
            self.producer.poll(0)  # non-blocking poll
            print(f"[Impression] Partner: {event['partnerId']}, Group: {event['groupId']}, "
                  f"Display: {event['displayTarget']}/{event['displayId']}, Amount: {event['amount']}")
            return True
        except Exception as e:
            print(f"Error sending impression: {e}")
            return False
    
    def send_click(self, topic: str = "ad-clicked", is_fraudulent: bool = False,
                   scenario: tuple = None):
        """Click 이벤트 발행"""
        if is_fraudulent and scenario:
            event = self.generate_click_event(
                is_fraudulent=True,
                partner_id=scenario[0],
                display_target=scenario[1],
                display_id=scenario[2],
                product_id=scenario[3]
            )
        else:
            event = self.generate_click_event()
        
        event_id = str(uuid.uuid4())
        
        try:
            value = json.dumps(event, default=str).encode('utf-8')
            self.producer.produce(
                topic,
                key=event_id.encode('utf-8'),
                value=value,
                callback=self.delivery_callback
            )
            self.producer.poll(0)  # non-blocking poll
            status = "[FRAUDULENT]" if is_fraudulent else "[Normal]"
            print(f"{status} [Click] Partner: {event['partnerId']}, Group: {event['groupId']}, "
                  f"Display: {event['displayTarget']}/{event['displayId']}, "
                  f"Product: {event['productId']}, Amount: {event['amount']}")
            return True
        except Exception as e:
            print(f"Error sending click: {e}")
            return False
    
    def run(self, duration_minutes: int = 10, events_per_second: float = 5.0):
        """
        이벤트 발행 실행
        
        Args:
            duration_minutes: 실행 시간 (분)
            events_per_second: 초당 이벤트 수
        """
        print(f"Starting event producer...")
        print(f"Duration: {duration_minutes} minutes")
        print(f"Events per second: {events_per_second}")
        print(f"Total events: ~{int(duration_minutes * 60 * events_per_second)}")
        print("-" * 80)
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        interval = 1.0 / events_per_second  # 이벤트 간 간격 (초)
        
        # 부정클릭 시나리오 생성 (3개 정도)
        fraudulent_scenarios = []
        for _ in range(3):
            scenario = self.create_fraudulent_scenario()
            fraudulent_scenarios.append(scenario)
            print(f"Created fraudulent scenario: Partner={scenario[0]}, "
                  f"Display={scenario[1]}/{scenario[2]}, Product={scenario[3]}")
        
        print("-" * 80)
        
        event_count = 0
        fraudulent_click_count = {scenario: 0 for scenario in fraudulent_scenarios}
        
        try:
            while time.time() < end_time:
                # Impression 또는 Click 중 랜덤 선택 (70% Impression, 30% Click)
                event_type = random.choices(
                    ['impression', 'click'],
                    weights=[70, 30]
                )[0]
                
                if event_type == 'impression':
                    self.send_impression()
                else:
                    # 부정클릭 여부 결정 (20% 확률로 부정클릭)
                    is_fraudulent = random.random() < 0.2
                    
                    if is_fraudulent and fraudulent_scenarios:
                        # 부정클릭 시나리오 중 하나 선택
                        scenario = random.choice(fraudulent_scenarios)
                        self.send_click(is_fraudulent=True, scenario=scenario)
                        fraudulent_click_count[scenario] += 1
                    else:
                        self.send_click()
                
                event_count += 1
                
                # 다음 이벤트까지 대기
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n\nStopping producer...")
        finally:
            # 남은 메시지 전송 대기
            self.producer.flush(timeout=10)
            
            elapsed_time = time.time() - start_time
            print("\n" + "=" * 80)
            print(f"Producer stopped.")
            print(f"Total events sent: {event_count}")
            print(f"Elapsed time: {elapsed_time:.2f} seconds")
            print(f"Average rate: {event_count / elapsed_time:.2f} events/second")
            print("\nFraudulent click counts by scenario:")
            for scenario, count in fraudulent_click_count.items():
                print(f"  Partner={scenario[0]}, Display={scenario[1]}/{scenario[2]}, "
                      f"Product={scenario[3]}: {count} clicks")
            print("=" * 80)


def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Kafka Event Producer')
    parser.add_argument(
        '--bootstrap-servers',
        type=str,
        default='localhost:30092',
        help='Kafka bootstrap servers (default: localhost:30092)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=10,
        help='Duration in minutes (default: 10)'
    )
    parser.add_argument(
        '--rate',
        type=float,
        default=5.0,
        help='Events per second (default: 5.0)'
    )
    parser.add_argument(
        '--impression-topic',
        type=str,
        default='ad-impressed',
        help='Impression topic name (default: ad-impressed)'
    )
    parser.add_argument(
        '--click-topic',
        type=str,
        default='ad-clicked',
        help='Click topic name (default: ad-clicked)'
    )
    
    args = parser.parse_args()
    
    producer = EventProducer(bootstrap_servers=args.bootstrap_servers)
    producer.run(duration_minutes=args.duration, events_per_second=args.rate)


if __name__ == "__main__":
    main()
