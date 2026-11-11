import uuid
import time
from datetime import datetime
from typing import List, Dict, Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
import json

app = FastAPI(title="Order Service (Write Side)")

# Инициализация Kafka-продюсера
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "orders-events"
producer: Optional[AIOKafkaProducer] = None

@app.on_event("startup")
async def startup_event():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

@app.on_event("shutdown")
async def shutdown_event():
    global producer
    if producer:
        await producer.stop()

# --- 1. Структура событий и Event Store ---
# Глобальное In-Memory хранилище событий: {order_id: [список_событий]}
EVENT_STORE: Dict[str, List[dict]] = {}

class EventModel(BaseModel):
    eventId: str
    orderId: str
    eventType: str  # OrderCreated, OrderPaid, OrderCancelled
    payload: dict
    timestamp: float
    version: int

# --- 2. Состояние Агрегата (Класс Заказа) ---
class OrderAggregate:
    def __init__(self, order_id: str):
        self.order_id = order_id
        self.status = None        # CREATED, PAID, CANCELLED
        self.amount = 0.0
        self.items = []
        self.version = 0

    # Проигрывание конкретного события для изменения состояния
    def apply(self, event: dict):
        self.version = event["version"]
        event_type = event["eventType"]
        payload = event["payload"]

        if event_type == "OrderCreated":
            self.status = "CREATED"
            self.amount = payload.get("amount", 0.0)
            self.items = payload.get("items", [])
        elif event_type == "OrderPaid":
            self.status = "PAID"
        elif event_type == "OrderCancelled":
            self.status = "CANCELLED"

# --- 3. Функция Восстановления Состояния (Rehydration) ---
def rehydrate_order(order_id: str) -> OrderAggregate:
    events = EVENT_STORE.get(order_id, [])
    aggregate = OrderAggregate(order_id)
    
    # Последовательно проигрываем события по цепочке
    for event in events:
        aggregate.apply(event)
        
    return aggregate

# --- Вспомогательная функция публикации в Kafka ---
async def publish_to_kafka(event: dict):
    if producer:
        event_json = json.dumps(event).encode("utf-8")
        await producer.send_and_wait(KAFKA_TOPIC, event_json)
        print(f"📦 [KAFKA] Событие {event['eventType']} успешно отправлено в топик {KAFKA_TOPIC}")

# --- 4. Эндпоинты Команд (Write Side) ---

class CreateOrderCommand(BaseModel):
    amount: float
    items: List[str]

@app.post("/orders")
async def create_order(command: CreateOrderCommand):
    order_id = str(uuid.uuid4())
    
    # Формируем событие создания
    event = {
        "eventId": str(uuid.uuid4()),
        "orderId": order_id,
        "eventType": "OrderCreated",
        "payload": {"amount": command.amount, "items": command.items},
        "timestamp": time.time(),
        "version": 1
    }
    
    # Сохраняем строго в Event Store
    EVENT_STORE[order_id] = [event]
    
    # Асинхронно отправляем в Kafka
    await publish_to_kafka(event)
    
    return {"message": "Команда на создание принята", "orderId": order_id}

@app.post("/orders/{id}/pay")
async def pay_order(id: str):
    if id not in EVENT_STORE:
        raise HTTPException(status_code=404, detail="Заказ не найден")
        
    # Шаг А. Восстановление состояния (Rehydration)
    order = rehydrate_order(id)
    
    # Шаг Б. Проверка бизнес-правил
    if order.status == "PAID":
        raise HTTPException(status_code=400, detail="Ошибка бизнес-логики: Заказ уже оплачен!")
    if order.status == "CANCELLED":
        raise HTTPException(status_code=400, detail="Ошибка бизнес-логики: Нельзя оплатить отмененный заказ!")
        
    # Шаг В. Генерация нового события
    event = {
        "eventId": str(uuid.uuid4()),
        "orderId": id,
        "eventType": "OrderPaid",
        "payload": {},
        "timestamp": time.time(),
        "version": order.version + 1
    }
    
    # Добавление в Event Store без изменения сущностей
    EVENT_STORE[id].append(event)
    await publish_to_kafka(event)
    
    return {"message": "Команда на оплату выполнена", "orderId": id}

@app.post("/orders/{id}/cancel")
async def cancel_order(id: str):
    if id not in EVENT_STORE:
        raise HTTPException(status_code=404, detail="Заказ не найден")
        
    # Шаг А. Rehydration
    order = rehydrate_order(id)
    
    # Шаг Б. Проверка бизнес-правил
    if order.status == "CANCELLED":
        raise HTTPException(status_code=400, detail="Ошибка бизнес-логики: Заказ уже отменен!")
        
    # Шаг В. Генерация нового события
    event = {
        "eventId": str(uuid.uuid4()),
        "orderId": id,
        "eventType": "OrderCancelled",
        "payload": {},
        "timestamp": time.time(),
        "version": order.version + 1
    }
    
    EVENT_STORE[id].append(event)
    await publish_to_kafka(event)
    
    return {"message": "Команда на отмену выполнена", "orderId": id}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)
