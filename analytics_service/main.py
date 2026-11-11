import asyncio
import json
from typing import Dict, Optional
from fastapi import FastAPI, HTTPException
from aiokafka import AIOKafkaConsumer

app = FastAPI(title="Analytics & Read Side Service")

# --- 1. Локальные In-Memory Хранилища ---

# Плоская Read-модель для быстрого чтения: {order_id: {данные_заказа}}
READ_MODEL_STORE: Dict[str, dict] = {}

# Хранилище агрегированной статистики аналитики
ANALYTICS_SUMMARY = {
    "total_orders": 0,
    "total_paid_amount": 0.0,
    "orders_by_status": {
        "CREATED": 0,
        "PAID": 0,
        "CANCELLED": 0
    },
    "average_order_amount": 0.0
}

# Настройки Kafka
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "orders-events"
KAFKA_GROUP_ID = "analytics-read-group"


# --- 2. Обработчик событий (Projection Handler) ---
async def consume_events():
    """Фоновый воркер, который асинхронно читает события из Kafka"""
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest"  # Читать топик с самого начала при перезапуске
    )
    
    await consumer.start()
    print(f"📡 [KAFKA CONSUMER] Успешно подписался на топик {KAFKA_TOPIC}")
    
    try:
        async for msg in consumer:
            event = json.loads(msg.value.decode("utf-8"))
            event_type = event["eventType"]
            order_id = event["orderId"]
            payload = event["payload"]
            
            print(f"📥 [CONSUMER] Получено событие {event_type} для заказа {order_id}")
            
            # --- ОБНОВЛЕНИЕ READ-МОДЕЛИ ---
            if event_type == "OrderCreated":
                READ_MODEL_STORE[order_id] = {
                    "id": order_id,
                    "status": "CREATED",
                    "amount": payload.get("amount", 0.0),
                    "items": payload.get("items", []),
                    "updated_at": event["timestamp"]
                }
            elif event_type == "OrderPaid":
                if order_id in READ_MODEL_STORE:
                    READ_MODEL_STORE[order_id]["status"] = "PAID"
                    READ_MODEL_STORE[order_id]["updated_at"] = event["timestamp"]
            elif event_type == "OrderCancelled":
                if order_id in READ_MODEL_STORE:
                    READ_MODEL_STORE[order_id]["status"] = "CANCELLED"
                    READ_MODEL_STORE[order_id]["updated_at"] = event["timestamp"]
            
            # --- ПЕРЕСЧЕТ СТАТИСТИКИ АНАЛИТИКИ ---
            recalculate_analytics()
            
    except Exception as e:
        print(f"🚨 Ошибка в работе Kafka Consumer: {e}")
    finally:
        await consumer.stop()


def recalculate_analytics():
    """Пересчитывает метрики на основе текущего среза Read-модели"""
    orders = list(READ_MODEL_STORE.values())
    
    total_orders = len(orders)
    paid_orders = [o for o in orders if o["status"] == "PAID"]
    total_paid_amount = sum(o["amount"] for o in paid_orders)
    
    # Считаем статусы
    status_counts = {"CREATED": 0, "PAID": 0, "CANCELLED": 0}
    for o in orders:
        status_counts[o["status"]] += 1
        
    # Вычисляем средний чек по всем созданным заказам
    avg_amount = sum(o["amount"] for o in orders) / total_orders if total_orders > 0 else 0.0
    
    # Обновляем глобальный объект
    ANALYTICS_SUMMARY["total_orders"] = total_orders
    ANALYTICS_SUMMARY["total_paid_amount"] = total_paid_amount
    ANALYTICS_SUMMARY["orders_by_status"] = status_counts
    ANALYTICS_SUMMARY["average_order_amount"] = round(avg_amount, 2)


# --- 3. Жизненный цикл приложения ---
@app.on_event("startup")
async def startup_event():
    # Запуск чтения Kafka в фоновой задаче, чтобы не блокировать API
    asyncio.create_task(consume_events())


# --- 4. Эндпоинты Чтения (Read Side — CQRS) ---

@app.get("/orders/{id}")
async def get_order_from_read_model(id: str):
    """Чтение данных ТОЛЬКО из read-модели, а не из Event Store"""
    order = READ_MODEL_STORE.get(id)
    if not order:
        raise HTTPException(status_code=404, detail="Заказ в Read-модели не найден")
    return order


@app.get("/analytics/summary")
async def get_analytics_summary():
    """Возвращает агрегированную статистику аналитики"""
    return ANALYTICS_SUMMARY


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8006)
