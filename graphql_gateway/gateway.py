import strawberry
import httpx
import asyncio
import json
from typing import List, Optional
from strawberry.fastapi import GraphQLRouter
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from strawberry.dataloader import DataLoader
import aio_pika # Библиотека для RabbitMQ

app = FastAPI(title="GraphQL Gateway & Order Saga API (Черкунова А.)", version="2.0.0")

# --- БЛОК ДАННЫХ И DATALOADER ИЗ ЛР №3 (СОХРАНЯЕМ) ---
# Направление запросов на балансировщик Nginx (порт 8080)
REST_CATALOG_URL = "http://127.0.0"

ORDERS_DB = [
    {
        "id": "1001", 
        "userId": "123", 
        "createdAt": "2026-10-13 14:00:00", 
        "status": "PENDING",
        "items": [{"productId": "1", "quantity": 1}, {"productId": "2", "quantity": 3}]
    },
    {
        "id": "1002", 
        "userId": "123", 
        "createdAt": "2026-10-13 15:30:00", 
        "status": "PENDING",
        "items": [{"productId": "3", "quantity": 1}]
    }
]

async def batch_load_products(keys: List[str]) -> List["Product"]:
    async with httpx.AsyncClient() as client:
        tasks = [client.get(f"{REST_CATALOG_URL}/{product_id}") for product_id in keys]
        responses = await asyncio.gather(*tasks)
        loaded_products = []
        for response in responses:
            if response.status_code == 200:
                data = response.json()
                loaded_products.append(Product(id=str(data["id"]), name=data["name"], price=data["price"]))
            else:
                loaded_products.append(None)
        return loaded_products

product_loader = DataLoader(load_fn=batch_load_products)

# --- БЛОК GRAPHQL СХЕМЫ ИЗ ЛР №3 (СОХРАНЯЕМ) ---
@strawberry.type
class Product:
    id: strawberry.ID
    name: str
    price: float

@strawberry.type
class OrderItem:
    quantity: int
    product_id: strawberry.Private[str]
    @strawberry.field
    async def product(self) -> Optional[Product]:
        return await product_loader.load(self.product_id)

@strawberry.type
class Order:
    id: strawberry.ID
    userId: strawberry.ID
    createdAt: str
    status: str # Статус Саги (PENDING, CONFIRMED, CANCELLED)
    items: List[OrderItem]

@strawberry.type
class Query:
    @strawberry.field
    async def ordersByUserId(self, userId: strawberry.ID) -> List[Order]:
        user_orders = [order for order in ORDERS_DB if order["userId"] == userId]
        result = []
        for order in user_orders:
            graphql_items = [
                OrderItem(quantity=item["quantity"], product_id=item["productId"])
                for item in order["items"]
            ]
            result.append(Order(id=order["id"], userId=order["userId"], createdAt=order["createdAt"], status=order["status"], items=graphql_items))
        return result

schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")

# --- НОВЫЙ БЛОК ЛР №4: SAGA И RABBITMQ ---
class NewOrderItemSchema(BaseModel):
    productId: str
    quantity: int

class CreateOrderRESTSchema(BaseModel):
    userId: str
    items: List[NewOrderItemSchema]
    totalAmount: float

async def get_rabbitmq_channel():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()
    await channel.declare_exchange("saga_exchange", aio_pika.ExchangeType.TOPIC, durable=True)
    return connection, channel

# REST-эндпоинт создания заказа (инициация Саги)
@app.post("/api/orders")
async def create_order(payload: CreateOrderRESTSchema):
    order_id = str(len(ORDERS_DB) + 1001)
    
    new_order = {
        "id": order_id,
        "userId": payload.userId,
        "createdAt": "2026-10-21 14:00:00",
        "status": "PENDING", # Обязательный стартовый статус ожидания оплаты
        "totalAmount": payload.totalAmount,
        "items": [item.model_dump() for item in payload.items]
    }
    ORDERS_DB.append(new_order)
    print(f"[Order Service] Заказ {order_id} сохранен в статусе PENDING")

    # Публикация события в RabbitMQ обменник
    connection, channel = await get_rabbitmq_channel()
    exchange = await channel.get_exchange("saga_exchange")
    #Топик (строковый ключ маршрутизации). Наш шлюз не отправляет сообщение в конкретную коробку-очередь. Он отправляет его в абстрактный обменник saga_exchange и маркирует этой темой.
    await exchange.publish(
        aio_pika.Message(body=json.dumps(new_order).encode(), content_type="application/json"),
        routing_key="order.events.created"
    )
    print(f"[Order Service] Событие OrderCreated отправлено в брокер для заказа {order_id}")
    await connection.close()
    return {"message": "Order processing started", "orderId": order_id}

# Асинхронный воркер для прослушивания ответов платежного сервиса
async def order_events_worker():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()
    exchange = await channel.get_exchange("saga_exchange")
    
    queue = await channel.declare_queue("order_reply_queue", durable=True)
    await queue.bind(exchange, routing_key="payment.events.*")
    
    print("[Order Service Worker] Слушает ответы от платежного сервиса...")
    
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                event_type = message.routing_key
                data = json.loads(message.body.decode())
                order_id = data.get("orderId")
                
                target_order = next((o for o in ORDERS_DB if o["id"] == order_id), None)
                if not target_order:
                    continue
                
                # Обеспечение ИДЕМПОТЕНТНОСТИ: Проверяем, не обработан ли уже заказ
                if target_order["status"] in ["CONFIRMED", "CANCELLED"]:
                    print(f"[Order Service] Идемпотентность: Заказ {order_id} уже обработан.")
                    continue

                if event_type == "payment.events.completed":
                    target_order["status"] = "CONFIRMED"
                    print(f"[Order Service] УСПЕХ: Заказ {order_id} переведен в статус CONFIRMED")
                    
                elif event_type == "payment.events.failed":
                    # Логика распределенной КОМПЕНСАЦИИ
                    target_order["status"] = "CANCELLED"
                    print(f"[Order Service] КОМПЕНСАЦИЯ: Заказ {order_id} -> CANCELLED. Причина: {data.get('reason')}")
                    
                    # Отправляем событие отмены для Notification Service
                    await exchange.publish(
                        aio_pika.Message(body=json.dumps({"orderId": order_id}).encode(), content_type="application/json"),
                        routing_key="order.events.cancelled"
                    )

@app.on_event("startup")
def startup_event():
    asyncio.create_task(order_events_worker())
