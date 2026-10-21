import asyncio, json, aio_pika

async def notification_worker():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()
    exchange = await channel.declare_exchange("saga_exchange", aio_pika.ExchangeType.TOPIC, durable=True)
    queue = await channel.declare_queue("notification_queue", durable=True)
    await queue.bind(exchange, routing_key="payment.events.completed")
    await queue.bind(exchange, routing_key="order.events.cancelled")

    print("[Notification Service] Запущен и ожидает финальных статусов...")
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                data = json.loads(message.body.decode())
                order_id = data.get("orderId")
                print(f"[Notification Service] Отправлено уведомление клиенту по заказу {order_id}")
if __name__ == "__main__":
    asyncio.run(notification_worker())
