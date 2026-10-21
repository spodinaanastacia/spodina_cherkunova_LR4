import asyncio, json, random, uuid, aio_pika

async def payment_worker():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()
    exchange = await channel.declare_exchange("saga_exchange", aio_pika.ExchangeType.TOPIC, durable=True)
    queue = await channel.declare_queue("payment_processing_queue", durable=True)
    await queue.bind(exchange, routing_key="order.events.created")

    print("[Payment Service] Запущен и ожидает транзакции...")
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                order_data = json.loads(message.body.decode())
                order_id = order_data["id"]
                total_amount = order_data.get("totalAmount", 0)
                print(f"[Payment Service] Получен заказ {order_id} на сумму {total_amount}")

                if total_amount < 0: # ДЕТЕРМИНИРОВАННЫЙ СБОЙ
                    reply_data = {"orderId": order_id, "reason": "Отрицательная сумма заказа недопустима"}
                    routing_key = "payment.events.failed"
                else:
                    if random.random() < 0.7:
                        reply_data = {"orderId": order_id, "transactionId": str(uuid.uuid4())}
                        routing_key = "payment.events.completed"
                    else:
                        reply_data = {"orderId": order_id, "reason": "Недостаточно средств на счете"}
                        routing_key = "payment.events.failed"

                await exchange.publish(
                    aio_pika.Message(body=json.dumps(reply_data).encode(), content_type="application/json"),
                    routing_key=routing_key
                )
if __name__ == "__main__":
    asyncio.run(payment_worker())
