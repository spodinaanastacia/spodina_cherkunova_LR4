import grpc
import pybreaker
import time
import random
from fastapi import FastAPI, Query

# Импортируем сгенерированные gRPC модули
import payment_pb2
import payment_pb2_grpc

# 1. Инициализация FastAPI приложения
app = FastAPI(title="GraphQL/API Gateway")

# 2. Настройка параметров Circuit Breaker (для требований отчета)
# fail_max=3 -> цепь разомкнется после 3 ошибок подряд
# reset_timeout=10 -> предохранитель пробудет в состоянии OPEN 10 секунд
cb_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=10)

# Функция непосредственного вызова gRPC сервера Насти Сподиной
def call_payment_grpc_service(order_id, amount, simulate_failure):
    # 'payment_service:50051' — сетевой адрес контейнера в Docker
    with grpc.insecure_channel('payment_service:50051') as channel:
        stub = payment_pb2_grpc.PaymentServiceStub(channel)
        
        # Жесткий таймаут ожидания gRPC вызова в 3 секунды
        response = stub.CheckPaymentStatus(
            payment_pb2.PaymentRequest(
                order_id=str(order_id),
                amount=float(amount),
                simulate_failure=simulate_failure
            ),
            timeout=3.0
        )
        return {
            "transaction_id": response.transaction_id,
            "status": response.status,
            "message": response.message
        }

# Функция обработки платежа с FALLBACK логикой
def safe_execute_payment(order_id, amount, simulate_failure=False):
    print(f"\n📢 [CIRCUIT BREAKER] Состояние цепи перед запросом: {cb_breaker.state.name} (Время: {time.strftime('%X')})\n")
    try:
        # Пропускаем вызов через предохранитель
        return cb_breaker.call(call_payment_grpc_service, order_id, amount, simulate_failure)
    except pybreaker.CircuitBreakerError:
        print(f"[{time.strftime('%X')}] 🚨 [Блокировка] Предохранитель РАЗОМКНУТ (OPEN). Отдаем Fallback JSON.")
        return {
            "transaction_id": "fallback-stub-student30", # Номер 30
            "status": "PROCESSING",
            "message": "Платежная система временно недоступна. Запрос обрабатывается в резервной очереди."
        }
    except grpc.RpcError as e:
        print(f"[{time.strftime('%X')}] ⚠️ Сетевой сбой gRPC: {e.details()}")
        return {
            "status": "PROCESSING",
            "message": "Прямая ошибка сети до размыкания цепи предохранителя."
        }

# 3. HTTP эндпоинты шлюза, которые слушает Nginx
@app.get("/")
def index():
    return {"status": "Gateway Active", "info": "Лабораторная работа №5"}

# Эндпоинт для проведения тестов оплаты
@app.get("/pay")
def pay(fail: bool = Query(default=False, alias="fail")):
    order_id = f"order-{random.randint(100, 999)}"
    amount = 2500.0
    
    # Запускаем защищенный платеж
    result = safe_execute_payment(order_id, amount, simulate_failure=fail)
    return result

if __name__ == '__main__':
    import uvicorn
    # Запускаем сервер шлюза на порту 8003 (как указано в docker-compose)
    uvicorn.run(app, host="0.0.0.0", port=8003)
