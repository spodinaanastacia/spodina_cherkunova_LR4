import strawberry
import httpx
import asyncio
import jwt
from typing import List, Optional
from strawberry.fastapi import GraphQLRouter
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from strawberry.dataloader import DataLoader

SECRET_KEY = "super-secret-key-for-lab"
ALGORITHM = "HS256"

app = FastAPI(title="GraphQL Gateway & Auth (Разработчик: Черкунова А.)")

# --- 1. REST AUTH-СЕРВИС ИЗ ЛР №2 ---
users_db = {} 

class AuthSchema(BaseModel):
    username: str = Field(..., description="Логин (email)")
    password: str = Field(..., description="Пароль")

@app.post("/api/auth/register", status_code=status.HTTP_201_CREATED)
def register(user: AuthSchema):
    if user.username in users_db:
        raise HTTPException(status_code=400, detail="User already exists")
    users_db[user.username] = user.password 
    return {"message": "User registered successfully"}

@app.post("/api/auth/login")
def login(user: AuthSchema):
    if users_db.get(user.username) != user.password:
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    token = jwt.encode({"sub": user.username}, SECRET_KEY, algorithm=ALGORITHM)
    return {"token": token}


# --- 2. GRAPHQL GATEWAY (АДРЕС ВЕДЕТ СТРОГО НА ЦИФРОВОЙ IP И ПОРТ 8001) ---
REST_CATALOG_URL = "http://127.0.0.1:8001/api/catalog/products"


ORDERS_DB = [
    {
        "id": "1001", 
        "userId": "123", 
        "createdAt": "2026-10-13 14:00:00", 
        "items": [{"productId": "1", "quantity": 1}, {"productId": "2", "quantity": 3}]
    },
    {
        "id": "1002", 
        "userId": "123", 
        "createdAt": "2026-10-13 15:30:00", 
        "items": [{"productId": "3", "quantity": 1}]
    }
]

async def batch_load_products(keys: List[str]) -> List["Product"]:
    async with httpx.AsyncClient() as client:
        # Передаем ID как int, чтобы база данных Сподиной А. его гарантированно нашла
        tasks = [client.get(f"{REST_CATALOG_URL}/{int(product_id)}") for product_id in keys]
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

@strawberry.type(description="Товар, запрашиваемый из Catalog Service по REST")
class Product:
    id: strawberry.ID = strawberry.field(description="Уникальный ID товара")
    name: str = strawberry.field(description="Наименование позиции")
    price: float = strawberry.field(description="Цена в рублях")

@strawberry.type(description="Элемент товарной позиции в заказе")
class OrderItem:
    quantity: int = strawberry.field(description="Количество единиц товара")
    product_id: strawberry.Private[str]

    @strawberry.field(description="Детали товара, подтягиваемые через DataLoader без N+1")
    async def product(self) -> Optional[Product]:
        return await product_loader.load(self.product_id)

@strawberry.type(description="Информация о самом заказе пользователя")
class Order:
    id: strawberry.ID = strawberry.field(description="Номер уникального заказа")
    userId: strawberry.ID = strawberry.field(description="ID покупателя")
    createdAt: str = strawberry.field(description="Дата совершения покупки")
    items: List[OrderItem] = strawberry.field(description="Список содержимого корзины")

@strawberry.type
class Query:
    @strawberry.field(description="Запрос получения списка заказов пользователя по его личному ID")
    async def ordersByUserId(self, userId: strawberry.ID) -> List[Order]:
        user_orders = [order for order in ORDERS_DB if order["userId"] == userId]
        
        result = []
        for order in user_orders:
            graphql_items = [
                OrderItem(quantity=item["quantity"], product_id=item["productId"])
                for item in order["items"]
            ]
            result.append(
                Order(
                    id=order["id"],
                    userId=order["userId"],
                    createdAt=order["createdAt"],
                    items=graphql_items
                )
            )
        return result

schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")
