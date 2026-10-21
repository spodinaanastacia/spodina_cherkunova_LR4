from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Dict

app = FastAPI(title="Catalog Service (Строго под ТЗ)")

# База данных со строковыми ключами для 100% совместимости со шлюзом
products_db: Dict[str, dict] = {
    "1": {"name": "Laptop", "price": 999.99},
    "2": {"name": "Smartphone", "price": 499.99},
    "3": {"name": "Mechanical Keyboard", "price": 120.00},
    "4": {"name": "Wireless Mouse", "price": 45.50}
}

@app.get("/api/catalog/products")
async def get_products(page: int = Query(1, ge=1), limit: int = Query(2, ge=1)):
    products_list = list(products_db.items())
    start = (page - 1) * limit
    end = start + limit
    
    paginated_data = []
    for p_id, p_info in products_list[start:end]:
        paginated_data.append({
            "id": p_id,
            "name": p_info["name"],
            "price": p_info["price"]
        })
    return paginated_data

@app.get("/api/catalog/products/{product_id}")
async def get_product_by_id(product_id: str):
    if product_id not in products_db:
        raise HTTPException(status_code=404, detail="Товар не найден")
    
    product_data = products_db[product_id].copy()
    product_data["id"] = product_id
    return product_data
