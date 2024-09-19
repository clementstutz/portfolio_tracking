from typing import Dict


class Order:
    def __init__(self, date: str, quantity: float, price: float) -> None:
        self.date = date
        self.quantity = quantity
        self.price = price

    def to_dict(self) -> Dict:
        return {
            "date": self.date,
            "quantity": self.quantity,
            "price": self.price
        }
