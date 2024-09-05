# app/schemas.py

from pydantic import BaseModel

# class Message(BaseModel):
#     topic: str
#     text: str

class Message(BaseModel):
    topic: str
    value: dict