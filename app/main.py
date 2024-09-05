# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel
# import logging

# from app.kafka_producer import CustomKafkaProducer

# app = FastAPI()

# # Initialize logging
# logging.basicConfig(level=logging.INFO)

# # Initialize Kafka Producer
# producer = CustomKafkaProducer(bootstrap_servers='localhost:9092')

# class Message(BaseModel):
#     topic: str
#     value: dict

# @app.post("/produce/")
# async def produce_message(message: Message):
#     try:
#         producer.send_message(topic=message.topic, value=message.value)
#         return {"status": "Message sent successfully"}
#     except Exception as e:
#         logging.error(f"Failed to send message: {e}")
#         raise HTTPException(status_code=500, detail="Failed to send message")

# @app.on_event("shutdown")
# def shutdown_event():
#     producer.close()

#****************************************************************************************


# app/main.py

# from fastapi import FastAPI, HTTPException, BackgroundTasks
# from app.kafka_producer import KafkaMessageProducer
# from app.kafka_consumer import KafkaMessageConsumer
# from app.schemas import Message
# import logging

# # Initialize logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# app = FastAPI()

# # Initialize Kafka components
# producer = KafkaMessageProducer(bootstrap_servers='localhost:9092')
# consumer = KafkaMessageConsumer(bootstrap_servers='localhost:9092')

# @app.post("/produce")
# async def produce_message(message: Message, background_tasks: BackgroundTasks):
#     try:
#         # Access 'value' directly from the message
#         background_tasks.add_task(producer.send_message, message.topic, message.value)
#         return {"message": "Message sent to Kafka"}
#     except Exception as e:
#         logger.error(f"Error producing message: {e}")
#         raise HTTPException(status_code=500, detail="Failed to produce message")

# @app.get("/consume")
# async def consume_messages():
#     try:
#         messages = consumer.get_messages()
#         return {"messages": messages}
#     except Exception as e:
#         logger.error(f"Error consuming messages: {e}")
#         raise HTTPException(status_code=500, detail="Failed to consume messages")




# app/main.py

from fastapi import FastAPI, HTTPException, BackgroundTasks
from app.kafka_producer import KafkaMessageProducer
from app.kafka_consumer import KafkaMessageConsumer
from app.schemas import Message
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Initialize Kafka components
producer = KafkaMessageProducer(bootstrap_servers='localhost:9092')
consumer = KafkaMessageConsumer(bootstrap_servers='localhost:9092', group_id='my-group', topic='test-topic')

@app.post("/produce")
async def produce_message(message: Message, background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(producer.send_message, message.topic, message.value)
        return {"message": "Message sent to Kafka"}
    except Exception as e:
        logger.error(f"Error producing message: {e}")
        raise HTTPException(status_code=500, detail="Failed to produce message")

@app.get("/consume")
async def consume_messages():
    try:
        messages = consumer.get_messages()
        return {"messages": messages}
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
        raise HTTPException(status_code=500, detail="Failed to consume messages")
