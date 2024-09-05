# # app/kafka_consumer.py

# from kafka import KafkaConsumer
# import json
# import logging

# class KafkaMessageConsumer:
#     def __init__(self, bootstrap_servers: str, group_id: str, topic: str):
#         self.consumer = KafkaConsumer(
#             topic,
#             bootstrap_servers=bootstrap_servers,
#             group_id=group_id,
#             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#             auto_offset_reset='earliest'
#         )
#         logging.info("Kafka Consumer initialized successfully.")
#         logging.info(f"Consumer subscribed to topic: {topic}")

#     def get_messages(self):
#         messages = []
#         try:
#             records = self.consumer.poll(timeout_ms=5000)
#             logging.debug(f"Poll records: {records}")
#             for topic_partition, messages_list in records.items():
#                 for message in messages_list:
#                     logging.debug(f"Received message: {message.value}")
#                     if message.value:
#                         messages.append(message.value)
#                     else:
#                         logging.warning("Received an empty message.")
#         except Exception as e:
#             logging.error(f"Failed to consume messages: {e}")
#             raise e
#         return messages



# app/kafka_consumer.py

# app/kafka_consumer.py

from kafka import KafkaConsumer
import json
import logging
import csv
import pandas as pd
from google.cloud import storage



class KafkaMessageConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, topic: str):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id="test",
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=False  # Disable auto-commit to manually control offset commits
        )
        logging.info("Kafka Consumer initialized successfully.")

    def get_messages(self):
        messages = []
        try:
            # Poll for messages with a timeout to ensure it waits for messages to arrive
            records = self.consumer.poll(timeout_ms=5000)
            logging.debug(f"Poll records: {records}")
            
            for topic_partition, messages_list in records.items():
                for message in messages_list:
                    logging.debug(f"Received message: {message.value}")
                    if message.value:
                        messages.append(message.value)
                        self.extract_and_preprocess_data(message.value, "flattened_order_data_with_clean_categories.csv")
                    else:
                        logging.warning("Received an empty message.")
            
            # Manually commit offsets after processing messages
            self.consumer.commit()
            
        except Exception as e:
            logging.error(f"Failed to consume messages: {e}")
            raise e
        
        return messages
    
    

    def extract_and_preprocess_data(self, event_data, output_file):
        extracted_data = []

        # Replace user_id with anonymous_id if user_id is missing, empty, or invalid
        user_id = event_data.get("user_id")
        anonymous_id = event_data.get("anonymous_id")

        if not user_id or user_id == "0":  # Check if user_id is missing or invalid
            user_id = anonymous_id  # Replace user_id with anonymous_id

        order_id = event_data["order_id"]
        event_timestamp = event_data["event_timestamp"]

        # Extract product IDs and categories
        for shipment in event_data["items"]:
            for item in shipment:
                product_id = item["id"]
                category_l1 = self.clean_category(item.get("l1_categories", ["Unknown"]))
                category_l2 = self.clean_category(item.get("l2_categories", ["Unknown"]))
                category_l3 = self.clean_category(item.get("l3_category_name", "Unknown"))

                # Append each product as a separate row of data
                extracted_data.append({
                    "user_id": user_id,
                    "order_id": order_id,
                    "anonymous_id": anonymous_id,
                    "product_id": product_id,
                    "category_l1": category_l1,
                    "category_l2": category_l2,
                    "category_l3": category_l3,
                    "event_timestamp": event_timestamp
                })

        # Step 2: Convert extracted data to a DataFrame directly
        df = pd.DataFrame(extracted_data)

        # Step 3: Clean category fields (remove extra characters if necessary)
        df["category_l1"] = df["category_l1"].str.strip("[]").str.replace("'", "")
        df["category_l2"] = df["category_l2"].str.strip("[]").str.replace("'", "")

        # Step 4: Save the preprocessed data to the final output file
        df.to_csv(output_file, index=False)

        print(f"Data successfully extracted, processed, and saved to {output_file}")
        #self.upload_to_gcs(output_file, '', 'path/in/bucket/to/save.csv')
        self.upload_to_gcs(output_file, 'cml-datalake-dev', 'buy_again/flattened_order_data_with_clean_categories.csv')





    def clean_category(self, category):
        if isinstance(category, list):
            # Return the first item in the list if it's valid, or "Unknown" if empty
            return category[0] if category else "Unknown"
        return category  # Return as is if it's not a list
    
    def upload_to_gcs(self, local_file_path, bucket_name, gcs_file_path):
    
        # Initialize the GCS client
        storage_client = storage.Client()

        # Get the bucket
        bucket = storage_client.get_bucket(bucket_name)

        # Create a blob (the file in GCS)
        blob = bucket.blob(gcs_file_path)

        # Upload the file to GCS
        blob.upload_from_filename(local_file_path)

        print(f"File {local_file_path} uploaded to {gcs_file_path} in bucket {bucket_name}.")