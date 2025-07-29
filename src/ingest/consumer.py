from aiokafka import AIOKafkaConsumer
import asyncio
import json
import gzip
import logging
from dotenv import load_dotenv
from datetime import datetime
import os

load_dotenv()
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("consumer.log"), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
GROUP_ID = os.getenv("KAFKA_GROUP_ID")
SAVE_PATH = os.getenv("DATA_SAVE_PATH")


async def consume():
    # Replace with your Kafka broker's IP address
    consumer = AIOKafkaConsumer(
        "citibike.station_status",  # Add all topics you want to consume
        "citibike.station_info",
        "weather.forecast",
        "mta.subway.lines",
        "mta.subway.alerts",
        "mta.bus.alerts",
        bootstrap_servers=KAFKA_BROKER,  # Replace with your broker's IP
        group_id=GROUP_ID,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                # Decompress the payload
                decompressed = gzip.decompress(msg.value)
                # Parse the JSON data
                data = json.loads(decompressed)
                logger.info(f"Received data from topic: {msg.topic}")
                # Process the data (e.g., store in a database, write to a file, etc.)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{msg.topic}_{timestamp}.json"
                save_dir = os.path.join(SAVE_PATH, msg.topic)
                os.makedirs(save_dir, exist_ok=True)

                filepath = os.path.join(save_dir, filename)
                with open(filepath, "w") as f:
                    json.dump(data, f, indent=2)

            except Exception as e:
                logger.error(
                    f"Error processing message for topic: {msg.topic}: \n error: {e}"
                )
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume())
