from aiokafka import AIOKafkaConsumer
import asyncio
import json
import gzip
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("consumer.log"), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


async def consume():
    # Replace with your Kafka broker's IP address
    consumer = AIOKafkaConsumer(
        "citibike.station_status",  # Add all topics you want to consume
        "citibike.station_info",
        "weather.forecast",
        bootstrap_servers="192.168.1.100:9092",  # Replace with your broker's IP
        group_id="my-group",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                # Decompress the payload
                decompressed = gzip.decompress(msg.value)
                # Parse the JSON data
                data = json.loads(decompressed)
                logger.info(f"Received: {data}")
                # Process the data (e.g., store in a database, write to a file, etc.)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume())
