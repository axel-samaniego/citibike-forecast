import os
import asyncio
import logging
from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer
from fetchers.weather import WeatherFetcher
from fetchers.citibike import CitibikeFetcher

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get Kafka broker address from environment variable, default to localhost
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "citibike-data"


async def main():
    try:
        # Initialize producer
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: str(v).encode("utf-8"),
        )

        # Start the producer
        await producer.start()
        logger.info(f"Connected to Kafka broker at {KAFKA_BROKER}")

        # Initialize fetchers
        weather_fetcher = WeatherFetcher()
        citibike_fetcher = CitibikeFetcher()

        while True:
            try:
                # Fetch data
                weather_data = await weather_fetcher.fetch()
                citibike_data = await citibike_fetcher.fetch()

                # Combine data
                combined_data = {"weather": weather_data, "citibike": citibike_data}

                # Send to Kafka
                await producer.send_and_wait(TOPIC, combined_data)
                logger.info("Data sent to Kafka successfully")

            except Exception as e:
                logger.error(f"Error in main loop: {e}")

            # Wait before next fetch
            await asyncio.sleep(300)  # 5 minutes

    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
