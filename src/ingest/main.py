from src.ingest.fetchers import citibike, weather, mta
from aiohttp import ClientSession
import yaml
from aiokafka import AIOKafkaProducer
import asyncio
import logging
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("ingest.log"), logging.StreamHandler()],
)


# Create a logger for this module
logger = logging.getLogger(__name__)

FETCHER_MAP = {
    "citibike": citibike.poll,
    "weather": weather.poll,
    "mta": mta.poll,
}


async def main():
    feeds = yaml.safe_load(open("src/ingest/feeds.yaml", "r"))
    feeds = feeds["feeds"]
    async with ClientSession() as session, AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER
    ) as producer:
        tasks = []
        for feed in feeds:
            fetcher = FETCHER_MAP[feed["type"]]
            task = asyncio.create_task(fetcher(feed, session, producer))
            tasks.append(task)

        await asyncio.gather(*tasks)

    session.close()
    producer.close()


if __name__ == "__main__":
    asyncio.run(main())
