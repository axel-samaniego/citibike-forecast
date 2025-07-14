from fetchers import citibike, weather, mta
from aiohttp import ClientSession
import yaml
from aiokafka import AIOKafkaProducer
import asyncio
import logging

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
    async with ClientSession() as session, AIOKafkaProducer() as producer:
        for feed in feeds:
            try:
                fetcher = FETCHER_MAP[feed["type"]]
                await fetcher(feed, session, producer)
            except Exception as e:
                logger.error(f"Error in {feed['name']}: {e}")

    session.close()
    producer.close()


if __name__ == "__main__":
    asyncio.run(main())
