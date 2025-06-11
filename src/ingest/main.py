from fetchers import citibike, weather
from aiohttp import ClientSession
import yaml
from aiokafka import AIOKafkaProducer
import asyncio

FETCHER_MAP = {
    "citibike": citibike.poll,
    "weather": weather.poll,
}


async def main():
    feeds = yaml.safe_load(open("src/ingest/feeds.yaml", "r"))
    feeds = feeds["feeds"]
    async with ClientSession() as session, AIOKafkaProducer() as producer:
        for feed in feeds:
            fetcher = FETCHER_MAP[feed["type"]]
            await fetcher(feed, session, producer)


if __name__ == "__main__":
    asyncio.run(main())
