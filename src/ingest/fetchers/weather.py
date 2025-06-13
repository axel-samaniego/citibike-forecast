import gzip, time
from aiokafka import AIOKafkaProducer
from aiohttp import ClientSession
import asyncio
from datetime import datetime
import json
import logging

# Create a logger for this module
logger = logging.getLogger(__name__)

NY_ZONES = {
    "Manhattan": "NYZ072",
    "Bronx": "NYZ073",
    "Brooklyn": "NYZ075",
    "Queens": "NYZ178",
    "Staten Island": "NYZ074",
}


async def poll(feed_cfg, session: ClientSession, producer: AIOKafkaProducer):
    while True:
        try:
            t0 = time.time()
            base_url = feed_cfg["url"]
            for bourough, zone in NY_ZONES.items():
                r = await session.get(base_url.format(zone=zone), timeout=5)
                data = await r.json()
                current_forecast = data["properties"]["periods"][0]["detailedForecast"]
                json_data = {
                    "forecast": current_forecast,
                    "bourough": bourough,
                    "weather_zone": zone,
                    "timestamp": datetime.now().isoformat(),
                }
                payload = gzip.compress(json.dumps(json_data).encode())
                await producer.send_and_wait(
                    topic=feed_cfg["topic"],
                    key=feed_cfg["name"].encode(),
                    value=payload,
                    timestamp_ms=int(t0 * 1000),
                )
                logger.info(f"Successfully fetched and sent data for {bourough}")
        except Exception as e:
            logger.error(f"Error in {feed_cfg['name']}: {e}")
        finally:
            await asyncio.sleep(feed_cfg.get("period", 30))
