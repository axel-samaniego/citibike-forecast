import gzip, time
from aiokafka import AIOKafkaProducer
from aiohttp import ClientSession
import asyncio
import logging
import hashlib
from datetime import datetime

logger = logging.getLogger(__name__)


async def poll(feed_cfg, session: ClientSession, producer: AIOKafkaProducer):
    while True:
        try:
            t0 = time.time()
            r = await session.get(feed_cfg["url"], timeout=5)
            payload = gzip.compress(await r.read())
            await producer.send_and_wait(
                topic=feed_cfg["topic"],
                key=feed_cfg["name"].encode(),
                value=payload,
                timestamp_ms=int(t0 * 1000),
            )
            logger.info(f"Successfully fetched and sent data for {feed_cfg['name']}")
        except Exception as e:
            logger.error(f"Error in {feed_cfg['name']}: {e}")
        finally:
            await asyncio.sleep(feed_cfg.get("period", 30))


def parse_weather(data):
    records = []
    ts = datetime.now()
    for row in data["data"]["stations"]:
        station_id = row["station_id"]
        hash_id = f"{station_id}_{ts.isoformat()}"
        records.append(
            {
                "id": hashlib.md5(hash_id.encode()).hexdigest()[:156],
                "station_id": station_id,
                "num_bikes_available": row["num_bikes_available"],
                "num_docks_available": row["num_docks_available"],
                "is_installed": row["is_installed"],
                "is_renting": row["is_renting"],
                "is_returning": row["is_returning"],
                "last_reported": row["last_reported"],
                "last_updated": data["last_updated"] * 1000,
                "created_at": ts.isoformat(),
            }
        )

    return data
