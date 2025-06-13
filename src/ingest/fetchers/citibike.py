import gzip, time
from aiokafka import AIOKafkaProducer
from aiohttp import ClientSession
import asyncio
import logging

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
