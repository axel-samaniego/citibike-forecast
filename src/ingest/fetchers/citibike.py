import gzip, time
from aiokafka import AIOKafkaProducer
from aiohttp import ClientSession
import asyncio


async def poll(feed_cfg, session: ClientSession, producer: AIOKafkaProducer):
    while True:
        t0 = time.time()
        r = await session.get(feed_cfg["url"], timeout=5)
        payload = gzip.compress(await r.read())
        await producer.send_and_wait(
            topic=feed_cfg["topic"],
            key=feed_cfg["name"].encode(),
            value=payload,
            timestamp_ms=int(t0 * 1000),
        )
        await asyncio.sleep(feed_cfg.get("period", 30))
