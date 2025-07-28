import gzip, time, json
from aiokafka import AIOKafkaProducer
from aiohttp import ClientSession
import asyncio
import logging
from google.transit import gtfs_realtime_pb2
from protobuf_to_dict import protobuf_to_dict

logger = logging.getLogger(__name__)

LINE_MAP = {
    "-ace": "ace",
    "-bdfm": "bdfm",
    "-g": "g",
    "-jz": "jz",
    "-nqrw": "nqrw",
    "-l": "l",
    "": "1234567S",
}


async def poll(feed_cfg, session: ClientSession, producer: AIOKafkaProducer):
    while True:
        try:
            t0 = time.time()
            url = feed_cfg["url"]
            if feed_cfg["name"] == "mta_subway_lines":
                payload = {}
                for line in LINE_MAP.keys():
                    r = await session.get(url.format(line=line), timeout=5)
                    content = await r.read()
                    feed = gtfs_realtime_pb2.FeedMessage()
                    feed.ParseFromString(content)
                    payload = protobuf_to_dict(feed)
                    payload[LINE_MAP[line]] = payload["entity"]
            else:
                r = await session.get(url, timeout=5)
                content = await r.read()
                payload = json.loads(content.decode())["entity"]
            payload = gzip.compress(payload)
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
