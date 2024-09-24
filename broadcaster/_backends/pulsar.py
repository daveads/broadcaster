import asyncio
import logging
import typing
from urllib.parse import urlparse
from pulsar import Client, ProducerConfiguration, ConsumerConfiguration, CompressionType, ConsumerType, ProducerAccessMode, PulsarException
from broadcaster._base import Event
from .base import BroadcastBackend

class PulsarBackend(BroadcastBackend):
    def __init__(self, url: str):
        parsed_url = urlparse(url)
        self._host = parsed_url.hostname or "localhost"
        self._port = parsed_url.port or 6650
        self._service_url = f"pulsar://{self._host}:{self._port}"
        self._client = None
        self._producer = None
        self._consumer = None

    async def connect(self) -> None:
        try:
            logging.info(f"Connecting to Pulsar broker at {self._service_url}")
            self._client = await asyncio.to_thread(
                Client,
                self._service_url,
                operation_timeout_seconds=30  # Increase operation timeout
            )

            producer_conf = ProducerConfiguration()
            producer_conf.send_timeout_millis(30000)  # Set send timeout

            self._producer = await asyncio.to_thread(
                self._client.create_producer,
                "broadcast",
                producer_conf
            )

            consumer_conf = ConsumerConfiguration()
            consumer_conf.consumer_type(ConsumerType.Shared)
            
            self._consumer = await asyncio.to_thread(
                self._client.subscribe,
                "broadcast",
                subscription_name="broadcast_subscription",
                consumer_type=ConsumerType.Shared,
                consumer_conf=consumer_conf
            )
            logging.info("Successfully connected to Pulsar broker")
        except PulsarException as e:
            logging.error(f"Pulsar error while connecting: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error while connecting to Pulsar: {e}")
            raise

    async def disconnect(self) -> None:
        if self._producer:
            await asyncio.to_thread(self._producer.close)
        if self._consumer:
            await asyncio.to_thread(self._consumer.close)
        if self._client:
            await asyncio.to_thread(self._client.close)

    async def subscribe(self, channel: str) -> None:
        # In this implementation, we're using a single topic 'broadcast'
        # So we don't need to do anything here
        pass

    async def unsubscribe(self, channel: str) -> None:
        # Similarly, we don't need to do anything here
        pass

    async def publish(self, channel: str, message: typing.Any) -> None:
        encoded_message = f"{channel}:{message}".encode("utf-8")
        
        def send_callback(producer, msg):
            logging.info(f"Message sent: {msg}")

        await asyncio.to_thread(self._producer.send_async, encoded_message, send_callback)

    async def next_published(self) -> Event:
        while True:
            try:
                msg = await asyncio.to_thread(self._consumer.receive)
                channel, content = msg.data().decode("utf-8").split(":", 1)
                await asyncio.to_thread(self._consumer.acknowledge, msg)
                return Event(channel=channel, message=content)
            except asyncio.CancelledError:
                logging.info("next_published task is being cancelled")
                raise
            except Exception as e:
                logging.error(f"Error in next_published: {e}")
                raise
