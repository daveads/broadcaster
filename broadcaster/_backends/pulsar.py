import asyncio
import logging
import typing
from urllib.parse import urlparse
import pulsar
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

        self._client_config = pulsar.ClientOptions(
            connection_timeout_ms=30000,  # Increase timeout to 30 seconds
            operation_timeout_seconds=30  # Increase operation timeout
        )

    async def connect(self) -> None:
        try:
            logging.info("Connecting to brokers")
            self._client = await asyncio.to_thread(pulsar.Client, self._service_url)

            self._producer = await asyncio.to_thread(
                self._client.create_producer, "broadcast",
                send_timeout_ms=30000,
            )
            self._consumer = await asyncio.to_thread(
                self._client.subscribe,
                "broadcast",
                subscription_name="broadcast_subscription",
                consumer_type=pulsar.ConsumerType.Shared,
                receiver_queue_size=10000
            )
            logging.info("Successfully connected to brokers")
        except Exception as e:
            logging.error(e)
            raise e

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
        await asyncio.to_thread(self._producer.send, encoded_message)

    async def next_published(self) -> Event:
        while True:
            try:
                msg = await asyncio.to_thread(self._consumer.receive)
                channel, content = msg.data().decode("utf-8").split(":", 1)
                await asyncio.to_thread(self._consumer.acknowledge, msg)
                return Event(channel=channel, message=content)

            except asyncio.CancelledError:
                # cancellation
                logging.info("next_published task is being cancelled")
                raise

            except Exception as e:
                logging.error(f"Error in next_published: {e}")
                raise
