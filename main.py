from pathlib import Path
from retry import retry
from zipfile import ZipFile, BadZipFile
import pulsar
import os
import time
from viaa.configuration import ConfigParser
from viaa.observability import logging

from cloudevents.events import (
    CEMessageMode,
    Event,
    EventAttributes,
    EventOutcome,
    PulsarBinding,
)

configParser = ConfigParser()
log = logging.get_logger(__name__, config=configParser)

APP_NAME = "unzip-service"
client = pulsar.Client(
    f"pulsar://{configParser.app_cfg['pulsar']['host']}:{configParser.app_cfg['pulsar']['port']}"
)


@retry(pulsar.ConnectError, tries=10, delay=1, backoff=2)
def create_producer_1_x():
    return client.create_producer(
        configParser.app_cfg["unzip-service"]["producer_topic_1_x"]
    )


@retry(pulsar.ConnectError, tries=10, delay=1, backoff=2)
def create_producer_2_x():
    return client.create_producer(
        configParser.app_cfg["unzip-service"]["producer_topic_2_x"]
    )


@retry(pulsar.ConnectError, tries=10, delay=1, backoff=2)
def subscribe():
    return client.subscribe(
        configParser.app_cfg["unzip-service"]["consumer_topic"],
        subscription_name=APP_NAME,
    )


producer_1_x = create_producer_1_x()
producer_2_x = create_producer_2_x()

consumer = subscribe()


def handle_event(event: Event) -> bool:
    """
    Handles an incoming pulsar event.
    If the event has a succesful outcome, the incoming zip will be extracted and an event will be produced.
    """
    if not event.has_successful_outcome():
        log.info(f"Dropping non succesful event: {event.get_data()}")
        return False

    log.debug(f"Incoming event: {event.get_data()}")

    try:
        filename = event.get_data()["destination"]
    except KeyError as e:
        error_msg = (
            f"The data payload of the incoming event is not conform. Missing key: {e}."
        )
        log.warning(error_msg)
        outcome = EventOutcome.FAIL
        data = {
            "source": "N/A",
            "message": error_msg,
        }
        send_event(data, outcome, event.correlation_id)
        return False

    # Use the folder of the incoming zip file to derive a part of the target folder.
    # Example: /path/to/sipin/incoming/file.zip -> /path/to/sipin/
    directory_base_path = Path(filename).parents[1]
    basename = os.path.basename(filename)
    extract_path = os.path.join(
        directory_base_path,
        configParser.app_cfg["unzip-service"]["target_folder"],
        basename,
    )
    data = {"destination": extract_path, "source": filename}

    try:
        with ZipFile(filename, "r") as zipObj:
            # Extract all the contents of zip file in the target directory
            zipObj.extractall(extract_path)

        outcome = EventOutcome.SUCCESS
        data["message"] = f"The bag '{basename}' unzipped in '{extract_path}'"
        log.info(data["message"])
    except BadZipFile:
        outcome = EventOutcome.FAIL
        data["message"] = f"{filename} is not a a valid zipfile."
        log.warning(data["message"])
    except OSError as e:
        outcome = EventOutcome.FAIL
        data["message"] = f"Error when unzipping: {str(e)}"
        log.warning(data["message"])

    # Check if SIP 1.X or 2.X
    filename_mets_1_x = os.path.join(extract_path, "data", "mets.xml")
    filename_mets_2_x = os.path.join(extract_path, "METS.xml")

    if filename_mets_1_x.exists():
        send_event(producer_1_x, data, outcome, event.correlation_id)
    elif filename_mets_2_x.exists():
        send_event(producer_2_x, data, outcome, event.correlation_id)
    else:
        send_event(producer_2_x, data, outcome, event.correlation_id)

    return outcome == EventOutcome.SUCCESS


def send_event(producer, data: dict, outcome: EventOutcome, correlation_id: str):
    attributes = EventAttributes(
        type=configParser.app_cfg["unzip-service"]["producer_topic"],
        source=APP_NAME,
        subject=data["source"],
        correlation_id=correlation_id,
        outcome=outcome,
    )

    event = Event(attributes, data)
    create_msg = PulsarBinding.to_protocol(event, CEMessageMode.STRUCTURED.value)

    producer.send(
        create_msg.data,
        properties=create_msg.attributes,
        event_timestamp=event.get_event_time_as_int(),
    )


if __name__ == "__main__":
    try:
        while True:
            msg = consumer.receive()
            event = PulsarBinding.from_protocol(msg)

            result = handle_event(event)

            consumer.acknowledge(msg)

            if result:
                time.sleep(int(configParser.app_cfg["unzip-service"]["sleep_time"]))
    except KeyboardInterrupt:
        client.close()
        exit()
