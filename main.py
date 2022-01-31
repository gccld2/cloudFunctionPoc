import base64
import json
import logging
import traceback
from datetime import datetime

from google.api_core import retry
from google.cloud import bigquery
from google.cloud import pubsub_v1

PROJECT_ID = 'bdtest-335709'
BQ_DATASET = 'cloud_functions_poc_ds'
BQ_TABLE = 'cloud_functions_poc'
PS = pubsub_v1.PublisherClient()
ERROR_TOPIC = f'projects/{PROJECT_ID}/topics/streaming_error_topic'
BQ = bigquery.Client()


def streaming(event, context):
    """This function is executed whenever an event is published to Cloud PubSub
    Args:
         event (dict):  The dictionary with data specific to this type of
                        event. The `@type` field maps to
                         `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
                        The `data` field maps to the PubsubMessage data
                        in a base64-encoded string. The `attributes` field maps
                        to the PubsubMessage attributes if any is present.
         context (google.cloud.functions.Context): Metadata of triggering event
                        including `event_id` which maps to the PubsubMessage
                        messageId, `timestamp` which maps to the PubsubMessage
                        publishTime, `event_type` which maps to
                        `google.pubsub.topic.publish`, and `resource` which is
                        a dictionary that describes the service API endpoint
                        pubsub.googleapis.com, the triggering topic's name, and
                        the triggering event type
                        `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
    Returns:
        None. The output is written to BigQuery.

    """
    if 'data' in event:
        row = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        row['poc_datetime_formatted'] = datetime.fromtimestamp(row['ts']).strftime("%Y-%m-%d %H:%M:%S")
        try:
            _insert_into_bigquery(row)
        except Exception:
            _handle_error(row)


def _insert_into_bigquery(row):
    table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
    errors = BQ.insert_rows_json(table,
                                 json_rows=[row],
                                 retry=retry.Retry(deadline=30))
    if errors:
        raise BigQueryError(errors)


def _handle_error(row):
    message = f'Error streaming event {row}. Cause: {traceback.format_exc()}'
    PS.publish(ERROR_TOPIC, data=message.encode('utf-8'))
    logging.error(message)


class BigQueryError(Exception):
    """Exception raised whenever a BigQuery error happened"""

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)
