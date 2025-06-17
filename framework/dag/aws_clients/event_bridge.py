import json
from datetime import datetime
from typing import Dict, List, Optional, Union

from botocore.exceptions import ParamValidationError

from iac.lib.aws_clients.boto3_clients import eventbridge
from iac.lib.logger.slog import slog

EVENT_TYPE = Union[str, Dict, List, int, float]


class EventBridgeClient:

    def __init__(self, bus_name: str):
        self.client = eventbridge()
        self.bus_name = bus_name
        self.events = []
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.flush()

    @classmethod
    def coerce_detail(cls, detail: EVENT_TYPE):
        if isinstance(detail, (list, dict)):
            return json.dumps(detail)
        return detail

    def append(self, source: str, detail_type: str, detail: EVENT_TYPE,
               resources: Optional[List[str]] = None,
               trace: Optional[str] = None):
        event = {
            'Time': datetime.utcnow(),
            'Source': source,
            'DetailType': detail_type,
            'Detail': self.coerce_detail(detail),
            'EventBusName': self.bus_name
        }
        if resources:
            event['Resources'] = resources
        if trace:
            event['TraceHeader'] = trace
        slog.info('Adding event to entries', entry=event)
        self.events.append(
            event
        )

    def flush(self):
        try:
            self.result = self.client.put_events(Entries=self.events)
        except ParamValidationError as perr:
            slog.error('Invalid PutEvents Call', entries=self.events, error=perr)
            raise perr
        except BaseException as berr:
            slog.error('Unable to put events', error=berr)
            raise berr
        finally:
            self.events.clear()
