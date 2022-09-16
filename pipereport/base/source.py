from abc import ABC, abstractmethod
from typing import Dict, Iterator, List, Optional, Tuple

from pipereport.base.sink import BaseSink
from pipereport.telemetry.telemetry import Telemetry


class BaseSource(ABC):
    def __init__(self, *args, **kwargs):
        self.sinks: Dict[str, BaseSink] = {}
        self.attrs = kwargs

        self.type = self.required_field("type")
        self.name = self.required_field("name")
        self.sink_names = self.required_field("sink_names")
        self.processes = kwargs.get("processes", -1)
        self.concurrency = kwargs.get("concurrency", -1)

    def required_field(self, field_name: str):
        field = self.attrs.pop(field_name, None)
        if field is None:
            raise Exception(f"Field '{field_name}' is not specified for sink!")
        return field

    def required_credential(self, credential_name: str):
        credentials = self.attrs.get("credentials", {})
        if credential_name not in credentials:
            raise Exception(
                f"Credential '{credential_name}' is not specified for sink!"
            )
        return credentials[credential_name]

    def connect(self):
        raise NotImplementedError()

    def connect_sinks(self):
        for _, sink in self.sinks.items():
            sink.connect()

    def add_sink(self, sink: BaseSink):
        self.sinks[sink.name] = sink

    def get_telemetry(self):
        return {sn: self.sinks[sn].telemetry for sn in self.sink_names}

    def write_block(
        self,
        source_iterator: Iterator[Tuple],
        object_id: str,
        blocksize: int = -1,
        columns: Optional[List[str]] = None,
        sep: str = "\t",
    ):
        sink_count = len(self.sink_names)
        if sink_count > 1:
            # TODO
            pass
        else:
            self.sinks[self.sink_names[0]].write_block(
                source_iterator=source_iterator,
                object_id=object_id,
                blocksize=blocksize,
                columns=columns,
                sep=sep,
            )

    @abstractmethod
    def run(self):
        raise NotImplementedError()
