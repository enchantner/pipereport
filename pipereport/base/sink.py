from abc import ABC, abstractmethod
from typing import Iterator, List, Optional, Tuple

from pipereport.telemetry.telemetry import Telemetry


class BaseSink(ABC):
    def __init__(self, *args, **kwargs):
        self.attrs = kwargs
        self.name = self.required_field("name")
        self.telemetry = None

    @abstractmethod
    def connect(self):
        raise NotImplementedError()

    def required_field(self, field_name):
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

    def enable_telemetry(self, telemetry: Telemetry):
        self.telemetry = telemetry

    @abstractmethod
    def write_block(
        self,
        source_iterator: Iterator[Tuple],
        object_id: str,
        blocksize: int = -1,
        columns: Optional[List[str]] = None,
        sep: str = "\t",
    ):
        raise NotImplementedError()
