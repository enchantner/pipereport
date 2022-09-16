from typing import Iterator, List, Optional

from pipereport.telemetry.telemetry import Telemetry


class BaseSink:

    def __init__(self, *args, **kwargs):
        self.attrs = kwargs 
        self.name = self.required_field("name")
        self.telemetry = None

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
            raise Exception(f"Credential '{credential_name}' is not specified for sink!")
        return credentials[credential_name]

    def enable_telemetry(self, telemetry: Telemetry):
        self.telemetry = telemetry

    def write_block(self, source_iterator: Iterator[str], object_id: str, blocksize: int=-1, columns: Optional[List[str]] = None):
        raise NotImplementedError()
