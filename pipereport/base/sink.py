from typing import Iterator

class BaseSink:

    def __init__(self, *args, **kwargs):
        self.attrs = kwargs 
        self.name = self.required_field("name")

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

    def write_block(self, source_iterator: Iterator[str], object_id: str, blocksize: int=-1):
        raise NotImplementedError()
