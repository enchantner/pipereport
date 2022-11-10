"""
Base sink implementation
"""


from abc import ABC, abstractmethod
from typing import Iterator, List, Optional, Tuple

from pipereport.telemetry.telemetry import Telemetry


class BaseSink(ABC):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.attrs = kwargs
        self.name = self.required_field("name")
        self.telemetry = None
        self.data_type = kwargs.pop("data_type", None)

    def connect(self):
        """
        Connect to Sink (called automatically by runner)
        """
        pass

    def required_field(self, field_name):
        """
        Check if field is present by name and return its value or raise an error

        Args:
            field_name (str): field name to lookup

        Returns:
            field value

        Raises:
            Exception: if a field with a given name is not found
        """
        field = self.attrs.pop(field_name, None)
        if field is None:
            raise Exception(f"Field '{field_name}' is not specified for sink!")
        return field

    def required_credential(self, credential_name: str):
        """
        Check if credential for Sink is present by name and return its value

        Args:
            credential_name (str): credential name to lookup

        Returns:
            credential value

        Raises:
            Exception: if a credential with a given name is not found
        """
        credentials = self.attrs.get("credentials", {})
        if credential_name not in credentials:
            raise Exception(
                f"Credential '{credential_name}' is not specified for sink!"
            )
        return credentials[credential_name]

    @property
    def telemetry_enabled(self):
        """
        Check if telemetry is enabled for a Sink instance

        Returns:
            bool: whether telemetry was enabled
        """
        return self.telemetry is not None

    def enable_telemetry(self, telemetry: Telemetry):
        """
        Enables telemetry for a Sink instance

        Args:
            telemetry (Telemetry): Telemetry instance to use
        """
        self.telemetry = telemetry

    @abstractmethod
    def write_block(
        self,
        source_iterator: Iterator[Tuple[str]],
        object_id: str,
        blocksize: int = -1,
        columns: Optional[List[str]] = None,
        sep: str = "\t",
    ):
        """
        Writes a block of data of a given size from an iterator into a Sink

        Args:
            source_iterator (Iterator[Tuple[str]]): iterator over tuples with fields represented as strings
            object_id (str): string representing an object written, e.g. file path in S3
            blocksize (int): number of entries to take from an iterator
            columns (Optional[List[str]]): list of column names to use in telemetry
            sep (str): separator to use when writing data to a Sink
            
        """
        raise NotImplementedError()
