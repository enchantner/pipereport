"""
Base source implementation
"""

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
        Check if credential for Source is present by name and return its value

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

    def connect(self):
        """
        Connect to Source (called automatically by runner)
        """
        pass

    def connect_sinks(self):
        """
        Call sink.connect() on all sinks in self.sinks
        """
        for _, sink in self.sinks.items():
            sink.connect()

    def add_sink(self, sink: BaseSink):
        """
        Add Sink instance to self.sinks

        Args:
            sink (BaseSink): sink instance
        """
        self.sinks[sink.name] = sink

    def get_telemetry(self):
        """
        Get telemetry data for all the Sinks in self.sinks

        Returns:
            (Dict[str, Telemetry]): dictionary of telemetry instances for all sinks in self.sinks
        """
        return {sn: self.sinks[sn].telemetry for sn in self.sink_names}

    @staticmethod
    def validate_config(config: dict):
        """
        Validate passed config. Called automatically by a runner. By default does nothing and has to raise exceptions in inherited classes

        Args:
            config (dict): parsed JSON config for Source as dictionary
        """
        pass

    def write_block(
        self,
        source_iterator: Iterator[Tuple],
        object_id: str,
        blocksize: int = -1,
        columns: Optional[List[str]] = None,
        sep: str = "\t",
    ):
        """
        IMPORTANT: currently only the first Sink instance is used!
        Writes a block of data of a given size from an iterator into a Sink

        Args:
            source_iterator (Iterator[Tuple[str]]): iterator over tuples with fields represented as strings
            object_id (str): string representing an object written, e.g. file path in S3
            blocksize (int): number of entries to take from an iterator
            columns (Optional[List[str]]): list of column names to use in telemetry
            sep (str): separator to use when writing data to a Sink
            
        """
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
        """
        Run all the logic on retrieving data from source. This method must be implemented in subclasses.
        """
        raise NotImplementedError()
