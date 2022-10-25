import os
from typing import Iterator, List, Optional, Tuple

from pipereport.base.sink import BaseSink


class LocalSink(BaseSink):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.local_dir = self.required_field("local_dir")

    def connect(self):
        pass
        
    def write_block(
        self,
        source_iterator: Iterator[Tuple],
        object_id: str,
        blocksize: int = -1,
        columns: Optional[List[str]] = None,
        sep: str = "\t",
    ):  
        if self.telemetry_enabled:
            self.telemetry.add_object(object_id, columns)

        if object_id.startswith("/"):
            object_id = object_id[1:]
 
        dirpath = os.path.join(self.local_dir, os.path.dirname(object_id))
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

        with open(os.path.join(dirpath, os.path.basename(object_id)), "w") as res:
            block_written = 0
            for entry in source_iterator:
                res.write(sep.join(entry) + "\n")
                block_written += 1
                if block_written == blocksize:
                    break
            if self.telemetry_enabled:
                self.telemetry.add_entries(object_id, block_written)

