import json

from typing import List, Optional


class Telemetry:
    
    def __init__(self):
        self.objects = {}

    def add_object(self, object_id: str, columns: Optional[List[str]] = None):
        self.objects[object_id] = {
            "columns": columns,
            "entries": 0
        }

    def add_entries(self, object_id: str, entries: int = 1):
        # TODO: thread/process safety?
        if object_id in self.objects:
            self.objects[object_id]["entries"] += entries

    def __str__(self):
        return json.dumps(self.objects)

