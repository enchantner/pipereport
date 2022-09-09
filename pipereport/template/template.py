from __future__ import annotations

import json
import importlib

from pipereport.base.source import BaseSource
from pipereport.base.sink import BaseSink
from pipereport.sink import get_sink


class Template:

    def __init__(self):
        self.sources = {}
        self.sinks = {}

    def add_source(self, source: BaseSource):
        self.sources[source.name] = source

    def add_sink(self, sink: BaseSink):
        self.sinks[sink.name] = sink

    @staticmethod
    def parse_with_config(template_dict: dict, config: dict) -> Template:
        tmpl = Template()
        tmpl_sinks_info = template_dict["sinks"]
        tmpl_src_info = template_dict["sources"]
        conf_sinks_info = config.get("sinks", [])
        conf_src_info = config.get("sources", [])

        tmpl_sinks_index = {}
        for sink_attrs in tmpl_sinks_info:
            if "name" not in sink_attrs:
                raise Exception(f"Name not specified in template for sink: {json.dumps(sink_attrs)}")
            tmpl_sinks_index[sink_attrs["name"]] = sink_attrs

        tmpl_src_index = {}
        for src_attrs in tmpl_src_info:
            if "name" not in src_attrs:
                raise Exception(f"Name not specified in template for source: {json.dumps(sink_attrs)}")
            tmpl_src_index[src_attrs["name"]] = src_attrs

        for sink_attrs in conf_sinks_info:
            if "name" not in sink_attrs:
                raise Exception(f"Name not specified in config for sink: {json.dumps(sink_attrs)}")
            tmpl_sinks_index[sink_attrs["name"]].update(sink_attrs)

        for src_attrs in conf_src_info:
            if "name" not in src_attrs:
                raise Exception(f"Name not specified in config for source: {json.dumps(src_attrs)}")
            tmpl_src_index[src_attrs["name"]].update(src_attrs)

        sinks = {}
        for sink_name, sink_attrs in tmpl_sinks_index.items():
            sink = get_sink(sink_attrs["type"])(**sink_attrs)
            tmpl.add_sink(sink)
            sinks[sink_name] = sink

        for _, src_attrs in tmpl_src_index.items():
            try:
                plugin = importlib.import_module(f'pipereport.source.{src_attrs["type"]}')
            except ImportError:
                # todo: explicit handle
                raise
            src = plugin.Source(**src_attrs)
            for sink_name in src_attrs["sink_names"]:
                src.add_sink(sinks[sink_name])
            tmpl.add_source(src)
        return tmpl

