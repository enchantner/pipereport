from __future__ import annotations

import importlib
import json

from copy import deepcopy

from pipereport.base.sink import BaseSink
from pipereport.base.source import BaseSource
from pipereport.sink import get_sink
from pipereport.telemetry.telemetry import Telemetry


class Template:
    def __init__(self):
        self.sources = {}
        self.sinks = {}
        self.compiled = {}

    def add_source(self, source: BaseSource):
        self.sources[source.name] = source

    def add_sink(self, sink: BaseSink):
        self.sinks[sink.name] = sink

    def __str__(self):
        return json.dumps(self.compiled, indent=4)

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
                raise Exception(
                    f"Name not specified in template for sink: {json.dumps(sink_attrs)}"
                )
            tmpl_sinks_index[sink_attrs["name"]] = sink_attrs

        tmpl_src_index = {}
        for src_attrs in tmpl_src_info:
            if "name" not in src_attrs:
                raise Exception(
                    f"Name not specified in template for source: {json.dumps(src_attrs)}"
                )
            tmpl_src_index[src_attrs["name"]] = src_attrs

        for sink_attrs in conf_sinks_info:
            if "name" not in sink_attrs:
                raise Exception(
                    f"Name not specified in config for sink: {json.dumps(sink_attrs)}"
                )
            if "parameters" in sink_attrs and "parameters" in tmpl_sinks_index[sink_attrs["name"]]:
                tmpl_sinks_index[sink_attrs["name"]]["parameters"].update(sink_attrs["parameters"])
                del sink_attrs["parameters"]
            tmpl_sinks_index[sink_attrs["name"]].update(sink_attrs)

        for src_attrs in conf_src_info:
            if "name" not in src_attrs:
                raise Exception(
                    f"Name not specified in config for source: {json.dumps(src_attrs)}"
                )
            if "fields" in src_attrs:
                raise Exception(f"Cannot override 'fields' in config for source '{src_attrs['name']}'")
            if "parameters" in src_attrs and "parameters" in tmpl_src_index[src_attrs["name"]]:
                tmpl_src_index[src_attrs["name"]]["parameters"].update(src_attrs["parameters"])
                del src_attrs["parameters"]
            tmpl_src_index[src_attrs["name"]].update(src_attrs)

        telemetry = Telemetry()

        tmpl.compiled = deepcopy(template_dict)

        sinks = {}
        for sink_name, sink_attrs in tmpl_sinks_index.items():
            sink = get_sink(sink_attrs["type"])(**sink_attrs)
            sink.enable_telemetry(telemetry)
            tmpl.add_sink(sink)
            sinks[sink_name] = sink

        for _, src_attrs in tmpl_src_index.items():
            try:
                plugin = importlib.import_module(
                    f'pipereport.source.{src_attrs["type"]}'
                )
            except ImportError:
                # todo: explicit handle
                raise
            plugin.Source.validate_config(src_attrs)
            src = plugin.Source(**src_attrs)
            for sink_name in src_attrs["sink_names"]:
                src.add_sink(sinks[sink_name])
            tmpl.add_source(src)
        return tmpl
