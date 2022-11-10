"""
Simple runner that can parse template and run all Sources to write in the Sinks
"""

import sys
from typing import Optional

from pipereport.base.templateregistry import BaseTemplateRegistry

from pipereport.template.template import Template
from pipereport.template.registry import GitFSTemplateRegistry


class PipeRunner:
    """
    Simplest sequential runner
    """

    def __init__(self, template_registry: Optional[BaseTemplateRegistry] = None):
        super().__init__()
        self.template_registry = (
            GitFSTemplateRegistry() if template_registry is None else template_registry
        )
        
    def render_config(self, config: dict) -> Template:
        """
        Convert config dictionary into a Template instance

        Args:
            config (dict): config dictionary

        Returns:
            template (Template): parsed template

        """
        tmpl_dict = self.template_registry.get_template_by_name(config['template_name'])
        tmpl = Template.parse_with_config(tmpl_dict, config)
        return tmpl
    
    def print_config(self, config: dict):
        """
        Renders and prints a config from dictionary

        Args:
            config (dict): config dictionary
        """
        sys.stdout.write(str(self.render_config(config)))
         
    def run_from_config(self, config: dict):
        """
        Runs all Sources from a config dictionary

        Args:
            config (dict): config dictionary

        Returns:
            (Dict[str, Dict]): dictionary with a resulting telemetry dictionary for each sink name
        """
        tmpl_dict = self.template_registry.get_template_by_name(config["template_name"])
        tmpl = Template.parse_with_config(tmpl_dict, config)
        for src in tmpl.sources.values():
            try:
                src.connect()
            except NotImplementedError:
                pass
            src.connect_sinks()
            src.run()
        return {
            sn: sink.telemetry.dump()
            for sn, sink in tmpl.sinks.items()
        } 
