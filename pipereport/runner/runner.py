from typing import Optional

from pipereport.base.templateregistry import BaseTemplateRegistry

from pipereport.template.template import Template
from pipereport.template.registry import FSTemplateRegistry


class PipeRunner:

    def __init__(self, template_registry: Optional[BaseTemplateRegistry] = None):  
        self.template_registry = (
            FSTemplateRegistry(".") if template_registry is None else template_registry
        )

    def run_from_config(self, config: dict):
        tmpl_dict = self.template_registry.get_template_by_name(config['template_name'])
        tmpl = Template.parse_with_config(tmpl_dict, config)
        for src in tmpl.sources.values():
            try:
                src.connect()
            except NotImplementedError:
                pass
            src.connect_sinks()
            src.run()

