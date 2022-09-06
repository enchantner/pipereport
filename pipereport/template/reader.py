import importlib


class Template:

    def __init__(self):
        self.sources = []

    def add_source(self, source):
        self.sources.append(source)


class TemplateReader:
    
    def read_template(self, template):
        src_info = template["sources"]
        tmpl = Template()
        for src_name, src_attrs in src_info.items():
            try:
                plugin = importlib.import_module(f'pipereport.source.{src_name}')
            except ImportError:
                # todo: explicit handle
                raise
            tmpl.add_source(plugin.Source(**src_attrs))
        return tmpl
