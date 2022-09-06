from pipereport.template.reader import TemplateReader


def run_from_config(template, config):
    reader = TemplateReader()
    tmpl = reader.read_template(template)
    print(tmpl.sources)
