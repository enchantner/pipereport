from __future__ import annotations


class BaseTemplateRegistry:

    def __init__(self, *args, **kwargs):
        pass

    def get_template_by_name(self, template_name: str) -> dict:
        raise NotImplementedError()
