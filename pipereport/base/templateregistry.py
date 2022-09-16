from __future__ import annotations

from abc import ABC, abstractmethod


class BaseTemplateRegistry(ABC):
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_template_by_name(self, template_name: str) -> dict:
        raise NotImplementedError()
