"""
Base template registry implementation
"""


from __future__ import annotations


from abc import ABC, abstractmethod

import os




class BaseTemplateRegistry(ABC):
    def __init__(self, *args, **kwargs):
        pass

    def check_env(self, env, default=None, exc_text=""):
        val = os.getenv(env)
        if val is None:
            if default is not None:
                return default
            if exc_text != "":
                exc_text = ": " + exc_text
            raise Exception(f'{env} is not specified' + exc_text)
        return val

    @abstractmethod
    def get_template_by_name(self, template_name: str) -> dict:
        """
        Retrieve a template from a registry and return it

        Returns:
            template (dict): template data as a dictionary
        """
        raise NotImplementedError()
