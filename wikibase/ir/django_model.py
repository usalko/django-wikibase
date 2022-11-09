from dataclasses import Field
from functools import lru_cache
from json import dumps
from threading import local as thread_local

from django.db.models import Model
from wikibase.ir.circuit_breaker import CircuitBreaker
from wikibase.ir.model_type_name import model_type_name


class DjangoModel(dict):

    @lru_cache(maxsize=1024)
    def __init__(self, model: Model):
        dict.__init__(self,
                      type=model_type_name(model),
                      table_name=model._meta.db_table,
                      application=model._meta.app_label,
                      fields=[CircuitBreaker._django_field(field) for field in model._meta.concrete_fields],
                      pk=model._meta.pk.name if model._meta.pk else None)

    def __repr__(self) -> str:
        return dumps(self)

    def __hash__(self):
        if len(self) == 0:
            return 0
        return hash(str(self['application'] + '/' + self['table_name']))

    def __eq__(self, other):
        return len(self) == len(other) and (len(self) == 0 or (self['application'] == other['application'] and self['table_name'] == other['table_name']))
