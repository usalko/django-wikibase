from functools import lru_cache
from json import dumps

from django.db.models import Field
from wikibase.ir.circuit_breaker import CircuitBreaker


class DjangoProperty(dict):

    @lru_cache(maxsize=1024*10)
    def __init__(self, field: Field):
        dict.__init__(self,
                      attribute_name=field.attname,
                      property_name=self._property_name(field),
                      property_type=type(field).__name__,
                      related_models=CircuitBreaker._related_models(field))

    @staticmethod
    def _property_name(field: Field):
        if type(field).__name__ == 'ForeignKey':
            return field.column[:len(field.column) - 3] if field.column.endswith('_id') else field.column
        return field.column

    def __repr__(self) -> str:
        return dumps(self)

    def __hash__(self):
        if len(self) == 0:
            return 0
        return hash(self['property_name'] + '/' + self['property_type'])

    def __eq__(self, other):
        return len(self) == len(other) and (len(self) == 0 or (self['property_name'] == other['property_name'] and self['property_type'] == other['property_type']))
