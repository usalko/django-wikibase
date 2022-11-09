from functools import lru_cache
from threading import local as thread_local
from typing import Any, Optional, Type

from django.db.models import Field, Model
from django.db.models.fields.related import ForeignKey
from wikibase.ir.get_stacktrace import get_stacktrace
from wikibase.ir.model_field_name import model_field_name
from wikibase.ir.model_type_name import model_type_name


@lru_cache(1)
def _django_model_type():
    from wikibase.ir.django_model import DjangoModel
    return DjangoModel


@lru_cache(1)
def _django_property_type():
    from wikibase.ir.django_property import DjangoProperty
    return DjangoProperty


class CircuitBreaker:

    storage = thread_local()

    @staticmethod
    def _field_in_creation(field: Field) -> Optional[Any]:
        fields = getattr(CircuitBreaker.storage, 'fields', None)
        if fields is None or not(model_field_name(field) in fields):
            return None
        return CircuitBreaker.storage.fields[model_field_name(field)]

    @staticmethod
    def _mark_creation(field: Field, django_property: Any):
        fields = getattr(CircuitBreaker.storage, 'fields', None)
        if fields is None:
            CircuitBreaker.storage.fields = dict()
        CircuitBreaker.storage.fields[model_field_name(
            field)] = django_property

    @staticmethod
    def _django_field(field: Field):
        django_field = CircuitBreaker._field_in_creation(field)
        if django_field:
            return django_field
        result = _django_property_type()(field)
        CircuitBreaker._mark_creation(field, result)
        return result

    @staticmethod
    def _model_in_creation(model: Model) -> Optional[Any]:
        models = getattr(CircuitBreaker.storage, 'models', None)
        if models is None or not (model_type_name(model) in models):
            return None
        return CircuitBreaker.storage.models[model_type_name(model)]

    @staticmethod
    def _mark_field_in_creation(field: Field, model: Model, django_model: Any):
        models = getattr(CircuitBreaker.storage, 'models', None)
        if models is None:
            CircuitBreaker.storage.models = dict()
        CircuitBreaker.storage.models[model_type_name(model)] = django_model

    @staticmethod
    def _django_model(field: Field, model: Model):
        django_model = CircuitBreaker._model_in_creation(model)
        if django_model:
            return django_model
        # depth = len(get_stacktrace())
        result = _django_model_type()(model)
        CircuitBreaker._mark_field_in_creation(field, model, result)
        return result

    @staticmethod
    def _related_models(field: Field):
        if isinstance(field, ForeignKey) and \
            not ('Tree' in str(type(field))) and \
            not (field.model == field.related_model):
            foreign_key: ForeignKey = field
            return [CircuitBreaker._django_model(field, foreign_key.related_model)]
        return None
