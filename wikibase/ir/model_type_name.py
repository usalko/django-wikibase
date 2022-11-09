from typing import Type


def model_type_name(django_model: Type) -> str:
    return f'{django_model.__module__}.{django_model._meta.object_name}'
