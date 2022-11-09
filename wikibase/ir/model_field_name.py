from typing import Type


def model_field_name(django_field: Type) -> str:
    django_model = django_field.model
    return f'{django_model.__module__}.{django_model._meta.object_name}.{django_field.name}'
