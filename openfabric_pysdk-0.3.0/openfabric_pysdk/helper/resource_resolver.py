from typing import Any, Dict, List, Tuple
from dataclasses import dataclass, field as dataclass_field
from marshmallow import Schema, fields, class_registry, missing, ValidationError
import io
import logging
import requests
import os
import importlib.util


from openfabric_pysdk.fields import Resource, DecimalField, PluginField

def get_schema_field_types(schema: Schema, path: str = "") -> Dict[str, type]:
    field_types = {}

    for field_name, field_obj in schema.fields.items():
        full_path = f"{path}.{field_name}" if path else field_name

        if isinstance(field_obj, fields.Nested):
            nested_types = get_schema_field_types(field_obj.schema, full_path)
            field_types.update(nested_types)

        elif isinstance(field_obj, fields.List) and isinstance(field_obj.inner, fields.Nested):
            nested_types = get_schema_field_types(field_obj.inner.schema, f"{full_path}[]")
            field_types.update(nested_types)

        else:
            python_type = get_python_type(field_obj)
            field_types[full_path] = python_type

    return field_types

def get_python_type(field_obj: fields.Field) -> type:
    if isinstance(field_obj, fields.String):
        return str
    elif isinstance(field_obj, fields.Integer):
        return int
    elif isinstance(field_obj, fields.Float):
        return float
    elif isinstance(field_obj, fields.Boolean):
        return bool
    elif isinstance(field_obj, fields.List):
        return list
    elif isinstance(field_obj, fields.Dict):
        return dict
    elif isinstance(field_obj, Resource):
        return Resource
    return Any

def find_values_by_type(json_obj: Any, expected_types: Dict[str, type], target_type: type, path: str = "") -> List[Tuple[str, Any]]:
    matches = []

    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            new_path = f"{path}.{key}" if path else key
            if new_path in expected_types and expected_types[new_path] == target_type:
                matches.append((new_path, value))
            matches.extend(find_values_by_type(value, expected_types, target_type, new_path))

    elif isinstance(json_obj, list):
        for index, item in enumerate(json_obj):
            new_path = f"{path}[{index}]"
            base_path = path + "[]" if path in expected_types else path
            if base_path in expected_types and expected_types[base_path] == target_type:
                matches.append((new_path, item))
            matches.extend(find_values_by_type(item, expected_types, target_type, new_path))

    return matches

def get_resource_paths(json_obj: Any, schema: Schema) -> List[Tuple[str, str]]:
    expected_types = get_schema_field_types(schema)
    return find_values_by_type(json_obj, expected_types, Resource)

def get_object_by_path(obj: Any, path: str) -> Any:
    elements = path.split(".")
    current = obj

    for element in elements:
        if "[" in element and "]" in element:
            key, index = element[:-1].split("[")
            index = int(index)

            if hasattr(current, key):
                current = getattr(current, key)[index]
            elif isinstance(current, dict) and key in current:
                current = current[key][index]
            else:
                raise KeyError(f"Invalid path: {path}")

        else:
            if hasattr(current, element):
                current = getattr(current, element)
            elif isinstance(current, dict) and element in current:
                current = current[element]
            else:
                raise KeyError(f"Invalid path: {path}")

    return current


def set_object_by_path(obj: Any, path: str, value: Any) -> None:
    elements = path.split(".")
    current = obj

    for i, element in enumerate(elements):
        if "[" in element and "]" in element:
            key, index = element[:-1].split("[")
            index = int(index)

            if hasattr(current, key):
                current = getattr(current, key)
            elif isinstance(current, dict) and key in current:
                current = current[key]
            else:
                raise KeyError(f"Invalid path: {path}")

            if i == len(elements) - 1:
                current[index] = value
            else:
                current = current[index]

        else:
            if hasattr(current, element):
                if i == len(elements) - 1:
                    setattr(current, element, value)
                else:
                    current = getattr(current, element)
            elif isinstance(current, dict):
                if i == len(elements) - 1:
                    current[element] = value
                else:
                    current = current[element]
            else:
                raise KeyError(f"Invalid path: {path}")

def fetch_data(url, params=None, headers=None, timeout=120):
    try:
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        return io.BytesIO(response.content).getvalue()
    except requests.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return None

def has_resource_fields(schema: Schema):
    expected_types = get_schema_field_types(schema)
    for path, value in expected_types.items():
        if value == Resource:
            return True
    return False

def resolve_resources(app_url_pattern: str, json_obj: Any, schema: Schema):
    cached = schema.load(json_obj, partial=True)
    paths = get_resource_paths(json_obj, schema)

    for path, resource in paths:
        # we'll assume that if the value of a resource is set, it was already fetched
        # so we won't fetch it again, as the hash matches.
        if resource != None and get_object_by_path(cached, path) == None:
            url = app_url_pattern.format(reid=resource)
            set_object_by_path(cached, path, fetch_data(url))

    return cached

def map_json_type(prop, definitions=None):
    field_type = prop.get("type", "string")
    is_resource = prop.get("is_resource", False)

    if isinstance(field_type, list):
        is_nullable = "null" in field_type
        field_type = next((t for t in field_type if t != "null"), "string")
    else:
        is_nullable = False

    if is_resource:
        field = Resource(resource_encoding=prop.get("resource_encoding", 'blob'), title=prop.get("title", 'blob'), resource_type=prop.get("resource_type", 'data'))
    elif prop.get("format") == "decimal":
        field = DecimalField()
    elif prop.get("format") == "float":
        field = fields.Float()
    elif field_type == "string":
        field = fields.Str()
    elif field_type == "number":
        field = fields.Float()
    elif field_type == "integer":
        field = fields.Int()
    elif field_type == "boolean":
        field = fields.Bool()
    elif field_type == "array":
        if "items" in prop:
            item_type = map_json_type(prop["items"], definitions)
        else:
            item_type = fields.Raw()
        field = fields.List(item_type)
    elif field_type == "object":
        if "$ref" in prop:
            ref_name = prop["$ref"].replace("#/definitions/", "")
            if definitions and ref_name in definitions:
                nested_schema = create_schema_from_definition(ref_name, definitions[ref_name], definitions)
                field = fields.Nested(nested_schema)
            else:
                field = fields.Nested(lambda: "self")
        else:
            field = fields.Nested(lambda: "self")
    else:
        field = fields.Raw()

    field.allow_none = is_nullable
    return field

def create_schema_from_definition(class_name, schema, definitions):
    schema_fields = {}
    
    for key, prop in schema.get("properties", {}).items():
        if "$ref" in prop:
            ref_name = prop["$ref"].replace("#/definitions/", "")
            if ref_name in definitions:
                # TODO: This does not seem right
                # should try to encode an additional property to hint that this is nullable or not
                schema_fields[key] = fields.Nested(ref_name, allow_none=True)
            else:
                raise ValueError(f"Reference {ref_name} not found in definitions.")
        elif prop.get("type") == "array" and "items" in prop and "$ref" in prop["items"]:
            ref_name = prop["items"]["$ref"].replace("#/definitions/", "")
            if ref_name in definitions:
                schema_fields[key] = fields.List(fields.Nested(ref_name))
        else:
            schema_fields[key] = map_json_type(prop, definitions)

    schema_class = type(class_name, (Schema,), schema_fields)

    # Register the schema
    # TODO: this is not entirely ok, and can cause issues if we overwrite a schema
    # ugly workaround to remove duplicate registration / reuse schema names
    if class_name in class_registry._registry:
        del class_registry._registry[class_name]

    class_registry.register(class_name, schema_class)
 
    return schema_class

def json_schema_to_marshmallow(json_schema):
    schemas = {}
    class_name = json_schema.get("$ref", "").replace("#/definitions/", "")
    definitions = json_schema.get("definitions", {})

    for schema_name, json_schema in definitions.items():
        schemas[schema_name] = create_schema_from_definition(schema_name, json_schema, definitions)
    
    return schemas[class_name]

def load_schemas_from_folder(folder_path):
    loaded_schemas = {}

    for filename in os.listdir(folder_path):
        if filename.endswith(".py") and not filename.startswith("__init__.py"):
            module_name = filename[:-3]
            module_path = os.path.join(folder_path, filename)

            spec = importlib.util.spec_from_file_location(module_name, module_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            if hasattr(module, "EXPOSED_SCHEMAS"):
                schemas_dict = getattr(module, "EXPOSED_SCHEMAS")
                if isinstance(schemas_dict, dict):
                    loaded_schemas.update(schemas_dict)

    return loaded_schemas

def one_of_many(valid_schemas):
    def validate(value):
        matched = False
        errors = {}

        for obj_type, schema in valid_schemas.items():
            if isinstance(value, obj_type):
                try:
                    schema.load(vars(value))
                    matched = True
                    break
                except ValidationError as err:
                    errors[obj_type.__name__] = err.messages
        
        if not matched:
            raise ValidationError(f"Value must match one of the defined schemas. Errors: {errors}")
    
    return validate

def resolve_plugins(base_schema, target_class_name=None):
    if not hasattr(base_schema, '_declared_fields'):
        raise ValueError("Invalid schema class")

    custom_class_name = f"Resolved{base_schema.__name__}"
    new_schema = type(target_class_name if target_class_name is not None else custom_class_name, (base_schema,), {})

    for field_name, field_instance in new_schema._declared_fields.items():
        if isinstance(field_instance, PluginField):
            selector = getattr(field_instance, 'selector', None)
            path = getattr(field_instance, 'path', None)

            if selector == "resource":
                valid_schemas = load_schemas_from_folder(path)
                new_schema._declared_fields[field_name] = fields.Raw(required=field_instance.required, allow_none=True, validate=one_of_many(valid_schemas))
            elif getattr(field_instance, 'selector', None) == "string":
                new_schema._declared_fields[field_name] = fields.String(required=field_instance.required, allow_none=True)
    
    return new_schema

def create_class_from_schema(schema, class_name):
    def create_class(schema, class_name):
        attrs = {}
        annotations = {}
        for key, field in schema.fields.items():
            if field.dump_default is not missing:
                default_value = field.dump_default
            elif field.allow_none:
                default_value = dataclass_field(default=None)
            else:
                if isinstance(field, fields.List):
                    if isinstance(field.inner, fields.Nested):
                        nested_class = create_class(field.inner.schema, f"{class_name}_{key}")
                        default_value = dataclass_field(default_factory=lambda: [nested_class()])
                    else:
                        default_value = dataclass_field(default_factory=list)
                elif isinstance(field, fields.Dict):
                    default_value = dataclass_field(default_factory=dict)
                elif isinstance(field, fields.String):
                    default_value = ""
                elif isinstance(field, fields.Integer):
                    default_value = 0
                elif isinstance(field, fields.Float):
                    default_value = 0.0
                elif isinstance(field, fields.Boolean):
                    default_value = False
                elif isinstance(field, fields.Nested):
                    if field.allow_none:
                        default_value = dataclass_field(default=None)
                    else:
                        nested_class = create_class(field.schema, f"{class_name}_{key}")
                        default_value = dataclass_field(default_factory=nested_class)
                    default_value = dataclass_field(default=None)
                else:
                    default_value = None

            attrs[key] = default_value
            annotations[key] = get_python_type(field)

        attrs['__annotations__'] = annotations
        return dataclass(type(class_name, (object,), attrs))

    return create_class(schema, class_name)