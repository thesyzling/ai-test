import os
import importlib.util
from marshmallow import Schema, fields, ValidationError

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

def load_plugin_schemas(path: str):
    valid_schemas = load_schemas_from_folder(path)
    valid_types = tuple(valid_schemas.keys())

    schema_fields = {
        "options": fields.Raw(validate=one_of_many(valid_schemas)),
    }

    return type("PluginSchema", (Schema,), schema_fields), valid_types