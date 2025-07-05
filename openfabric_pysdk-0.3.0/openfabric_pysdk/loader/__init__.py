from marshmallow import Schema
from typing import Literal

from openfabric_pysdk.helper import create_class_from_schema
from openfabric_pysdk.loader.config import manifest, execution, state_config
from openfabric_pysdk.utility import LoaderUtil

# Output concept
InputClass = LoaderUtil.get_class("input_class")
InputSchema = LoaderUtil.get_class("input_schema")
InputSchemaInst: Schema = InputSchema() if InputSchema else None

# Output concept
OutputClass = LoaderUtil.get_class("output_class")
OutputSchema = LoaderUtil.get_class("output_schema")
OutputSchemaInst: Schema = OutputSchema() if OutputSchema else None

# Config concept
ConfigClass = LoaderUtil.get_class("config_class")
ConfigSchema = LoaderUtil.get_class("config_schema")
ConfigSchemaInst: Schema = ConfigSchema() if ConfigSchema else None

schemaUpdateCallbacks = set()

def registerOnSchemaUpdateCb(callback):
    global schemaUpdateCallbacks
    schemaUpdateCallbacks.add(callback)

def getSchema(schema: Literal['in', 'out', 'config']):
    if schema == 'in':
        return InputSchema
    if schema == 'out':
        return OutputSchema
    if schema == 'config':
        return ConfigSchema

def getSchemaInst(schema: Literal['in', 'out', 'config']):
    if schema == 'in':
        return InputSchemaInst
    if schema == 'out':
        return OutputSchemaInst
    if schema == 'config':
        return ConfigSchemaInst

def getClass(schema: Literal['in', 'out', 'config']):
    if schema == 'in':
        return InputClass
    if schema == 'out':
        return OutputClass
    if schema == 'config':
        return ConfigClass

def setSchemas(input=None, output=None, config=None):
    global InputClass
    global OutputClass
    global ConfigClass
    global InputSchema
    global OutputSchema
    global ConfigSchema
    global InputSchemaInst
    global OutputSchemaInst
    global ConfigSchemaInst
    global schemaUpdateCallbacks

    if input != None:
        InputSchema = input
        InputSchemaInst = InputSchema()
        InputClass = create_class_from_schema(InputSchemaInst, "InputClass")

    if output != None:
        OutputSchema = output
        OutputSchemaInst = OutputSchema()
        OutputClass = create_class_from_schema(OutputSchemaInst, "OutputClass")

    if config != None:
        ConfigSchema = config
        ConfigSchemaInst = ConfigSchema()
        ConfigClass = create_class_from_schema(ConfigSchemaInst, "ConfigClass")

    for cb in schemaUpdateCallbacks:
        cb(input=input, output=output, config=config)
