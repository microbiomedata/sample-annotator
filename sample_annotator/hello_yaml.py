from linkml_runtime.dumpers.yaml_dumper import YAMLDumper
from linkml_runtime.linkml_model import SchemaDefinition

hello_schema = SchemaDefinition(name="hello_schema", id="http://example.com/hello_schema")

yd = YAMLDumper()

yd.dump(hello_schema, 'hello.yaml')
