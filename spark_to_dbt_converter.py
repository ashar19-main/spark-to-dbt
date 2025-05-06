# spark_to_dbt_converter.py

import os
import ast
import yaml
from typing import List, Dict
from sqlglot import parse_one, exp

# === Intermediate Representation Nodes ===

class SourceNode:
    def __init__(self, name, path_or_table, format):
        self.name = name
        self.path_or_table = path_or_table
        self.format = format

class TransformationNode:
    def __init__(self, name, parent_names, operation, details):
        self.name = name
        self.parent_names = parent_names
        self.operation = operation
        self.details = details

class UDFNode:
    def __init__(self, name, args, body):
        self.name = name
        self.args = args
        self.body = body

class OutputNode:
    def __init__(self, name, parent, target_table, partition_by):
        self.name = name
        self.parent = parent
        self.target_table = target_table
        self.partition_by = partition_by

# === AST Parser ===

class PySparkParser(ast.NodeVisitor):
    def __init__(self):
        self.sources = []
        self.transformations = []
        self.udfs = []
        self.outputs = []
        self.symbol_table = {}

    def visit_Assign(self, node):
        target = node.targets[0].id if isinstance(node.targets[0], ast.Name) else None
        value = node.value

        if isinstance(value, ast.Call):
            func = value.func
            if isinstance(func, ast.Attribute):
                if func.attr == 'read':
                    # spark.read.format(...).load(...) or spark.read.csv(...)
                    self.sources.append(SourceNode(target, "unknown_path", "unknown_format"))
                    self.symbol_table[target] = target
                elif func.attr in ["filter", "join", "withColumn"]:
                    parent = self._get_var(func.value)
                    self.transformations.append(TransformationNode(target, [parent], func.attr, ast.dump(value)))
                    self.symbol_table[target] = target
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        if any(isinstance(dec, ast.Name) and dec.id == 'udf' for dec in node.decorator_list):
            self.udfs.append(UDFNode(node.name, [arg.arg for arg in node.args.args], ast.dump(node.body)))
        self.generic_visit(node)

    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call):
            func = node.value.func
            if isinstance(func, ast.Attribute) and func.attr == 'saveAsTable':
                target = func.value
                parent = self._get_var(target)
                self.outputs.append(OutputNode("output_"+parent, parent, node.value.args[0].s, []))

    def _get_var(self, node):
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return self._get_var(node.value)
        return "unknown"

# === DBT Model & YAML Generator ===

class DBTGenerator:
    def __init__(self, out_dir):
        self.out_dir = out_dir
        os.makedirs(os.path.join(out_dir, 'models'), exist_ok=True)
        os.makedirs(os.path.join(out_dir, 'macros'), exist_ok=True)

    def generate_model(self, name, parents, operation, details):
        sql = f"-- Auto-generated model: {name}\nSELECT * FROM {{ ref('{parents[0]}') }} -- {operation} placeholder"
        with open(os.path.join(self.out_dir, 'models', f"{name}.sql"), 'w') as f:
            f.write(sql)

    def generate_source_yaml(self, sources: List[SourceNode]):
        source_dict = {
            'version': 2,
            'sources': [
                {
                    'name': s.name,
                    'tables': [
                        {'name': s.path_or_table.split('.')[-1]}
                    ]
                } for s in sources
            ]
        }
        with open(os.path.join(self.out_dir, 'models', 'sources.yml'), 'w') as f:
            yaml.dump(source_dict, f)

    def generate_macro(self, udf: UDFNode):
        body_sql = "-- translate Python logic to SQL manually"
        macro = f"""{{% macro {udf.name}({', '.join(udf.args)}) %}}
    {body_sql}
{{% endmacro %}}"""
        with open(os.path.join(self.out_dir, 'macros', f"{udf.name}.sql"), 'w') as f:
            f.write(macro)

    def generate_output_model(self, output: OutputNode):
        sql = f"{{{{ config(materialized='table', partition_by={output.partition_by}) }}}}\nSELECT * FROM {{ ref('{output.parent}') }}"
        with open(os.path.join(self.out_dir, 'models', f"{output.name}.sql"), 'w') as f:
            f.write(sql)

# === Entry Point ===

def convert_pyspark_to_dbt(pyspark_file: str, output_dir: str):
    with open(pyspark_file) as f:
        tree = ast.parse(f.read())

    parser = PySparkParser()
    parser.visit(tree)

    generator = DBTGenerator(output_dir)
    for s in parser.sources:
        generator.generate_source_yaml([s])
    for t in parser.transformations:
        generator.generate_model(t.name, t.parent_names, t.operation, t.details)
    for u in parser.udfs:
        generator.generate_macro(u)
    for o in parser.outputs:
        generator.generate_output_model(o)

# Example Usage
# convert_pyspark_to_dbt('example_job.py', 'output_dbt_project')
