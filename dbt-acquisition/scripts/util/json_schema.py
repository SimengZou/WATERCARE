import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, overload

from scripts.util.data_utils import DataType
from scripts.util.logger import log
from scripts.util.metadata import CTEInfo, Column, FullRegister, ModelRegistry, SQLMetadata, SourceInfo
from scripts.util.config.keywords import RESERVED_KEYWORDS

COLUMN_DELIMITER = '__'

def get_primary_keys(schema: dict) -> list[str]:
  properties = schema.get('properties', {})
  identifiers = []
  if properties:
    paths = properties.get('IdentifierPaths', [])
    for path in paths:
      match = re.search(r"['\"]?(\w+)['\"]?\]?$", path)
      if match:
        identifiers.append(match.group(1).lower())
  return identifiers

def get_variation_paths(schema: dict) -> list[str]:
  properties = schema.get('properties', {})
  if properties:
    path = properties.get('VariationPath')
    if path:
      match = re.search(r"['\"]?(\w+)['\"]?\]?$", path)
      if match:
        return [match.group(1).lower()]
  return ['etl_load_datetime']

def get_snowflake_path(path: List[str], current_cte: str, json_col: str) -> str:
    if current_cte == 'root':
      clean_segments = [p for p in path if p != '[*]']
      return f'{json_col}:' + ':'.join(clean_segments)
    else:
      try:
        last_star_index = len(path) - 1 - path[::-1].index('[*]')
        segments_after_flatten = path[last_star_index + 1:]
        if not segments_after_flatten:
          return 'f.value'
        return 'f.value:' + ':'.join(segments_after_flatten)
      except ValueError:
        return 'f.value'

def _append_schema_annotations(description: str, schema: dict, schema_type: str | None) -> str:
    parts = []

    enum_values = schema.get('enum')
    if enum_values is not None:
      formatted = ', '.join(str(v) for v in enum_values)
      parts.append(f'Allowed values: {formatted}')

    max_length = schema.get('maxLength')
    if max_length is not None and schema_type == 'string':
      parts.append(f'Max length: {max_length}')

    if not parts:
      return description

    annotation_str = '. '.join(parts)
    return f'{description}. {annotation_str}' if description else annotation_str


def walk_json_schema(
  schema: dict,
  path: List[str] = None,
  current_cte: str = 'root',
  ctes: Dict[str, CTEInfo] = None,
  json_col: str = 'src',
  parent_descriptions: List[str] = None,
  parent_required: set = None
):
  if path is None: path = []
  if ctes is None: ctes = {'root': CTEInfo(description='Root table')}
  if parent_descriptions is None: parent_descriptions = []
  if parent_required is None: parent_required = set()

  current_desc = schema.get('description', '').strip()
  new_descriptions = parent_descriptions.copy()
  if current_desc and len(path) > 0:
    new_descriptions.append(current_desc)

  for logic_key in ['anyOf', 'oneOf', 'allOf']:
    if logic_key in schema:
      for sub_schema in schema[logic_key]:
        yield from walk_json_schema(
          sub_schema,
          path,
          current_cte,
          ctes,
          json_col,
          new_descriptions,
          parent_required
        )
      return

  schema_type: str|None = schema.get('type')
  schema_format = schema.get('format')
  
  if schema_type == 'object' or 'properties' in schema:
    properties = schema.get('properties', {})
    required_fields = set(schema.get('required', []))
    for prop_name, prop_schema in properties.items():
      yield from walk_json_schema(
        prop_schema,
        path + [prop_name],
        current_cte,
        ctes,
        json_col,
        new_descriptions,
        parent_required=required_fields
      )

  elif schema_type == 'array' or 'items' in schema:
    items_schema = schema.get('items', {})
    if items_schema:
      new_cte_name = '_'.join([p for p in path if p != '[*]'])
      
      cte_desc = ' - '.join(new_descriptions) if new_descriptions else f'Flattened array: {new_cte_name}'
      ctes[new_cte_name] = CTEInfo(
        parent=current_cte,
        flatten_path=get_snowflake_path(path, current_cte, json_col=json_col),
        description=cte_desc
      )
      
      yield from walk_json_schema(
        items_schema,
        path + ['[*]'],
        new_cte_name,
        ctes,
        json_col,
        new_descriptions
      )

  else:
    clean_name = COLUMN_DELIMITER.join(
      [p.lower() if p.upper() not in RESERVED_KEYWORDS else f'_{p.lower()}'
       for p in path if p != '[*]']
    ).replace('.', '_')
    data_type = DataType.from_json_schema(schema_type, schema_format)
    
    description = ' - '.join(new_descriptions)
    description = _append_schema_annotations(description, schema, schema_type)
    
    is_req = path[-1] in parent_required if path else False
    if is_req:
      description = f"(required) {description}" if description else "(required)"

    yield Column(
      name=path[-1].lower() if path else 'value',
      clean_name=clean_name,
      description=description,
      data_type=data_type,
      is_required=is_req,
      source_origin='json-schema',
      sql_meta=SQLMetadata(
        cte=current_cte,
        raw_path=get_snowflake_path(path, current_cte, json_col),
        path=path,
        parent_cte=None if current_cte == 'root' else ctes[current_cte].parent,
        is_array_pointer=False
      )
    )

def build_register(file_path: str, model_id: str, table_name: str, table_schema: str) -> FullRegister:
    with open(file_path, 'r', encoding='utf-8') as f:
      try:
        schema_file = json.load(f)
      except json.JSONDecodeError:
        schema_file = {}
            
    if schema_file is None:
      schema_file = {}
    
    shared_ctes = {'root': CTEInfo(description='Root level document attributes')}
    
    try:
      if 'schema' in schema_file and schema_file['schema']:
        columns = list(walk_json_schema(schema_file.get('schema'), ctes=shared_ctes))
      else:
        columns = []
            
      schema_inner = schema_file.get('schema', {})
      model_reg = ModelRegistry(
        description=schema_inner.get('description', ''),
        source_table=table_name,
        source_schema=table_schema,
        primary_keys=get_primary_keys(schema_file),
        variation_paths=get_variation_paths(schema_file),
        ctes=shared_ctes,
        columns=columns
      )
      model_reg.apply_all()
    except Exception as e:
      print(f"Error building model {model_id}: {e}")
      return None
    
    return model_reg

def build_full_register(file_path: str):
  path = Path(file_path)
  models = {}
  model_name = 'default'
  for f in path.glob('*.json'):
    model_name = f.stem
    model_schema = model_name.split('_')[0]
    models[f.name] = build_register(str(f), model_name, model_name, model_schema)
  return FullRegister(
    version='1.0.0',
    generated_at=datetime.now().isoformat(),
    model_identifier=model_name,
    models=models
  )
