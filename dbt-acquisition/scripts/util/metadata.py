from typing import Dict, List, Optional

import yaml
from pydantic import BaseModel

from scripts.util.config.keywords import RESERVED_KEYWORDS
from scripts.util.data_utils import TypeMapping

class SQLMetadata(BaseModel):
  cte: str
  raw_path: str
  path: list[str]
  parent_cte: Optional[str] = None
  is_array_pointer: bool = False

class Column(BaseModel):
  name: str
  clean_name: str
  description: Optional[str] = ''
  data_type: TypeMapping
  is_primary: bool = False
  is_required: bool = False
  source_origin: str = 'schema'
  dbt_tests: List[str] = []
  sql_meta: SQLMetadata

  def render_snowflake_sql(self) -> str:
    path = self.sql_meta.raw_path
    if ":" in path:
      base_path, leaf = path.rsplit(":", 1)
      if " " in leaf or "-" in leaf:
        path = f'{base_path}:"{leaf}"'
    return f"{path}::{self.data_type.snowflake} as {self.clean_name}"
    
  def render_dbt_sql(self) -> str:
    if len(self.sql_meta.path) == 0:
      return f'{self.name}::{self.data_type.snowflake} as {self.clean_name}'
    formatted_path = ', '.join([f"'{p}'" for p in self.sql_meta.path])
    return ''.join([
      '{{ ',
      f"trx_json_extract('{self.sql_meta.raw_path.split(':')[0]}', [{formatted_path}], ",
      f"type='{self.data_type.snowflake}', alias='{self.clean_name}')",
      ' }}'
    ])
    
  def as_dbt_yml_dict(self) -> dict:
    d = {'name': self.clean_name}
    if self.description:
      d['description'] = self.description
    if self.dbt_tests:
      d['data_tests'] = self.dbt_tests
    return d

class CTEInfo(BaseModel):
  parent: Optional[str] = None
  flatten_path: Optional[str] = None
  description: str

class SourceInfo(BaseModel):
  alias: str
  relation: str
  join_logic: str = 'base'
  on_clause: Optional[str] = None

class ModelRegistry(BaseModel):
  description: str
  source_table: str
  source_schema: str
  source_variant_col: str = 'src'
  primary_keys: List[str]
  primary_keys_alias: List[str] = []
  variation_paths: List[str] = []
  variation_paths_alias: List[str] = []
  retrieval_type: str = 'incremental'
  sources: List[SourceInfo] = []
  ctes: Dict[str, CTEInfo]
  columns: List[Column]

  def _apply_primary_keys(self) -> None:
    for col in self.columns:
      if col.clean_name in self.primary_keys:
        col.is_primary = True
    
  def _apply_dbt_tests(self) -> None:
    for column in self.columns:
      if column.is_primary:
        if len(self.primary_keys) == 1:
          column.dbt_tests.append('unique')
          
      if column.is_required:
        if 'not_null' not in column.dbt_tests:
          column.dbt_tests.append('not_null')
  
  def _apply_primary_keys_alias(self) -> None:
    if not self.primary_keys or len(self.primary_keys) == 0:
      return
    
    self.primary_keys_alias = [
      f'{k.lower().replace(".", "_")}' if k.upper() not in RESERVED_KEYWORDS else f'_{k.lower().replace(".", "_")}'
         for k in self.primary_keys
    ]
    
  def _apply_variation_paths_alias(self) -> None:
    if not self.variation_paths or len(self.variation_paths) == 0:
      return
    
    self.variation_paths_alias = [
      f'{k.lower().replace(".", "_")}' if k.upper() not in RESERVED_KEYWORDS else f'_{k.lower().replace(".", "_")}'
         for k in self.variation_paths
    ]
    
  def apply_all(self) -> None:
    self._apply_primary_keys()
    self._apply_dbt_tests()
    self._apply_primary_keys_alias()
    self._apply_variation_paths_alias()
    
  def render_dbt_sql(self, incremental_column: str = 'etl_load_datetime', extra_tags: List[str] = None) -> str:
    unique_key = '[\n' + ',\n'.join([' ' * 8 + f"'{k.lower()}'" for k in self.primary_keys]) + '\n    ]' \
                 if len(self.primary_keys) else '[]'
    escaped_desc = self.description.replace("'", "\\'") if self.description else 'Auto-generated model'
    
    tags = ['auto-generated', self.source_schema]
    if extra_tags:
        tags.extend(extra_tags)
    tags_formatted = '[' + ', '.join([f"'{t}'" for t in tags]) + ']'
    
    config = f'''
{{{{ config(
    materialized='{self.retrieval_type}',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='{escaped_desc}',
    tags={tags_formatted}
) }}}}
'''

    cols_by_cte = {}
    for col in self.columns:
      cte = col.sql_meta.cte
      if cte not in cols_by_cte:
        cols_by_cte[cte] = []
      cols_by_cte[cte].append(col)

    cte_sql = []
    source_tbl = f"{{{{ source('source_landing_{self.source_schema}', '{self.source_table}') }}}}"
    
    root_cols = cols_by_cte.get('root', [])
    root_selects = [col.render_dbt_sql() for col in root_cols]
    root_selects.append('{{ trx_etl_columns() }}')
    root_sql = f"select\n        {',\n        '.join(root_selects)}\n    from {source_tbl}"
    cte_sql.append(f"cte_flattened as (\n    {root_sql}\n)")
    
    last_cte_name = 'cte_flattened'
    for cte_name, cte_info in self.ctes.items():
      if cte_name == 'root':
        continue
      
      parent = cte_info.parent
      if parent == 'root' or parent is None:
        parent = 'cte_flattened'
      
      this_cte_cols = cols_by_cte.get(cte_name, [])
      this_cte_selects = [col.render_dbt_sql() for col in this_cte_cols]
      
      select_list = [f"{parent}.*"] + this_cte_selects
      
      source_sql = f"select\n        {',\n        '.join(select_list)}\n    from {parent}, lateral flatten(input => {cte_info.flatten_path}) f"
      cte_sql.append(f"{cte_name} as (\n    {source_sql}\n)")
      last_cte_name = cte_name
        
    audit_sql = '{{ trx_audit_columns() }}'
    

    pk_arg = f"columns=[{', '.join(f"'{k}'" for k in self.primary_keys_alias)}]" if self.primary_keys_alias else ''
    surrogate_sql = f"{{{{ trx_audit_surrogate({pk_arg}) }}}}"
    
    current_state_sql = ''
    if self.variation_paths_alias:
        ord_by = ', '.join(f"'{k}'" for k in self.variation_paths_alias)
        current_state_sql = f"{{{{ trx_current_state(order_by=[{ord_by}]) }}}}"
    
    final_sql = f"select\n    *,\n    {audit_sql},\n    {surrogate_sql}\nfrom {last_cte_name}"
    
    incremental_sql = f'''
{{% if is_incremental() %}}
where {incremental_column.lower()} > (select max({incremental_column.lower()}) from {{{{ this }}}})
{{% endif %}}
'''
        
    return '\n'.join(x for x in [
      config.strip(),
      '',
      'with ' + ',\n'.join(cte_sql),
      '',
      final_sql.strip(),
      incremental_sql.strip(),
      current_state_sql.strip() if current_state_sql else ''
    ] if x is not None) + '\n'
    
  def render_dbt_yml(self, model_name: str) -> str:
    columns = [c.as_dbt_yml_dict() for c in self.columns]
    columns.extend([
      {'name': 'etl_load_datetime', 'description': 'The datetime in which the ETL process loaded the data into the curated layer.'},
      {'name': 'etl_load_metadatafilename', 'description': 'The filename of the metadata file that was used to load the data into the curated layer.'},
      {'name': 'dbt_record_updated_at', 'description': 'The datetime in which the record was last updated by dbt.'},
      {'name': 'dbt_record_created_at', 'description': 'The datetime in which the record was created by dbt.'},
      {'name': 'dbt_invocation_id', 'description': 'The invocation ID of the dbt run that created the record.'},
      {'name': 'dbt_surrogate_key', 'description': 'Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata.'}
    ])
    
    model_dict = {
      'name': model_name,
      'columns': columns
    }
    if self.description:
      model_dict['description'] = self.description
    
    if len(self.primary_keys) > 1:
      model_dict['data_tests'] = [{
        'dbt_utils.unique_combination_of_columns': {
          'arguments': {'combination_of_columns': self.primary_keys_alias}
        }
      }]
    
    yml_dict = {'version': 2, 'models': [model_dict]}
    
    return yaml.dump(yml_dict, sort_keys=False, indent=2, default_flow_style=False)

  def render_markdown(self, model_name: str, source_name: str) -> str:
    lines: List[str] = [f'# {model_name}', '']
    
    if self.description:
      lines.extend([self.description, ''])
    
    lines.extend([
      '## Overview',
      '',
      '| Property | Value |',
      '|---|---|',
      f'| **Source system** | `{source_name}` |',
      f'| **Source table** | `{self.source_table}` |',
      f'| **Materialization** | `{self.retrieval_type}` |'
    ])

    if self.primary_keys:
      pk_formatted = ', '.join(f'`{k}`' for k in self.primary_keys)
      lines.append(f'| **Primary keys** | {pk_formatted} |')
    
    lines.extend([
      f'| **Column count** | {len(self.columns) + 5} |',
      '',
      '## Columns',
      '',
      '| Column | dbt Type | Snowflake Type | Flags | Tests | Description |',
      '|---|---|---|---|---|---|'
    ])

    for col in self.columns:
      flags = []
      if col.is_primary:
        flags.append('PK')
      if col.sql_meta.cte != 'root':
        flags.append(f'`{col.sql_meta.cte}`')
      flags_str = ' '.join([f'`{f}`' for f in flags])
      tests_str = ', '.join(f'`{t}`' for t in col.dbt_tests) if col.dbt_tests else ''
      desc = (col.description or '').replace('|', '\\|')
      lines.append(f'| `{col.clean_name}` | `{col.data_type.generic}` '
                   f'| `{col.data_type.snowflake}` | {flags_str} | {tests_str} | {desc} |')
    lines.extend([
      '| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |',
      '| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |',
      '| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |',
      '| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |',
      '| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |',
      '| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |'
    ])
    
    return '\n'.join(lines + [''])

class FullRegister(BaseModel):
  version: str
  generated_at: str
  model_identifier: str
  models: Dict[str, ModelRegistry]
