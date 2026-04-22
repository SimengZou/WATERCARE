import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text, TextClause
from typing import Literal, List, overload
from pathlib import Path
from scripts.util.logger import log
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

credential_args = {k: os.environ.get(f'SNOWFLAKE_{k.upper()}') for k in
                   ['user', 'account', 'authenticator']}
connection_args = {k: os.environ.get(f'SNOWFLAKE_{k.upper()}') for k in
                   ['warehouse', 'database', 'schema'#, 'role',
                    # 'pool_size', 'max_overflow', 'pool_recycle'
                    ]}

SNOWFLAKE_POOL_SIZE = 10
SNOWFLAKE_MAX_OVERFLOW = 5
THREADPOOL_SIZE = SNOWFLAKE_MAX_OVERFLOW + SNOWFLAKE_MAX_OVERFLOW

engine = create_engine(
    URL(**credential_args),
    connect_args=connection_args,
    pool_size=SNOWFLAKE_POOL_SIZE,
    max_overflow=SNOWFLAKE_MAX_OVERFLOW,
    pool_recycle=60 * 60,
    pool_pre_ping=True
)
with engine.connect() as conn:
  log.info('Testing Snowflake connection')
  conn.close()

@dataclass
class Table:
  collection_name: str
  schema_name: str
  table_name: str

  def full_name(self, delimiter: str = '.') -> str:
    d = delimiter
    return f'{self.collection_name}{d}{self.schema_name}{d}{self.table_name}'

def _query_json(
  query: str|TextClause,
  json_col: str
) -> dict|None:
  query = text(query) if isinstance(query, str) else query
  try:
    with engine.connect() as conn:
      log.debug(f'Running query {' '.join(str(query).split('\n'))}')
      result = conn.execute(query).fetchone()
      if not result:
        log.debug(f"No result for {' '.join(str(query).split('\n'))}")
        return None

      return json.loads(result._asdict()[json_col])
  except Exception as e:
    print(e)

@overload
def _query(query: str|TextClause, fetch_one: Literal[True]) -> list[dict]|None:
  ...
@overload
def _query(query: str|TextClause, fetch_one: Literal[False]) -> dict|None:
  ...
def _query(
    query: str|TextClause,
    fetch_one: bool = True
) -> dict|list[dict]|None:
  query = text(query) if isinstance(query, str) else query
  try:
    with engine.connect() as conn:
      log.debug(f'Running query {' '.join(str(query).split('\n'))}')
      if (fetch_one):
        result = conn.execute(query).fetchone()
      else:
        result = conn.execute(query).fetchall()
      
      if not result or not len(result):
        log.debug(f'No result for {' '.join(str(query).split('\n'))}')
        return None

      if (fetch_one):
        return result._asdict()
      
      return [row._asdict() for row in result]
  except Exception as e:
    log.error(e)

def download_information_schema(
  tables: List[Table], #TODO make "tables object"
  json_col: str = 'src'

):
  def _table_cols_template(
      table: Table,
  ) -> text:
    return text('\n'.join([
      '(select',
      f"    '{table.schema_name}' as table_schema,",
      f"    '{table.table_name}' as table_name,",
      '     cols.value::string as column_name',
      f'from {table.full_name(delimiter='.')} tbl,',
      f'lateral flatten(input => object_keys(tbl.{json_col})) cols)'
      'union all',
      '(select cols.table_schema, cols.table_name, cols.column_name',
      f'from {table.collection_name}.information_schema.columns cols',
      f"where lower(cols.table_schema) = '{table.schema_name}'",
      f"    and lower(cols.table_name) = '{table.table_name}'",
      f"    and lower(cols.column_name) != '{json_col}'",
      'order by cols.ordinal_position)'
    ]))


def download_json_samples(
  tables: List[str],
  json_col: str = 'src',
  order_by: str = 'ETL_LOAD_DATETIME',
  output_dir: str = 'tmp/json',
  enrich: bool = True
) -> None:
  def _sample_template(table: str) -> text:
    return text('\n'.join([
      'with cte_key_counts as (',
      f'    select {json_col}, {order_by},',
      f'        array_size(object_keys({json_col})) as key_count',
      f'    from {table}',
      f'    where {json_col} is not null',
      ')',
      f'select {json_col}',
      f'from cte_key_counts',
      f'order by key_count desc, {order_by} desc',
      'limit 1'
    ]))
  
  def _sample_targeted_template(table: str, column: str|list[str], alias: str = 'tgt') -> text:
    json_path = '.'.join(column) if isinstance(column, list) else column
    return text('\n'.join([
      f"select get_path({json_col}, '{json_path}') as {alias}",
      f'from {table}',
      f"where get_path({json_col}, '{json_path}') is not null",
      f"    and get_path({json_col}, '{json_path}')::varchar != ''",
      f'order by {order_by} desc',
      'limit 1'
    ]))

  os.makedirs(output_dir, exist_ok=True)
  
  with ThreadPoolExecutor(max_workers=THREADPOOL_SIZE) as executor:
    future_to_json = {executor.submit(
      _query_json, _sample_template(table), json_col
    ): table for table in tables}
    
    for future in as_completed(future_to_json):
      tgt_table = future_to_json[future]
      result = future.result()
      if result == None:
        continue
      
      if enrich:
        for key, value in result.items():
          if value == None or value == '':
            preserved_value = '' if value == '' else None
            log.info(f'Searching for non-null value for {tgt_table}.{key}')
            future_to_tgt_json = {executor.submit(
              _query_json, _sample_targeted_template(tgt_table, key, alias='tgt'), 'tgt'
            ): key}

            for future_tgt in as_completed(future_to_tgt_json):
              tgt_key = future_to_tgt_json[future_tgt]
              tgt_value = future_tgt.result()
              if tgt_value == None:
                log.info(f'No value found for {tgt_table}.{key}')
              else:
                result[tgt_key] = tgt_value or preserved_value
    
      safe_file_name = tgt_table.replace('"', '')
      file_path = Path(output_dir) / f'{safe_file_name}.json'
      log.info(f'Writing {str(file_path)}')
      file_path.write_text(json.dumps(result, indent=2, default=str), encoding='utf-8')

def download_json_metadata(
  tables: List[str] = None,
  output_dir: str = './tmp/json/metadata'
) -> None:
  def _metadata_template() -> text:
    where = 'where table_name in (' + ', '.join([f"'{tbl}'" for tbl in tables]) + ')' \
      if tables else ''
    return text('\n'.join([
      'select'
      '    src as source_name,',
      '    table_name,'
      '    array_agg(primary_key) within group (order by primary_key asc) as key_constraints',
      'from dev_wcc_datawarehouse.datahub_source_metadata.vw_datacatalog_keys',
      where,
      'group by src, table_name',
      'order by src asc, table_name asc'
    ]))

  os.makedirs(output_dir, exist_ok=True)

  data = _query(_metadata_template(), fetch_one=False)
  for row in data:
    row['key_constraints'] = json.loads(row['key_constraints'])
  output_path = Path(output_dir) / 'metadata_pk.json'
  output_path.write_text(json.dumps(data, indent=2, default=str), encoding='utf-8')

def download_full_set(
  tables: List[str] = None,
  output_sample_dir: str = './tmp/json/samples',
  output_meta_dir: str = './tmp/json/metadata',
  refresh_meta: bool = True
) -> None:
  if refresh_meta:
    download_json_metadata(tables, output_meta_dir)
  
  md_pk = json.loads((Path(output_meta_dir) / 'metadata_pk.json').read_text(encoding='utf-8'))

  if not tables:
    tables = [f"landing_dev.{item['source_name']}.{item['table_name']}" for item in md_pk]
  log.info(f'Found {len(tables)} tables')

  download_json_samples(tables, output_dir=output_sample_dir, enrich=True)

