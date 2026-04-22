import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from re import Pattern

import yaml

from scripts.util.infor_api import InforAPIClient
from scripts.util.json_schema import build_register
from scripts.util.logger import log
from scripts.util.metadata import ModelRegistry
from scripts.util.snowflake import Table

class Manager:
  register: ModelRegistry

  def __init__(self, json_directory: str, dbt_project_path: str, api_client: InforAPIClient = None):
    self.json_directory = Path(json_directory)
    self.dbt_project_path = Path(dbt_project_path)
    self._register_path = self.json_directory / 'register.json'
    self._sample_directory = self.json_directory / 'samples'
    self._snowflake_info_path = self.json_directory / 'snowflake_info.json'
    self._api_info_directory = self.json_directory / 'api-info'
    self.api_client = api_client

  def load_register(self, json_content: str = None) -> None:
    if json_content is None:
      if self._register_path.exists():
        self.register = ModelRegistry.model_validate(self._register_path.read_text())
      else:
        
        return
    self.register = ModelRegistry.model_validate(json_content)
  
  def save_register(self) -> None:
    self._register_path.parent.mkdir(exist_ok=True)
    self._register_path.write_text(self.register.model_dump_json(indent=2))
  
  def _get_api_schema_path(self, object_name: str) -> Path:
    parts = object_name.lower().split('_', 1)
    source_name = parts[0] if len(parts) == 2 else 'default'
    return self._api_info_directory / source_name / f'{object_name.lower()}.json'

  def load_api_schema(self, object_name: str) -> dict:
    file_path = self._get_api_schema_path(object_name)
    if file_path.exists():
      return json.loads(file_path.read_text(encoding='utf-8'))
    else:
      json_schema = self.api_client.get_object_definition(object_name)
      self.save_api_schema(object_name, json_schema)
      return self.load_api_schema(object_name)
  
  def download_api_schema(self, query: Pattern) -> list[dict]:
    document_list = self.api_client.get_object_list(query)
    with ThreadPoolExecutor(max_workers=15) as executor:
      futures = [executor.submit(self.load_api_schema, doc) for doc in document_list]
      
      for future in as_completed(futures):
          log.debug(future.result())
  
  def save_api_schema(self, object_name: str, data: dict) -> None:
    file_path = self._get_api_schema_path(object_name)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(data, indent=2))
  
  def load_snowflake_info(self) -> dict:
    return json.load(self._snowflake_info_path)
  
  def save_snowflake_info(self, data: dict) -> None:
    self._snowflake_info_path.parent.mkdir(exist_ok=True)
    self._snowflake_info_path.write_text(json.dumps(data, indent=2))
  
  def load_sample(self, table: Table) -> dict:
    return json.load(self._sample_directory / f'{table.full_name()}.json')
  
  def save_sample(self, table: Table, data: dict) -> None:
    self._sample_directory.mkdir(exist_ok=True)
    (self._sample_directory / f'{table.full_name()}.json').write_text(json.dumps(data, indent=2))

  def generate_dbt_models(self, output_dir: str = None, generate_sources: bool = True, batch_size: int = 250) -> None:
    if output_dir is None:
      output_dir = str(self.dbt_project_path)
        
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    
    tables_by_source = {}
    source_counters = {}
    
    for json_file in sorted(self._api_info_directory.rglob('*.json')):
      full_name = json_file.stem.lower()
      parts = full_name.split('_', 1)
      if len(parts) == 2:
        source_name = parts[0]
        object_name = parts[1]
      else:
        source_name = 'default'
        object_name = full_name
          
      registry = build_register(str(json_file), full_name, full_name, source_name)
      if not registry:
        continue
              
      if source_name not in source_counters:
        source_counters[source_name] = 0
              
      source_counters[source_name] += 1
      stage_number = ((source_counters[source_name] - 1) // batch_size) + 1
      stage_tag = f'stage{stage_number}'
              
      sql_content = registry.render_dbt_sql(extra_tags=[stage_tag])
      yml_content = registry.render_dbt_yml(object_name)
          
      source_dir = out_path / source_name
      source_dir.mkdir(parents=True, exist_ok=True)
          
      sql_file = (source_dir / f'{object_name}.sql')
      sql_file.write_text(sql_content, encoding='utf-8')
      yml_file = (source_dir / f'{object_name}.yml')
      yml_file.write_text(yml_content, encoding='utf-8')
      log.info(f"Generated {object_name}.sql and {object_name}.yml in {source_dir}")
          
      if source_name not in tables_by_source:
        tables_by_source[source_name] = []
              
      table_entry = {'name': full_name}
      if registry.description:
        table_entry['description'] = registry.description
              
      tables_by_source[source_name].append(table_entry)
          
    if generate_sources:
      self._update_sources_yml(out_path, tables_by_source)

  def generate_docs(self, output_dir: str = None) -> None:
    if output_dir is None:
      output_dir = str(self.dbt_project_path / 'docs')

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    index_by_source: dict[str, list[dict]] = {}

    for json_file in sorted(self._api_info_directory.rglob('*.json')):
      full_name = json_file.stem.lower()
      parts = full_name.split('_', 1)
      if len(parts) == 2:
        source_name = parts[0]
        object_name = parts[1]
      else:
        source_name = 'default'
        object_name = full_name

      registry = build_register(str(json_file), full_name, full_name, source_name)
      if not registry:
        continue

      source_dir = out_path / source_name
      source_dir.mkdir(parents=True, exist_ok=True)

      md_content = registry.render_markdown(object_name, source_name)
      (source_dir / f'{object_name}.md').write_text(md_content, encoding='utf-8')
      log.info(f'Generated docs/{source_name}/{object_name}.md')

      if source_name not in index_by_source:
        index_by_source[source_name] = []
      index_by_source[source_name].append({
        'name': object_name,
        'description': registry.description or '',
        'columns': len(registry.columns),
      })

    for source_name, models in index_by_source.items():
      self._write_source_index(out_path / source_name, source_name, models)

    self._write_docs_index(out_path, index_by_source)

  def _write_source_index(self, source_dir: Path, source_name: str, models: list[dict]) -> None:
    lines = [
      f'# {source_name.upper()} — Model Reference',
      '',
      f'{len(models)} models sourced from the `{source_name}` landing schema.',
      '',
      '| Model | Columns | Description |',
      '|---|---|---|',
    ]
    for m in sorted(models, key=lambda x: x['name']):
      desc = m['description'].replace('|', '\\|')
      lines.append(f"| [{m['name']}](./{m['name']}.md) | {m['columns']} | {desc} |")
    lines.append('')
    (source_dir / 'README.md').write_text('\n'.join(lines), encoding='utf-8')
    log.info(f"Written docs/{source_name}/README.md ({len(models)} models)")

  def _write_docs_index(self, out_path: Path, index_by_source: dict[str, list[dict]]) -> None:
    total = sum(len(v) for v in index_by_source.values())
    lines = [
      '# Data Catalogue',
      '',
      f'Auto-generated reference documentation covering {total} models across '
      f'{len(index_by_source)} source system(s).',
      '',
      '## Source Systems',
      '',
      '| Source | Models | Description |',
      '|---|---|---|',
    ]
    source_descriptions = {
      'depm': 'D/EPM planning and project management',
      'eam': 'Enterprise Asset Management',
      'ips': 'IPS infrastructure and planning',
      'ln': 'LN ERP tables',
    }
    for source_name in sorted(index_by_source):
      count = len(index_by_source[source_name])
      desc = source_descriptions.get(source_name, '')
      lines.append(f'| [{source_name.upper()}](./{source_name}/README.md) | {count} | {desc} |')
    lines.append('')
    (out_path / 'README.md').write_text('\n'.join(lines), encoding='utf-8')
    log.info(f"Written docs/README.md ({total} total models)")

  def _update_sources_yml(self, out_path: Path, tables_by_source: dict) -> None:
    for source_name, tables in tables_by_source.items():
      source_dir = out_path / source_name
      sources_file = source_dir / '__sources.yml'
          
      sources_data = {'version': 2, 'sources': []}
      if sources_file.exists():
        try:
          sources_data = yaml.safe_load(sources_file.read_text(encoding='utf-8')) or sources_data
        except Exception:
          pass
                  
      if 'sources' not in sources_data or not isinstance(sources_data['sources'], list):
        sources_data['sources'] = []
              
      source_landing_name = f'source_landing_{source_name}'
      source_block = next((s for s in sources_data['sources'] if s.get('name') == source_landing_name), None)
          
      if not source_block:
        source_block = {
          'name': source_landing_name,
          'database': "{{ 'landing_prod' if target.name == 'prod' else ('landing_sit' if target.name == 'sit' else 'landing_dev') }}",
          'schema': source_name,
          'tables': []
        }
        sources_data['sources'].append(source_block)
              
      existing_tables = source_block.get('tables', [])
      table_map = {t['name']: t for t in existing_tables}
          
      for t in tables:
        if t['name'] not in table_map:
          table_map[t['name']] = t
        else:
          if 'description' in t:
            table_map[t['name']]['description'] = t['description']
          elif 'description' in table_map[t['name']]:
            del table_map[t['name']]['description']
                  
      source_block['tables'] = list(table_map.values())
          
      sources_file.write_text(
        yaml.dump(sources_data, sort_keys=False, default_flow_style=False, indent=2),
        encoding='utf-8'
      )
      log.info(f"Updated {sources_file} with {len(tables)} table targets.")
