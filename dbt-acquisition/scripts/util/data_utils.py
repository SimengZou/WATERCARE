import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar

from pydantic import BaseModel

_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
_DATETIME_RE = re.compile(r'\d{4}-\d{2}-\d{2}[T ](?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?$')
_TIME_RE = re.compile(r'^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(Z|[+-](?:2[0-3]|[01]\d):[0-5]\d)?$')
_TIMESTAMP_RE = re.compile(r'^\d{4}-\d{2}-\d{2}[T ](?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(Z|[+-](?:2[0-3]|[01]\d):[0-5]\d)?$')


# @dataclass
class TypeMapping(BaseModel):
  snowflake: str
  dbt: str
  generic: str

# @dataclass
class DataType(BaseModel):
  INTEGER: ClassVar[TypeMapping] = TypeMapping(snowflake='int', dbt='integer', generic='integer')
  BIGINT: ClassVar[TypeMapping] = TypeMapping(snowflake='bigint', dbt='bigint', generic='integer')
  FLOAT: ClassVar[TypeMapping] = TypeMapping(snowflake='float', dbt='float', generic='float')
  STRING: ClassVar[TypeMapping] = TypeMapping(snowflake='varchar', dbt='string', generic='string')
  BOOLEAN: ClassVar[TypeMapping] = TypeMapping(snowflake='boolean', dbt='boolean', generic='boolean')
  DATE: ClassVar[TypeMapping] = TypeMapping(snowflake='date', dbt='date', generic='date')
  DATETIME: ClassVar[TypeMapping] = TypeMapping(snowflake='timestamp_ntz', dbt='timestamp_ntz', generic='timestamp')
  TIME: ClassVar[TypeMapping] = TypeMapping(snowflake='time', dbt='time', generic='time')
  TIMESTAMP: ClassVar[TypeMapping] = TypeMapping(snowflake='timestamp_ntz', dbt='timestamp_ntz', generic='timestamp')
  MIXED: ClassVar[TypeMapping] = TypeMapping(snowflake='mixed', dbt='mixed', generic='string')
  NODATA: ClassVar[TypeMapping] = TypeMapping(snowflake='nodata', dbt='nodata', generic='string')
  DEFAULT: ClassVar[TypeMapping] = TypeMapping(snowflake='default', dbt='default', generic='string')

  _snowflake_map = None
  _api_schema_map = None

  @property
  def snowflake(self) -> str:
    return self.value.snowflake
  
  @property
  def dbt(self) -> str:
    return self.value.dbt
  
  @property
  def generic(self) -> str:
    return self.value.generic

  @classmethod
  def from_sample(self, sample: Any) -> TypeMapping:
    if isinstance(sample, int):
      return self.INTEGER
    if isinstance(sample, float):
      return self.FLOAT
    if isinstance(sample, str):
      if _DATE_RE.match(sample):
        return self.DATE
      if _DATETIME_RE.match(sample):
        return self.DATETIME
      if _TIME_RE.match(sample):
        return self.TIME
      if _TIMESTAMP_RE.match(sample):
        return self.TIMESTAMP
      return self.STRING
    if isinstance(sample, bool):
      return self.BOOLEAN
    return self.DEFAULT

  @classmethod
  def from_json_schema(self, data_type: str|None, format: str|None = None) -> TypeMapping:
    if data_type == 'integer':
      return self.INTEGER
    if data_type == 'number':
      return self.FLOAT
    if data_type == 'boolean':
      return self.BOOLEAN
    if data_type == 'string':
      if format == 'date':
        return self.DATE
      if format == 'date-time':
        return self.DATETIME
      if format == 'time':
        return self.TIME
      return self.STRING
    if data_type == None:
      return self.NODATA
    return self.DEFAULT
