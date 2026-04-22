import re
from sqlglot import exp, optimizer, parse_one
from sqlglot.dialects.snowflake import Snowflake
from sqlglot.optimizer.annotate_types import annotate_types as _annotate_types
from sqlglot.optimizer.canonicalize import canonicalize as _canonicalize
from sqlglot.optimizer.eliminate_ctes import eliminate_ctes as _eliminate_ctes
from sqlglot.optimizer.eliminate_joins import eliminate_joins as _eliminate_joins
from sqlglot.optimizer.eliminate_subqueries import eliminate_subqueries as _eliminate_subqueries
from sqlglot.optimizer.merge_subqueries import merge_subqueries as _merge_subqueries
from sqlglot.optimizer.normalize import normalize as _normalize
from sqlglot.optimizer.optimize_joins import optimize_joins as _optimize_joins
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates as _pushdown_predicates
from sqlglot.optimizer.pushdown_projections import pushdown_projections as _pushdown_projections
from sqlglot.optimizer.qualify import qualify as _qualify
from sqlglot.optimizer.qualify_columns import quote_identifiers as _quote_identifiers
from sqlglot.optimizer.simplify import simplify as _simplify
from sqlglot.optimizer.unnest_subqueries import unnest_subqueries as _unnest_subqueries


glot_rules = [rule for rule, enabled in {
  _qualify: True,
  _pushdown_projections: True,
  _normalize: True,
  _unnest_subqueries: True,
  _pushdown_predicates: True,
  _optimize_joins: True,
  _eliminate_subqueries: True,
  _merge_subqueries: True,
  _eliminate_joins: True,
  _eliminate_ctes: True,
  _quote_identifiers: False,
  _annotate_types: True,
  _canonicalize: True,
  _simplify: True
}.items() if enabled]

class LowercaseSnowflake(Snowflake):
    class Generator(Snowflake.Generator):
        def join_sql(self, expression):
            kind = expression.args.get("kind")
            if not kind:
                expression.set('kind', exp.Identifier(this="INNER", quoted=False))
            return super().join_sql(expression)
        
        def generate(self, expression, copy=True):
            sql = super().generate(expression, copy=copy)
            return re.sub(
                r'("[^"]*"|\'[^\']*\'|\b[A-Z_]{2,}\b)',
                lambda match: match.group(0).lower() if not (
                    match.group(0).startswith('"') or match.group(0).startswith("'")
                ) else match.group(0),
                sql
            )

def optimise_sql(sql: str, dialect: str = 'snowflake') -> str:
  cus_dialect = LowercaseSnowflake if dialect.lower() == 'snowflake' else dialect
  parsed = parse_one(sql, dialect=cus_dialect)
  parsed = optimizer.optimize(parsed, dialect=cus_dialect, rules=glot_rules) # type: ignore[arg-type]

  return parsed.sql(dialect=LowercaseSnowflake, pretty=True)
