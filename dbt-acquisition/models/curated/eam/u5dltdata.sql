{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5DLTDATA',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['COLUMNNAME'], type='varchar', alias='columnname') }},
        {{ trx_json_extract('src', ['COLUMNTYPE'], type='varchar', alias='columntype') }},
        {{ trx_json_extract('src', ['COLUMNPRECISION'], type='float', alias='columnprecision') }},
        {{ trx_json_extract('src', ['COLUMNSCALING'], type='float', alias='columnscaling') }},
        {{ trx_json_extract('src', ['COLUMNNULLABLE'], type='varchar', alias='columnnullable') }},
        {{ trx_json_extract('src', ['TABLENAME'], type='varchar', alias='tablename') }},
        {{ trx_json_extract('src', ['COLUMNMAXLENGTH'], type='float', alias='columnmaxlength') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5dltdata') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
