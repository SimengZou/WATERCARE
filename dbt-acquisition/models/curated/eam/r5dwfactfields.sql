{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWFACTFIELDS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FFL_LASTSAVED'], type='timestamp_ntz', alias='ffl_lastsaved') }},
        {{ trx_json_extract('src', ['FFL_DMTTABLE'], type='varchar', alias='ffl_dmttable') }},
        {{ trx_json_extract('src', ['FFL_DESC'], type='varchar', alias='ffl_desc') }},
        {{ trx_json_extract('src', ['FFL_BOTNUMBER'], type='float', alias='ffl_botnumber') }},
        {{ trx_json_extract('src', ['FFL_FIELDID'], type='float', alias='ffl_fieldid') }},
        {{ trx_json_extract('src', ['FFL_FIELD'], type='varchar', alias='ffl_field') }},
        {{ trx_json_extract('src', ['FFL_ADDITIVITY'], type='varchar', alias='ffl_additivity') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwfactfields') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ffl_lastsaved']) }}
