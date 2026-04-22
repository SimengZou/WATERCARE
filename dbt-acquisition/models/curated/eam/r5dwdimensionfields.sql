{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWDIMENSIONFIELDS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DMF_LASTSAVED'], type='timestamp_ntz', alias='dmf_lastsaved') }},
        {{ trx_json_extract('src', ['DMF_DESC'], type='varchar', alias='dmf_desc') }},
        {{ trx_json_extract('src', ['DMF_FIELD'], type='varchar', alias='dmf_field') }},
        {{ trx_json_extract('src', ['DMF_FIELDID'], type='float', alias='dmf_fieldid') }},
        {{ trx_json_extract('src', ['DMF_DIMTABLE'], type='varchar', alias='dmf_dimtable') }},
        {{ trx_json_extract('src', ['DMF_BOTNUMBER'], type='float', alias='dmf_botnumber') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwdimensionfields') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['dmf_lastsaved']) }}
