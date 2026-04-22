{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWDATAMARTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DMT_LASTSAVED'], type='timestamp_ntz', alias='dmt_lastsaved') }},
        {{ trx_json_extract('src', ['DMT_DESC'], type='varchar', alias='dmt_desc') }},
        {{ trx_json_extract('src', ['DMT_TABLETYPE'], type='varchar', alias='dmt_tabletype') }},
        {{ trx_json_extract('src', ['DMT_BOTNUMBER'], type='float', alias='dmt_botnumber') }},
        {{ trx_json_extract('src', ['DMT_TABLE'], type='varchar', alias='dmt_table') }},
        {{ trx_json_extract('src', ['DMT_GRAIN'], type='varchar', alias='dmt_grain') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwdatamarts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['dmt_lastsaved']) }}
