{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5REPORTSUBTOTAL',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['RST_LASTSAVED'], type='timestamp_ntz', alias='rst_lastsaved') }},
        {{ trx_json_extract('src', ['RST_GROUPLINE'], type='float', alias='rst_groupline') }},
        {{ trx_json_extract('src', ['RST_LINE'], type='float', alias='rst_line') }},
        {{ trx_json_extract('src', ['RST_BOTNUMBER'], type='float', alias='rst_botnumber') }},
        {{ trx_json_extract('src', ['RST_DATATYPE'], type='varchar', alias='rst_datatype') }},
        {{ trx_json_extract('src', ['RST_WIDTH'], type='float', alias='rst_width') }},
        {{ trx_json_extract('src', ['RST_UPDATECOUNT'], type='float', alias='rst_updatecount') }},
        {{ trx_json_extract('src', ['RST_UPDATED'], type='timestamp_ntz', alias='rst_updated') }},
        {{ trx_json_extract('src', ['RST_FUNCTION'], type='varchar', alias='rst_function') }},
        {{ trx_json_extract('src', ['RST_FIELD'], type='varchar', alias='rst_field') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5reportsubtotal') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rst_function', 'rst_groupline', 'rst_line']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rst_lastsaved']) }}
