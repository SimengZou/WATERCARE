{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FILTERTABCOLUMNS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FTC_LASTSAVED'], type='timestamp_ntz', alias='ftc_lastsaved') }},
        {{ trx_json_extract('src', ['FTC_COLUMN'], type='varchar', alias='ftc_column') }},
        {{ trx_json_extract('src', ['FTC_CLOB'], type='varchar', alias='ftc_clob') }},
        {{ trx_json_extract('src', ['FTC_UPDATECOUNT'], type='float', alias='ftc_updatecount') }},
        {{ trx_json_extract('src', ['FTC_TABLE'], type='varchar', alias='ftc_table') }},
        {{ trx_json_extract('src', ['FTC_DATATYPE'], type='varchar', alias='ftc_datatype') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5filtertabcolumns') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ftc_table', 'ftc_column']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ftc_lastsaved']) }}
