{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ORGTABCOLUMNS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['OTC_LASTSAVED'], type='timestamp_ntz', alias='otc_lastsaved') }},
        {{ trx_json_extract('src', ['OTC_UPDATECOUNT'], type='float', alias='otc_updatecount') }},
        {{ trx_json_extract('src', ['OTC_TABLE'], type='varchar', alias='otc_table') }},
        {{ trx_json_extract('src', ['OTC_COLUMN'], type='varchar', alias='otc_column') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5orgtabcolumns') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['otc_table', 'otc_column']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['otc_lastsaved']) }}
