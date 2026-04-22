{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5REPORTFUNCTIONS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['RFN_LASTSAVED'], type='timestamp_ntz', alias='rfn_lastsaved') }},
        {{ trx_json_extract('src', ['RFN_FIELDORDER'], type='varchar', alias='rfn_fieldorder') }},
        {{ trx_json_extract('src', ['RFN_GROUPBY'], type='varchar', alias='rfn_groupby') }},
        {{ trx_json_extract('src', ['RFN_ORDERBY'], type='varchar', alias='rfn_orderby') }},
        {{ trx_json_extract('src', ['RFN_ORDERTYPE'], type='varchar', alias='rfn_ordertype') }},
        {{ trx_json_extract('src', ['RFN_WHERECLAUSE1'], type='varchar', alias='rfn_whereclause1') }},
        {{ trx_json_extract('src', ['RFN_WHERECLAUSE2'], type='varchar', alias='rfn_whereclause2') }},
        {{ trx_json_extract('src', ['RFN_UPDATECOUNT'], type='float', alias='rfn_updatecount') }},
        {{ trx_json_extract('src', ['RFN_UPDATED'], type='timestamp_ntz', alias='rfn_updated') }},
        {{ trx_json_extract('src', ['RFN_FUNCTION'], type='varchar', alias='rfn_function') }},
        {{ trx_json_extract('src', ['RFN_FROMCLAUSE'], type='varchar', alias='rfn_fromclause') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5reportfunctions') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rfn_function']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rfn_lastsaved']) }}
