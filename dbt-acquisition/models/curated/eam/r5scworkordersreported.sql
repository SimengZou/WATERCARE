{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5SCWORKORDERSREPORTED',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CWR_LASTSAVED'], type='timestamp_ntz', alias='cwr_lastsaved') }},
        {{ trx_json_extract('src', ['CWR_DATE'], type='timestamp_ntz', alias='cwr_date') }},
        {{ trx_json_extract('src', ['CWR_ORG'], type='varchar', alias='cwr_org') }},
        {{ trx_json_extract('src', ['CWR_MRC'], type='varchar', alias='cwr_mrc') }},
        {{ trx_json_extract('src', ['CWR_WOSREPORTED'], type='float', alias='cwr_wosreported') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5scworkordersreported') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cwr_org', 'cwr_mrc', 'cwr_date']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['cwr_lastsaved']) }}
