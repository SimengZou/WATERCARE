{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5SCWORKORDERCOST',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CWS_LASTSAVED'], type='timestamp_ntz', alias='cws_lastsaved') }},
        {{ trx_json_extract('src', ['CWS_JOBTYPE'], type='varchar', alias='cws_jobtype') }},
        {{ trx_json_extract('src', ['CWS_COST'], type='float', alias='cws_cost') }},
        {{ trx_json_extract('src', ['CWS_COSTDEFCURR'], type='float', alias='cws_costdefcurr') }},
        {{ trx_json_extract('src', ['CWS_ORG'], type='varchar', alias='cws_org') }},
        {{ trx_json_extract('src', ['CWS_DATE'], type='timestamp_ntz', alias='cws_date') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5scworkordercost') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cws_org', 'cws_jobtype', 'cws_date']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['cws_lastsaved']) }}
