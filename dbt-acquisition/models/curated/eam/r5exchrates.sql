{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5EXCHRATES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CRR_LASTSAVED'], type='timestamp_ntz', alias='crr_lastsaved') }},
        {{ trx_json_extract('src', ['CRR_EXCH'], type='float', alias='crr_exch') }},
        {{ trx_json_extract('src', ['CRR_START'], type='timestamp_ntz', alias='crr_start') }},
        {{ trx_json_extract('src', ['CRR_END'], type='timestamp_ntz', alias='crr_end') }},
        {{ trx_json_extract('src', ['CRR_ACTIVE'], type='varchar', alias='crr_active') }},
        {{ trx_json_extract('src', ['CRR_SOURCECODE'], type='varchar', alias='crr_sourcecode') }},
        {{ trx_json_extract('src', ['CRR_INTERFACE'], type='timestamp_ntz', alias='crr_interface') }},
        {{ trx_json_extract('src', ['CRR_ORGCURR'], type='varchar', alias='crr_orgcurr') }},
        {{ trx_json_extract('src', ['CRR_UPDATECOUNT'], type='float', alias='crr_updatecount') }},
        {{ trx_json_extract('src', ['CRR_CODE'], type='varchar', alias='crr_code') }},
        {{ trx_json_extract('src', ['CRR_CURR'], type='varchar', alias='crr_curr') }},
        {{ trx_json_extract('src', ['CRR_SOURCESYSTEM'], type='varchar', alias='crr_sourcesystem') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5exchrates') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['crr_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['crr_lastsaved']) }}
