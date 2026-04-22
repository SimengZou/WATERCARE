{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FUTUREDUPLICATES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FDP_LASTSAVED'], type='timestamp_ntz', alias='fdp_lastsaved') }},
        {{ trx_json_extract('src', ['FDP_DATE'], type='timestamp_ntz', alias='fdp_date') }},
        {{ trx_json_extract('src', ['FDP_PPOPK'], type='float', alias='fdp_ppopk') }},
        {{ trx_json_extract('src', ['FDP_SEQNO'], type='float', alias='fdp_seqno') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5futureduplicates') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['fdp_ppopk', 'fdp_seqno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['fdp_lastsaved']) }}
