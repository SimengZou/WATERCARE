{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DSCHAVAIL',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['SHA_LASTSAVED'], type='timestamp_ntz', alias='sha_lastsaved') }},
        {{ trx_json_extract('src', ['SHA_PERSON'], type='varchar', alias='sha_person') }},
        {{ trx_json_extract('src', ['SHA_SHIFT'], type='varchar', alias='sha_shift') }},
        {{ trx_json_extract('src', ['SHA_HOURS'], type='float', alias='sha_hours') }},
        {{ trx_json_extract('src', ['SHA_MRC'], type='varchar', alias='sha_mrc') }},
        {{ trx_json_extract('src', ['SHA_GENDATE'], type='timestamp_ntz', alias='sha_gendate') }},
        {{ trx_json_extract('src', ['SHA_UPDATECOUNT'], type='float', alias='sha_updatecount') }},
        {{ trx_json_extract('src', ['SHA_DATE'], type='timestamp_ntz', alias='sha_date') }},
        {{ trx_json_extract('src', ['SHA_TRADE'], type='varchar', alias='sha_trade') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dschavail') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sha_lastsaved']) }}
