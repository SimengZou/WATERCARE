{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5EDITIONFILTERS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['EDF_LASTSAVED'], type='timestamp_ntz', alias='edf_lastsaved') }},
        {{ trx_json_extract('src', ['EDF_TYPE'], type='varchar', alias='edf_type') }},
        {{ trx_json_extract('src', ['EDF_FILTER'], type='varchar', alias='edf_filter') }},
        {{ trx_json_extract('src', ['EDF_CODE'], type='varchar', alias='edf_code') }},
        {{ trx_json_extract('src', ['EDF_MEFLAG'], type='varchar', alias='edf_meflag') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5editionfilters') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['edf_code', 'edf_type', 'edf_meflag']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['edf_lastsaved']) }}
