{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5RELIABILITYRANKINGANSWER',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['RRW_LASTSAVED'], type='timestamp_ntz', alias='rrw_lastsaved') }},
        {{ trx_json_extract('src', ['RRW_SEVERITY'], type='varchar', alias='rrw_severity') }},
        {{ trx_json_extract('src', ['RRW_CODE'], type='varchar', alias='rrw_code') }},
        {{ trx_json_extract('src', ['RRW_DESC'], type='varchar', alias='rrw_desc') }},
        {{ trx_json_extract('src', ['RRW_VALUE'], type='float', alias='rrw_value') }},
        {{ trx_json_extract('src', ['RRW_UPDATECOUNT'], type='float', alias='rrw_updatecount') }},
        {{ trx_json_extract('src', ['RRW_YES'], type='varchar', alias='rrw_yes') }},
        {{ trx_json_extract('src', ['RRW_NO'], type='varchar', alias='rrw_no') }},
        {{ trx_json_extract('src', ['RRW_FINDING'], type='varchar', alias='rrw_finding') }},
        {{ trx_json_extract('src', ['RRW_OK'], type='varchar', alias='rrw_ok') }},
        {{ trx_json_extract('src', ['RRW_REPAIRSNEEDED'], type='varchar', alias='rrw_repairsneeded') }},
        {{ trx_json_extract('src', ['RRW_RESOLUTION'], type='varchar', alias='rrw_resolution') }},
        {{ trx_json_extract('src', ['RRW_GOOD'], type='varchar', alias='rrw_good') }},
        {{ trx_json_extract('src', ['RRW_POOR'], type='varchar', alias='rrw_poor') }},
        {{ trx_json_extract('src', ['RRW_ADJUSTED'], type='varchar', alias='rrw_adjusted') }},
        {{ trx_json_extract('src', ['RRW_NONCONFORMITY'], type='varchar', alias='rrw_nonconformity') }},
        {{ trx_json_extract('src', ['RRW_PK'], type='varchar', alias='rrw_pk') }},
        {{ trx_json_extract('src', ['RRW_LEVELPK'], type='varchar', alias='rrw_levelpk') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5reliabilityrankinganswer') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rrw_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rrw_lastsaved']) }}
