{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5EXTMENUS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['EMN_LASTSAVED'], type='timestamp_ntz', alias='emn_lastsaved') }},
        {{ trx_json_extract('src', ['EMN_SEQUENCE'], type='float', alias='emn_sequence') }},
        {{ trx_json_extract('src', ['EMN_GROUP'], type='varchar', alias='emn_group') }},
        {{ trx_json_extract('src', ['EMN_FUNCTION'], type='varchar', alias='emn_function') }},
        {{ trx_json_extract('src', ['EMN_PARENT'], type='varchar', alias='emn_parent') }},
        {{ trx_json_extract('src', ['EMN_TYPE'], type='varchar', alias='emn_type') }},
        {{ trx_json_extract('src', ['EMN_DUXMOBILE'], type='varchar', alias='emn_duxmobile') }},
        {{ trx_json_extract('src', ['EMN_MEFLAG'], type='varchar', alias='emn_meflag') }},
        {{ trx_json_extract('src', ['EMN_HIDE'], type='varchar', alias='emn_hide') }},
        {{ trx_json_extract('src', ['EMN_MOBILE'], type='varchar', alias='emn_mobile') }},
        {{ trx_json_extract('src', ['EMN_ICON'], type='varchar', alias='emn_icon') }},
        {{ trx_json_extract('src', ['EMN_TRANSIT'], type='varchar', alias='emn_transit') }},
        {{ trx_json_extract('src', ['EMN_CODE'], type='varchar', alias='emn_code') }},
        {{ trx_json_extract('src', ['EMN_UPDATECOUNT'], type='float', alias='emn_updatecount') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5extmenus') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['emn_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['emn_lastsaved']) }}
