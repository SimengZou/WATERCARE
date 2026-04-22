{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5TABPERMISSIONS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['TRP_LASTSAVED'], type='timestamp_ntz', alias='trp_lastsaved') }},
        {{ trx_json_extract('src', ['TRP_GROUP'], type='varchar', alias='trp_group') }},
        {{ trx_json_extract('src', ['TRP_VISIBLE'], type='varchar', alias='trp_visible') }},
        {{ trx_json_extract('src', ['TRP_SELECT'], type='varchar', alias='trp_select') }},
        {{ trx_json_extract('src', ['TRP_UPDATE'], type='varchar', alias='trp_update') }},
        {{ trx_json_extract('src', ['TRP_INSERT'], type='varchar', alias='trp_insert') }},
        {{ trx_json_extract('src', ['TRP_DELETE'], type='varchar', alias='trp_delete') }},
        {{ trx_json_extract('src', ['TRP_SYSREQUIRED'], type='varchar', alias='trp_sysrequired') }},
        {{ trx_json_extract('src', ['TRP_UPDATECOUNT'], type='float', alias='trp_updatecount') }},
        {{ trx_json_extract('src', ['TRP_SEQUENCE'], type='float', alias='trp_sequence') }},
        {{ trx_json_extract('src', ['TRP_SECURITYDDSPYID'], type='float', alias='trp_securityddspyid') }},
        {{ trx_json_extract('src', ['TRP_ALTERSAVE'], type='varchar', alias='trp_altersave') }},
        {{ trx_json_extract('src', ['TRP_UPDATED'], type='timestamp_ntz', alias='trp_updated') }},
        {{ trx_json_extract('src', ['TRP_FUNCTION'], type='varchar', alias='trp_function') }},
        {{ trx_json_extract('src', ['TRP_TAB'], type='varchar', alias='trp_tab') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5tabpermissions') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['trp_function', 'trp_group', 'trp_tab']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['trp_lastsaved']) }}
