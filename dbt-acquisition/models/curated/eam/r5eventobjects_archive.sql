{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5EVENTOBJECTS_ARCHIVE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['AEO_LASTSAVED'], type='timestamp_ntz', alias='aeo_lastsaved') }},
        {{ trx_json_extract('src', ['AEO_EVENT'], type='varchar', alias='aeo_event') }},
        {{ trx_json_extract('src', ['AEO_OBTYPE'], type='varchar', alias='aeo_obtype') }},
        {{ trx_json_extract('src', ['AEO_OBRTYPE'], type='varchar', alias='aeo_obrtype') }},
        {{ trx_json_extract('src', ['AEO_OBJECT'], type='varchar', alias='aeo_object') }},
        {{ trx_json_extract('src', ['AEO_LEVEL'], type='float', alias='aeo_level') }},
        {{ trx_json_extract('src', ['AEO_DOWNTIME'], type='varchar', alias='aeo_downtime') }},
        {{ trx_json_extract('src', ['AEO_OBJECT_ORG'], type='varchar', alias='aeo_object_org') }},
        {{ trx_json_extract('src', ['AEO_UPDATECOUNT'], type='float', alias='aeo_updatecount') }},
        {{ trx_json_extract('src', ['AEO_FROMPOINT'], type='float', alias='aeo_frompoint') }},
        {{ trx_json_extract('src', ['AEO_TOPOINT'], type='float', alias='aeo_topoint') }},
        {{ trx_json_extract('src', ['AEO_WEIGHTPERCENTAGE'], type='float', alias='aeo_weightpercentage') }},
        {{ trx_json_extract('src', ['AEO_ARCHIVE'], type='float', alias='aeo_archive') }},
        {{ trx_json_extract('src', ['AEO_ROLLUP'], type='varchar', alias='aeo_rollup') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5eventobjects_archive') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['aeo_lastsaved']) }}
