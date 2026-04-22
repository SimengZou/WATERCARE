{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5HAZARDPRECAUTIONS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['HZP_LASTSAVED'], type='timestamp_ntz', alias='hzp_lastsaved') }},
        {{ trx_json_extract('src', ['HZP_HAZARD_ORG'], type='varchar', alias='hzp_hazard_org') }},
        {{ trx_json_extract('src', ['HZP_REVISION'], type='float', alias='hzp_revision') }},
        {{ trx_json_extract('src', ['HZP_PRECAUTION_ORG'], type='varchar', alias='hzp_precaution_org') }},
        {{ trx_json_extract('src', ['HZP_SEQUENCE'], type='float', alias='hzp_sequence') }},
        {{ trx_json_extract('src', ['HZP_UPDATECOUNT'], type='float', alias='hzp_updatecount') }},
        {{ trx_json_extract('src', ['HZP_HAZARD'], type='varchar', alias='hzp_hazard') }},
        {{ trx_json_extract('src', ['HZP_PRECAUTION'], type='varchar', alias='hzp_precaution') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5hazardprecautions') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['hzp_hazard', 'hzp_hazard_org', 'hzp_revision', 'hzp_precaution', 'hzp_precaution_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['hzp_lastsaved']) }}
