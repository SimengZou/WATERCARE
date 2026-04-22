{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5STRUCTURES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['STC_LASTSAVED'], type='timestamp_ntz', alias='stc_lastsaved') }},
        {{ trx_json_extract('src', ['STC_CHILD'], type='varchar', alias='stc_child') }},
        {{ trx_json_extract('src', ['STC_PARENTTYPE'], type='varchar', alias='stc_parenttype') }},
        {{ trx_json_extract('src', ['STC_PARENTRTYPE'], type='varchar', alias='stc_parentrtype') }},
        {{ trx_json_extract('src', ['STC_PARENT'], type='varchar', alias='stc_parent') }},
        {{ trx_json_extract('src', ['STC_ROLLDOWN'], type='varchar', alias='stc_rolldown') }},
        {{ trx_json_extract('src', ['STC_ROLLUP'], type='varchar', alias='stc_rollup') }},
        {{ trx_json_extract('src', ['STC_EQUIVALENT'], type='varchar', alias='stc_equivalent') }},
        {{ trx_json_extract('src', ['STC_DOWNTIME'], type='varchar', alias='stc_downtime') }},
        {{ trx_json_extract('src', ['STC_CHILD_ORG'], type='varchar', alias='stc_child_org') }},
        {{ trx_json_extract('src', ['STC_PARENT_ORG'], type='varchar', alias='stc_parent_org') }},
        {{ trx_json_extract('src', ['STC_UPDATECOUNT'], type='float', alias='stc_updatecount') }},
        {{ trx_json_extract('src', ['STC_UPDATED'], type='timestamp_ntz', alias='stc_updated') }},
        {{ trx_json_extract('src', ['STC_SEQUENCE'], type='float', alias='stc_sequence') }},
        {{ trx_json_extract('src', ['STC_MNBDEFINITION'], type='varchar', alias='stc_mnbdefinition') }},
        {{ trx_json_extract('src', ['STC_CHILDTYPE'], type='varchar', alias='stc_childtype') }},
        {{ trx_json_extract('src', ['STC_CHILDRTYPE'], type='varchar', alias='stc_childrtype') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5structures') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['stc_child', 'stc_child_org', 'stc_parent', 'stc_parent_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['stc_lastsaved']) }}
