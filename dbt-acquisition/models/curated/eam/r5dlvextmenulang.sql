{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DLVEXTMENULANG',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['EML_LASTSAVED'], type='timestamp_ntz', alias='eml_lastsaved') }},
        {{ trx_json_extract('src', ['EML_TRANS'], type='varchar', alias='eml_trans') }},
        {{ trx_json_extract('src', ['EML_TEXT'], type='varchar', alias='eml_text') }},
        {{ trx_json_extract('src', ['EML_UPDATECOUNT'], type='float', alias='eml_updatecount') }},
        {{ trx_json_extract('src', ['EML_EXTMENU'], type='varchar', alias='eml_extmenu') }},
        {{ trx_json_extract('src', ['EML_LANG'], type='varchar', alias='eml_lang') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dlvextmenulang') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['eml_lastsaved']) }}
