{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5GLREFERENCESCTRL',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['GRC_LASTSAVED'], type='timestamp_ntz', alias='grc_lastsaved') }},
        {{ trx_json_extract('src', ['GRC_DISPLAYNAME'], type='varchar', alias='grc_displayname') }},
        {{ trx_json_extract('src', ['GRC_LENGTH'], type='float', alias='grc_length') }},
        {{ trx_json_extract('src', ['GRC_DISPLAYSEQ'], type='float', alias='grc_displayseq') }},
        {{ trx_json_extract('src', ['GRC_UPDATECOUNT'], type='float', alias='grc_updatecount') }},
        {{ trx_json_extract('src', ['GRC_R5COLUMN'], type='varchar', alias='grc_r5column') }},
        {{ trx_json_extract('src', ['GRC_DATATYPE'], type='varchar', alias='grc_datatype') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5glreferencesctrl') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['grc_r5column']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['grc_lastsaved']) }}
