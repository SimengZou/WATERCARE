{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5BARESC',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['BCE_LASTSAVED'], type='timestamp_ntz', alias='bce_lastsaved') }},
        {{ trx_json_extract('src', ['BCE_TEXT2'], type='varchar', alias='bce_text2') }},
        {{ trx_json_extract('src', ['BCE_LINE'], type='float', alias='bce_line') }},
        {{ trx_json_extract('src', ['BCE_ESCAPE'], type='varchar', alias='bce_escape') }},
        {{ trx_json_extract('src', ['BCE_UPDATECOUNT'], type='float', alias='bce_updatecount') }},
        {{ trx_json_extract('src', ['BCE_TEXT1'], type='varchar', alias='bce_text1') }},
        {{ trx_json_extract('src', ['BCE_COLUMN'], type='varchar', alias='bce_column') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5baresc') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['bce_line', 'bce_escape', 'bce_column']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['bce_lastsaved']) }}
