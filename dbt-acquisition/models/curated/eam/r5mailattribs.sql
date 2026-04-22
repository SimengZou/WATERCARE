{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MAILATTRIBS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MAA_LASTSAVED'], type='timestamp_ntz', alias='maa_lastsaved') }},
        {{ trx_json_extract('src', ['MAA_ENTEREDBY'], type='varchar', alias='maa_enteredby') }},
        {{ trx_json_extract('src', ['MAA_COMMENT'], type='varchar', alias='maa_comment') }},
        {{ trx_json_extract('src', ['MAA_INSERT'], type='varchar', alias='maa_insert') }},
        {{ trx_json_extract('src', ['MAA_UPDATE'], type='varchar', alias='maa_update') }},
        {{ trx_json_extract('src', ['MAA_DELETE'], type='varchar', alias='maa_delete') }},
        {{ trx_json_extract('src', ['MAA_OLDSTATUS'], type='varchar', alias='maa_oldstatus') }},
        {{ trx_json_extract('src', ['MAA_NEWSTATUS'], type='varchar', alias='maa_newstatus') }},
        {{ trx_json_extract('src', ['MAA_WORKFLOW'], type='varchar', alias='maa_workflow') }},
        {{ trx_json_extract('src', ['MAA_UPDATECOUNT'], type='float', alias='maa_updatecount') }},
        {{ trx_json_extract('src', ['MAA_PK'], type='varchar', alias='maa_pk') }},
        {{ trx_json_extract('src', ['MAA_ACTIVE'], type='varchar', alias='maa_active') }},
        {{ trx_json_extract('src', ['MAA_INCLUDEURL'], type='varchar', alias='maa_includeurl') }},
        {{ trx_json_extract('src', ['MAA_INCLUDEATTACHMENT'], type='varchar', alias='maa_includeattachment') }},
        {{ trx_json_extract('src', ['MAA_TABLE'], type='varchar', alias='maa_table') }},
        {{ trx_json_extract('src', ['MAA_TEMPLATE'], type='varchar', alias='maa_template') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mailattribs') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['maa_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['maa_lastsaved']) }}
