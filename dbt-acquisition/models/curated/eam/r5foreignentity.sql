{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FOREIGNENTITY',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FEN_LASTSAVED'], type='timestamp_ntz', alias='fen_lastsaved') }},
        {{ trx_json_extract('src', ['FEN_SYSTEMFUNCTION'], type='varchar', alias='fen_systemfunction') }},
        {{ trx_json_extract('src', ['FEN_FOREIGNUSERFUNCTION'], type='varchar', alias='fen_foreignuserfunction') }},
        {{ trx_json_extract('src', ['FEN_FOREIGNSYSTEMFUNCTION'], type='varchar', alias='fen_foreignsystemfunction') }},
        {{ trx_json_extract('src', ['FEN_ELEMENTID'], type='varchar', alias='fen_elementid') }},
        {{ trx_json_extract('src', ['FEN_ORGFIELD'], type='varchar', alias='fen_orgfield') }},
        {{ trx_json_extract('src', ['FEN_RENTITY'], type='varchar', alias='fen_rentity') }},
        {{ trx_json_extract('src', ['FEN_UPDATECOUNT'], type='float', alias='fen_updatecount') }},
        {{ trx_json_extract('src', ['FEN_ALLOWDRILLBACK'], type='varchar', alias='fen_allowdrillback') }},
        {{ trx_json_extract('src', ['FEN_USERFUNCTION'], type='varchar', alias='fen_userfunction') }},
        {{ trx_json_extract('src', ['FEN_FOREIGNELEMENTID'], type='varchar', alias='fen_foreignelementid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5foreignentity') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['fen_userfunction', 'fen_systemfunction', 'fen_foreignuserfunction', 'fen_foreignsystemfunction', 'fen_elementid', 'fen_foreignelementid', 'fen_rentity']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['fen_lastsaved']) }}
