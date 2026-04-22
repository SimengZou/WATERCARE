{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PROMPTWEBSERVICES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PWS_LASTSAVED'], type='timestamp_ntz', alias='pws_lastsaved') }},
        {{ trx_json_extract('src', ['PWS_FUNCTION'], type='varchar', alias='pws_function') }},
        {{ trx_json_extract('src', ['PWS_TAB'], type='varchar', alias='pws_tab') }},
        {{ trx_json_extract('src', ['PWS_ACTIONCODE'], type='varchar', alias='pws_actioncode') }},
        {{ trx_json_extract('src', ['PWS_WEBSERVICE'], type='varchar', alias='pws_webservice') }},
        {{ trx_json_extract('src', ['PWS_ORGXPATH'], type='varchar', alias='pws_orgxpath') }},
        {{ trx_json_extract('src', ['PWS_UPDATED'], type='timestamp_ntz', alias='pws_updated') }},
        {{ trx_json_extract('src', ['PWS_UPDATECOUNT'], type='float', alias='pws_updatecount') }},
        {{ trx_json_extract('src', ['PWS_WSTITLE'], type='varchar', alias='pws_wstitle') }},
        {{ trx_json_extract('src', ['PWS_CFKEYCODE'], type='varchar', alias='pws_cfkeycode') }},
        {{ trx_json_extract('src', ['PWS_TOPBLOCKTITLE'], type='varchar', alias='pws_topblocktitle') }},
        {{ trx_json_extract('src', ['PWS_BOTTOMBLOCKTITLE'], type='varchar', alias='pws_bottomblocktitle') }},
        {{ trx_json_extract('src', ['PWS_CFBLOCKTITLE'], type='varchar', alias='pws_cfblocktitle') }},
        {{ trx_json_extract('src', ['PWS_PROCESSGROUP'], type='float', alias='pws_processgroup') }},
        {{ trx_json_extract('src', ['PWS_WSPRMCODE'], type='varchar', alias='pws_wsprmcode') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5promptwebservices') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pws_processgroup', 'pws_wsprmcode']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pws_lastsaved']) }}
