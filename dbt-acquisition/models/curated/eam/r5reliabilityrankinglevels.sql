{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5RELIABILITYRANKINGLEVELS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['RRL_LASTSAVED'], type='timestamp_ntz', alias='rrl_lastsaved') }},
        {{ trx_json_extract('src', ['RRL_ALLOWOPERATORCHECKLIST'], type='varchar', alias='rrl_allowoperatorchecklist') }},
        {{ trx_json_extract('src', ['RRL_ORG'], type='varchar', alias='rrl_org') }},
        {{ trx_json_extract('src', ['RRL_REVISION'], type='float', alias='rrl_revision') }},
        {{ trx_json_extract('src', ['RRL_PARENT'], type='varchar', alias='rrl_parent') }},
        {{ trx_json_extract('src', ['RRL_CODE'], type='varchar', alias='rrl_code') }},
        {{ trx_json_extract('src', ['RRL_DESC'], type='varchar', alias='rrl_desc') }},
        {{ trx_json_extract('src', ['RRL_QUESTIONLEVEL'], type='varchar', alias='rrl_questionlevel') }},
        {{ trx_json_extract('src', ['RRL_QUESTION'], type='varchar', alias='rrl_question') }},
        {{ trx_json_extract('src', ['RRL_SEQ'], type='float', alias='rrl_seq') }},
        {{ trx_json_extract('src', ['RRL_FORMULA'], type='varchar', alias='rrl_formula') }},
        {{ trx_json_extract('src', ['RRL_UPDATECOUNT'], type='float', alias='rrl_updatecount') }},
        {{ trx_json_extract('src', ['RRL_NUMERIC'], type='varchar', alias='rrl_numeric') }},
        {{ trx_json_extract('src', ['RRL_INTEGER'], type='varchar', alias='rrl_integer') }},
        {{ trx_json_extract('src', ['RRL_MIN'], type='float', alias='rrl_min') }},
        {{ trx_json_extract('src', ['RRL_MAX'], type='float', alias='rrl_max') }},
        {{ trx_json_extract('src', ['RRL_CALCULATED'], type='varchar', alias='rrl_calculated') }},
        {{ trx_json_extract('src', ['RRL_QUERYCODE'], type='varchar', alias='rrl_querycode') }},
        {{ trx_json_extract('src', ['RRL_ASPECT'], type='varchar', alias='rrl_aspect') }},
        {{ trx_json_extract('src', ['RRL_CHECKLISTTYPE'], type='varchar', alias='rrl_checklisttype') }},
        {{ trx_json_extract('src', ['RRL_PK'], type='varchar', alias='rrl_pk') }},
        {{ trx_json_extract('src', ['RRL_RELIABILITYRANKING'], type='varchar', alias='rrl_reliabilityranking') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5reliabilityrankinglevels') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rrl_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rrl_lastsaved']) }}
