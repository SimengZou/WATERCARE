{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ELECSIGN',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ELS_LASTSAVED'], type='timestamp_ntz', alias='els_lastsaved') }},
        {{ trx_json_extract('src', ['ELS_ORG'], type='varchar', alias='els_org') }},
        {{ trx_json_extract('src', ['ELS_USER'], type='varchar', alias='els_user') }},
        {{ trx_json_extract('src', ['ELS_DATE'], type='timestamp_ntz', alias='els_date') }},
        {{ trx_json_extract('src', ['ELS_ENTITY'], type='varchar', alias='els_entity') }},
        {{ trx_json_extract('src', ['ELS_SIGNTYPE'], type='varchar', alias='els_signtype') }},
        {{ trx_json_extract('src', ['ELS_STATUS'], type='varchar', alias='els_status') }},
        {{ trx_json_extract('src', ['ELS_PARENT'], type='varchar', alias='els_parent') }},
        {{ trx_json_extract('src', ['ELS_CERTIFYNUM'], type='varchar', alias='els_certifynum') }},
        {{ trx_json_extract('src', ['ELS_CERTIFYTYPE'], type='varchar', alias='els_certifytype') }},
        {{ trx_json_extract('src', ['ELS_EXTERNALDATE'], type='timestamp_ntz', alias='els_externaldate') }},
        {{ trx_json_extract('src', ['ELS_USERDESC'], type='varchar', alias='els_userdesc') }},
        {{ trx_json_extract('src', ['ELS_SIGNTYPEDESC'], type='varchar', alias='els_signtypedesc') }},
        {{ trx_json_extract('src', ['ELS_LASTCHANGED'], type='varchar', alias='els_lastchanged') }},
        {{ trx_json_extract('src', ['ELS_ENTCODE2'], type='varchar', alias='els_entcode2') }},
        {{ trx_json_extract('src', ['ELS_CODE'], type='varchar', alias='els_code') }},
        {{ trx_json_extract('src', ['ELS_ENTCODE'], type='varchar', alias='els_entcode') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5elecsign') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['els_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['els_lastsaved']) }}
