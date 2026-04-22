{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5CONTRACTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CON_LASTSAVED'], type='timestamp_ntz', alias='con_lastsaved') }},
        {{ trx_json_extract('src', ['CON_CLASS'], type='varchar', alias='con_class') }},
        {{ trx_json_extract('src', ['CON_SUPPLIER'], type='varchar', alias='con_supplier') }},
        {{ trx_json_extract('src', ['CON_LANG'], type='varchar', alias='con_lang') }},
        {{ trx_json_extract('src', ['CON_CURR'], type='varchar', alias='con_curr') }},
        {{ trx_json_extract('src', ['CON_COPY'], type='varchar', alias='con_copy') }},
        {{ trx_json_extract('src', ['CON_STORE'], type='varchar', alias='con_store') }},
        {{ trx_json_extract('src', ['CON_STATUS'], type='varchar', alias='con_status') }},
        {{ trx_json_extract('src', ['CON_RSTATUS'], type='varchar', alias='con_rstatus') }},
        {{ trx_json_extract('src', ['CON_OWN'], type='varchar', alias='con_own') }},
        {{ trx_json_extract('src', ['CON_REF'], type='varchar', alias='con_ref') }},
        {{ trx_json_extract('src', ['CON_CREATE'], type='timestamp_ntz', alias='con_create') }},
        {{ trx_json_extract('src', ['CON_START'], type='timestamp_ntz', alias='con_start') }},
        {{ trx_json_extract('src', ['CON_END'], type='timestamp_ntz', alias='con_end') }},
        {{ trx_json_extract('src', ['CON_RENEW'], type='timestamp_ntz', alias='con_renew') }},
        {{ trx_json_extract('src', ['CON_PRINTED'], type='varchar', alias='con_printed') }},
        {{ trx_json_extract('src', ['CON_OWNCONTACT'], type='varchar', alias='con_owncontact') }},
        {{ trx_json_extract('src', ['CON_CONTACT'], type='varchar', alias='con_contact') }},
        {{ trx_json_extract('src', ['CON_ORG'], type='varchar', alias='con_org') }},
        {{ trx_json_extract('src', ['CON_CLASS_ORG'], type='varchar', alias='con_class_org') }},
        {{ trx_json_extract('src', ['CON_SUPPLIER_ORG'], type='varchar', alias='con_supplier_org') }},
        {{ trx_json_extract('src', ['CON_UPDATECOUNT'], type='float', alias='con_updatecount') }},
        {{ trx_json_extract('src', ['CON_CODE'], type='varchar', alias='con_code') }},
        {{ trx_json_extract('src', ['CON_DESC'], type='varchar', alias='con_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5contracts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['con_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['con_lastsaved']) }}
