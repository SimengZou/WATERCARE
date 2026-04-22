{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ADDRESS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ADR_LASTSAVED'], type='timestamp_ntz', alias='adr_lastsaved') }},
        {{ trx_json_extract('src', ['ADR_RTYPE'], type='varchar', alias='adr_rtype') }},
        {{ trx_json_extract('src', ['ADR_TEXT'], type='varchar', alias='adr_text') }},
        {{ trx_json_extract('src', ['ADR_ADDRESS1'], type='varchar', alias='adr_address1') }},
        {{ trx_json_extract('src', ['ADR_ADDRESS2'], type='varchar', alias='adr_address2') }},
        {{ trx_json_extract('src', ['ADR_ADDRESS3'], type='varchar', alias='adr_address3') }},
        {{ trx_json_extract('src', ['ADR_CITY'], type='varchar', alias='adr_city') }},
        {{ trx_json_extract('src', ['ADR_STATE'], type='varchar', alias='adr_state') }},
        {{ trx_json_extract('src', ['ADR_ZIP'], type='varchar', alias='adr_zip') }},
        {{ trx_json_extract('src', ['ADR_COUNTRY'], type='varchar', alias='adr_country') }},
        {{ trx_json_extract('src', ['ADR_PHONE'], type='varchar', alias='adr_phone') }},
        {{ trx_json_extract('src', ['ADR_PHONEEXTN'], type='varchar', alias='adr_phoneextn') }},
        {{ trx_json_extract('src', ['ADR_FAX'], type='varchar', alias='adr_fax') }},
        {{ trx_json_extract('src', ['ADR_EMAIL'], type='varchar', alias='adr_email') }},
        {{ trx_json_extract('src', ['ADR_UPDATECOUNT'], type='float', alias='adr_updatecount') }},
        {{ trx_json_extract('src', ['ADR_RENTITY'], type='varchar', alias='adr_rentity') }},
        {{ trx_json_extract('src', ['ADR_CODE'], type='varchar', alias='adr_code') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5address') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['adr_code', 'adr_rentity', 'adr_rtype']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['adr_lastsaved']) }}
