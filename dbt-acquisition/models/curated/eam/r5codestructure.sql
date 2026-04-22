{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5CODESTRUCTURE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CSR_LASTSAVED'], type='timestamp_ntz', alias='csr_lastsaved') }},
        {{ trx_json_extract('src', ['CSR_IMAGE'], type='varchar', alias='csr_image') }},
        {{ trx_json_extract('src', ['CSR_RENTITY'], type='varchar', alias='csr_rentity') }},
        {{ trx_json_extract('src', ['CSR_ENTITY'], type='varchar', alias='csr_entity') }},
        {{ trx_json_extract('src', ['CSR_STRLEVEL1'], type='varchar', alias='csr_strlevel1') }},
        {{ trx_json_extract('src', ['CSR_STRLEVEL2'], type='varchar', alias='csr_strlevel2') }},
        {{ trx_json_extract('src', ['CSR_STRLEVEL3'], type='varchar', alias='csr_strlevel3') }},
        {{ trx_json_extract('src', ['CSR_STRLEVEL4'], type='varchar', alias='csr_strlevel4') }},
        {{ trx_json_extract('src', ['CSR_STRLEVEL5'], type='varchar', alias='csr_strlevel5') }},
        {{ trx_json_extract('src', ['CSR_STRLEVEL6'], type='varchar', alias='csr_strlevel6') }},
        {{ trx_json_extract('src', ['CSR_STRLEVEL7'], type='varchar', alias='csr_strlevel7') }},
        {{ trx_json_extract('src', ['CSR_STRLEVEL8'], type='varchar', alias='csr_strlevel8') }},
        {{ trx_json_extract('src', ['CSR_STRUCTURE'], type='varchar', alias='csr_structure') }},
        {{ trx_json_extract('src', ['CSR_UPDATECOUNT'], type='float', alias='csr_updatecount') }},
        {{ trx_json_extract('src', ['CSR_UPDATED'], type='timestamp_ntz', alias='csr_updated') }},
        {{ trx_json_extract('src', ['CSR_ICON'], type='varchar', alias='csr_icon') }},
        {{ trx_json_extract('src', ['CSR_ICONPATH'], type='varchar', alias='csr_iconpath') }},
        {{ trx_json_extract('src', ['CSR_IMPORTANCE'], type='varchar', alias='csr_importance') }},
        {{ trx_json_extract('src', ['CSR_MATERIALTYPE'], type='varchar', alias='csr_materialtype') }},
        {{ trx_json_extract('src', ['CSR_NOTUSED'], type='varchar', alias='csr_notused') }},
        {{ trx_json_extract('src', ['CSR_CODE'], type='varchar', alias='csr_code') }},
        {{ trx_json_extract('src', ['CSR_DESC'], type='varchar', alias='csr_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5codestructure') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['csr_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['csr_lastsaved']) }}
