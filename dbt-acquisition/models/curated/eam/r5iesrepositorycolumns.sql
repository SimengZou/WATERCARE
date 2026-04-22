{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5IESREPOSITORYCOLUMNS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ENC_LASTSAVED'], type='timestamp_ntz', alias='enc_lastsaved') }},
        {{ trx_json_extract('src', ['ENC_COLUMNNAME'], type='varchar', alias='enc_columnname') }},
        {{ trx_json_extract('src', ['ENC_FIELDNAME'], type='varchar', alias='enc_fieldname') }},
        {{ trx_json_extract('src', ['ENC_ALIAS'], type='varchar', alias='enc_alias') }},
        {{ trx_json_extract('src', ['ENC_DATATYPE'], type='varchar', alias='enc_datatype') }},
        {{ trx_json_extract('src', ['ENC_PKORDER'], type='float', alias='enc_pkorder') }},
        {{ trx_json_extract('src', ['ENC_ISJSPKEYFIELD'], type='varchar', alias='enc_isjspkeyfield') }},
        {{ trx_json_extract('src', ['ENC_FACET'], type='varchar', alias='enc_facet') }},
        {{ trx_json_extract('src', ['ENC_UPDATECOUNT'], type='float', alias='enc_updatecount') }},
        {{ trx_json_extract('src', ['ENC_HIDDENFROMSEARCHRESULTS'], type='varchar', alias='enc_hiddenfromsearchresults') }},
        {{ trx_json_extract('src', ['ENC_REPOSITORYID'], type='varchar', alias='enc_repositoryid') }},
        {{ trx_json_extract('src', ['ENC_DISPLAYORDER'], type='float', alias='enc_displayorder') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5iesrepositorycolumns') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['enc_repositoryid', 'enc_columnname']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['enc_lastsaved']) }}
