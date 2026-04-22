{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ENTITIES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ENT_LASTSAVED'], type='timestamp_ntz', alias='ent_lastsaved') }},
        {{ trx_json_extract('src', ['ENT_STATENT'], type='varchar', alias='ent_statent') }},
        {{ trx_json_extract('src', ['ENT_TYPENT'], type='varchar', alias='ent_typent') }},
        {{ trx_json_extract('src', ['ENT_MULTILANG'], type='varchar', alias='ent_multilang') }},
        {{ trx_json_extract('src', ['ENT_CLASSIF'], type='varchar', alias='ent_classif') }},
        {{ trx_json_extract('src', ['ENT_TABLE'], type='varchar', alias='ent_table') }},
        {{ trx_json_extract('src', ['ENT_ADDATTRIBS'], type='varchar', alias='ent_addattribs') }},
        {{ trx_json_extract('src', ['ENT_FREETEXT'], type='varchar', alias='ent_freetext') }},
        {{ trx_json_extract('src', ['ENT_ADDRESSES'], type='varchar', alias='ent_addresses') }},
        {{ trx_json_extract('src', ['ENT_DOCUMENTS'], type='varchar', alias='ent_documents') }},
        {{ trx_json_extract('src', ['ENT_ASSPARTS'], type='varchar', alias='ent_assparts') }},
        {{ trx_json_extract('src', ['ENT_ASSPERMITS'], type='varchar', alias='ent_asspermits') }},
        {{ trx_json_extract('src', ['ENT_FTAUDIT'], type='varchar', alias='ent_ftaudit') }},
        {{ trx_json_extract('src', ['ENT_CAAUDIT'], type='varchar', alias='ent_caaudit') }},
        {{ trx_json_extract('src', ['ENT_ERECORD'], type='varchar', alias='ent_erecord') }},
        {{ trx_json_extract('src', ['ENT_AUDITS'], type='varchar', alias='ent_audits') }},
        {{ trx_json_extract('src', ['ENT_UPDATECOUNT'], type='float', alias='ent_updatecount') }},
        {{ trx_json_extract('src', ['ENT_UPDATED'], type='timestamp_ntz', alias='ent_updated') }},
        {{ trx_json_extract('src', ['ENT_ENTITY'], type='varchar', alias='ent_entity') }},
        {{ trx_json_extract('src', ['ENT_RENTITY'], type='varchar', alias='ent_rentity') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5entities') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ent_rentity']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ent_lastsaved']) }}
