{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5IESREPOSITORIES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ENS_LASTSAVED'], type='timestamp_ntz', alias='ens_lastsaved') }},
        {{ trx_json_extract('src', ['ENS_DATEUPDATEDCOL'], type='varchar', alias='ens_dateupdatedcol') }},
        {{ trx_json_extract('src', ['ENS_INTERESTCENTER'], type='varchar', alias='ens_interestcenter') }},
        {{ trx_json_extract('src', ['ENS_NOTUSED'], type='varchar', alias='ens_notused') }},
        {{ trx_json_extract('src', ['ENS_ISINCREMENTAL'], type='varchar', alias='ens_isincremental') }},
        {{ trx_json_extract('src', ['ENS_ISPOPUP'], type='varchar', alias='ens_ispopup') }},
        {{ trx_json_extract('src', ['ENS_DATELASTCRAWL'], type='timestamp_ntz', alias='ens_datelastcrawl') }},
        {{ trx_json_extract('src', ['ENS_UPDATECOUNT'], type='float', alias='ens_updatecount') }},
        {{ trx_json_extract('src', ['ENS_CODE'], type='varchar', alias='ens_code') }},
        {{ trx_json_extract('src', ['ENS_DESC'], type='varchar', alias='ens_desc') }},
        {{ trx_json_extract('src', ['ENS_TABLENAME'], type='varchar', alias='ens_tablename') }},
        {{ trx_json_extract('src', ['ENS_USERFUNCTION'], type='varchar', alias='ens_userfunction') }},
        {{ trx_json_extract('src', ['ENS_TAB'], type='varchar', alias='ens_tab') }},
        {{ trx_json_extract('src', ['ENS_THUMBNAIL'], type='varchar', alias='ens_thumbnail') }},
        {{ trx_json_extract('src', ['ENS_DATECREATEDCOL'], type='varchar', alias='ens_datecreatedcol') }},
        {{ trx_json_extract('src', ['ENS_ORGCOL'], type='varchar', alias='ens_orgcol') }},
        {{ trx_json_extract('src', ['ENS_TABLEPREFIX'], type='varchar', alias='ens_tableprefix') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5iesrepositories') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ens_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ens_lastsaved']) }}
