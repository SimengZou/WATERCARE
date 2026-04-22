{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5USERORGANIZATION',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['UOG_LASTSAVED'], type='timestamp_ntz', alias='uog_lastsaved') }},
        {{ trx_json_extract('src', ['UOG_ROLE'], type='varchar', alias='uog_role') }},
        {{ trx_json_extract('src', ['UOG_DEFAULT'], type='varchar', alias='uog_default') }},
        {{ trx_json_extract('src', ['UOG_COMMON'], type='varchar', alias='uog_common') }},
        {{ trx_json_extract('src', ['UOG_GROUP'], type='varchar', alias='uog_group') }},
        {{ trx_json_extract('src', ['UOG_REQAPPVLIMIT'], type='float', alias='uog_reqappvlimit') }},
        {{ trx_json_extract('src', ['UOG_REQAUTHAPPVLIMIT'], type='float', alias='uog_reqauthappvlimit') }},
        {{ trx_json_extract('src', ['UOG_PORDAPPVLIMIT'], type='float', alias='uog_pordappvlimit') }},
        {{ trx_json_extract('src', ['UOG_PORDAUTHAPPVLIMIT'], type='float', alias='uog_pordauthappvlimit') }},
        {{ trx_json_extract('src', ['UOG_PICAPPVLIMIT'], type='float', alias='uog_picappvlimit') }},
        {{ trx_json_extract('src', ['UOG_INVAPPVLIMIT'], type='float', alias='uog_invappvlimit') }},
        {{ trx_json_extract('src', ['UOG_INVAPPVLIMITNONPO'], type='float', alias='uog_invappvlimitnonpo') }},
        {{ trx_json_extract('src', ['UOG_UPDATECOUNT'], type='float', alias='uog_updatecount') }},
        {{ trx_json_extract('src', ['UOG_CREATED'], type='timestamp_ntz', alias='uog_created') }},
        {{ trx_json_extract('src', ['UOG_UPDATED'], type='timestamp_ntz', alias='uog_updated') }},
        {{ trx_json_extract('src', ['UOG_USER'], type='varchar', alias='uog_user') }},
        {{ trx_json_extract('src', ['UOG_ORG'], type='varchar', alias='uog_org') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5userorganization') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['uog_user', 'uog_org', 'uog_role']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['uog_lastsaved']) }}
