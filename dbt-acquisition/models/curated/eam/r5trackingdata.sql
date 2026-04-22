{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5TRACKINGDATA',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['TKD_LASTSAVED'], type='timestamp_ntz', alias='tkd_lastsaved') }},
        {{ trx_json_extract('src', ['TKD_SOURCESYSTEM'], type='varchar', alias='tkd_sourcesystem') }},
        {{ trx_json_extract('src', ['TKD_SOURCECODE'], type='varchar', alias='tkd_sourcecode') }},
        {{ trx_json_extract('src', ['TKD_TRANS'], type='varchar', alias='tkd_trans') }},
        {{ trx_json_extract('src', ['TKD_TRACKDATE'], type='timestamp_ntz', alias='tkd_trackdate') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA1'], type='varchar', alias='tkd_promptdata1') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA2'], type='varchar', alias='tkd_promptdata2') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA3'], type='varchar', alias='tkd_promptdata3') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA4'], type='varchar', alias='tkd_promptdata4') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA5'], type='varchar', alias='tkd_promptdata5') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA6'], type='varchar', alias='tkd_promptdata6') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA7'], type='varchar', alias='tkd_promptdata7') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA8'], type='varchar', alias='tkd_promptdata8') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA9'], type='varchar', alias='tkd_promptdata9') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA10'], type='varchar', alias='tkd_promptdata10') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA11'], type='varchar', alias='tkd_promptdata11') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA12'], type='varchar', alias='tkd_promptdata12') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA13'], type='varchar', alias='tkd_promptdata13') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA14'], type='varchar', alias='tkd_promptdata14') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA15'], type='varchar', alias='tkd_promptdata15') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA16'], type='varchar', alias='tkd_promptdata16') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA17'], type='varchar', alias='tkd_promptdata17') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA18'], type='varchar', alias='tkd_promptdata18') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA19'], type='varchar', alias='tkd_promptdata19') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA20'], type='varchar', alias='tkd_promptdata20') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA21'], type='varchar', alias='tkd_promptdata21') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA22'], type='varchar', alias='tkd_promptdata22') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA23'], type='varchar', alias='tkd_promptdata23') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA24'], type='varchar', alias='tkd_promptdata24') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA25'], type='varchar', alias='tkd_promptdata25') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA26'], type='varchar', alias='tkd_promptdata26') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA27'], type='varchar', alias='tkd_promptdata27') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA28'], type='varchar', alias='tkd_promptdata28') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA29'], type='varchar', alias='tkd_promptdata29') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA30'], type='varchar', alias='tkd_promptdata30') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA31'], type='varchar', alias='tkd_promptdata31') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA32'], type='varchar', alias='tkd_promptdata32') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA33'], type='varchar', alias='tkd_promptdata33') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA34'], type='varchar', alias='tkd_promptdata34') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA35'], type='varchar', alias='tkd_promptdata35') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA36'], type='varchar', alias='tkd_promptdata36') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA37'], type='varchar', alias='tkd_promptdata37') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA38'], type='varchar', alias='tkd_promptdata38') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA39'], type='varchar', alias='tkd_promptdata39') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA40'], type='varchar', alias='tkd_promptdata40') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA41'], type='varchar', alias='tkd_promptdata41') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA42'], type='varchar', alias='tkd_promptdata42') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA43'], type='varchar', alias='tkd_promptdata43') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA44'], type='varchar', alias='tkd_promptdata44') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA45'], type='varchar', alias='tkd_promptdata45') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA46'], type='varchar', alias='tkd_promptdata46') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA47'], type='varchar', alias='tkd_promptdata47') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA48'], type='varchar', alias='tkd_promptdata48') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA49'], type='varchar', alias='tkd_promptdata49') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA50'], type='varchar', alias='tkd_promptdata50') }},
        {{ trx_json_extract('src', ['TKD_SESSIONID'], type='float', alias='tkd_sessionid') }},
        {{ trx_json_extract('src', ['TKD_RSTATUS'], type='varchar', alias='tkd_rstatus') }},
        {{ trx_json_extract('src', ['TKD_CHANGED'], type='varchar', alias='tkd_changed') }},
        {{ trx_json_extract('src', ['TKD_PROMPTDATA51'], type='varchar', alias='tkd_promptdata51') }},
        {{ trx_json_extract('src', ['TKD_TRANSID'], type='float', alias='tkd_transid') }},
        {{ trx_json_extract('src', ['TKD_CREATED'], type='timestamp_ntz', alias='tkd_created') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5trackingdata') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['tkd_transid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['tkd_lastsaved']) }}
