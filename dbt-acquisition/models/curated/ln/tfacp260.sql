{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tfacp260 Approval Error Log table, Generated 2026-04-10T19:41:30Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['ityp'], type='varchar', alias='ityp') }},
        {{ trx_json_extract('src', ['idoc'], type='int', alias='idoc') }},
        {{ trx_json_extract('src', ['loco'], type='int', alias='loco') }},
        {{ trx_json_extract('src', ['otyp'], type='int', alias='otyp') }},
        {{ trx_json_extract('src', ['otyp_kw'], type='varchar', alias='otyp_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['pono'], type='int', alias='pono') }},
        {{ trx_json_extract('src', ['sqnb'], type='int', alias='sqnb') }},
        {{ trx_json_extract('src', ['load'], type='varchar', alias='load') }},
        {{ trx_json_extract('src', ['shpi'], type='varchar', alias='shpi') }},
        {{ trx_json_extract('src', ['clus'], type='varchar', alias='clus') }},
        {{ trx_json_extract('src', ['logd'], type='date', alias='logd') }},
        {{ trx_json_extract('src', ['logt'], type='int', alias='logt') }},
        {{ trx_json_extract('src', ['user'], type='varchar', alias='user') }},
        {{ trx_json_extract('src', ['byer'], type='varchar', alias='byer') }},
        {{ trx_json_extract('src', ['mess', 'en_US'], type='varchar', alias='mess__en_us') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tfacp260') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'ityp', 'idoc', 'loco', 'otyp', 'orno', 'pono', 'sqnb']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
