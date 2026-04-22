{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tsctm450 Estimated Contract Revenues per Period table, Generated 2026-04-10T19:42:34Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['csco'], type='varchar', alias='csco') }},
        {{ trx_json_extract('src', ['cchn'], type='int', alias='cchn') }},
        {{ trx_json_extract('src', ['cfln'], type='int', alias='cfln') }},
        {{ trx_json_extract('src', ['fsyr'], type='int', alias='fsyr') }},
        {{ trx_json_extract('src', ['fspr'], type='int', alias='fspr') }},
        {{ trx_json_extract('src', ['nody'], type='int', alias='nody') }},
        {{ trx_json_extract('src', ['cvdy'], type='int', alias='cvdy') }},
        {{ trx_json_extract('src', ['esrv'], type='float', alias='esrv') }},
        {{ trx_json_extract('src', ['rcrv'], type='float', alias='rcrv') }},
        {{ trx_json_extract('src', ['esco_1'], type='float', alias='esco_1') }},
        {{ trx_json_extract('src', ['esco_2'], type='float', alias='esco_2') }},
        {{ trx_json_extract('src', ['esco_3'], type='float', alias='esco_3') }},
        {{ trx_json_extract('src', ['acpp_1'], type='float', alias='acpp_1') }},
        {{ trx_json_extract('src', ['acpp_2'], type='float', alias='acpp_2') }},
        {{ trx_json_extract('src', ['acpp_3'], type='float', alias='acpp_3') }},
        {{ trx_json_extract('src', ['csco_cchn_ref_compnr'], type='int', alias='csco_cchn_ref_compnr') }},
        {{ trx_json_extract('src', ['csco_ref_compnr'], type='int', alias='csco_ref_compnr') }},
        {{ trx_json_extract('src', ['esrv_homc'], type='float', alias='esrv_homc') }},
        {{ trx_json_extract('src', ['esrv_rpc1'], type='float', alias='esrv_rpc1') }},
        {{ trx_json_extract('src', ['esrv_rpc2'], type='float', alias='esrv_rpc2') }},
        {{ trx_json_extract('src', ['esrv_refc'], type='float', alias='esrv_refc') }},
        {{ trx_json_extract('src', ['esrv_dwhc'], type='float', alias='esrv_dwhc') }},
        {{ trx_json_extract('src', ['rcrv_homc'], type='float', alias='rcrv_homc') }},
        {{ trx_json_extract('src', ['rcrv_rpc1'], type='float', alias='rcrv_rpc1') }},
        {{ trx_json_extract('src', ['rcrv_rpc2'], type='float', alias='rcrv_rpc2') }},
        {{ trx_json_extract('src', ['rcrv_refc'], type='float', alias='rcrv_refc') }},
        {{ trx_json_extract('src', ['rcrv_dwhc'], type='float', alias='rcrv_dwhc') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tsctm450') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'csco', 'cchn', 'cfln', 'fsyr', 'fspr']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
