{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='Generated from anySQL Connector dEPM_IPS_ASSET_VALUATION',
    tags=['auto-generated', 'depm', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['AssetRecordType'], type='varchar', alias='assetrecordtype') }},
        {{ trx_json_extract('src', ['AssetKey'], type='varchar', alias='assetkey') }},
        {{ trx_json_extract('src', ['AssetBookID'], type='varchar', alias='assetbookid') }},
        {{ trx_json_extract('src', ['AssetDescription'], type='varchar', alias='assetdescription') }},
        {{ trx_json_extract('src', ['AssetClass'], type='varchar', alias='assetclass') }},
        {{ trx_json_extract('src', ['AssetCostCentre'], type='varchar', alias='assetcostcentre') }},
        {{ trx_json_extract('src', ['AssetAquisitionType'], type='varchar', alias='assetaquisitiontype') }},
        {{ trx_json_extract('src', ['AssetAquisitionDate'], type='timestamp_ntz', alias='assetaquisitiondate') }},
        {{ trx_json_extract('src', ['AssetDepreciationMethod'], type='int', alias='assetdepreciationmethod') }},
        {{ trx_json_extract('src', ['AssetExpectedLife'], type='float', alias='assetexpectedlife') }},
        {{ trx_json_extract('src', ['AssetInitialCost'], type='float', alias='assetinitialcost') }},
        {{ trx_json_extract('src', ['Comments'], type='varchar', alias='comments') }},
        {{ trx_json_extract('src', ['IONStatus'], type='int', alias='ionstatus') }},
        {{ trx_json_extract('src', ['Timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['Project'], type='varchar', alias='project') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_depm', 'depm_ips_asset_valuation') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['assetkey', 'assetbookid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['assetrecordtype']) }}
