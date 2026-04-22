# r5catalogue

eam_R5CATALOGUE

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5catalogue` |
| **Materialization** | `incremental` |
| **Primary keys** | `cat_part`, `cat_part_org`, `cat_supplier`, `cat_supplier_org` |
| **Column count** | 42 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `cat_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CAT_LASTSAVED |
| `cat_repref` | `string` | `varchar` |  |  | CAT_REPREF |
| `cat_iplastupdate` | `timestamp` | `timestamp_ntz` |  |  | CAT_IPLASTUPDATE |
| `cat_iperror` | `float` | `float` |  |  | CAT_IPERROR |
| `cat_iperrormessage` | `string` | `varchar` |  |  | CAT_IPERRORMESSAGE |
| `cat_insertoldreference` | `string` | `varchar` |  |  | CAT_INSERTOLDREFERENCE |
| `cat_costcode` | `string` | `varchar` |  |  | CAT_COSTCODE |
| `cat_documoto_bookid` | `string` | `varchar` |  |  | CAT_DOCUMOTO_BOOKID |
| `cat_documoto_part` | `string` | `varchar` |  |  | CAT_DOCUMOTO_PART |
| `cat_part` | `string` | `varchar` | `PK` |  | CAT_PART |
| `cat_supplier` | `string` | `varchar` | `PK` |  | CAT_SUPPLIER |
| `cat_date` | `timestamp` | `timestamp_ntz` |  |  | CAT_DATE |
| `cat_gross` | `float` | `float` |  |  | CAT_GROSS |
| `cat_leadtime` | `float` | `float` |  |  | CAT_LEADTIME |
| `cat_quotation` | `string` | `varchar` |  |  | CAT_QUOTATION |
| `cat_expquot` | `timestamp` | `timestamp_ntz` |  |  | CAT_EXPQUOT |
| `cat_quotprice` | `float` | `float` |  |  | CAT_QUOTPRICE |
| `cat_quotuom` | `string` | `varchar` |  |  | CAT_QUOTUOM |
| `cat_exppurpr` | `timestamp` | `timestamp_ntz` |  |  | CAT_EXPPURPR |
| `cat_purprice` | `float` | `float` |  |  | CAT_PURPRICE |
| `cat_puruom` | `string` | `varchar` |  |  | CAT_PURUOM |
| `cat_ref` | `string` | `varchar` |  |  | CAT_REF |
| `cat_multiply` | `float` | `float` |  |  | CAT_MULTIPLY |
| `cat_curr` | `string` | `varchar` |  |  | CAT_CURR |
| `cat_exch` | `float` | `float` |  |  | CAT_EXCH |
| `cat_tax` | `string` | `varchar` |  |  | CAT_TAX |
| `cat_sourcesystem` | `string` | `varchar` |  |  | CAT_SOURCESYSTEM |
| `cat_sourcecode` | `string` | `varchar` |  |  | CAT_SOURCECODE |
| `cat_exchtodual` | `float` | `float` |  |  | CAT_EXCHTODUAL |
| `cat_exchfromdual` | `float` | `float` |  |  | CAT_EXCHFROMDUAL |
| `cat_supplier_org` | `string` | `varchar` | `PK` |  | CAT_SUPPLIER_ORG |
| `cat_part_org` | `string` | `varchar` | `PK` |  | CAT_PART_ORG |
| `cat_desc` | `string` | `varchar` |  |  | CAT_DESC |
| `cat_minordqty` | `float` | `float` |  |  | CAT_MINORDQTY |
| `cat_repprice` | `float` | `float` |  |  | CAT_REPPRICE |
| `cat_updatecount` | `float` | `float` |  |  | CAT_UPDATECOUNT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
