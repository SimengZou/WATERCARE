CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLBILLING_ACCOUNTSERVICEDELETE AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCOUNTSERVICEDELETEKEY::integer AS ACCOUNTSERVICEDELETEKEY, 
                        src:ACCOUNTSERVICEKEY::integer AS ACCOUNTSERVICEKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESSSERVICEKEY::integer AS ADDRESSSERVICEKEY, 
                        src:AREA::varchar AS AREA, 
                        src:CONSUMPTIONPERCENTAGE::numeric(38, 10) AS CONSUMPTIONPERCENTAGE, 
                        src:DELETED::boolean AS DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ORIGADDBY::varchar AS ORIGADDBY, 
                        src:ORIGADDDTTM::datetime AS ORIGADDDTTM, 
                        src:ORIGMODBY::varchar AS ORIGMODBY, 
                        src:ORIGMODDTTM::datetime AS ORIGMODDTTM, 
                        src:SERVICECLASS1::varchar AS SERVICECLASS1, 
                        src:SERVICEFIELD1VALUE::numeric(38, 10) AS SERVICEFIELD1VALUE, 
                        src:SERVICEFIELD2VALUE::numeric(38, 10) AS SERVICEFIELD2VALUE, 
                        src:SERVICEFIELD3VALUE::numeric(38, 10) AS SERVICEFIELD3VALUE, 
                        src:SERVICEOPTIONSKEY::integer AS SERVICEOPTIONSKEY, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
                        src:STATUS::varchar AS STATUS, 
                        src:STATUSDTTM::datetime AS STATUSDTTM, 
                        src:STOPDTTM::datetime AS STOPDTTM, 
                        src:VARIATION_ID::integer AS VARIATION_ID, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
            etl_load_datetime,
            etl_load_metadatafilename
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename
                from
                (
                    SELECT
                        
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ACCOUNTSERVICEDELETEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLBILLING_ACCOUNTSERVICEDELETE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTSERVICEDELETEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTSERVICEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRESSSERVICEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONSUMPTIONPERCENTAGE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORIGADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ORIGMODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVICEFIELD1VALUE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVICEFIELD2VALUE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVICEFIELD3VALUE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVICEOPTIONSKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STATUSDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STOPDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null