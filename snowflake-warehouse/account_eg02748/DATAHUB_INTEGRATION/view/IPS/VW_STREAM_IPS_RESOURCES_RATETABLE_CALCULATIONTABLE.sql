CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_RATETABLE_CALCULATIONTABLE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CALCULATIONTABLEKEY::integer AS CALCULATIONTABLEKEY, 
                        src:CYCLEDAYS::integer AS CYCLEDAYS, 
                        src:DELETED::boolean AS DELETED, 
                        src:FORMULAINPUT::numeric(38, 10) AS FORMULAINPUT, 
                        src:FORMULALAST::varchar AS FORMULALAST, 
                        src:FORMULANAME::varchar AS FORMULANAME, 
                        src:FORMULAOUTPUT::numeric(38, 10) AS FORMULAOUTPUT, 
                        src:FORMULAUSED::varchar AS FORMULAUSED, 
                        src:MAXIMUMCHARGE::numeric(38, 10) AS MAXIMUMCHARGE, 
                        src:MAXIMUMUSED::varchar AS MAXIMUMUSED, 
                        src:MINIMUMCHARGE::numeric(38, 10) AS MINIMUMCHARGE, 
                        src:MINIMUMUSED::varchar AS MINIMUMUSED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PRORATED::varchar AS PRORATED, 
                        src:RATETABLESETUPKEY::integer AS RATETABLESETUPKEY, 
                        src:USAGEDAYS::integer AS USAGEDAYS, 
                        src:USEFORPRORATING::integer AS USEFORPRORATING, 
                        src:USEOVERAGE::varchar AS USEOVERAGE, 
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
                                        
                src:CALCULATIONTABLEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_RATETABLE_CALCULATIONTABLE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CALCULATIONTABLEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CYCLEDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FORMULAINPUT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FORMULAOUTPUT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAXIMUMCHARGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MINIMUMCHARGE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATETABLESETUPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USAGEDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USEFORPRORATING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null