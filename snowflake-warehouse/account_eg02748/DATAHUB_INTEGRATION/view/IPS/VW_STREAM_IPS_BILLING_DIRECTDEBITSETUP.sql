CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_DIRECTDEBITSETUP AS SELECT
                        src:ACHFORMATFILE::varchar AS ACHFORMATFILE, 
                        src:ACHOUTPUTDIRECTORY::varchar AS ACHOUTPUTDIRECTORY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:COMBINEAMOUNT::varchar AS COMBINEAMOUNT, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DIRECTDEBITAMTCRITERIA::varchar AS DIRECTDEBITAMTCRITERIA, 
                        src:DIRECTDEBITCYCLE1DAY::integer AS DIRECTDEBITCYCLE1DAY, 
                        src:DIRECTDEBITCYCLE2DAY::integer AS DIRECTDEBITCYCLE2DAY, 
                        src:DIRECTDEBITCYCLE3DAY::integer AS DIRECTDEBITCYCLE3DAY, 
                        src:DIRECTDEBITCYCLE4DAY::integer AS DIRECTDEBITCYCLE4DAY, 
                        src:DIRECTDEBITCYCLENUMBER::integer AS DIRECTDEBITCYCLENUMBER, 
                        src:DIRECTDEBITSETUPKEY::integer AS DIRECTDEBITSETUPKEY, 
                        src:ENABLEOTHERFLAG::varchar AS ENABLEOTHERFLAG, 
                        src:FREQUENCYDAYS::varchar AS FREQUENCYDAYS, 
                        src:FREQUENCYMONTHS::varchar AS FREQUENCYMONTHS, 
                        src:FREQUENCYWEEKS::varchar AS FREQUENCYWEEKS, 
                        src:INCLUDEUNBILLED::varchar AS INCLUDEUNBILLED, 
                        src:MAXIMUMAMOUNT::numeric(38, 10) AS MAXIMUMAMOUNT, 
                        src:MAXIMUMSUSPENSIONDAY::integer AS MAXIMUMSUSPENSIONDAY, 
                        src:MAXIMUMSUSPENSIONNUMBER::integer AS MAXIMUMSUSPENSIONNUMBER, 
                        src:MINIMUMAMOUNT::numeric(38, 10) AS MINIMUMAMOUNT, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PRENOTE::varchar AS PRENOTE, 
                        src:PRENOTEEXTRACTDATEDAYS::integer AS PRENOTEEXTRACTDATEDAYS, 
                        src:PRENOTENUMBEROFDAYS::integer AS PRENOTENUMBEROFDAYS, 
                        src:REFBALTHRUEXTRDATE::varchar AS REFBALTHRUEXTRDATE, 
                        src:REFRESHEXTRACTAMOUNT::varchar AS REFRESHEXTRACTAMOUNT, 
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
                                        
                src:DIRECTDEBITSETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_DIRECTDEBITSETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIRECTDEBITCYCLE1DAY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIRECTDEBITCYCLE2DAY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIRECTDEBITCYCLE3DAY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIRECTDEBITCYCLE4DAY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIRECTDEBITCYCLENUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIRECTDEBITSETUPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAXIMUMAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAXIMUMSUSPENSIONDAY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAXIMUMSUSPENSIONNUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MINIMUMAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRENOTEEXTRACTDATEDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRENOTENUMBEROFDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null