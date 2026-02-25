CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLASSETWATER_READINGREVIEWPARAMETERS AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ATTACHZEROCODE::varchar AS ATTACHZEROCODE, 
                        src:CATEGORY::varchar AS CATEGORY, 
                        src:CODE::varchar AS CODE, 
                        src:DAILYAVERAGEPERC::integer AS DAILYAVERAGEPERC, 
                        src:DAILYAVERAGEVALUEMAX::numeric(38, 10) AS DAILYAVERAGEVALUEMAX, 
                        src:DAILYAVERAGEVALUEMIN::numeric(38, 10) AS DAILYAVERAGEVALUEMIN, 
                        src:DELETED::boolean AS DELETED, 
                        src:DESCRIPT::varchar AS DESCRIPT, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:ISAUDIT::varchar AS ISAUDIT, 
                        src:ISCHECKDAILYAVGPER::varchar AS ISCHECKDAILYAVGPER, 
                        src:ISCREATEESTIMATE::varchar AS ISCREATEESTIMATE, 
                        src:ISCREATEMAINSR::varchar AS ISCREATEMAINSR, 
                        src:ISCREATESPATCODE::varchar AS ISCREATESPATCODE, 
                        src:ISCREATESTAFFSR::varchar AS ISCREATESTAFFSR, 
                        src:ISHIGHLOWPROCESS::varchar AS ISHIGHLOWPROCESS, 
                        src:ISNEGATIVEZEROPROCESS::varchar AS ISNEGATIVEZEROPROCESS, 
                        src:ISREADINGAPPROVED::varchar AS ISREADINGAPPROVED, 
                        src:ISSRAUTOAPPROVED::varchar AS ISSRAUTOAPPROVED, 
                        src:ISSUEDESCRIPTION::varchar AS ISSUEDESCRIPTION, 
                        src:ISSUETYPE::varchar AS ISSUETYPE, 
                        src:ISZEROCODE::varchar AS ISZEROCODE, 
                        src:MAINTENANCESRCODE::varchar AS MAINTENANCESRCODE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OCCURRENCE::integer AS OCCURRENCE, 
                        src:PRINTHOUSEACTIONCODE::varchar AS PRINTHOUSEACTIONCODE, 
                        src:READERCODE::varchar AS READERCODE, 
                        src:READERCODEDESC::varchar AS READERCODEDESC, 
                        src:STAFFSRCODE::varchar AS STAFFSRCODE, 
                        src:USAGEVALUEMAX::numeric(38, 10) AS USAGEVALUEMAX, 
                        src:USAGEVALUEMIN::numeric(38, 10) AS USAGEVALUEMIN, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:ZEROCODE::varchar AS ZEROCODE, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:CODE  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLASSETWATER_READINGREVIEWPARAMETERS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DAILYAVERAGEPERC), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DAILYAVERAGEVALUEMAX), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DAILYAVERAGEVALUEMIN), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OCCURRENCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USAGEVALUEMAX), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USAGEVALUEMIN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null