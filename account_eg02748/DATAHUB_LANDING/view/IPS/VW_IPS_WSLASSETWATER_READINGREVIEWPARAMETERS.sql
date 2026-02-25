CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLASSETWATER_READINGREVIEWPARAMETERS AS SELECT
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
                        src:ZEROCODE::varchar AS ZEROCODE, 
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:CODE,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CODE  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLASSETWATER_READINGREVIEWPARAMETERS as strm))