CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_NSFIMPORTACTIVITY AS SELECT
                        src:ACTIVITYDATE::datetime AS ACTIVITYDATE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRETURNCODEFLAG::varchar AS ADDRETURNCODEFLAG, 
                        src:DELETED::boolean AS DELETED, 
                        src:EXCEPTIONDESC::varchar AS EXCEPTIONDESC, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NSFFILEPATH::varchar AS NSFFILEPATH, 
                        src:NSFIMPORTACTIVITYKEY::integer AS NSFIMPORTACTIVITYKEY, 
                        src:NSFIMPORTFILENAME::varchar AS NSFIMPORTFILENAME, 
                        src:NSFTEMPLATE::integer AS NSFTEMPLATE, 
                        src:PAYMENTTYPE::integer AS PAYMENTTYPE, 
                        src:RUNNUMBER::integer AS RUNNUMBER, 
                        src:STATUS::integer AS STATUS, 
                        src:USEDEFAULTFLAG::varchar AS USEDEFAULTFLAG, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
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
    
                        
                src:NSFIMPORTACTIVITYKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:NSFIMPORTACTIVITYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_NSFIMPORTACTIVITY as strm))