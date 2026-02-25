CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_PAYMENTIMPORTRUN AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ARCHIVEDFILENAME::varchar AS ARCHIVEDFILENAME, 
                        src:BATCHEXCEPTIONCOUNT::integer AS BATCHEXCEPTIONCOUNT, 
                        src:DELETED::boolean AS DELETED, 
                        src:EXCEPTIONTYPE::integer AS EXCEPTIONTYPE, 
                        src:FILENAME::varchar AS FILENAME, 
                        src:INITIATEDBY::varchar AS INITIATEDBY, 
                        src:INITIATEDTTM::datetime AS INITIATEDTTM, 
                        src:LASTINVOCATIONSTATUS::integer AS LASTINVOCATIONSTATUS, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAYMENTIMPORTRUNKEY::integer AS PAYMENTIMPORTRUNKEY, 
                        src:PROCESSINGFLAG::varchar AS PROCESSINGFLAG, 
                        src:SCHEDULEKEY::integer AS SCHEDULEKEY, 
                        src:STATUS::integer AS STATUS, 
                        src:TEMPLATEID::varchar AS TEMPLATEID, 
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
    
                        
                src:PAYMENTIMPORTRUNKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PAYMENTIMPORTRUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_PAYMENTIMPORTRUN as strm))