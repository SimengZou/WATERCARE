CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_DIRECTDEBITRUN AS SELECT
                        src:ACCTGRP::varchar AS ACCTGRP, 
                        src:ACHENTRYAMOUNT::numeric(38, 10) AS ACHENTRYAMOUNT, 
                        src:ACHFILENAME::varchar AS ACHFILENAME, 
                        src:ACHFORMATFILE::varchar AS ACHFORMATFILE, 
                        src:ACHOUTPUTDIRECTORY::varchar AS ACHOUTPUTDIRECTORY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BILLINGCYCLE::varchar AS BILLINGCYCLE, 
                        src:BILLRUNKEY::integer AS BILLRUNKEY, 
                        src:BILLTYPEKEY::integer AS BILLTYPEKEY, 
                        src:COMMENTSKEY::integer AS COMMENTSKEY, 
                        src:CREATEACHFILE::varchar AS CREATEACHFILE, 
                        src:DELETED::boolean AS DELETED, 
                        src:DIRECTDEBITRUNKEY::integer AS DIRECTDEBITRUNKEY, 
                        src:EXTRACTTHROUGHDATE::datetime AS EXTRACTTHROUGHDATE, 
                        src:INCLUDEDEBITSCHEDULE::varchar AS INCLUDEDEBITSCHEDULE, 
                        src:LASTINVOCATIONSTATUS::integer AS LASTINVOCATIONSTATUS, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NUMBEROFACCOUNTS::integer AS NUMBEROFACCOUNTS, 
                        src:NUMBEROFACHENTRIES::integer AS NUMBEROFACHENTRIES, 
                        src:NUMBEROFBILLS::integer AS NUMBEROFBILLS, 
                        src:NUMBEROFDEBITSCHEDULE::integer AS NUMBEROFDEBITSCHEDULE, 
                        src:PAYMENTBATCHCOUNT::integer AS PAYMENTBATCHCOUNT, 
                        src:PRENOTEBANKACCOUNTCOUNT::integer AS PRENOTEBANKACCOUNTCOUNT, 
                        src:PROCESSINGFLAG::varchar AS PROCESSINGFLAG, 
                        src:RERUN::varchar AS RERUN, 
                        src:SCHEDULEKEY::integer AS SCHEDULEKEY, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
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
    
                        
                src:DIRECTDEBITRUNKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:DIRECTDEBITRUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_DIRECTDEBITRUN as strm))