CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_RECONCILERUN AS SELECT
                        src:ACCOUNTFROM::integer AS ACCOUNTFROM, 
                        src:ACCOUNTGROUP::varchar AS ACCOUNTGROUP, 
                        src:ACCOUNTSRECONCILED::integer AS ACCOUNTSRECONCILED, 
                        src:ACCOUNTSUBGROUP::varchar AS ACCOUNTSUBGROUP, 
                        src:ACCOUNTTO::integer AS ACCOUNTTO, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:COMMITNUMBER::integer AS COMMITNUMBER, 
                        src:COMPLETEDDTTM::datetime AS COMPLETEDDTTM, 
                        src:DELETED::boolean AS DELETED, 
                        src:INITIATEDBY::varchar AS INITIATEDBY, 
                        src:INITIATEDDTTM::datetime AS INITIATEDDTTM, 
                        src:LASTINVOCATIONSTATUS::integer AS LASTINVOCATIONSTATUS, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PROCESSINGFLAG::varchar AS PROCESSINGFLAG, 
                        src:RECONCILERUNKEY::integer AS RECONCILERUNKEY, 
                        src:SCHEDULEKEY::integer AS SCHEDULEKEY, 
                        src:TOTALNUMBEROFACCOUNTS::integer AS TOTALNUMBEROFACCOUNTS, 
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
    
                        
                src:RECONCILERUNKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:RECONCILERUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_RECONCILERUN as strm))