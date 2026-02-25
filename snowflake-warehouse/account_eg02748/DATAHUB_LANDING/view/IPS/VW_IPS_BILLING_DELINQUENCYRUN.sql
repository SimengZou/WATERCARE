CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_DELINQUENCYRUN AS SELECT
                        src:ACCOUNTAREA::varchar AS ACCOUNTAREA, 
                        src:ACCOUNTBILLINGCYCLE::varchar AS ACCOUNTBILLINGCYCLE, 
                        src:ACCOUNTCLASS::varchar AS ACCOUNTCLASS, 
                        src:ACCOUNTGROUP::varchar AS ACCOUNTGROUP, 
                        src:ACCOUNTSUBGROUP::varchar AS ACCOUNTSUBGROUP, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AUTODECANDRESOLVEFLAG::varchar AS AUTODECANDRESOLVEFLAG, 
                        src:BILLTYPEKEY::integer AS BILLTYPEKEY, 
                        src:COMMITNUMBER::integer AS COMMITNUMBER, 
                        src:DELETED::boolean AS DELETED, 
                        src:DELINQUENCYPROCESSTYPE::varchar AS DELINQUENCYPROCESSTYPE, 
                        src:DELINQUENCYRUNKEY::integer AS DELINQUENCYRUNKEY, 
                        src:ENDADVANCEMENTRUN::datetime AS ENDADVANCEMENTRUN, 
                        src:ENDENTRYRUN::datetime AS ENDENTRYRUN, 
                        src:INITIATEDBY::varchar AS INITIATEDBY, 
                        src:INITIATEDTTM::datetime AS INITIATEDTTM, 
                        src:LASTINVOCATIONSTATUS::integer AS LASTINVOCATIONSTATUS, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NUMBEROFADVANCEDACCOUNTS::integer AS NUMBEROFADVANCEDACCOUNTS, 
                        src:NUMBEROFENTRYACCOUNTS::integer AS NUMBEROFENTRYACCOUNTS, 
                        src:NUMBEROFEXCEPTIONS::integer AS NUMBEROFEXCEPTIONS, 
                        src:PROCESSINGFLAG::varchar AS PROCESSINGFLAG, 
                        src:SCHEDULEKEY::integer AS SCHEDULEKEY, 
                        src:STARTADVANCEMENTRUN::datetime AS STARTADVANCEMENTRUN, 
                        src:STARTENTRYRUN::datetime AS STARTENTRYRUN, 
                        src:TOTALNUMBEROFACCOUNTS::integer AS TOTALNUMBEROFACCOUNTS, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WATERMETERADDRESSROUTEKEY::integer AS WATERMETERADDRESSROUTEKEY, 
                        src:WATERMETERREADINGCYCLE::varchar AS WATERMETERREADINGCYCLE, 
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
    
                        
                src:DELINQUENCYRUNKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:DELINQUENCYRUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_DELINQUENCYRUN as strm))