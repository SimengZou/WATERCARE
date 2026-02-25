CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_DELINQUENCYRUN AS SELECT
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
                        src:WATERMETERREADINGCYCLE::varchar AS WATERMETERREADINGCYCLE, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:DELINQUENCYRUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_DELINQUENCYRUN as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMMITNUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DELINQUENCYRUNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ENDADVANCEMENTRUN), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ENDENTRYRUN), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:INITIATEDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NUMBEROFADVANCEDACCOUNTS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NUMBEROFENTRYACCOUNTS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NUMBEROFEXCEPTIONS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STARTADVANCEMENTRUN), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STARTENTRYRUN), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TOTALNUMBEROFACCOUNTS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WATERMETERADDRESSROUTEKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null