CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_RECONCILERUN AS SELECT
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
                                        
                src:RECONCILERUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_RECONCILERUN as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTFROM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTSRECONCILED), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTTO), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMMITNUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:COMPLETEDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:INITIATEDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RECONCILERUNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TOTALNUMBEROFACCOUNTS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null