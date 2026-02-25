CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_BILLRUN AS SELECT
                        src:ACCOUNTAREA::varchar AS ACCOUNTAREA, 
                        src:ACCOUNTCLASS::varchar AS ACCOUNTCLASS, 
                        src:ACCOUNTGROUP::varchar AS ACCOUNTGROUP, 
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCOUNTSUBGROUP::varchar AS ACCOUNTSUBGROUP, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BILLDATE::datetime AS BILLDATE, 
                        src:BILLINGCYCLE::varchar AS BILLINGCYCLE, 
                        src:BILLINGPERIODFROMDATE::datetime AS BILLINGPERIODFROMDATE, 
                        src:BILLINGPERIODTODATE::datetime AS BILLINGPERIODTODATE, 
                        src:BILLINGSTATUS::varchar AS BILLINGSTATUS, 
                        src:BILLKEY::integer AS BILLKEY, 
                        src:BILLRUNKEY::integer AS BILLRUNKEY, 
                        src:BILLRUNSCHEDULEKEY::integer AS BILLRUNSCHEDULEKEY, 
                        src:BILLTYPEKEY::integer AS BILLTYPEKEY, 
                        src:CALCULATEDFLAG::varchar AS CALCULATEDFLAG, 
                        src:CALCULATINGFLAG::varchar AS CALCULATINGFLAG, 
                        src:CDRAPDEFNKEY::integer AS CDRAPDEFNKEY, 
                        src:CDRAPPROCESSSTATEKEY::integer AS CDRAPPROCESSSTATEKEY, 
                        src:CDRPRODFAMILY::integer AS CDRPRODFAMILY, 
                        src:COMMITNUMBER::integer AS COMMITNUMBER, 
                        src:DELETED::boolean AS DELETED, 
                        src:DESCRIPTION::varchar AS DESCRIPTION, 
                        src:DUEDATE::datetime AS DUEDATE, 
                        src:EXCLUDEFINALBILLSFLAG::varchar AS EXCLUDEFINALBILLSFLAG, 
                        src:INCLUDESUBBILLTYPESFLAG::varchar AS INCLUDESUBBILLTYPESFLAG, 
                        src:INITIALIZEPHASEFLAG::varchar AS INITIALIZEPHASEFLAG, 
                        src:INITIATEDBY::varchar AS INITIATEDBY, 
                        src:INITIATEDTTM::datetime AS INITIATEDTTM, 
                        src:LASTINVOCATIONSTATUS::integer AS LASTINVOCATIONSTATUS, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NUMBEROFBILLSINRUN::integer AS NUMBEROFBILLSINRUN, 
                        src:NUMBEROFEXCEPTIONS::integer AS NUMBEROFEXCEPTIONS, 
                        src:OUTPUTTEDFLAG::varchar AS OUTPUTTEDFLAG, 
                        src:OUTPUTTINGFLAG::varchar AS OUTPUTTINGFLAG, 
                        src:POSTEDFLAG::varchar AS POSTEDFLAG, 
                        src:POSTINGFLAG::varchar AS POSTINGFLAG, 
                        src:PRESELECTEDFLAG::varchar AS PRESELECTEDFLAG, 
                        src:PRINCIPALTOTAL::numeric(38, 10) AS PRINCIPALTOTAL, 
                        src:PROCESSINGFLAG::varchar AS PROCESSINGFLAG, 
                        src:REFRESHBILLBALANCESFLAG::varchar AS REFRESHBILLBALANCESFLAG, 
                        src:REFRESHBILLMAILTOINFOFLAG::varchar AS REFRESHBILLMAILTOINFOFLAG, 
                        src:REVERSEDFLAG::varchar AS REVERSEDFLAG, 
                        src:REVERSEKEY::integer AS REVERSEKEY, 
                        src:ROUTEKEY::integer AS ROUTEKEY, 
                        src:RUNAVERAGEBILLSPERMINUTE::numeric(38, 10) AS RUNAVERAGEBILLSPERMINUTE, 
                        src:RUNTYPE::varchar AS RUNTYPE, 
                        src:SCHEDULEKEY::integer AS SCHEDULEKEY, 
                        src:TOTALNUMBEROFBILLS::integer AS TOTALNUMBEROFBILLS, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WASRESUMED::varchar AS WASRESUMED, 
                        src:WASSECONDSTEPPOSTED::varchar AS WASSECONDSTEPPOSTED, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:BILLRUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_BILLRUN as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLINGPERIODFROMDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLINGPERIODTODATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLRUNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLRUNSCHEDULEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CDRAPDEFNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CDRAPPROCESSSTATEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CDRPRODFAMILY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMMITNUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DUEDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:INITIATEDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NUMBEROFBILLSINRUN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NUMBEROFEXCEPTIONS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRINCIPALTOTAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVERSEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROUTEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RUNAVERAGEBILLSPERMINUTE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TOTALNUMBEROFBILLS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null