CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_JOURNALRUN AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AVGTRANPERMINUTE::numeric(38, 10) AS AVGTRANPERMINUTE, 
                        src:COMMITNUMBER::integer AS COMMITNUMBER, 
                        src:DELETED::boolean AS DELETED, 
                        src:ENDOFRUNDTTM::datetime AS ENDOFRUNDTTM, 
                        src:INITIATEDBY::varchar AS INITIATEDBY, 
                        src:INITIATEDDTTM::datetime AS INITIATEDDTTM, 
                        src:JOURNALRUNKEY::integer AS JOURNALRUNKEY, 
                        src:LASTINVOCATIONSTATUS::integer AS LASTINVOCATIONSTATUS, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NUMBEROFATPROCESSED::integer AS NUMBEROFATPROCESSED, 
                        src:NUMBEROFDTPROCESSED::integer AS NUMBEROFDTPROCESSED, 
                        src:PROCESSINGFLAG::varchar AS PROCESSINGFLAG, 
                        src:PROCWITHEXCEPTFLAG::varchar AS PROCWITHEXCEPTFLAG, 
                        src:SCHEDULEKEY::integer AS SCHEDULEKEY, 
                        src:STARTOFRUNDTTM::datetime AS STARTOFRUNDTTM, 
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
    
                        
                src:JOURNALRUNKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:JOURNALRUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_JOURNALRUN as strm))