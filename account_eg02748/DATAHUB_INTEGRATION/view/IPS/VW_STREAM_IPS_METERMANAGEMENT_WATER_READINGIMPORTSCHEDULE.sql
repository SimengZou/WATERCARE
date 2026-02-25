CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_METERMANAGEMENT_WATER_READINGIMPORTSCHEDULE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CYCLEBASEDFLAG::varchar AS CYCLEBASEDFLAG, 
                        src:DELETED::boolean AS DELETED, 
                        src:IMPEXPDAYS::integer AS IMPEXPDAYS, 
                        src:IMPORTDIRPATH::varchar AS IMPORTDIRPATH, 
                        src:IMPORTFILETEMPLATE::varchar AS IMPORTFILETEMPLATE, 
                        src:IMPORTRUNNUMBER::integer AS IMPORTRUNNUMBER, 
                        src:IMPORTTYPE::integer AS IMPORTTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PROCESSREADYTOBILLFLAG::varchar AS PROCESSREADYTOBILLFLAG, 
                        src:READINGEXPORTSCHEDULEKEY::integer AS READINGEXPORTSCHEDULEKEY, 
                        src:READINGIMPORTSCHEDULEKEY::integer AS READINGIMPORTSCHEDULEKEY, 
                        src:RECEIVERCODE::varchar AS RECEIVERCODE, 
                        src:REJECTNONEXROUTE::varchar AS REJECTNONEXROUTE, 
                        src:REJECTPRIORMETERREAD::varchar AS REJECTPRIORMETERREAD, 
                        src:RERUNFLAG::varchar AS RERUNFLAG, 
                        src:RERUNNUMBER::integer AS RERUNNUMBER, 
                        src:RESOLVESRFLAG::varchar AS RESOLVESRFLAG, 
                        src:RUNNUMBER::integer AS RUNNUMBER, 
                        src:SCHEDULEKEY::integer AS SCHEDULEKEY, 
                        src:SELFIMPORTFLAG::varchar AS SELFIMPORTFLAG, 
                        src:SENDERCODE::varchar AS SENDERCODE, 
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
                                        
                src:READINGIMPORTSCHEDULEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_READINGIMPORTSCHEDULE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IMPEXPDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IMPORTRUNNUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IMPORTTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:READINGEXPORTSCHEDULEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:READINGIMPORTSCHEDULEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RERUNNUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RUNNUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null