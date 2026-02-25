CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_METERMANAGEMENT_WATER_READINGEXPORTSCHEDULE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ALLOWARCHIVEFLAG::varchar AS ALLOWARCHIVEFLAG, 
                        src:ALLOWOUTSTANDINGROUTESFLAG::varchar AS ALLOWOUTSTANDINGROUTESFLAG, 
                        src:CONCATENATEFILEFLAG::varchar AS CONCATENATEFILEFLAG, 
                        src:DAYSBEFORENEXTREADINGDAY::integer AS DAYSBEFORENEXTREADINGDAY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DELETEPREVFILEVERFLAG::varchar AS DELETEPREVFILEVERFLAG, 
                        src:DISABLEAUTOSCHEDULING::varchar AS DISABLEAUTOSCHEDULING, 
                        src:EXCLBILLEDDAYS::integer AS EXCLBILLEDDAYS, 
                        src:EXCLBILLEDFLAG::varchar AS EXCLBILLEDFLAG, 
                        src:EXCLEXCEPMETERS::varchar AS EXCLEXCEPMETERS, 
                        src:EXCLLASTREADDAYSAGO::integer AS EXCLLASTREADDAYSAGO, 
                        src:EXCLLASTREADFLAG::varchar AS EXCLLASTREADFLAG, 
                        src:EXCLNEWMETERS::varchar AS EXCLNEWMETERS, 
                        src:EXCLNOACCTMETERS::varchar AS EXCLNOACCTMETERS, 
                        src:EXCLSRAFTERDAYS::integer AS EXCLSRAFTERDAYS, 
                        src:EXCLSRBEFOREDAYS::integer AS EXCLSRBEFOREDAYS, 
                        src:EXCLSRFLAG::varchar AS EXCLSRFLAG, 
                        src:EXCLZEROREADINGMETERS::varchar AS EXCLZEROREADINGMETERS, 
                        src:EXCLZEROUSAGEMETERS::varchar AS EXCLZEROUSAGEMETERS, 
                        src:EXPORTDATE::datetime AS EXPORTDATE, 
                        src:EXPORTDIRPATH::varchar AS EXPORTDIRPATH, 
                        src:EXPORTRUNNUMBER::integer AS EXPORTRUNNUMBER, 
                        src:EXPORTTEMPLATEFILE::varchar AS EXPORTTEMPLATEFILE, 
                        src:EXPORTTYPE::integer AS EXPORTTYPE, 
                        src:ISRERUN::varchar AS ISRERUN, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEXTREADINGDATE::datetime AS NEXTREADINGDATE, 
                        src:READINGEXPORTSCHEDULEKEY::integer AS READINGEXPORTSCHEDULEKEY, 
                        src:RECEIVERCODE::varchar AS RECEIVERCODE, 
                        src:RERUNNUMBER::integer AS RERUNNUMBER, 
                        src:RUNNUMBER::integer AS RUNNUMBER, 
                        src:SCHEDULEKEY::integer AS SCHEDULEKEY, 
                        src:SENDERCODE::varchar AS SENDERCODE, 
                        src:SORTBYCOLUMNS::varchar AS SORTBYCOLUMNS, 
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
                                        
                src:READINGEXPORTSCHEDULEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_READINGEXPORTSCHEDULE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DAYSBEFORENEXTREADINGDAY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EXCLBILLEDDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EXCLLASTREADDAYSAGO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EXCLSRAFTERDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EXCLSRBEFOREDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPORTDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EXPORTRUNNUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EXPORTTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:NEXTREADINGDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:READINGEXPORTSCHEDULEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RERUNNUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RUNNUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null