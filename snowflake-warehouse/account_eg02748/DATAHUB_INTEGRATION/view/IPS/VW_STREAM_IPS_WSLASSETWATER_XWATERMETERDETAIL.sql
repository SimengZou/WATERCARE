CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLASSETWATER_XWATERMETERDETAIL AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:DAILYAVERAGE::numeric(38, 10) AS DAILYAVERAGE, 
                        src:DELETED::boolean AS DELETED, 
                        src:ISSMARTLOGGER::varchar AS ISSMARTLOGGER, 
                        src:LASTREADERCODE::varchar AS LASTREADERCODE, 
                        src:LASTREADERCODEDATE::datetime AS LASTREADERCODEDATE, 
                        src:LASTVALIDATEDDATETIME::datetime AS LASTVALIDATEDDATETIME, 
                        src:LASTYEARDAILYAVERAGE::numeric(38, 10) AS LASTYEARDAILYAVERAGE, 
                        src:METERZEROCODE::varchar AS METERZEROCODE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PHONENUMBER::varchar AS PHONENUMBER, 
                        src:READERCODECOUNT::integer AS READERCODECOUNT, 
                        src:READINGROLLOVER::varchar AS READINGROLLOVER, 
                        src:RECALCDAILYAVG::varchar AS RECALCDAILYAVG, 
                        src:SMS::varchar AS SMS, 
                        src:VALIDATEDBY::varchar AS VALIDATEDBY, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XWATERMETERDETAILKEY::integer AS XWATERMETERDETAILKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XWATERMETERDETAILKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLASSETWATER_XWATERMETERDETAIL as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DAILYAVERAGE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTREADERCODEDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTVALIDATEDDATETIME), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LASTYEARDAILYAVERAGE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:READERCODECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XWATERMETERDETAILKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null