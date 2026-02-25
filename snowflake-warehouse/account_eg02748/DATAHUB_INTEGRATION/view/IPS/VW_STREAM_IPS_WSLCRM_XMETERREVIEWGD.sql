CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCRM_XMETERREVIEWGD AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSET::integer AS ASSET, 
                        src:COMPAREOPTION::integer AS COMPAREOPTION, 
                        src:COMPAREREASON::varchar AS COMPAREREASON, 
                        src:COMPAREUSAGE::numeric(38, 10) AS COMPAREUSAGE, 
                        src:CURRENTBILLABLE::numeric(38, 10) AS CURRENTBILLABLE, 
                        src:CURRENTREAD::numeric(38, 10) AS CURRENTREAD, 
                        src:CURRENTREADDATE::datetime AS CURRENTREADDATE, 
                        src:DAILYAVERAGE::numeric(38, 10) AS DAILYAVERAGE, 
                        src:DELETED::boolean AS DELETED, 
                        src:EXCEPTION::varchar AS EXCEPTION, 
                        src:FIELDNOTES::varchar AS FIELDNOTES, 
                        src:HIGH::numeric(38, 10) AS HIGH, 
                        src:LASTYEARDAILYAVG::numeric(38, 10) AS LASTYEARDAILYAVG, 
                        src:LASTYEARREAD::numeric(38, 10) AS LASTYEARREAD, 
                        src:LOW::numeric(38, 10) AS LOW, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PREVIOUSREAD::numeric(38, 10) AS PREVIOUSREAD, 
                        src:READERCODE::varchar AS READERCODE, 
                        src:READINGCYCLE::varchar AS READINGCYCLE, 
                        src:READINGIMPORTACTIVITY::integer AS READINGIMPORTACTIVITY, 
                        src:READINGKEY::integer AS READINGKEY, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XMETERREVIEWDP::integer AS XMETERREVIEWDP, 
                        src:XMETERREVIEWGDKEY::integer AS XMETERREVIEWGDKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XMETERREVIEWGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCRM_XMETERREVIEWGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPAREOPTION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPAREUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTBILLABLE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTREAD), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CURRENTREADDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DAILYAVERAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HIGH), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LASTYEARDAILYAVG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LASTYEARREAD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LOW), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PREVIOUSREAD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:READINGIMPORTACTIVITY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:READINGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XMETERREVIEWDP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XMETERREVIEWGDKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null