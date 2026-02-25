CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCRM_XMETERREVIEWGD AS SELECT
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
                        src:XMETERREVIEWGDKEY::integer AS XMETERREVIEWGDKEY, 
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
    
                        
                src:XMETERREVIEWGDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XMETERREVIEWGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCRM_XMETERREVIEWGD as strm))