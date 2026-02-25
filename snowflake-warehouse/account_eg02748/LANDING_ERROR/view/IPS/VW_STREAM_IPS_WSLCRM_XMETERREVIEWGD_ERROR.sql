CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLCRM_XMETERREVIEWGD_ERROR AS SELECT src, 'IPS_WSLCRM_XMETERREVIEWGD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSET), '0'), 38, 10) is null and 
                    src:ASSET is not null) THEN
                    'ASSET contains a non-numeric value : \'' || AS_VARCHAR(src:ASSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREOPTION), '0'), 38, 10) is null and 
                    src:COMPAREOPTION is not null) THEN
                    'COMPAREOPTION contains a non-numeric value : \'' || AS_VARCHAR(src:COMPAREOPTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREUSAGE), '0'), 38, 10) is null and 
                    src:COMPAREUSAGE is not null) THEN
                    'COMPAREUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:COMPAREUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTBILLABLE), '0'), 38, 10) is null and 
                    src:CURRENTBILLABLE is not null) THEN
                    'CURRENTBILLABLE contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTBILLABLE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTREAD), '0'), 38, 10) is null and 
                    src:CURRENTREAD is not null) THEN
                    'CURRENTREAD contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTREAD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CURRENTREADDATE), '1900-01-01')) is null and 
                    src:CURRENTREADDATE is not null) THEN
                    'CURRENTREADDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:CURRENTREADDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAILYAVERAGE), '0'), 38, 10) is null and 
                    src:DAILYAVERAGE is not null) THEN
                    'DAILYAVERAGE contains a non-numeric value : \'' || AS_VARCHAR(src:DAILYAVERAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGH), '0'), 38, 10) is null and 
                    src:HIGH is not null) THEN
                    'HIGH contains a non-numeric value : \'' || AS_VARCHAR(src:HIGH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTYEARDAILYAVG), '0'), 38, 10) is null and 
                    src:LASTYEARDAILYAVG is not null) THEN
                    'LASTYEARDAILYAVG contains a non-numeric value : \'' || AS_VARCHAR(src:LASTYEARDAILYAVG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTYEARREAD), '0'), 38, 10) is null and 
                    src:LASTYEARREAD is not null) THEN
                    'LASTYEARREAD contains a non-numeric value : \'' || AS_VARCHAR(src:LASTYEARREAD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOW), '0'), 38, 10) is null and 
                    src:LOW is not null) THEN
                    'LOW contains a non-numeric value : \'' || AS_VARCHAR(src:LOW) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PREVIOUSREAD), '0'), 38, 10) is null and 
                    src:PREVIOUSREAD is not null) THEN
                    'PREVIOUSREAD contains a non-numeric value : \'' || AS_VARCHAR(src:PREVIOUSREAD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGIMPORTACTIVITY), '0'), 38, 10) is null and 
                    src:READINGIMPORTACTIVITY is not null) THEN
                    'READINGIMPORTACTIVITY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGIMPORTACTIVITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGKEY), '0'), 38, 10) is null and 
                    src:READINGKEY is not null) THEN
                    'READINGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XMETERREVIEWDP), '0'), 38, 10) is null and 
                    src:XMETERREVIEWDP is not null) THEN
                    'XMETERREVIEWDP contains a non-numeric value : \'' || AS_VARCHAR(src:XMETERREVIEWDP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XMETERREVIEWGDKEY), '0'), 38, 10) is null and 
                    src:XMETERREVIEWGDKEY is not null) THEN
                    'XMETERREVIEWGDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XMETERREVIEWGDKEY) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null) THEN
                'VARIATION_ID contains a non-timestamp value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSET), '0'), 38, 10) is null and 
                    src:ASSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREOPTION), '0'), 38, 10) is null and 
                    src:COMPAREOPTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREUSAGE), '0'), 38, 10) is null and 
                    src:COMPAREUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTBILLABLE), '0'), 38, 10) is null and 
                    src:CURRENTBILLABLE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTREAD), '0'), 38, 10) is null and 
                    src:CURRENTREAD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CURRENTREADDATE), '1900-01-01')) is null and 
                    src:CURRENTREADDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAILYAVERAGE), '0'), 38, 10) is null and 
                    src:DAILYAVERAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGH), '0'), 38, 10) is null and 
                    src:HIGH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTYEARDAILYAVG), '0'), 38, 10) is null and 
                    src:LASTYEARDAILYAVG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTYEARREAD), '0'), 38, 10) is null and 
                    src:LASTYEARREAD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOW), '0'), 38, 10) is null and 
                    src:LOW is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PREVIOUSREAD), '0'), 38, 10) is null and 
                    src:PREVIOUSREAD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGIMPORTACTIVITY), '0'), 38, 10) is null and 
                    src:READINGIMPORTACTIVITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGKEY), '0'), 38, 10) is null and 
                    src:READINGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XMETERREVIEWDP), '0'), 38, 10) is null and 
                    src:XMETERREVIEWDP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XMETERREVIEWGDKEY), '0'), 38, 10) is null and 
                    src:XMETERREVIEWGDKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)