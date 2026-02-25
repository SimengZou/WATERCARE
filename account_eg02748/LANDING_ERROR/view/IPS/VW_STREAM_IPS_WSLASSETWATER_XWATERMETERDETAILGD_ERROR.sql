CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLASSETWATER_XWATERMETERDETAILGD_ERROR AS SELECT src, 'IPS_WSLASSETWATER_XWATERMETERDETAILGD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETKEY), '0'), 38, 10) is null and 
                    src:ASSETKEY is not null) THEN
                    'ASSETKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSETKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CURRENTREADDATE), '1900-01-01')) is null and 
                    src:CURRENTREADDATE is not null) THEN
                    'CURRENTREADDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:CURRENTREADDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAILYAVERAGE), '0'), 38, 10) is null and 
                    src:DAILYAVERAGE is not null) THEN
                    'DAILYAVERAGE contains a non-numeric value : \'' || AS_VARCHAR(src:DAILYAVERAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAILYAVERAGEPERC), '0'), 38, 10) is null and 
                    src:DAILYAVERAGEPERC is not null) THEN
                    'DAILYAVERAGEPERC contains a non-numeric value : \'' || AS_VARCHAR(src:DAILYAVERAGEPERC) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTYEARDAILYAVERAGE), '0'), 38, 10) is null and 
                    src:LASTYEARDAILYAVERAGE is not null) THEN
                    'LASTYEARDAILYAVERAGE contains a non-numeric value : \'' || AS_VARCHAR(src:LASTYEARDAILYAVERAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MONTHLYAVERAGE), '0'), 38, 10) is null and 
                    src:MONTHLYAVERAGE is not null) THEN
                    'MONTHLYAVERAGE contains a non-numeric value : \'' || AS_VARCHAR(src:MONTHLYAVERAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READINGDATE), '1900-01-01')) is null and 
                    src:READINGDATE is not null) THEN
                    'READINGDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:READINGDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XWATERMETERDETAIL), '0'), 38, 10) is null and 
                    src:XWATERMETERDETAIL is not null) THEN
                    'XWATERMETERDETAIL contains a non-numeric value : \'' || AS_VARCHAR(src:XWATERMETERDETAIL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XWATERMETERDETAILGDKEY), '0'), 38, 10) is null and 
                    src:XWATERMETERDETAILGDKEY is not null) THEN
                    'XWATERMETERDETAILGDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XWATERMETERDETAILGDKEY) || '\'' WHEN 
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
                                    
                src:XWATERMETERDETAILGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLASSETWATER_XWATERMETERDETAILGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETKEY), '0'), 38, 10) is null and 
                    src:ASSETKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CURRENTREADDATE), '1900-01-01')) is null and 
                    src:CURRENTREADDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAILYAVERAGE), '0'), 38, 10) is null and 
                    src:DAILYAVERAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAILYAVERAGEPERC), '0'), 38, 10) is null and 
                    src:DAILYAVERAGEPERC is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTYEARDAILYAVERAGE), '0'), 38, 10) is null and 
                    src:LASTYEARDAILYAVERAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MONTHLYAVERAGE), '0'), 38, 10) is null and 
                    src:MONTHLYAVERAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READINGDATE), '1900-01-01')) is null and 
                    src:READINGDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XWATERMETERDETAIL), '0'), 38, 10) is null and 
                    src:XWATERMETERDETAIL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XWATERMETERDETAILGDKEY), '0'), 38, 10) is null and 
                    src:XWATERMETERDETAILGDKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)