CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLASSETWATER_XWATERMETERDETAIL_ERROR AS SELECT src, 'IPS_WSLASSETWATER_XWATERMETERDETAIL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAILYAVERAGE), '0'), 38, 10) is null and 
                    src:DAILYAVERAGE is not null) THEN
                    'DAILYAVERAGE contains a non-numeric value : \'' || AS_VARCHAR(src:DAILYAVERAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTREADERCODEDATE), '1900-01-01')) is null and 
                    src:LASTREADERCODEDATE is not null) THEN
                    'LASTREADERCODEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:LASTREADERCODEDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTVALIDATEDDATETIME), '1900-01-01')) is null and 
                    src:LASTVALIDATEDDATETIME is not null) THEN
                    'LASTVALIDATEDDATETIME contains a non-timestamp value : \'' || AS_VARCHAR(src:LASTVALIDATEDDATETIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTYEARDAILYAVERAGE), '0'), 38, 10) is null and 
                    src:LASTYEARDAILYAVERAGE is not null) THEN
                    'LASTYEARDAILYAVERAGE contains a non-numeric value : \'' || AS_VARCHAR(src:LASTYEARDAILYAVERAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READERCODECOUNT), '0'), 38, 10) is null and 
                    src:READERCODECOUNT is not null) THEN
                    'READERCODECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:READERCODECOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XWATERMETERDETAILKEY), '0'), 38, 10) is null and 
                    src:XWATERMETERDETAILKEY is not null) THEN
                    'XWATERMETERDETAILKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XWATERMETERDETAILKEY) || '\'' WHEN 
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
                                    
                src:XWATERMETERDETAILKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLASSETWATER_XWATERMETERDETAIL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAILYAVERAGE), '0'), 38, 10) is null and 
                    src:DAILYAVERAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTREADERCODEDATE), '1900-01-01')) is null and 
                    src:LASTREADERCODEDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTVALIDATEDDATETIME), '1900-01-01')) is null and 
                    src:LASTVALIDATEDDATETIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTYEARDAILYAVERAGE), '0'), 38, 10) is null and 
                    src:LASTYEARDAILYAVERAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READERCODECOUNT), '0'), 38, 10) is null and 
                    src:READERCODECOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XWATERMETERDETAILKEY), '0'), 38, 10) is null and 
                    src:XWATERMETERDETAILKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)