CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ALERTWO_ERROR AS SELECT src, 'EAM_R5ALERTWO' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_DELAY), '0'), 38, 10) is null and 
                    src:ALW_DELAY is not null) THEN
                    'ALW_DELAY contains a non-numeric value : \'' || AS_VARCHAR(src:ALW_DELAY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_DURATIONFIELDID), '0'), 38, 10) is null and 
                    src:ALW_DURATIONFIELDID is not null) THEN
                    'ALW_DURATIONFIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALW_DURATIONFIELDID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_JOBTYPEFIELDID), '0'), 38, 10) is null and 
                    src:ALW_JOBTYPEFIELDID is not null) THEN
                    'ALW_JOBTYPEFIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALW_JOBTYPEFIELDID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALW_LASTSAVED), '1900-01-01')) is null and 
                    src:ALW_LASTSAVED is not null) THEN
                    'ALW_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ALW_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_OBJECTFIELDID), '0'), 38, 10) is null and 
                    src:ALW_OBJECTFIELDID is not null) THEN
                    'ALW_OBJECTFIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALW_OBJECTFIELDID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_OBJECTORGFIELDID), '0'), 38, 10) is null and 
                    src:ALW_OBJECTORGFIELDID is not null) THEN
                    'ALW_OBJECTORGFIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALW_OBJECTORGFIELDID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_PRIORITYFIELDID), '0'), 38, 10) is null and 
                    src:ALW_PRIORITYFIELDID is not null) THEN
                    'ALW_PRIORITYFIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALW_PRIORITYFIELDID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_PROBLEMCODEFIELDID), '0'), 38, 10) is null and 
                    src:ALW_PROBLEMCODEFIELDID is not null) THEN
                    'ALW_PROBLEMCODEFIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALW_PROBLEMCODEFIELDID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_REQUESTENDFIELDID), '0'), 38, 10) is null and 
                    src:ALW_REQUESTENDFIELDID is not null) THEN
                    'ALW_REQUESTENDFIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALW_REQUESTENDFIELDID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_REQUESTSTARTFIELDID), '0'), 38, 10) is null and 
                    src:ALW_REQUESTSTARTFIELDID is not null) THEN
                    'ALW_REQUESTSTARTFIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALW_REQUESTSTARTFIELDID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_SCHEDSTARTFIELDID), '0'), 38, 10) is null and 
                    src:ALW_SCHEDSTARTFIELDID is not null) THEN
                    'ALW_SCHEDSTARTFIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALW_SCHEDSTARTFIELDID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ALW_UPDATECOUNT is not null) THEN
                    'ALW_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ALW_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_WORKORDERORGFIELDID), '0'), 38, 10) is null and 
                    src:ALW_WORKORDERORGFIELDID is not null) THEN
                    'ALW_WORKORDERORGFIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALW_WORKORDERORGFIELDID) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALW_LASTSAVED), '1900-01-01')) is null and 
                src:ALW_LASTSAVED is not null) THEN
                'ALW_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ALW_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ALW_ALERT  ORDER BY 
            src:ALW_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTWO as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_DELAY), '0'), 38, 10) is null and 
                    src:ALW_DELAY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_DURATIONFIELDID), '0'), 38, 10) is null and 
                    src:ALW_DURATIONFIELDID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_JOBTYPEFIELDID), '0'), 38, 10) is null and 
                    src:ALW_JOBTYPEFIELDID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALW_LASTSAVED), '1900-01-01')) is null and 
                    src:ALW_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_OBJECTFIELDID), '0'), 38, 10) is null and 
                    src:ALW_OBJECTFIELDID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_OBJECTORGFIELDID), '0'), 38, 10) is null and 
                    src:ALW_OBJECTORGFIELDID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_PRIORITYFIELDID), '0'), 38, 10) is null and 
                    src:ALW_PRIORITYFIELDID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_PROBLEMCODEFIELDID), '0'), 38, 10) is null and 
                    src:ALW_PROBLEMCODEFIELDID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_REQUESTENDFIELDID), '0'), 38, 10) is null and 
                    src:ALW_REQUESTENDFIELDID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_REQUESTSTARTFIELDID), '0'), 38, 10) is null and 
                    src:ALW_REQUESTSTARTFIELDID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_SCHEDSTARTFIELDID), '0'), 38, 10) is null and 
                    src:ALW_SCHEDSTARTFIELDID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ALW_UPDATECOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALW_WORKORDERORGFIELDID), '0'), 38, 10) is null and 
                    src:ALW_WORKORDERORGFIELDID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALW_LASTSAVED), '1900-01-01')) is null and 
                src:ALW_LASTSAVED is not null)