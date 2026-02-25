CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5HOME_ERROR AS SELECT src, 'EAM_R5HOME' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_CURVALUE), '0'), 38, 10) is null and 
                    src:HOM_CURVALUE is not null) THEN
                    'HOM_CURVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_CURVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_EWSDATASPYID), '0'), 38, 10) is null and 
                    src:HOM_EWSDATASPYID is not null) THEN
                    'HOM_EWSDATASPYID contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_EWSDATASPYID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_LASTSAVED), '1900-01-01')) is null and 
                    src:HOM_LASTSAVED is not null) THEN
                    'HOM_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:HOM_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_MAX), '0'), 38, 10) is null and 
                    src:HOM_MAX is not null) THEN
                    'HOM_MAX contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_MAX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_MIN), '0'), 38, 10) is null and 
                    src:HOM_MIN is not null) THEN
                    'HOM_MIN contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_MIN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_NORMSCORE), '0'), 38, 10) is null and 
                    src:HOM_NORMSCORE is not null) THEN
                    'HOM_NORMSCORE contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_NORMSCORE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_NXTUPDATE), '1900-01-01')) is null and 
                    src:HOM_NXTUPDATE is not null) THEN
                    'HOM_NXTUPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:HOM_NXTUPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_RADIUSPERCENTAGE), '0'), 38, 10) is null and 
                    src:HOM_RADIUSPERCENTAGE is not null) THEN
                    'HOM_RADIUSPERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_RADIUSPERCENTAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_UDFDATE01), '1900-01-01')) is null and 
                    src:HOM_UDFDATE01 is not null) THEN
                    'HOM_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:HOM_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_UDFDATE02), '1900-01-01')) is null and 
                    src:HOM_UDFDATE02 is not null) THEN
                    'HOM_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:HOM_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_UDFDATE03), '1900-01-01')) is null and 
                    src:HOM_UDFDATE03 is not null) THEN
                    'HOM_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:HOM_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_UDFDATE04), '1900-01-01')) is null and 
                    src:HOM_UDFDATE04 is not null) THEN
                    'HOM_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:HOM_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_UDFDATE05), '1900-01-01')) is null and 
                    src:HOM_UDFDATE05 is not null) THEN
                    'HOM_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:HOM_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM01), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM01 is not null) THEN
                    'HOM_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM02), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM02 is not null) THEN
                    'HOM_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM03), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM03 is not null) THEN
                    'HOM_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM04), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM04 is not null) THEN
                    'HOM_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM05), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM05 is not null) THEN
                    'HOM_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM06), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM06 is not null) THEN
                    'HOM_UDFNUM06 contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_UDFNUM06) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM07), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM07 is not null) THEN
                    'HOM_UDFNUM07 contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_UDFNUM07) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM08), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM08 is not null) THEN
                    'HOM_UDFNUM08 contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_UDFNUM08) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM09), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM09 is not null) THEN
                    'HOM_UDFNUM09 contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_UDFNUM09) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM10), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM10 is not null) THEN
                    'HOM_UDFNUM10 contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_UDFNUM10) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_UPDATE), '1900-01-01')) is null and 
                    src:HOM_UPDATE is not null) THEN
                    'HOM_UPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:HOM_UPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:HOM_UPDATECOUNT is not null) THEN
                    'HOM_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UPDFREQ), '0'), 38, 10) is null and 
                    src:HOM_UPDFREQ is not null) THEN
                    'HOM_UPDFREQ contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_UPDFREQ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_VALUE), '0'), 38, 10) is null and 
                    src:HOM_VALUE is not null) THEN
                    'HOM_VALUE contains a non-numeric value : \'' || AS_VARCHAR(src:HOM_VALUE) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_LASTSAVED), '1900-01-01')) is null and 
                src:HOM_LASTSAVED is not null) THEN
                'HOM_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:HOM_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:HOM_CODE,
                src:HOM_TYPE  ORDER BY 
            src:HOM_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5HOME as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_CURVALUE), '0'), 38, 10) is null and 
                    src:HOM_CURVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_EWSDATASPYID), '0'), 38, 10) is null and 
                    src:HOM_EWSDATASPYID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_LASTSAVED), '1900-01-01')) is null and 
                    src:HOM_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_MAX), '0'), 38, 10) is null and 
                    src:HOM_MAX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_MIN), '0'), 38, 10) is null and 
                    src:HOM_MIN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_NORMSCORE), '0'), 38, 10) is null and 
                    src:HOM_NORMSCORE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_NXTUPDATE), '1900-01-01')) is null and 
                    src:HOM_NXTUPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_RADIUSPERCENTAGE), '0'), 38, 10) is null and 
                    src:HOM_RADIUSPERCENTAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_UDFDATE01), '1900-01-01')) is null and 
                    src:HOM_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_UDFDATE02), '1900-01-01')) is null and 
                    src:HOM_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_UDFDATE03), '1900-01-01')) is null and 
                    src:HOM_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_UDFDATE04), '1900-01-01')) is null and 
                    src:HOM_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_UDFDATE05), '1900-01-01')) is null and 
                    src:HOM_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM01), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM02), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM03), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM04), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM05), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM06), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM06 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM07), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM07 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM08), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM08 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM09), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM09 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UDFNUM10), '0'), 38, 10) is null and 
                    src:HOM_UDFNUM10 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_UPDATE), '1900-01-01')) is null and 
                    src:HOM_UPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:HOM_UPDATECOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_UPDFREQ), '0'), 38, 10) is null and 
                    src:HOM_UPDFREQ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOM_VALUE), '0'), 38, 10) is null and 
                    src:HOM_VALUE is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:HOM_LASTSAVED), '1900-01-01')) is null and 
                src:HOM_LASTSAVED is not null)