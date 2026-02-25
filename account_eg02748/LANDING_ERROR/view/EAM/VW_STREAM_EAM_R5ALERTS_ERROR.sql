CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ALERTS_ERROR AS SELECT src, 'EAM_R5ALERTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_CODEFIELDID), '0'), 38, 10) is null and 
                    src:ALT_CODEFIELDID is not null) THEN
                    'ALT_CODEFIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_CODEFIELDID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_CREATED), '1900-01-01')) is null and 
                    src:ALT_CREATED is not null) THEN
                    'ALT_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:ALT_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_DATASPYID), '0'), 38, 10) is null and 
                    src:ALT_DATASPYID is not null) THEN
                    'ALT_DATASPYID contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_DATASPYID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_FREQ), '0'), 38, 10) is null and 
                    src:ALT_FREQ is not null) THEN
                    'ALT_FREQ contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_FREQ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_GRIDID), '0'), 38, 10) is null and 
                    src:ALT_GRIDID is not null) THEN
                    'ALT_GRIDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_GRIDID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_LASTALERT), '1900-01-01')) is null and 
                    src:ALT_LASTALERT is not null) THEN
                    'ALT_LASTALERT contains a non-timestamp value : \'' || AS_VARCHAR(src:ALT_LASTALERT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_LASTEVAL), '1900-01-01')) is null and 
                    src:ALT_LASTEVAL is not null) THEN
                    'ALT_LASTEVAL contains a non-timestamp value : \'' || AS_VARCHAR(src:ALT_LASTEVAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_LASTSAVED), '1900-01-01')) is null and 
                    src:ALT_LASTSAVED is not null) THEN
                    'ALT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ALT_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_MAXVALUE), '0'), 38, 10) is null and 
                    src:ALT_MAXVALUE is not null) THEN
                    'ALT_MAXVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_MAXVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_MINVALUE), '0'), 38, 10) is null and 
                    src:ALT_MINVALUE is not null) THEN
                    'ALT_MINVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_MINVALUE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_NEXTEVAL), '1900-01-01')) is null and 
                    src:ALT_NEXTEVAL is not null) THEN
                    'ALT_NEXTEVAL contains a non-timestamp value : \'' || AS_VARCHAR(src:ALT_NEXTEVAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_ORGFIELDID), '0'), 38, 10) is null and 
                    src:ALT_ORGFIELDID is not null) THEN
                    'ALT_ORGFIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_ORGFIELDID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_UDFDATE01), '1900-01-01')) is null and 
                    src:ALT_UDFDATE01 is not null) THEN
                    'ALT_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:ALT_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_UDFDATE02), '1900-01-01')) is null and 
                    src:ALT_UDFDATE02 is not null) THEN
                    'ALT_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:ALT_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_UDFDATE03), '1900-01-01')) is null and 
                    src:ALT_UDFDATE03 is not null) THEN
                    'ALT_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:ALT_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_UDFDATE04), '1900-01-01')) is null and 
                    src:ALT_UDFDATE04 is not null) THEN
                    'ALT_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:ALT_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_UDFDATE05), '1900-01-01')) is null and 
                    src:ALT_UDFDATE05 is not null) THEN
                    'ALT_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:ALT_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM01), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM01 is not null) THEN
                    'ALT_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM02), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM02 is not null) THEN
                    'ALT_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM03), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM03 is not null) THEN
                    'ALT_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM04), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM04 is not null) THEN
                    'ALT_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM05), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM05 is not null) THEN
                    'ALT_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM06), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM06 is not null) THEN
                    'ALT_UDFNUM06 contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_UDFNUM06) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM07), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM07 is not null) THEN
                    'ALT_UDFNUM07 contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_UDFNUM07) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM08), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM08 is not null) THEN
                    'ALT_UDFNUM08 contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_UDFNUM08) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM09), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM09 is not null) THEN
                    'ALT_UDFNUM09 contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_UDFNUM09) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM10), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM10 is not null) THEN
                    'ALT_UDFNUM10 contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_UDFNUM10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ALT_UPDATECOUNT is not null) THEN
                    'ALT_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_VALUEFIELDID), '0'), 38, 10) is null and 
                    src:ALT_VALUEFIELDID is not null) THEN
                    'ALT_VALUEFIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:ALT_VALUEFIELDID) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_LASTSAVED), '1900-01-01')) is null and 
                src:ALT_LASTSAVED is not null) THEN
                'ALT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ALT_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ALT_CODE  ORDER BY 
            src:ALT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_CODEFIELDID), '0'), 38, 10) is null and 
                    src:ALT_CODEFIELDID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_CREATED), '1900-01-01')) is null and 
                    src:ALT_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_DATASPYID), '0'), 38, 10) is null and 
                    src:ALT_DATASPYID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_FREQ), '0'), 38, 10) is null and 
                    src:ALT_FREQ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_GRIDID), '0'), 38, 10) is null and 
                    src:ALT_GRIDID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_LASTALERT), '1900-01-01')) is null and 
                    src:ALT_LASTALERT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_LASTEVAL), '1900-01-01')) is null and 
                    src:ALT_LASTEVAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_LASTSAVED), '1900-01-01')) is null and 
                    src:ALT_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_MAXVALUE), '0'), 38, 10) is null and 
                    src:ALT_MAXVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_MINVALUE), '0'), 38, 10) is null and 
                    src:ALT_MINVALUE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_NEXTEVAL), '1900-01-01')) is null and 
                    src:ALT_NEXTEVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_ORGFIELDID), '0'), 38, 10) is null and 
                    src:ALT_ORGFIELDID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_UDFDATE01), '1900-01-01')) is null and 
                    src:ALT_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_UDFDATE02), '1900-01-01')) is null and 
                    src:ALT_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_UDFDATE03), '1900-01-01')) is null and 
                    src:ALT_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_UDFDATE04), '1900-01-01')) is null and 
                    src:ALT_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_UDFDATE05), '1900-01-01')) is null and 
                    src:ALT_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM01), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM02), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM03), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM04), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM05), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM06), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM06 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM07), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM07 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM08), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM08 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM09), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM09 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UDFNUM10), '0'), 38, 10) is null and 
                    src:ALT_UDFNUM10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ALT_UPDATECOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALT_VALUEFIELDID), '0'), 38, 10) is null and 
                    src:ALT_VALUEFIELDID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ALT_LASTSAVED), '1900-01-01')) is null and 
                src:ALT_LASTSAVED is not null)