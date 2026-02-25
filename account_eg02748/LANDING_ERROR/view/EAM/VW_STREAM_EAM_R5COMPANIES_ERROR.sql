CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5COMPANIES_ERROR AS SELECT src, 'EAM_R5COMPANIES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_CAPACITY), '0'), 38, 10) is null and 
                    src:COM_CAPACITY is not null) THEN
                    'COM_CAPACITY contains a non-numeric value : \'' || AS_VARCHAR(src:COM_CAPACITY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_CREATED), '1900-01-01')) is null and 
                    src:COM_CREATED is not null) THEN
                    'COM_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:COM_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_INTERFACE), '1900-01-01')) is null and 
                    src:COM_INTERFACE is not null) THEN
                    'COM_INTERFACE contains a non-timestamp value : \'' || AS_VARCHAR(src:COM_INTERFACE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_IPVENDOR), '0'), 38, 10) is null and 
                    src:COM_IPVENDOR is not null) THEN
                    'COM_IPVENDOR contains a non-numeric value : \'' || AS_VARCHAR(src:COM_IPVENDOR) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_LASTSAVED), '1900-01-01')) is null and 
                    src:COM_LASTSAVED is not null) THEN
                    'COM_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:COM_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:COM_LASTSTATUSUPDATE is not null) THEN
                    'COM_LASTSTATUSUPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:COM_LASTSTATUSUPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_LEADTIME), '0'), 38, 10) is null and 
                    src:COM_LEADTIME is not null) THEN
                    'COM_LEADTIME contains a non-numeric value : \'' || AS_VARCHAR(src:COM_LEADTIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_MAXORD), '0'), 38, 10) is null and 
                    src:COM_MAXORD is not null) THEN
                    'COM_MAXORD contains a non-numeric value : \'' || AS_VARCHAR(src:COM_MAXORD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_MINORD), '0'), 38, 10) is null and 
                    src:COM_MINORD is not null) THEN
                    'COM_MINORD contains a non-numeric value : \'' || AS_VARCHAR(src:COM_MINORD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_UDFDATE01), '1900-01-01')) is null and 
                    src:COM_UDFDATE01 is not null) THEN
                    'COM_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:COM_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_UDFDATE02), '1900-01-01')) is null and 
                    src:COM_UDFDATE02 is not null) THEN
                    'COM_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:COM_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_UDFDATE03), '1900-01-01')) is null and 
                    src:COM_UDFDATE03 is not null) THEN
                    'COM_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:COM_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_UDFDATE04), '1900-01-01')) is null and 
                    src:COM_UDFDATE04 is not null) THEN
                    'COM_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:COM_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_UDFDATE05), '1900-01-01')) is null and 
                    src:COM_UDFDATE05 is not null) THEN
                    'COM_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:COM_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_UDFNUM01), '0'), 38, 10) is null and 
                    src:COM_UDFNUM01 is not null) THEN
                    'COM_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:COM_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_UDFNUM02), '0'), 38, 10) is null and 
                    src:COM_UDFNUM02 is not null) THEN
                    'COM_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:COM_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_UDFNUM03), '0'), 38, 10) is null and 
                    src:COM_UDFNUM03 is not null) THEN
                    'COM_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:COM_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_UDFNUM04), '0'), 38, 10) is null and 
                    src:COM_UDFNUM04 is not null) THEN
                    'COM_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:COM_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_UDFNUM05), '0'), 38, 10) is null and 
                    src:COM_UDFNUM05 is not null) THEN
                    'COM_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:COM_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:COM_UPDATECOUNT is not null) THEN
                    'COM_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:COM_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_UPDATED), '1900-01-01')) is null and 
                    src:COM_UPDATED is not null) THEN
                    'COM_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:COM_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_LASTSAVED), '1900-01-01')) is null and 
                src:COM_LASTSAVED is not null) THEN
                'COM_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:COM_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:COM_CODE,
                src:COM_ORG  ORDER BY 
            src:COM_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5COMPANIES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_CAPACITY), '0'), 38, 10) is null and 
                    src:COM_CAPACITY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_CREATED), '1900-01-01')) is null and 
                    src:COM_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_INTERFACE), '1900-01-01')) is null and 
                    src:COM_INTERFACE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_IPVENDOR), '0'), 38, 10) is null and 
                    src:COM_IPVENDOR is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_LASTSAVED), '1900-01-01')) is null and 
                    src:COM_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:COM_LASTSTATUSUPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_LEADTIME), '0'), 38, 10) is null and 
                    src:COM_LEADTIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_MAXORD), '0'), 38, 10) is null and 
                    src:COM_MAXORD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_MINORD), '0'), 38, 10) is null and 
                    src:COM_MINORD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_UDFDATE01), '1900-01-01')) is null and 
                    src:COM_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_UDFDATE02), '1900-01-01')) is null and 
                    src:COM_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_UDFDATE03), '1900-01-01')) is null and 
                    src:COM_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_UDFDATE04), '1900-01-01')) is null and 
                    src:COM_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_UDFDATE05), '1900-01-01')) is null and 
                    src:COM_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_UDFNUM01), '0'), 38, 10) is null and 
                    src:COM_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_UDFNUM02), '0'), 38, 10) is null and 
                    src:COM_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_UDFNUM03), '0'), 38, 10) is null and 
                    src:COM_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_UDFNUM04), '0'), 38, 10) is null and 
                    src:COM_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_UDFNUM05), '0'), 38, 10) is null and 
                    src:COM_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COM_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:COM_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_UPDATED), '1900-01-01')) is null and 
                    src:COM_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COM_LASTSAVED), '1900-01-01')) is null and 
                src:COM_LASTSAVED is not null)