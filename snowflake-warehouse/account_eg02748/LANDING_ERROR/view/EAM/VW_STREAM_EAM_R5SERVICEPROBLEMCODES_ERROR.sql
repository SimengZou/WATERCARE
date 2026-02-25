CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5SERVICEPROBLEMCODES_ERROR AS SELECT src, 'EAM_R5SERVICEPROBLEMCODES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_ESTHOURS), '0'), 38, 10) is null and 
                    src:SPB_ESTHOURS is not null) THEN
                    'SPB_ESTHOURS contains a non-numeric value : \'' || AS_VARCHAR(src:SPB_ESTHOURS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SPB_LASTSAVED), '1900-01-01')) is null and 
                    src:SPB_LASTSAVED is not null) THEN
                    'SPB_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:SPB_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_PENALTYFACTOR), '0'), 38, 10) is null and 
                    src:SPB_PENALTYFACTOR is not null) THEN
                    'SPB_PENALTYFACTOR contains a non-numeric value : \'' || AS_VARCHAR(src:SPB_PENALTYFACTOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_PEOPLEREQ), '0'), 38, 10) is null and 
                    src:SPB_PEOPLEREQ is not null) THEN
                    'SPB_PEOPLEREQ contains a non-numeric value : \'' || AS_VARCHAR(src:SPB_PEOPLEREQ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_PERMFIXTURNAROUND), '0'), 38, 10) is null and 
                    src:SPB_PERMFIXTURNAROUND is not null) THEN
                    'SPB_PERMFIXTURNAROUND contains a non-numeric value : \'' || AS_VARCHAR(src:SPB_PERMFIXTURNAROUND) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_STDRESPTIME), '0'), 38, 10) is null and 
                    src:SPB_STDRESPTIME is not null) THEN
                    'SPB_STDRESPTIME contains a non-numeric value : \'' || AS_VARCHAR(src:SPB_STDRESPTIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_TEMPFIXTURNAROUND), '0'), 38, 10) is null and 
                    src:SPB_TEMPFIXTURNAROUND is not null) THEN
                    'SPB_TEMPFIXTURNAROUND contains a non-numeric value : \'' || AS_VARCHAR(src:SPB_TEMPFIXTURNAROUND) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_TOTALESTCOST), '0'), 38, 10) is null and 
                    src:SPB_TOTALESTCOST is not null) THEN
                    'SPB_TOTALESTCOST contains a non-numeric value : \'' || AS_VARCHAR(src:SPB_TOTALESTCOST) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SPB_UDFDATE01), '1900-01-01')) is null and 
                    src:SPB_UDFDATE01 is not null) THEN
                    'SPB_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:SPB_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SPB_UDFDATE02), '1900-01-01')) is null and 
                    src:SPB_UDFDATE02 is not null) THEN
                    'SPB_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:SPB_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SPB_UDFDATE03), '1900-01-01')) is null and 
                    src:SPB_UDFDATE03 is not null) THEN
                    'SPB_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:SPB_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SPB_UDFDATE04), '1900-01-01')) is null and 
                    src:SPB_UDFDATE04 is not null) THEN
                    'SPB_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:SPB_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SPB_UDFDATE05), '1900-01-01')) is null and 
                    src:SPB_UDFDATE05 is not null) THEN
                    'SPB_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:SPB_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_UDFNUM01), '0'), 38, 10) is null and 
                    src:SPB_UDFNUM01 is not null) THEN
                    'SPB_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:SPB_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_UDFNUM02), '0'), 38, 10) is null and 
                    src:SPB_UDFNUM02 is not null) THEN
                    'SPB_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:SPB_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_UDFNUM03), '0'), 38, 10) is null and 
                    src:SPB_UDFNUM03 is not null) THEN
                    'SPB_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:SPB_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_UDFNUM04), '0'), 38, 10) is null and 
                    src:SPB_UDFNUM04 is not null) THEN
                    'SPB_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:SPB_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_UDFNUM05), '0'), 38, 10) is null and 
                    src:SPB_UDFNUM05 is not null) THEN
                    'SPB_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:SPB_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:SPB_UPDATECOUNT is not null) THEN
                    'SPB_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:SPB_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SPB_LASTSAVED), '1900-01-01')) is null and 
                src:SPB_LASTSAVED is not null) THEN
                'SPB_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:SPB_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:SPB_CODE,
                src:SPB_ORG  ORDER BY 
            src:SPB_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5SERVICEPROBLEMCODES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_ESTHOURS), '0'), 38, 10) is null and 
                    src:SPB_ESTHOURS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SPB_LASTSAVED), '1900-01-01')) is null and 
                    src:SPB_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_PENALTYFACTOR), '0'), 38, 10) is null and 
                    src:SPB_PENALTYFACTOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_PEOPLEREQ), '0'), 38, 10) is null and 
                    src:SPB_PEOPLEREQ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_PERMFIXTURNAROUND), '0'), 38, 10) is null and 
                    src:SPB_PERMFIXTURNAROUND is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_STDRESPTIME), '0'), 38, 10) is null and 
                    src:SPB_STDRESPTIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_TEMPFIXTURNAROUND), '0'), 38, 10) is null and 
                    src:SPB_TEMPFIXTURNAROUND is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_TOTALESTCOST), '0'), 38, 10) is null and 
                    src:SPB_TOTALESTCOST is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SPB_UDFDATE01), '1900-01-01')) is null and 
                    src:SPB_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SPB_UDFDATE02), '1900-01-01')) is null and 
                    src:SPB_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SPB_UDFDATE03), '1900-01-01')) is null and 
                    src:SPB_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SPB_UDFDATE04), '1900-01-01')) is null and 
                    src:SPB_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SPB_UDFDATE05), '1900-01-01')) is null and 
                    src:SPB_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_UDFNUM01), '0'), 38, 10) is null and 
                    src:SPB_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_UDFNUM02), '0'), 38, 10) is null and 
                    src:SPB_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_UDFNUM03), '0'), 38, 10) is null and 
                    src:SPB_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_UDFNUM04), '0'), 38, 10) is null and 
                    src:SPB_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_UDFNUM05), '0'), 38, 10) is null and 
                    src:SPB_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPB_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:SPB_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SPB_LASTSAVED), '1900-01-01')) is null and 
                src:SPB_LASTSAVED is not null)