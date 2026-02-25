CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5STANDWORKS_ERROR AS SELECT src, 'EAM_R5STANDWORKS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_CREATED), '1900-01-01')) is null and 
                    src:STW_CREATED is not null) THEN
                    'STW_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_DURATION), '0'), 38, 10) is null and 
                    src:STW_DURATION is not null) THEN
                    'STW_DURATION contains a non-numeric value : \'' || AS_VARCHAR(src:STW_DURATION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_LASTSAVED), '1900-01-01')) is null and 
                    src:STW_LASTSAVED is not null) THEN
                    'STW_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_PERMITREVIEWED), '1900-01-01')) is null and 
                    src:STW_PERMITREVIEWED is not null) THEN
                    'STW_PERMITREVIEWED contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_PERMITREVIEWED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_PERMITREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:STW_PERMITREVIEWREQUIRED is not null) THEN
                    'STW_PERMITREVIEWREQUIRED contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_PERMITREVIEWREQUIRED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_SAFETYREVIEWED), '1900-01-01')) is null and 
                    src:STW_SAFETYREVIEWED is not null) THEN
                    'STW_SAFETYREVIEWED contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_SAFETYREVIEWED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_SAFETYREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:STW_SAFETYREVIEWREQUIRED is not null) THEN
                    'STW_SAFETYREVIEWREQUIRED contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_SAFETYREVIEWREQUIRED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE01), '1900-01-01')) is null and 
                    src:STW_UDFDATE01 is not null) THEN
                    'STW_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE02), '1900-01-01')) is null and 
                    src:STW_UDFDATE02 is not null) THEN
                    'STW_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE03), '1900-01-01')) is null and 
                    src:STW_UDFDATE03 is not null) THEN
                    'STW_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE04), '1900-01-01')) is null and 
                    src:STW_UDFDATE04 is not null) THEN
                    'STW_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE05), '1900-01-01')) is null and 
                    src:STW_UDFDATE05 is not null) THEN
                    'STW_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE06), '1900-01-01')) is null and 
                    src:STW_UDFDATE06 is not null) THEN
                    'STW_UDFDATE06 contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_UDFDATE06) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE07), '1900-01-01')) is null and 
                    src:STW_UDFDATE07 is not null) THEN
                    'STW_UDFDATE07 contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_UDFDATE07) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE08), '1900-01-01')) is null and 
                    src:STW_UDFDATE08 is not null) THEN
                    'STW_UDFDATE08 contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_UDFDATE08) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE09), '1900-01-01')) is null and 
                    src:STW_UDFDATE09 is not null) THEN
                    'STW_UDFDATE09 contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_UDFDATE09) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE10), '1900-01-01')) is null and 
                    src:STW_UDFDATE10 is not null) THEN
                    'STW_UDFDATE10 contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_UDFDATE10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM01), '0'), 38, 10) is null and 
                    src:STW_UDFNUM01 is not null) THEN
                    'STW_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:STW_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM02), '0'), 38, 10) is null and 
                    src:STW_UDFNUM02 is not null) THEN
                    'STW_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:STW_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM03), '0'), 38, 10) is null and 
                    src:STW_UDFNUM03 is not null) THEN
                    'STW_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:STW_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM04), '0'), 38, 10) is null and 
                    src:STW_UDFNUM04 is not null) THEN
                    'STW_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:STW_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM05), '0'), 38, 10) is null and 
                    src:STW_UDFNUM05 is not null) THEN
                    'STW_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:STW_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM06), '0'), 38, 10) is null and 
                    src:STW_UDFNUM06 is not null) THEN
                    'STW_UDFNUM06 contains a non-numeric value : \'' || AS_VARCHAR(src:STW_UDFNUM06) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM07), '0'), 38, 10) is null and 
                    src:STW_UDFNUM07 is not null) THEN
                    'STW_UDFNUM07 contains a non-numeric value : \'' || AS_VARCHAR(src:STW_UDFNUM07) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM08), '0'), 38, 10) is null and 
                    src:STW_UDFNUM08 is not null) THEN
                    'STW_UDFNUM08 contains a non-numeric value : \'' || AS_VARCHAR(src:STW_UDFNUM08) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM09), '0'), 38, 10) is null and 
                    src:STW_UDFNUM09 is not null) THEN
                    'STW_UDFNUM09 contains a non-numeric value : \'' || AS_VARCHAR(src:STW_UDFNUM09) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM10), '0'), 38, 10) is null and 
                    src:STW_UDFNUM10 is not null) THEN
                    'STW_UDFNUM10 contains a non-numeric value : \'' || AS_VARCHAR(src:STW_UDFNUM10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:STW_UPDATECOUNT is not null) THEN
                    'STW_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:STW_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UPDATED), '1900-01-01')) is null and 
                    src:STW_UPDATED is not null) THEN
                    'STW_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_LASTSAVED), '1900-01-01')) is null and 
                src:STW_LASTSAVED is not null) THEN
                'STW_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:STW_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:STW_CODE  ORDER BY 
            src:STW_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5STANDWORKS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_CREATED), '1900-01-01')) is null and 
                    src:STW_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_DURATION), '0'), 38, 10) is null and 
                    src:STW_DURATION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_LASTSAVED), '1900-01-01')) is null and 
                    src:STW_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_PERMITREVIEWED), '1900-01-01')) is null and 
                    src:STW_PERMITREVIEWED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_PERMITREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:STW_PERMITREVIEWREQUIRED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_SAFETYREVIEWED), '1900-01-01')) is null and 
                    src:STW_SAFETYREVIEWED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_SAFETYREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:STW_SAFETYREVIEWREQUIRED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE01), '1900-01-01')) is null and 
                    src:STW_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE02), '1900-01-01')) is null and 
                    src:STW_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE03), '1900-01-01')) is null and 
                    src:STW_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE04), '1900-01-01')) is null and 
                    src:STW_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE05), '1900-01-01')) is null and 
                    src:STW_UDFDATE05 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE06), '1900-01-01')) is null and 
                    src:STW_UDFDATE06 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE07), '1900-01-01')) is null and 
                    src:STW_UDFDATE07 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE08), '1900-01-01')) is null and 
                    src:STW_UDFDATE08 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE09), '1900-01-01')) is null and 
                    src:STW_UDFDATE09 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UDFDATE10), '1900-01-01')) is null and 
                    src:STW_UDFDATE10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM01), '0'), 38, 10) is null and 
                    src:STW_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM02), '0'), 38, 10) is null and 
                    src:STW_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM03), '0'), 38, 10) is null and 
                    src:STW_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM04), '0'), 38, 10) is null and 
                    src:STW_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM05), '0'), 38, 10) is null and 
                    src:STW_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM06), '0'), 38, 10) is null and 
                    src:STW_UDFNUM06 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM07), '0'), 38, 10) is null and 
                    src:STW_UDFNUM07 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM08), '0'), 38, 10) is null and 
                    src:STW_UDFNUM08 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM09), '0'), 38, 10) is null and 
                    src:STW_UDFNUM09 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UDFNUM10), '0'), 38, 10) is null and 
                    src:STW_UDFNUM10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STW_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:STW_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_UPDATED), '1900-01-01')) is null and 
                    src:STW_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STW_LASTSAVED), '1900-01-01')) is null and 
                src:STW_LASTSAVED is not null)