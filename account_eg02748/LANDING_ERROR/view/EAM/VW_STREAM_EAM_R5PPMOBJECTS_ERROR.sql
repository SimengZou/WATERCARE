CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5PPMOBJECTS_ERROR AS SELECT src, 'EAM_R5PPMOBJECTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_DEACTIVE), '1900-01-01')) is null and 
                    src:PPO_DEACTIVE is not null) THEN
                    'PPO_DEACTIVE contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_DEACTIVE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_DORMEND), '1900-01-01')) is null and 
                    src:PPO_DORMEND is not null) THEN
                    'PPO_DORMEND contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_DORMEND) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_DORMSTART), '1900-01-01')) is null and 
                    src:PPO_DORMSTART is not null) THEN
                    'PPO_DORMSTART contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_DORMSTART) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_DUE), '1900-01-01')) is null and 
                    src:PPO_DUE is not null) THEN
                    'PPO_DUE contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_DUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_FREQ), '0'), 38, 10) is null and 
                    src:PPO_FREQ is not null) THEN
                    'PPO_FREQ contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_FREQ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_FROMPOINT), '0'), 38, 10) is null and 
                    src:PPO_FROMPOINT is not null) THEN
                    'PPO_FROMPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_FROMPOINT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_LASTSAVED), '1900-01-01')) is null and 
                    src:PPO_LASTSAVED is not null) THEN
                    'PPO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_LOCKEDBYSESSION), '0'), 38, 10) is null and 
                    src:PPO_LOCKEDBYSESSION is not null) THEN
                    'PPO_LOCKEDBYSESSION contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_LOCKEDBYSESSION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_METER), '0'), 38, 10) is null and 
                    src:PPO_METER is not null) THEN
                    'PPO_METER contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_METER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_METER2), '0'), 38, 10) is null and 
                    src:PPO_METER2 is not null) THEN
                    'PPO_METER2 contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_METER2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_METERDUE), '0'), 38, 10) is null and 
                    src:PPO_METERDUE is not null) THEN
                    'PPO_METERDUE contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_METERDUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_METERDUE2), '0'), 38, 10) is null and 
                    src:PPO_METERDUE2 is not null) THEN
                    'PPO_METERDUE2 contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_METERDUE2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_PERFORMONDAY), '0'), 38, 10) is null and 
                    src:PPO_PERFORMONDAY is not null) THEN
                    'PPO_PERFORMONDAY contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_PERFORMONDAY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_PK), '0'), 38, 10) is null and 
                    src:PPO_PK is not null) THEN
                    'PPO_PK contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_PK) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_REVISION), '0'), 38, 10) is null and 
                    src:PPO_REVISION is not null) THEN
                    'PPO_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_REVISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_SETID), '0'), 38, 10) is null and 
                    src:PPO_SETID is not null) THEN
                    'PPO_SETID contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_SETID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_TOPOINT), '0'), 38, 10) is null and 
                    src:PPO_TOPOINT is not null) THEN
                    'PPO_TOPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_TOPOINT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE01), '1900-01-01')) is null and 
                    src:PPO_UDFDATE01 is not null) THEN
                    'PPO_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE02), '1900-01-01')) is null and 
                    src:PPO_UDFDATE02 is not null) THEN
                    'PPO_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE03), '1900-01-01')) is null and 
                    src:PPO_UDFDATE03 is not null) THEN
                    'PPO_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE04), '1900-01-01')) is null and 
                    src:PPO_UDFDATE04 is not null) THEN
                    'PPO_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE05), '1900-01-01')) is null and 
                    src:PPO_UDFDATE05 is not null) THEN
                    'PPO_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE06), '1900-01-01')) is null and 
                    src:PPO_UDFDATE06 is not null) THEN
                    'PPO_UDFDATE06 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_UDFDATE06) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE07), '1900-01-01')) is null and 
                    src:PPO_UDFDATE07 is not null) THEN
                    'PPO_UDFDATE07 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_UDFDATE07) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE08), '1900-01-01')) is null and 
                    src:PPO_UDFDATE08 is not null) THEN
                    'PPO_UDFDATE08 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_UDFDATE08) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE09), '1900-01-01')) is null and 
                    src:PPO_UDFDATE09 is not null) THEN
                    'PPO_UDFDATE09 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_UDFDATE09) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE10), '1900-01-01')) is null and 
                    src:PPO_UDFDATE10 is not null) THEN
                    'PPO_UDFDATE10 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_UDFDATE10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM01), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM01 is not null) THEN
                    'PPO_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM02), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM02 is not null) THEN
                    'PPO_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM03), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM03 is not null) THEN
                    'PPO_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM04), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM04 is not null) THEN
                    'PPO_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM05), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM05 is not null) THEN
                    'PPO_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM06), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM06 is not null) THEN
                    'PPO_UDFNUM06 contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_UDFNUM06) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM07), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM07 is not null) THEN
                    'PPO_UDFNUM07 contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_UDFNUM07) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM08), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM08 is not null) THEN
                    'PPO_UDFNUM08 contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_UDFNUM08) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM09), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM09 is not null) THEN
                    'PPO_UDFNUM09 contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_UDFNUM09) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM10), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM10 is not null) THEN
                    'PPO_UDFNUM10 contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_UDFNUM10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PPO_UPDATECOUNT is not null) THEN
                    'PPO_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PPO_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_LASTSAVED), '1900-01-01')) is null and 
                src:PPO_LASTSAVED is not null) THEN
                'PPO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PPO_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PPO_PK,
                src:PPO_REVISION  ORDER BY 
            src:PPO_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PPMOBJECTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_DEACTIVE), '1900-01-01')) is null and 
                    src:PPO_DEACTIVE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_DORMEND), '1900-01-01')) is null and 
                    src:PPO_DORMEND is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_DORMSTART), '1900-01-01')) is null and 
                    src:PPO_DORMSTART is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_DUE), '1900-01-01')) is null and 
                    src:PPO_DUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_FREQ), '0'), 38, 10) is null and 
                    src:PPO_FREQ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_FROMPOINT), '0'), 38, 10) is null and 
                    src:PPO_FROMPOINT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_LASTSAVED), '1900-01-01')) is null and 
                    src:PPO_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_LOCKEDBYSESSION), '0'), 38, 10) is null and 
                    src:PPO_LOCKEDBYSESSION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_METER), '0'), 38, 10) is null and 
                    src:PPO_METER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_METER2), '0'), 38, 10) is null and 
                    src:PPO_METER2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_METERDUE), '0'), 38, 10) is null and 
                    src:PPO_METERDUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_METERDUE2), '0'), 38, 10) is null and 
                    src:PPO_METERDUE2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_PERFORMONDAY), '0'), 38, 10) is null and 
                    src:PPO_PERFORMONDAY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_PK), '0'), 38, 10) is null and 
                    src:PPO_PK is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_REVISION), '0'), 38, 10) is null and 
                    src:PPO_REVISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_SETID), '0'), 38, 10) is null and 
                    src:PPO_SETID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_TOPOINT), '0'), 38, 10) is null and 
                    src:PPO_TOPOINT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE01), '1900-01-01')) is null and 
                    src:PPO_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE02), '1900-01-01')) is null and 
                    src:PPO_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE03), '1900-01-01')) is null and 
                    src:PPO_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE04), '1900-01-01')) is null and 
                    src:PPO_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE05), '1900-01-01')) is null and 
                    src:PPO_UDFDATE05 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE06), '1900-01-01')) is null and 
                    src:PPO_UDFDATE06 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE07), '1900-01-01')) is null and 
                    src:PPO_UDFDATE07 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE08), '1900-01-01')) is null and 
                    src:PPO_UDFDATE08 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE09), '1900-01-01')) is null and 
                    src:PPO_UDFDATE09 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_UDFDATE10), '1900-01-01')) is null and 
                    src:PPO_UDFDATE10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM01), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM02), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM03), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM04), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM05), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM06), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM06 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM07), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM07 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM08), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM08 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM09), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM09 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UDFNUM10), '0'), 38, 10) is null and 
                    src:PPO_UDFNUM10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPO_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PPO_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPO_LASTSAVED), '1900-01-01')) is null and 
                src:PPO_LASTSAVED is not null)