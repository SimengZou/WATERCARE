CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5WSPROMPTFIELDS_ERROR AS SELECT src, 'EAM_R5WSPROMPTFIELDS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_BRANCHGOTO), '0'), 38, 10) is null and 
                    src:WSF_BRANCHGOTO is not null) THEN
                    'WSF_BRANCHGOTO contains a non-numeric value : \'' || AS_VARCHAR(src:WSF_BRANCHGOTO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WSF_LASTSAVED), '1900-01-01')) is null and 
                    src:WSF_LASTSAVED is not null) THEN
                    'WSF_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:WSF_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_LINE), '0'), 38, 10) is null and 
                    src:WSF_LINE is not null) THEN
                    'WSF_LINE contains a non-numeric value : \'' || AS_VARCHAR(src:WSF_LINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_MAXLENGTH), '0'), 38, 10) is null and 
                    src:WSF_MAXLENGTH is not null) THEN
                    'WSF_MAXLENGTH contains a non-numeric value : \'' || AS_VARCHAR(src:WSF_MAXLENGTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_MINLENGTH), '0'), 38, 10) is null and 
                    src:WSF_MINLENGTH is not null) THEN
                    'WSF_MINLENGTH contains a non-numeric value : \'' || AS_VARCHAR(src:WSF_MINLENGTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_NEXTLINE), '0'), 38, 10) is null and 
                    src:WSF_NEXTLINE is not null) THEN
                    'WSF_NEXTLINE contains a non-numeric value : \'' || AS_VARCHAR(src:WSF_NEXTLINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_PRECISION), '0'), 38, 10) is null and 
                    src:WSF_PRECISION is not null) THEN
                    'WSF_PRECISION contains a non-numeric value : \'' || AS_VARCHAR(src:WSF_PRECISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_PROCESSGROUP), '0'), 38, 10) is null and 
                    src:WSF_PROCESSGROUP is not null) THEN
                    'WSF_PROCESSGROUP contains a non-numeric value : \'' || AS_VARCHAR(src:WSF_PROCESSGROUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_RETRIEVEFROMGROUP), '0'), 38, 10) is null and 
                    src:WSF_RETRIEVEFROMGROUP is not null) THEN
                    'WSF_RETRIEVEFROMGROUP contains a non-numeric value : \'' || AS_VARCHAR(src:WSF_RETRIEVEFROMGROUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_SCALE), '0'), 38, 10) is null and 
                    src:WSF_SCALE is not null) THEN
                    'WSF_SCALE contains a non-numeric value : \'' || AS_VARCHAR(src:WSF_SCALE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:WSF_UPDATECOUNT is not null) THEN
                    'WSF_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:WSF_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WSF_UPDATED), '1900-01-01')) is null and 
                    src:WSF_UPDATED is not null) THEN
                    'WSF_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:WSF_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WSF_LASTSAVED), '1900-01-01')) is null and 
                src:WSF_LASTSAVED is not null) THEN
                'WSF_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:WSF_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:WSF_LINE,
                src:WSF_WSPMTCODE  ORDER BY 
            src:WSF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5WSPROMPTFIELDS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_BRANCHGOTO), '0'), 38, 10) is null and 
                    src:WSF_BRANCHGOTO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WSF_LASTSAVED), '1900-01-01')) is null and 
                    src:WSF_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_LINE), '0'), 38, 10) is null and 
                    src:WSF_LINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_MAXLENGTH), '0'), 38, 10) is null and 
                    src:WSF_MAXLENGTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_MINLENGTH), '0'), 38, 10) is null and 
                    src:WSF_MINLENGTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_NEXTLINE), '0'), 38, 10) is null and 
                    src:WSF_NEXTLINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_PRECISION), '0'), 38, 10) is null and 
                    src:WSF_PRECISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_PROCESSGROUP), '0'), 38, 10) is null and 
                    src:WSF_PROCESSGROUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_RETRIEVEFROMGROUP), '0'), 38, 10) is null and 
                    src:WSF_RETRIEVEFROMGROUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_SCALE), '0'), 38, 10) is null and 
                    src:WSF_SCALE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WSF_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:WSF_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WSF_UPDATED), '1900-01-01')) is null and 
                    src:WSF_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WSF_LASTSAVED), '1900-01-01')) is null and 
                src:WSF_LASTSAVED is not null)