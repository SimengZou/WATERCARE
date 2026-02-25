CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5DEFAULTPAGELAYOUT_ERROR AS SELECT src, 'EAM_R5DEFAULTPAGELAYOUT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLD_FIELDCONTTYPE), '0'), 38, 10) is null and 
                    src:PLD_FIELDCONTTYPE is not null) THEN
                    'PLD_FIELDCONTTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:PLD_FIELDCONTTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLD_FIELDGROUP), '0'), 38, 10) is null and 
                    src:PLD_FIELDGROUP is not null) THEN
                    'PLD_FIELDGROUP contains a non-numeric value : \'' || AS_VARCHAR(src:PLD_FIELDGROUP) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PLD_LASTSAVED), '1900-01-01')) is null and 
                    src:PLD_LASTSAVED is not null) THEN
                    'PLD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PLD_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLD_MAXLENGTH), '0'), 38, 10) is null and 
                    src:PLD_MAXLENGTH is not null) THEN
                    'PLD_MAXLENGTH contains a non-numeric value : \'' || AS_VARCHAR(src:PLD_MAXLENGTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLD_POSITIONINGROUP), '0'), 38, 10) is null and 
                    src:PLD_POSITIONINGROUP is not null) THEN
                    'PLD_POSITIONINGROUP contains a non-numeric value : \'' || AS_VARCHAR(src:PLD_POSITIONINGROUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLD_SIZE), '0'), 38, 10) is null and 
                    src:PLD_SIZE is not null) THEN
                    'PLD_SIZE contains a non-numeric value : \'' || AS_VARCHAR(src:PLD_SIZE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLD_TABINDEX), '0'), 38, 10) is null and 
                    src:PLD_TABINDEX is not null) THEN
                    'PLD_TABINDEX contains a non-numeric value : \'' || AS_VARCHAR(src:PLD_TABINDEX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLD_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PLD_UPDATECOUNT is not null) THEN
                    'PLD_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PLD_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PLD_LASTSAVED), '1900-01-01')) is null and 
                src:PLD_LASTSAVED is not null) THEN
                'PLD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PLD_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PLD_ELEMENTID,
                src:PLD_PAGENAME  ORDER BY 
            src:PLD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DEFAULTPAGELAYOUT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLD_FIELDCONTTYPE), '0'), 38, 10) is null and 
                    src:PLD_FIELDCONTTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLD_FIELDGROUP), '0'), 38, 10) is null and 
                    src:PLD_FIELDGROUP is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PLD_LASTSAVED), '1900-01-01')) is null and 
                    src:PLD_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLD_MAXLENGTH), '0'), 38, 10) is null and 
                    src:PLD_MAXLENGTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLD_POSITIONINGROUP), '0'), 38, 10) is null and 
                    src:PLD_POSITIONINGROUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLD_SIZE), '0'), 38, 10) is null and 
                    src:PLD_SIZE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLD_TABINDEX), '0'), 38, 10) is null and 
                    src:PLD_TABINDEX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLD_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PLD_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PLD_LASTSAVED), '1900-01-01')) is null and 
                src:PLD_LASTSAVED is not null)