CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5DUXLAYOUT_ERROR AS SELECT src, 'EAM_R5DUXLAYOUT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DXL_FIELDSIZE), '0'), 38, 10) is null and 
                    src:DXL_FIELDSIZE is not null) THEN
                    'DXL_FIELDSIZE contains a non-numeric value : \'' || AS_VARCHAR(src:DXL_FIELDSIZE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DXL_LASTSAVED), '1900-01-01')) is null and 
                    src:DXL_LASTSAVED is not null) THEN
                    'DXL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DXL_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DXL_POSITIONINGROUP), '0'), 38, 10) is null and 
                    src:DXL_POSITIONINGROUP is not null) THEN
                    'DXL_POSITIONINGROUP contains a non-numeric value : \'' || AS_VARCHAR(src:DXL_POSITIONINGROUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DXL_SECTIONPOSITION), '0'), 38, 10) is null and 
                    src:DXL_SECTIONPOSITION is not null) THEN
                    'DXL_SECTIONPOSITION contains a non-numeric value : \'' || AS_VARCHAR(src:DXL_SECTIONPOSITION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DXL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DXL_UPDATECOUNT is not null) THEN
                    'DXL_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DXL_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DXL_LASTSAVED), '1900-01-01')) is null and 
                src:DXL_LASTSAVED is not null) THEN
                'DXL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DXL_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:DXL_ELEMENTID,
                src:DXL_PAGENAME,
                src:DXL_USERGROUP  ORDER BY 
            src:DXL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DUXLAYOUT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DXL_FIELDSIZE), '0'), 38, 10) is null and 
                    src:DXL_FIELDSIZE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DXL_LASTSAVED), '1900-01-01')) is null and 
                    src:DXL_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DXL_POSITIONINGROUP), '0'), 38, 10) is null and 
                    src:DXL_POSITIONINGROUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DXL_SECTIONPOSITION), '0'), 38, 10) is null and 
                    src:DXL_SECTIONPOSITION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DXL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DXL_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DXL_LASTSAVED), '1900-01-01')) is null and 
                src:DXL_LASTSAVED is not null)