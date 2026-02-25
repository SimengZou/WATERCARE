CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5GRID_ERROR AS SELECT src, 'EAM_R5GRID' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRD_GRIDID), '0'), 38, 10) is null and 
                    src:GRD_GRIDID is not null) THEN
                    'GRD_GRIDID contains a non-numeric value : \'' || AS_VARCHAR(src:GRD_GRIDID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRD_GRIDTYPE), '0'), 38, 10) is null and 
                    src:GRD_GRIDTYPE is not null) THEN
                    'GRD_GRIDTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:GRD_GRIDTYPE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GRD_LASTSAVED), '1900-01-01')) is null and 
                    src:GRD_LASTSAVED is not null) THEN
                    'GRD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:GRD_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRD_MAXGRIDCOST), '0'), 38, 10) is null and 
                    src:GRD_MAXGRIDCOST is not null) THEN
                    'GRD_MAXGRIDCOST contains a non-numeric value : \'' || AS_VARCHAR(src:GRD_MAXGRIDCOST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRD_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:GRD_UPDATECOUNT is not null) THEN
                    'GRD_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:GRD_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GRD_UPDATED), '1900-01-01')) is null and 
                    src:GRD_UPDATED is not null) THEN
                    'GRD_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:GRD_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GRD_LASTSAVED), '1900-01-01')) is null and 
                src:GRD_LASTSAVED is not null) THEN
                'GRD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:GRD_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:GRD_GRIDID  ORDER BY 
            src:GRD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5GRID as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRD_GRIDID), '0'), 38, 10) is null and 
                    src:GRD_GRIDID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRD_GRIDTYPE), '0'), 38, 10) is null and 
                    src:GRD_GRIDTYPE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GRD_LASTSAVED), '1900-01-01')) is null and 
                    src:GRD_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRD_MAXGRIDCOST), '0'), 38, 10) is null and 
                    src:GRD_MAXGRIDCOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRD_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:GRD_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GRD_UPDATED), '1900-01-01')) is null and 
                    src:GRD_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:GRD_LASTSAVED), '1900-01-01')) is null and 
                src:GRD_LASTSAVED is not null)