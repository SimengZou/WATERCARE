CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5DDDATASPY_ERROR AS SELECT src, 'EAM_R5DDDATASPY' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDS_BLACKLISTVIOLATIONS), '0'), 38, 10) is null and 
                    src:DDS_BLACKLISTVIOLATIONS is not null) THEN
                    'DDS_BLACKLISTVIOLATIONS contains a non-numeric value : \'' || AS_VARCHAR(src:DDS_BLACKLISTVIOLATIONS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDS_CLIENTROWS), '0'), 38, 10) is null and 
                    src:DDS_CLIENTROWS is not null) THEN
                    'DDS_CLIENTROWS contains a non-numeric value : \'' || AS_VARCHAR(src:DDS_CLIENTROWS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DDS_CREATEDSTAMP), '1900-01-01')) is null and 
                    src:DDS_CREATEDSTAMP is not null) THEN
                    'DDS_CREATEDSTAMP contains a non-timestamp value : \'' || AS_VARCHAR(src:DDS_CREATEDSTAMP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDS_DDSPYID), '0'), 38, 10) is null and 
                    src:DDS_DDSPYID is not null) THEN
                    'DDS_DDSPYID contains a non-numeric value : \'' || AS_VARCHAR(src:DDS_DDSPYID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDS_DISPLAYROW), '0'), 38, 10) is null and 
                    src:DDS_DISPLAYROW is not null) THEN
                    'DDS_DISPLAYROW contains a non-numeric value : \'' || AS_VARCHAR(src:DDS_DISPLAYROW) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDS_GRIDID), '0'), 38, 10) is null and 
                    src:DDS_GRIDID is not null) THEN
                    'DDS_GRIDID contains a non-numeric value : \'' || AS_VARCHAR(src:DDS_GRIDID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DDS_LASTSAVED), '1900-01-01')) is null and 
                    src:DDS_LASTSAVED is not null) THEN
                    'DDS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DDS_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDS_PORTLETROWS), '0'), 38, 10) is null and 
                    src:DDS_PORTLETROWS is not null) THEN
                    'DDS_PORTLETROWS contains a non-numeric value : \'' || AS_VARCHAR(src:DDS_PORTLETROWS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DDS_UPDATECOUNT is not null) THEN
                    'DDS_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DDS_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DDS_UPDATED), '1900-01-01')) is null and 
                    src:DDS_UPDATED is not null) THEN
                    'DDS_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:DDS_UPDATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DDS_UPDATESTAMP), '1900-01-01')) is null and 
                    src:DDS_UPDATESTAMP is not null) THEN
                    'DDS_UPDATESTAMP contains a non-timestamp value : \'' || AS_VARCHAR(src:DDS_UPDATESTAMP) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DDS_LASTSAVED), '1900-01-01')) is null and 
                src:DDS_LASTSAVED is not null) THEN
                'DDS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DDS_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:DDS_DDSPYID  ORDER BY 
            src:DDS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DDDATASPY as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDS_BLACKLISTVIOLATIONS), '0'), 38, 10) is null and 
                    src:DDS_BLACKLISTVIOLATIONS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDS_CLIENTROWS), '0'), 38, 10) is null and 
                    src:DDS_CLIENTROWS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DDS_CREATEDSTAMP), '1900-01-01')) is null and 
                    src:DDS_CREATEDSTAMP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDS_DDSPYID), '0'), 38, 10) is null and 
                    src:DDS_DDSPYID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDS_DISPLAYROW), '0'), 38, 10) is null and 
                    src:DDS_DISPLAYROW is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDS_GRIDID), '0'), 38, 10) is null and 
                    src:DDS_GRIDID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DDS_LASTSAVED), '1900-01-01')) is null and 
                    src:DDS_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDS_PORTLETROWS), '0'), 38, 10) is null and 
                    src:DDS_PORTLETROWS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DDS_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DDS_UPDATED), '1900-01-01')) is null and 
                    src:DDS_UPDATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DDS_UPDATESTAMP), '1900-01-01')) is null and 
                    src:DDS_UPDATESTAMP is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DDS_LASTSAVED), '1900-01-01')) is null and 
                src:DDS_LASTSAVED is not null)