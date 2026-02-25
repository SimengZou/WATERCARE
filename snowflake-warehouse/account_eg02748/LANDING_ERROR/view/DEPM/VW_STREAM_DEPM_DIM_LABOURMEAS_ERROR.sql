CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_DEPM_DIM_LABOURMEAS_ERROR AS SELECT src, 'DEPM_DIM_LABOURMEAS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:Timestamp), '1900-01-01')) is null and 
                    src:Timestamp is not null) THEN
                    'Timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:Timestamp) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:Timestamp), '1900-01-01')) is null and 
                src:Timestamp is not null) THEN
                'Timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:Timestamp) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,etl_load_datetime, etl_load_metadatafilename FROM (SELECT * FROM ( SELECT
            src,
            etl_load_datetime,
            etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
                                    
                src:Parent,
                src:ElementName  ORDER BY 
            src:Timestamp desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_DEPM_DIM_LABOURMEAS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:Timestamp), '1900-01-01')) is null and 
                    src:Timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:Timestamp), '1900-01-01')) is null and 
                src:Timestamp is not null)