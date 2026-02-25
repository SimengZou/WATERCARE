CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BUDGETING_CATBGTLINK_ERROR AS SELECT src, 'IPS_BUDGETING_CATBGTLINK' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALLOCKEY), '0'), 38, 10) is null and 
                    src:ALLOCKEY is not null) THEN
                    'ALLOCKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ALLOCKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTDFNKEY), '0'), 38, 10) is null and 
                    src:BGTDFNKEY is not null) THEN
                    'BGTDFNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BGTDFNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTNO), '0'), 38, 10) is null and 
                    src:BGTNO is not null) THEN
                    'BGTNO contains a non-numeric value : \'' || AS_VARCHAR(src:BGTNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPNOKEY), '0'), 38, 10) is null and 
                    src:EXPNOKEY is not null) THEN
                    'EXPNOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:EXPNOKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRCALLCKEY), '0'), 38, 10) is null and 
                    src:SRCALLCKEY is not null) THEN
                    'SRCALLCKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SRCALLCKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null) THEN
                'VARIATION_ID contains a non-timestamp value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:EXPNOKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BUDGETING_CATBGTLINK as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALLOCKEY), '0'), 38, 10) is null and 
                    src:ALLOCKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTDFNKEY), '0'), 38, 10) is null and 
                    src:BGTDFNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTNO), '0'), 38, 10) is null and 
                    src:BGTNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPNOKEY), '0'), 38, 10) is null and 
                    src:EXPNOKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRCALLCKEY), '0'), 38, 10) is null and 
                    src:SRCALLCKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)