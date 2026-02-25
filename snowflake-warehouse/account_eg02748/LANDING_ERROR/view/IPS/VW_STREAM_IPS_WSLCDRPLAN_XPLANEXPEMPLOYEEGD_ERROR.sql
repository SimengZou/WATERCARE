CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLCDRPLAN_XPLANEXPEMPLOYEEGD_ERROR AS SELECT src, 'IPS_WSLCDRPLAN_XPLANEXPEMPLOYEEGD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANKEY), '0'), 38, 10) is null and 
                    src:APPLANKEY is not null) THEN
                    'APPLANKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FROMDTTM), '1900-01-01')) is null and 
                    src:FROMDTTM is not null) THEN
                    'FROMDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:FROMDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TODTTM), '1900-01-01')) is null and 
                    src:TODTTM is not null) THEN
                    'TODTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:TODTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALHRS), '0'), 38, 10) is null and 
                    src:TOTALHRS is not null) THEN
                    'TOTALHRS contains a non-numeric value : \'' || AS_VARCHAR(src:TOTALHRS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XPLANEXPEMPLOYEEDPKEY), '0'), 38, 10) is null and 
                    src:XPLANEXPEMPLOYEEDPKEY is not null) THEN
                    'XPLANEXPEMPLOYEEDPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XPLANEXPEMPLOYEEDPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XPLANEXPEMPLOYEEGDKEY), '0'), 38, 10) is null and 
                    src:XPLANEXPEMPLOYEEGDKEY is not null) THEN
                    'XPLANEXPEMPLOYEEGDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XPLANEXPEMPLOYEEGDKEY) || '\'' WHEN 
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
                                    
                src:XPLANEXPEMPLOYEEGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRPLAN_XPLANEXPEMPLOYEEGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANKEY), '0'), 38, 10) is null and 
                    src:APPLANKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FROMDTTM), '1900-01-01')) is null and 
                    src:FROMDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TODTTM), '1900-01-01')) is null and 
                    src:TODTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALHRS), '0'), 38, 10) is null and 
                    src:TOTALHRS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XPLANEXPEMPLOYEEDPKEY), '0'), 38, 10) is null and 
                    src:XPLANEXPEMPLOYEEDPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XPLANEXPEMPLOYEEGDKEY), '0'), 38, 10) is null and 
                    src:XPLANEXPEMPLOYEEGDKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)