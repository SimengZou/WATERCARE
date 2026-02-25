CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLCDRBUILD_XCDRBUILDQUOTEGD_ERROR AS SELECT src, 'IPS_WSLCDRBUILD_XCDRBUILDQUOTEGD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUOTEQUANTITY), '0'), 38, 10) is null and 
                    src:QUOTEQUANTITY is not null) THEN
                    'QUOTEQUANTITY contains a non-numeric value : \'' || AS_VARCHAR(src:QUOTEQUANTITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUOTERATE), '0'), 38, 10) is null and 
                    src:QUOTERATE is not null) THEN
                    'QUOTERATE contains a non-numeric value : \'' || AS_VARCHAR(src:QUOTERATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUOTETOTAL), '0'), 38, 10) is null and 
                    src:QUOTETOTAL is not null) THEN
                    'QUOTETOTAL contains a non-numeric value : \'' || AS_VARCHAR(src:QUOTETOTAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCDRBUILDQUOTEACCEPTDPKEY), '0'), 38, 10) is null and 
                    src:XCDRBUILDQUOTEACCEPTDPKEY is not null) THEN
                    'XCDRBUILDQUOTEACCEPTDPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XCDRBUILDQUOTEACCEPTDPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCDRBUILDQUOTEGDKEY), '0'), 38, 10) is null and 
                    src:XCDRBUILDQUOTEGDKEY is not null) THEN
                    'XCDRBUILDQUOTEGDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XCDRBUILDQUOTEGDKEY) || '\'' WHEN 
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
                                    
                src:XCDRBUILDQUOTEGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRBUILD_XCDRBUILDQUOTEGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUOTEQUANTITY), '0'), 38, 10) is null and 
                    src:QUOTEQUANTITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUOTERATE), '0'), 38, 10) is null and 
                    src:QUOTERATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUOTETOTAL), '0'), 38, 10) is null and 
                    src:QUOTETOTAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCDRBUILDQUOTEACCEPTDPKEY), '0'), 38, 10) is null and 
                    src:XCDRBUILDQUOTEACCEPTDPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCDRBUILDQUOTEGDKEY), '0'), 38, 10) is null and 
                    src:XCDRBUILDQUOTEGDKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)