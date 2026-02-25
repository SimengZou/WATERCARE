CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLCDRPROJ_XPROJCOMPQUOTEGD_ERROR AS SELECT src, 'IPS_WSLCDRPROJ_XPROJCOMPQUOTEGD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITY), '0'), 38, 10) is null and 
                    src:QUANTITY is not null) THEN
                    'QUANTITY contains a non-numeric value : \'' || AS_VARCHAR(src:QUANTITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTAL), '0'), 38, 10) is null and 
                    src:TOTAL is not null) THEN
                    'TOTAL contains a non-numeric value : \'' || AS_VARCHAR(src:TOTAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UNITCHARGE), '0'), 38, 10) is null and 
                    src:UNITCHARGE is not null) THEN
                    'UNITCHARGE contains a non-numeric value : \'' || AS_VARCHAR(src:UNITCHARGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCDRPROJQUOTEACCEPTDPKEY), '0'), 38, 10) is null and 
                    src:XCDRPROJQUOTEACCEPTDPKEY is not null) THEN
                    'XCDRPROJQUOTEACCEPTDPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XCDRPROJQUOTEACCEPTDPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XPROJCOMPQUOTEGDKEY), '0'), 38, 10) is null and 
                    src:XPROJCOMPQUOTEGDKEY is not null) THEN
                    'XPROJCOMPQUOTEGDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XPROJCOMPQUOTEGDKEY) || '\'' WHEN 
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
                                    
                src:XPROJCOMPQUOTEGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRPROJ_XPROJCOMPQUOTEGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITY), '0'), 38, 10) is null and 
                    src:QUANTITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTAL), '0'), 38, 10) is null and 
                    src:TOTAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UNITCHARGE), '0'), 38, 10) is null and 
                    src:UNITCHARGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCDRPROJQUOTEACCEPTDPKEY), '0'), 38, 10) is null and 
                    src:XCDRPROJQUOTEACCEPTDPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XPROJCOMPQUOTEGDKEY), '0'), 38, 10) is null and 
                    src:XPROJCOMPQUOTEGDKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)