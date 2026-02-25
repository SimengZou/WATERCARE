CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_APTRN_ERROR AS SELECT src, 'IPS_CDR_APTRN' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APFEEKEY), '0'), 38, 10) is null and 
                    src:APFEEKEY is not null) THEN
                    'APFEEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APFEEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APKEY), '0'), 38, 10) is null and 
                    src:APKEY is not null) THEN
                    'APKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPAYKEY), '0'), 38, 10) is null and 
                    src:APPAYKEY is not null) THEN
                    'APPAYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPAYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APRFNDTRNNO), '0'), 38, 10) is null and 
                    src:APRFNDTRNNO is not null) THEN
                    'APRFNDTRNNO contains a non-numeric value : \'' || AS_VARCHAR(src:APRFNDTRNNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APTRNNO), '0'), 38, 10) is null and 
                    src:APTRNNO is not null) THEN
                    'APTRNNO contains a non-numeric value : \'' || AS_VARCHAR(src:APTRNNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTNOKEY), '0'), 38, 10) is null and 
                    src:BGTNOKEY is not null) THEN
                    'BGTNOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BGTNOKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRPRODFAMILY), '0'), 38, 10) is null and 
                    src:CDRPRODFAMILY is not null) THEN
                    'CDRPRODFAMILY contains a non-numeric value : \'' || AS_VARCHAR(src:CDRPRODFAMILY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DSCFEEKEY), '0'), 38, 10) is null and 
                    src:DSCFEEKEY is not null) THEN
                    'DSCFEEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DSCFEEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFTRNNO), '0'), 38, 10) is null and 
                    src:REFTRNNO is not null) THEN
                    'REFTRNNO contains a non-numeric value : \'' || AS_VARCHAR(src:REFTRNNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRNAMT), '0'), 38, 10) is null and 
                    src:TRNAMT is not null) THEN
                    'TRNAMT contains a non-numeric value : \'' || AS_VARCHAR(src:TRNAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRNDTTM), '1900-01-01')) is null and 
                    src:TRNDTTM is not null) THEN
                    'TRNDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:TRNDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XFRAPKEY), '0'), 38, 10) is null and 
                    src:XFRAPKEY is not null) THEN
                    'XFRAPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XFRAPKEY) || '\'' WHEN 
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
                                    
                src:APTRNNO  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_APTRN as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APFEEKEY), '0'), 38, 10) is null and 
                    src:APFEEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APKEY), '0'), 38, 10) is null and 
                    src:APKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPAYKEY), '0'), 38, 10) is null and 
                    src:APPAYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APRFNDTRNNO), '0'), 38, 10) is null and 
                    src:APRFNDTRNNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APTRNNO), '0'), 38, 10) is null and 
                    src:APTRNNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTNOKEY), '0'), 38, 10) is null and 
                    src:BGTNOKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRPRODFAMILY), '0'), 38, 10) is null and 
                    src:CDRPRODFAMILY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DSCFEEKEY), '0'), 38, 10) is null and 
                    src:DSCFEEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFTRNNO), '0'), 38, 10) is null and 
                    src:REFTRNNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRNAMT), '0'), 38, 10) is null and 
                    src:TRNAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRNDTTM), '1900-01-01')) is null and 
                    src:TRNDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XFRAPKEY), '0'), 38, 10) is null and 
                    src:XFRAPKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)