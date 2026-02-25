CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_QUANTITYITEM_ERROR AS SELECT src, 'IPS_BILLING_QUANTITYITEM' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEFAULTQTY), '0'), 38, 10) is null and 
                    src:DEFAULTQTY is not null) THEN
                    'DEFAULTQTY contains a non-numeric value : \'' || AS_VARCHAR(src:DEFAULTQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAYORDER), '0'), 38, 10) is null and 
                    src:DISPLAYORDER is not null) THEN
                    'DISPLAYORDER contains a non-numeric value : \'' || AS_VARCHAR(src:DISPLAYORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXQTY), '0'), 38, 10) is null and 
                    src:MAXQTY is not null) THEN
                    'MAXQTY contains a non-numeric value : \'' || AS_VARCHAR(src:MAXQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINQTY), '0'), 38, 10) is null and 
                    src:MINQTY is not null) THEN
                    'MINQTY contains a non-numeric value : \'' || AS_VARCHAR(src:MINQTY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITYFIELDKEY), '0'), 38, 10) is null and 
                    src:QUANTITYFIELDKEY is not null) THEN
                    'QUANTITYFIELDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:QUANTITYFIELDKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITYITEMKEY), '0'), 38, 10) is null and 
                    src:QUANTITYITEMKEY is not null) THEN
                    'QUANTITYITEMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:QUANTITYITEMKEY) || '\'' WHEN 
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
                                    
                src:QUANTITYITEMKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_QUANTITYITEM as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEFAULTQTY), '0'), 38, 10) is null and 
                    src:DEFAULTQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAYORDER), '0'), 38, 10) is null and 
                    src:DISPLAYORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXQTY), '0'), 38, 10) is null and 
                    src:MAXQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINQTY), '0'), 38, 10) is null and 
                    src:MINQTY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITYFIELDKEY), '0'), 38, 10) is null and 
                    src:QUANTITYFIELDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITYITEMKEY), '0'), 38, 10) is null and 
                    src:QUANTITYITEMKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)