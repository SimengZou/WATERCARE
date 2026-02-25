CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLCDRBUILD_XBUILDAPPESTGD_ERROR AS SELECT src, 'IPS_WSLCDRBUILD_XBUILDAPPESTGD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEEAMOUNT), '0'), 38, 10) is null and 
                    src:FEEAMOUNT is not null) THEN
                    'FEEAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:FEEAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEEQUANTITY), '0'), 38, 10) is null and 
                    src:FEEQUANTITY is not null) THEN
                    'FEEQUANTITY contains a non-numeric value : \'' || AS_VARCHAR(src:FEEQUANTITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEEVALUE), '0'), 38, 10) is null and 
                    src:FEEVALUE is not null) THEN
                    'FEEVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:FEEVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPNO), '0'), 38, 10) is null and 
                    src:INSPNO is not null) THEN
                    'INSPNO contains a non-numeric value : \'' || AS_VARCHAR(src:INSPNO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XBUILDAPPESTDPKEY), '0'), 38, 10) is null and 
                    src:XBUILDAPPESTDPKEY is not null) THEN
                    'XBUILDAPPESTDPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XBUILDAPPESTDPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XBUILDAPPESTGDKEY), '0'), 38, 10) is null and 
                    src:XBUILDAPPESTGDKEY is not null) THEN
                    'XBUILDAPPESTGDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XBUILDAPPESTGDKEY) || '\'' WHEN 
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
                                    
                src:XBUILDAPPESTGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRBUILD_XBUILDAPPESTGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEEAMOUNT), '0'), 38, 10) is null and 
                    src:FEEAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEEQUANTITY), '0'), 38, 10) is null and 
                    src:FEEQUANTITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEEVALUE), '0'), 38, 10) is null and 
                    src:FEEVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPNO), '0'), 38, 10) is null and 
                    src:INSPNO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XBUILDAPPESTDPKEY), '0'), 38, 10) is null and 
                    src:XBUILDAPPESTDPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XBUILDAPPESTGDKEY), '0'), 38, 10) is null and 
                    src:XBUILDAPPESTGDKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)