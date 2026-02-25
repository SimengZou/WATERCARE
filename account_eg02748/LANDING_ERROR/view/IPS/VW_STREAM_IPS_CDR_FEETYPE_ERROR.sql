CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_FEETYPE_ERROR AS SELECT src, 'IPS_CDR_FEETYPE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ATCOSTUSEDFOR), '0'), 38, 10) is null and 
                    src:ATCOSTUSEDFOR is not null) THEN
                    'ATCOSTUSEDFOR contains a non-numeric value : \'' || AS_VARCHAR(src:ATCOSTUSEDFOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRPRODUCTFAMILY), '0'), 38, 10) is null and 
                    src:CDRPRODUCTFAMILY is not null) THEN
                    'CDRPRODUCTFAMILY contains a non-numeric value : \'' || AS_VARCHAR(src:CDRPRODUCTFAMILY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DESTBGTNOKEY), '0'), 38, 10) is null and 
                    src:DESTBGTNOKEY is not null) THEN
                    'DESTBGTNOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DESTBGTNOKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEECLASS), '0'), 38, 10) is null and 
                    src:FEECLASS is not null) THEN
                    'FEECLASS contains a non-numeric value : \'' || AS_VARCHAR(src:FEECLASS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEETYPEKEY), '0'), 38, 10) is null and 
                    src:FEETYPEKEY is not null) THEN
                    'FEETYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:FEETYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INITIALDEPFEETYPEKEY), '0'), 38, 10) is null and 
                    src:INITIALDEPFEETYPEKEY is not null) THEN
                    'INITIALDEPFEETYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:INITIALDEPFEETYPEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORDERING), '0'), 38, 10) is null and 
                    src:ORDERING is not null) THEN
                    'ORDERING contains a non-numeric value : \'' || AS_VARCHAR(src:ORDERING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTMETHODFLAG), '0'), 38, 10) is null and 
                    src:PAYMENTMETHODFLAG is not null) THEN
                    'PAYMENTMETHODFLAG contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTMETHODFLAG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYORD), '0'), 38, 10) is null and 
                    src:PAYORD is not null) THEN
                    'PAYORD contains a non-numeric value : \'' || AS_VARCHAR(src:PAYORD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRCBGTNOKEY), '0'), 38, 10) is null and 
                    src:SRCBGTNOKEY is not null) THEN
                    'SRCBGTNOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SRCBGTNOKEY) || '\'' WHEN 
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
                                    
                src:FEETYPEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_FEETYPE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ATCOSTUSEDFOR), '0'), 38, 10) is null and 
                    src:ATCOSTUSEDFOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRPRODUCTFAMILY), '0'), 38, 10) is null and 
                    src:CDRPRODUCTFAMILY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DESTBGTNOKEY), '0'), 38, 10) is null and 
                    src:DESTBGTNOKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEECLASS), '0'), 38, 10) is null and 
                    src:FEECLASS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEETYPEKEY), '0'), 38, 10) is null and 
                    src:FEETYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INITIALDEPFEETYPEKEY), '0'), 38, 10) is null and 
                    src:INITIALDEPFEETYPEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORDERING), '0'), 38, 10) is null and 
                    src:ORDERING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTMETHODFLAG), '0'), 38, 10) is null and 
                    src:PAYMENTMETHODFLAG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYORD), '0'), 38, 10) is null and 
                    src:PAYORD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRCBGTNOKEY), '0'), 38, 10) is null and 
                    src:SRCBGTNOKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)