CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_APFEE_ERROR AS SELECT src, 'IPS_CDR_APFEE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMT), '0'), 38, 10) is null and 
                    src:AMT is not null) THEN
                    'AMT contains a non-numeric value : \'' || AS_VARCHAR(src:AMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APFEEKEY), '0'), 38, 10) is null and 
                    src:APFEEKEY is not null) THEN
                    'APFEEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APFEEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APFEETYPEKEY), '0'), 38, 10) is null and 
                    src:APFEETYPEKEY is not null) THEN
                    'APFEETYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APFEETYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APKEY), '0'), 38, 10) is null and 
                    src:APKEY is not null) THEN
                    'APKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APPLDDTTM), '1900-01-01')) is null and 
                    src:APPLDDTTM is not null) THEN
                    'APPLDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:APPLDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTNOKEY), '0'), 38, 10) is null and 
                    src:BGTNOKEY is not null) THEN
                    'BGTNOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BGTNOKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLACCTKEY), '0'), 38, 10) is null and 
                    src:BILLACCTKEY is not null) THEN
                    'BILLACCTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLACCTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLNO), '0'), 38, 10) is null and 
                    src:BILLNO is not null) THEN
                    'BILLNO contains a non-numeric value : \'' || AS_VARCHAR(src:BILLNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRPRODFAMILY), '0'), 38, 10) is null and 
                    src:CDRPRODFAMILY is not null) THEN
                    'CDRPRODFAMILY contains a non-numeric value : \'' || AS_VARCHAR(src:CDRPRODFAMILY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEETYPEVERSIONKEY), '0'), 38, 10) is null and 
                    src:FEETYPEVERSIONKEY is not null) THEN
                    'FEETYPEVERSIONKEY contains a non-numeric value : \'' || AS_VARCHAR(src:FEETYPEVERSIONKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INITDEPFEEKEY), '0'), 38, 10) is null and 
                    src:INITDEPFEEKEY is not null) THEN
                    'INITDEPFEEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:INITDEPFEEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAIDDTTM), '1900-01-01')) is null and 
                    src:PAIDDTTM is not null) THEN
                    'PAIDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:PAIDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYORD), '0'), 38, 10) is null and 
                    src:PAYORD is not null) THEN
                    'PAYORD contains a non-numeric value : \'' || AS_VARCHAR(src:PAYORD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENRUNKEY), '0'), 38, 10) is null and 
                    src:PENRUNKEY is not null) THEN
                    'PENRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PENRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PNAPFEEKEY), '0'), 38, 10) is null and 
                    src:PNAPFEEKEY is not null) THEN
                    'PNAPFEEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PNAPFEEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PNRVRUNKEY), '0'), 38, 10) is null and 
                    src:PNRVRUNKEY is not null) THEN
                    'PNRVRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PNRVRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QTY), '0'), 38, 10) is null and 
                    src:QTY is not null) THEN
                    'QTY contains a non-numeric value : \'' || AS_VARCHAR(src:QTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRCBGTNOKEY), '0'), 38, 10) is null and 
                    src:SRCBGTNOKEY is not null) THEN
                    'SRCBGTNOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SRCBGTNOKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VAL), '0'), 38, 10) is null and 
                    src:VAL is not null) THEN
                    'VAL contains a non-numeric value : \'' || AS_VARCHAR(src:VAL) || '\'' WHEN 
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
                                    
                src:APFEEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_APFEE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMT), '0'), 38, 10) is null and 
                    src:AMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APFEEKEY), '0'), 38, 10) is null and 
                    src:APFEEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APFEETYPEKEY), '0'), 38, 10) is null and 
                    src:APFEETYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APKEY), '0'), 38, 10) is null and 
                    src:APKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APPLDDTTM), '1900-01-01')) is null and 
                    src:APPLDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTNOKEY), '0'), 38, 10) is null and 
                    src:BGTNOKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLACCTKEY), '0'), 38, 10) is null and 
                    src:BILLACCTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLNO), '0'), 38, 10) is null and 
                    src:BILLNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRPRODFAMILY), '0'), 38, 10) is null and 
                    src:CDRPRODFAMILY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEETYPEVERSIONKEY), '0'), 38, 10) is null and 
                    src:FEETYPEVERSIONKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INITDEPFEEKEY), '0'), 38, 10) is null and 
                    src:INITDEPFEEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAIDDTTM), '1900-01-01')) is null and 
                    src:PAIDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYORD), '0'), 38, 10) is null and 
                    src:PAYORD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENRUNKEY), '0'), 38, 10) is null and 
                    src:PENRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PNAPFEEKEY), '0'), 38, 10) is null and 
                    src:PNAPFEEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PNRVRUNKEY), '0'), 38, 10) is null and 
                    src:PNRVRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QTY), '0'), 38, 10) is null and 
                    src:QTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRCBGTNOKEY), '0'), 38, 10) is null and 
                    src:SRCBGTNOKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VAL), '0'), 38, 10) is null and 
                    src:VAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)