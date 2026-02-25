CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLASSETWATER_XMETERACCOUNT_ERROR AS SELECT src, 'IPS_WSLASSETWATER_XMETERACCOUNT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:"ACCOUNT"), '0'), 38, 10) is null and 
                    src:"ACCOUNT" is not null) THEN
                    '"ACCOUNT" contains a non-numeric value : \'' || AS_VARCHAR(src:"ACCOUNT") || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCTSRVPOSKEY), '0'), 38, 10) is null and 
                    src:ACCTSRVPOSKEY is not null) THEN
                    'ACCTSRVPOSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCTSRVPOSKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SHAREPERC), '0'), 38, 10) is null and 
                    src:SHAREPERC is not null) THEN
                    'SHAREPERC contains a non-numeric value : \'' || AS_VARCHAR(src:SHAREPERC) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) THEN
                    'STARTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XMETERACCOUNTKEY), '0'), 38, 10) is null and 
                    src:XMETERACCOUNTKEY is not null) THEN
                    'XMETERACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XMETERACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XWATERMETERDETAILKEY), '0'), 38, 10) is null and 
                    src:XWATERMETERDETAILKEY is not null) THEN
                    'XWATERMETERDETAILKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XWATERMETERDETAILKEY) || '\'' WHEN 
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
                                    
                src:XMETERACCOUNTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLASSETWATER_XMETERACCOUNT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:"ACCOUNT"), '0'), 38, 10) is null and 
                    src:"ACCOUNT" is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCTSRVPOSKEY), '0'), 38, 10) is null and 
                    src:ACCTSRVPOSKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SHAREPERC), '0'), 38, 10) is null and 
                    src:SHAREPERC is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XMETERACCOUNTKEY), '0'), 38, 10) is null and 
                    src:XMETERACCOUNTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XWATERMETERDETAILKEY), '0'), 38, 10) is null and 
                    src:XWATERMETERDETAILKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)