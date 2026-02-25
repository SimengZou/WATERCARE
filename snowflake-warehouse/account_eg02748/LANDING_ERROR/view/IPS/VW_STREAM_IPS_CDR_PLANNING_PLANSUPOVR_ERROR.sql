CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_PLANNING_PLANSUPOVR_ERROR AS SELECT src, 'IPS_CDR_PLANNING_PLANSUPOVR' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBILLACCTCHNGLOGKEY), '0'), 38, 10) is null and 
                    src:APBILLACCTCHNGLOGKEY is not null) THEN
                    'APBILLACCTCHNGLOGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBILLACCTCHNGLOGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APFEEKEY), '0'), 38, 10) is null and 
                    src:APFEEKEY is not null) THEN
                    'APFEEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APFEEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPAYKEY), '0'), 38, 10) is null and 
                    src:APPAYKEY is not null) THEN
                    'APPAYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPAYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANCONDKEY), '0'), 38, 10) is null and 
                    src:APPLANCONDKEY is not null) THEN
                    'APPLANCONDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANCONDKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANDEFNKEY), '0'), 38, 10) is null and 
                    src:APPLANDEFNKEY is not null) THEN
                    'APPLANDEFNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANDEFNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANKEY), '0'), 38, 10) is null and 
                    src:APPLANKEY is not null) THEN
                    'APPLANKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANPLCONDKEY), '0'), 38, 10) is null and 
                    src:APPLANPLCONDKEY is not null) THEN
                    'APPLANPLCONDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANPLCONDKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANSUPOVRKEY), '0'), 38, 10) is null and 
                    src:APPLANSUPOVRKEY is not null) THEN
                    'APPLANSUPOVRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANSUPOVRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APRFNDTRNNO), '0'), 38, 10) is null and 
                    src:APRFNDTRNNO is not null) THEN
                    'APRFNDTRNNO contains a non-numeric value : \'' || AS_VARCHAR(src:APRFNDTRNNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLPAYKEY), '0'), 38, 10) is null and 
                    src:BILLPAYKEY is not null) THEN
                    'BILLPAYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLPAYKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MSFROMKEY), '0'), 38, 10) is null and 
                    src:MSFROMKEY is not null) THEN
                    'MSFROMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MSFROMKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MSTOKEY), '0'), 38, 10) is null and 
                    src:MSTOKEY is not null) THEN
                    'MSTOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MSTOKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUPERVISOR), '0'), 38, 10) is null and 
                    src:SUPERVISOR is not null) THEN
                    'SUPERVISOR contains a non-numeric value : \'' || AS_VARCHAR(src:SUPERVISOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUPOVRTYPE), '0'), 38, 10) is null and 
                    src:SUPOVRTYPE is not null) THEN
                    'SUPOVRTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:SUPOVRTYPE) || '\'' WHEN 
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
                                    
                src:APPLANSUPOVRKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_PLANNING_PLANSUPOVR as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBILLACCTCHNGLOGKEY), '0'), 38, 10) is null and 
                    src:APBILLACCTCHNGLOGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APFEEKEY), '0'), 38, 10) is null and 
                    src:APFEEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPAYKEY), '0'), 38, 10) is null and 
                    src:APPAYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANCONDKEY), '0'), 38, 10) is null and 
                    src:APPLANCONDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANDEFNKEY), '0'), 38, 10) is null and 
                    src:APPLANDEFNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANKEY), '0'), 38, 10) is null and 
                    src:APPLANKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANPLCONDKEY), '0'), 38, 10) is null and 
                    src:APPLANPLCONDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANSUPOVRKEY), '0'), 38, 10) is null and 
                    src:APPLANSUPOVRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APRFNDTRNNO), '0'), 38, 10) is null and 
                    src:APRFNDTRNNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLPAYKEY), '0'), 38, 10) is null and 
                    src:BILLPAYKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MSFROMKEY), '0'), 38, 10) is null and 
                    src:MSFROMKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MSTOKEY), '0'), 38, 10) is null and 
                    src:MSTOKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUPERVISOR), '0'), 38, 10) is null and 
                    src:SUPERVISOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUPOVRTYPE), '0'), 38, 10) is null and 
                    src:SUPOVRTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)