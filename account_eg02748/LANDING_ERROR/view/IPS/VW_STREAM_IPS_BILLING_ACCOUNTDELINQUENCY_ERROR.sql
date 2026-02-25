CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ACCOUNTDELINQUENCY_ERROR AS SELECT src, 'IPS_BILLING_ACCOUNTDELINQUENCY' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTDELINQUENCYKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTDELINQUENCYKEY is not null) THEN
                    'ACCOUNTDELINQUENCYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTDELINQUENCYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSLIENKEY), '0'), 38, 10) is null and 
                    src:ADDRESSLIENKEY is not null) THEN
                    'ADDRESSLIENKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSLIENKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) THEN
                    'BILLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) THEN
                    'BILLTYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLTYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COLLECTIONSKEY), '0'), 38, 10) is null and 
                    src:COLLECTIONSKEY is not null) THEN
                    'COLLECTIONSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COLLECTIONSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENCYENTRYAMOUNT), '0'), 38, 10) is null and 
                    src:DELINQUENCYENTRYAMOUNT is not null) THEN
                    'DELINQUENCYENTRYAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DELINQUENCYENTRYAMOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DELINQUENCYENTRYDATE), '1900-01-01')) is null and 
                    src:DELINQUENCYENTRYDATE is not null) THEN
                    'DELINQUENCYENTRYDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DELINQUENCYENTRYDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DELINQUENTTHROUGHDATE), '1900-01-01')) is null and 
                    src:DELINQUENTTHROUGHDATE is not null) THEN
                    'DELINQUENTTHROUGHDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DELINQUENTTHROUGHDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXTENSION), '0'), 38, 10) is null and 
                    src:EXTENSION is not null) THEN
                    'EXTENSION contains a non-numeric value : \'' || AS_VARCHAR(src:EXTENSION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MILESTONEDUEDATE), '1900-01-01')) is null and 
                    src:MILESTONEDUEDATE is not null) THEN
                    'MILESTONEDUEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:MILESTONEDUEDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MILESTONEENTRYDATE), '1900-01-01')) is null and 
                    src:MILESTONEENTRYDATE is not null) THEN
                    'MILESTONEENTRYDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:MILESTONEENTRYDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MILESTONEKEY), '0'), 38, 10) is null and 
                    src:MILESTONEKEY is not null) THEN
                    'MILESTONEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MILESTONEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEXTMILESTONEDATE), '1900-01-01')) is null and 
                    src:NEXTMILESTONEDATE is not null) THEN
                    'NEXTMILESTONEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:NEXTMILESTONEDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARCELLIENKEY), '0'), 38, 10) is null and 
                    src:PARCELLIENKEY is not null) THEN
                    'PARCELLIENKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PARCELLIENKEY) || '\'' WHEN 
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
                                    
                src:ACCOUNTDELINQUENCYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ACCOUNTDELINQUENCY as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTDELINQUENCYKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTDELINQUENCYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSLIENKEY), '0'), 38, 10) is null and 
                    src:ADDRESSLIENKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COLLECTIONSKEY), '0'), 38, 10) is null and 
                    src:COLLECTIONSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENCYENTRYAMOUNT), '0'), 38, 10) is null and 
                    src:DELINQUENCYENTRYAMOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DELINQUENCYENTRYDATE), '1900-01-01')) is null and 
                    src:DELINQUENCYENTRYDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DELINQUENTTHROUGHDATE), '1900-01-01')) is null and 
                    src:DELINQUENTTHROUGHDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXTENSION), '0'), 38, 10) is null and 
                    src:EXTENSION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MILESTONEDUEDATE), '1900-01-01')) is null and 
                    src:MILESTONEDUEDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MILESTONEENTRYDATE), '1900-01-01')) is null and 
                    src:MILESTONEENTRYDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MILESTONEKEY), '0'), 38, 10) is null and 
                    src:MILESTONEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEXTMILESTONEDATE), '1900-01-01')) is null and 
                    src:NEXTMILESTONEDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARCELLIENKEY), '0'), 38, 10) is null and 
                    src:PARCELLIENKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)