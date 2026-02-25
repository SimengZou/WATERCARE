CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_RESOURCES_JOURNAL_ERROR AS SELECT src, 'IPS_RESOURCES_JOURNAL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTINGPERIOD), '0'), 38, 10) is null and 
                    src:ACCOUNTINGPERIOD is not null) THEN
                    'ACCOUNTINGPERIOD contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTINGPERIOD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMOUNT), '0'), 38, 10) is null and 
                    src:AMOUNT is not null) THEN
                    'AMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:AMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CREDITBUDGETNUMBERKEY), '0'), 38, 10) is null and 
                    src:CREDITBUDGETNUMBERKEY is not null) THEN
                    'CREDITBUDGETNUMBERKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CREDITBUDGETNUMBERKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEBITBUDGETNUMBERKEY), '0'), 38, 10) is null and 
                    src:DEBITBUDGETNUMBERKEY is not null) THEN
                    'DEBITBUDGETNUMBERKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DEBITBUDGETNUMBERKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FISCALYEAR), '0'), 38, 10) is null and 
                    src:FISCALYEAR is not null) THEN
                    'FISCALYEAR contains a non-numeric value : \'' || AS_VARCHAR(src:FISCALYEAR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:JOURNALKEY), '0'), 38, 10) is null and 
                    src:JOURNALKEY is not null) THEN
                    'JOURNALKEY contains a non-numeric value : \'' || AS_VARCHAR(src:JOURNALKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:JOURNALRUNKEY), '0'), 38, 10) is null and 
                    src:JOURNALRUNKEY is not null) THEN
                    'JOURNALRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:JOURNALRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:JOURNALSUMMARYKEY), '0'), 38, 10) is null and 
                    src:JOURNALSUMMARYKEY is not null) THEN
                    'JOURNALSUMMARYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:JOURNALSUMMARYKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRANSACTIONDTTM), '1900-01-01')) is null and 
                    src:TRANSACTIONDTTM is not null) THEN
                    'TRANSACTIONDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:TRANSACTIONDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANSACTIONNUMBER), '0'), 38, 10) is null and 
                    src:TRANSACTIONNUMBER is not null) THEN
                    'TRANSACTIONNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:TRANSACTIONNUMBER) || '\'' WHEN 
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
                                    
                src:JOURNALKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_JOURNAL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTINGPERIOD), '0'), 38, 10) is null and 
                    src:ACCOUNTINGPERIOD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMOUNT), '0'), 38, 10) is null and 
                    src:AMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CREDITBUDGETNUMBERKEY), '0'), 38, 10) is null and 
                    src:CREDITBUDGETNUMBERKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEBITBUDGETNUMBERKEY), '0'), 38, 10) is null and 
                    src:DEBITBUDGETNUMBERKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FISCALYEAR), '0'), 38, 10) is null and 
                    src:FISCALYEAR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:JOURNALKEY), '0'), 38, 10) is null and 
                    src:JOURNALKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:JOURNALRUNKEY), '0'), 38, 10) is null and 
                    src:JOURNALRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:JOURNALSUMMARYKEY), '0'), 38, 10) is null and 
                    src:JOURNALSUMMARYKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRANSACTIONDTTM), '1900-01-01')) is null and 
                    src:TRANSACTIONDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANSACTIONNUMBER), '0'), 38, 10) is null and 
                    src:TRANSACTIONNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)