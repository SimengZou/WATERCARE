CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CASHIERING_DRAWERGENERATION_ERROR AS SELECT src, 'IPS_CASHIERING_DRAWERGENERATION' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASHINAMT), '0'), 38, 10) is null and 
                    src:CASHINAMT is not null) THEN
                    'CASHINAMT contains a non-numeric value : \'' || AS_VARCHAR(src:CASHINAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASHLIMITAMT), '0'), 38, 10) is null and 
                    src:CASHLIMITAMT is not null) THEN
                    'CASHLIMITAMT contains a non-numeric value : \'' || AS_VARCHAR(src:CASHLIMITAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEFAULTREGISTERGROUPKEY), '0'), 38, 10) is null and 
                    src:DEFAULTREGISTERGROUPKEY is not null) THEN
                    'DEFAULTREGISTERGROUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DEFAULTREGISTERGROUPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DRWRGENKEY), '0'), 38, 10) is null and 
                    src:DRWRGENKEY is not null) THEN
                    'DRWRGENKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DRWRGENKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBGTNO), '0'), 38, 10) is null and 
                    src:OBGTNO is not null) THEN
                    'OBGTNO contains a non-numeric value : \'' || AS_VARCHAR(src:OBGTNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGKEY), '0'), 38, 10) is null and 
                    src:REGKEY is not null) THEN
                    'REGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SBGTNO), '0'), 38, 10) is null and 
                    src:SBGTNO is not null) THEN
                    'SBGTNO contains a non-numeric value : \'' || AS_VARCHAR(src:SBGTNO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTTM), '1900-01-01')) is null and 
                    src:STARTTM is not null) THEN
                    'STARTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STOPTM), '1900-01-01')) is null and 
                    src:STOPTM is not null) THEN
                    'STOPTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STOPTM) || '\'' WHEN 
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
                                    
                src:DRWRGENKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CASHIERING_DRAWERGENERATION as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASHINAMT), '0'), 38, 10) is null and 
                    src:CASHINAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASHLIMITAMT), '0'), 38, 10) is null and 
                    src:CASHLIMITAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEFAULTREGISTERGROUPKEY), '0'), 38, 10) is null and 
                    src:DEFAULTREGISTERGROUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DRWRGENKEY), '0'), 38, 10) is null and 
                    src:DRWRGENKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBGTNO), '0'), 38, 10) is null and 
                    src:OBGTNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGKEY), '0'), 38, 10) is null and 
                    src:REGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SBGTNO), '0'), 38, 10) is null and 
                    src:SBGTNO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTTM), '1900-01-01')) is null and 
                    src:STARTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STOPTM), '1900-01-01')) is null and 
                    src:STOPTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)