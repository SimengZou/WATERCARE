CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ACCOUNTSERVICE_ERROR AS SELECT src, 'IPS_BILLING_ACCOUNTSERVICE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AACCALCULATIONDT), '1900-01-01')) is null and 
                    src:AACCALCULATIONDT is not null) THEN
                    'AACCALCULATIONDT contains a non-timestamp value : \'' || AS_VARCHAR(src:AACCALCULATIONDT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AACEXCEPTION), '0'), 38, 10) is null and 
                    src:AACEXCEPTION is not null) THEN
                    'AACEXCEPTION contains a non-numeric value : \'' || AS_VARCHAR(src:AACEXCEPTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AACMONTHLYAVERAGE), '0'), 38, 10) is null and 
                    src:AACMONTHLYAVERAGE is not null) THEN
                    'AACMONTHLYAVERAGE contains a non-numeric value : \'' || AS_VARCHAR(src:AACMONTHLYAVERAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTSERVICEKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTSERVICEKEY is not null) THEN
                    'ACCOUNTSERVICEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTSERVICEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSSERVICEKEY), '0'), 38, 10) is null and 
                    src:ADDRESSSERVICEKEY is not null) THEN
                    'ADDRESSSERVICEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSSERVICEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) THEN
                    'COMMENTSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMMENTSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONSUMPTIONPERCENTAGE), '0'), 38, 10) is null and 
                    src:CONSUMPTIONPERCENTAGE is not null) THEN
                    'CONSUMPTIONPERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:CONSUMPTIONPERCENTAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMPERVIOUSPERCENTAGE), '0'), 38, 10) is null and 
                    src:IMPERVIOUSPERCENTAGE is not null) THEN
                    'IMPERVIOUSPERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:IMPERVIOUSPERCENTAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEFIELD1VALUE), '0'), 38, 10) is null and 
                    src:SERVICEFIELD1VALUE is not null) THEN
                    'SERVICEFIELD1VALUE contains a non-numeric value : \'' || AS_VARCHAR(src:SERVICEFIELD1VALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEFIELD2VALUE), '0'), 38, 10) is null and 
                    src:SERVICEFIELD2VALUE is not null) THEN
                    'SERVICEFIELD2VALUE contains a non-numeric value : \'' || AS_VARCHAR(src:SERVICEFIELD2VALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEFIELD3VALUE), '0'), 38, 10) is null and 
                    src:SERVICEFIELD3VALUE is not null) THEN
                    'SERVICEFIELD3VALUE contains a non-numeric value : \'' || AS_VARCHAR(src:SERVICEFIELD3VALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEOPTIONSKEY), '0'), 38, 10) is null and 
                    src:SERVICEOPTIONSKEY is not null) THEN
                    'SERVICEOPTIONSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SERVICEOPTIONSKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) THEN
                    'STARTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STATUSDTTM), '1900-01-01')) is null and 
                    src:STATUSDTTM is not null) THEN
                    'STATUSDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STATUSDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STOPDTTM), '1900-01-01')) is null and 
                    src:STOPDTTM is not null) THEN
                    'STOPDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STOPDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUSPENDFROMDATE), '1900-01-01')) is null and 
                    src:SUSPENDFROMDATE is not null) THEN
                    'SUSPENDFROMDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:SUSPENDFROMDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUSPENDTODATE), '1900-01-01')) is null and 
                    src:SUSPENDTODATE is not null) THEN
                    'SUSPENDTODATE contains a non-timestamp value : \'' || AS_VARCHAR(src:SUSPENDTODATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WINAVGCALDATE), '1900-01-01')) is null and 
                    src:WINAVGCALDATE is not null) THEN
                    'WINAVGCALDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:WINAVGCALDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WINTERAVG), '0'), 38, 10) is null and 
                    src:WINTERAVG is not null) THEN
                    'WINTERAVG contains a non-numeric value : \'' || AS_VARCHAR(src:WINTERAVG) || '\'' WHEN 
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
                                    
                src:ACCOUNTSERVICEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ACCOUNTSERVICE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AACCALCULATIONDT), '1900-01-01')) is null and 
                    src:AACCALCULATIONDT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AACEXCEPTION), '0'), 38, 10) is null and 
                    src:AACEXCEPTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AACMONTHLYAVERAGE), '0'), 38, 10) is null and 
                    src:AACMONTHLYAVERAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTSERVICEKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTSERVICEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSSERVICEKEY), '0'), 38, 10) is null and 
                    src:ADDRESSSERVICEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONSUMPTIONPERCENTAGE), '0'), 38, 10) is null and 
                    src:CONSUMPTIONPERCENTAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMPERVIOUSPERCENTAGE), '0'), 38, 10) is null and 
                    src:IMPERVIOUSPERCENTAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEFIELD1VALUE), '0'), 38, 10) is null and 
                    src:SERVICEFIELD1VALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEFIELD2VALUE), '0'), 38, 10) is null and 
                    src:SERVICEFIELD2VALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEFIELD3VALUE), '0'), 38, 10) is null and 
                    src:SERVICEFIELD3VALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEOPTIONSKEY), '0'), 38, 10) is null and 
                    src:SERVICEOPTIONSKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STATUSDTTM), '1900-01-01')) is null and 
                    src:STATUSDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STOPDTTM), '1900-01-01')) is null and 
                    src:STOPDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUSPENDFROMDATE), '1900-01-01')) is null and 
                    src:SUSPENDFROMDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUSPENDTODATE), '1900-01-01')) is null and 
                    src:SUSPENDTODATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WINAVGCALDATE), '1900-01-01')) is null and 
                    src:WINAVGCALDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WINTERAVG), '0'), 38, 10) is null and 
                    src:WINTERAVG is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)