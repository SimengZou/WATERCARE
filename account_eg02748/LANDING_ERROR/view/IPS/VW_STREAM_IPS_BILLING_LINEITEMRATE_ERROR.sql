CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_LINEITEMRATE_ERROR AS SELECT src, 'IPS_BILLING_LINEITEMRATE' as TABLE_OBJECT, CASE WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNKEY), '0'), 38, 10) is null and 
                    src:BILLRUNKEY is not null) THEN
                    'BILLRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCULATIONDETAILKEY), '0'), 38, 10) is null and 
                    src:CALCULATIONDETAILKEY is not null) THEN
                    'CALCULATIONDETAILKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CALCULATIONDETAILKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITCHARGEKEY), '0'), 38, 10) is null and 
                    src:DEPOSITCHARGEKEY is not null) THEN
                    'DEPOSITCHARGEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DEPOSITCHARGEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FIXEDCHGCHARGEDAYS), '0'), 38, 10) is null and 
                    src:FIXEDCHGCHARGEDAYS is not null) THEN
                    'FIXEDCHGCHARGEDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:FIXEDCHGCHARGEDAYS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FIXEDCHGFROMDATE), '1900-01-01')) is null and 
                    src:FIXEDCHGFROMDATE is not null) THEN
                    'FIXEDCHGFROMDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:FIXEDCHGFROMDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FIXEDCHGNUMBERPERIODS), '0'), 38, 10) is null and 
                    src:FIXEDCHGNUMBERPERIODS is not null) THEN
                    'FIXEDCHGNUMBERPERIODS contains a non-numeric value : \'' || AS_VARCHAR(src:FIXEDCHGNUMBERPERIODS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FIXEDCHGPERIODDAYS), '0'), 38, 10) is null and 
                    src:FIXEDCHGPERIODDAYS is not null) THEN
                    'FIXEDCHGPERIODDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:FIXEDCHGPERIODDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FIXEDCHGPERIODSINYEAR), '0'), 38, 10) is null and 
                    src:FIXEDCHGPERIODSINYEAR is not null) THEN
                    'FIXEDCHGPERIODSINYEAR contains a non-numeric value : \'' || AS_VARCHAR(src:FIXEDCHGPERIODSINYEAR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FIXEDCHGRATEPERPERIOD), '0'), 38, 10) is null and 
                    src:FIXEDCHGRATEPERPERIOD is not null) THEN
                    'FIXEDCHGRATEPERPERIOD contains a non-numeric value : \'' || AS_VARCHAR(src:FIXEDCHGRATEPERPERIOD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FIXEDCHGTODATE), '1900-01-01')) is null and 
                    src:FIXEDCHGTODATE is not null) THEN
                    'FIXEDCHGTODATE contains a non-timestamp value : \'' || AS_VARCHAR(src:FIXEDCHGTODATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) THEN
                    'LINEITEMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMRATEKEY), '0'), 38, 10) is null and 
                    src:LINEITEMRATEKEY is not null) THEN
                    'LINEITEMRATEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMRATEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ONEOFFCHARGEKEY), '0'), 38, 10) is null and 
                    src:ONEOFFCHARGEKEY is not null) THEN
                    'ONEOFFCHARGEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ONEOFFCHARGEKEY) || '\'' WHEN 
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
                                    
                src:LINEITEMRATEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_LINEITEMRATE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTSERVICEKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTSERVICEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSSERVICEKEY), '0'), 38, 10) is null and 
                    src:ADDRESSSERVICEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNKEY), '0'), 38, 10) is null and 
                    src:BILLRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCULATIONDETAILKEY), '0'), 38, 10) is null and 
                    src:CALCULATIONDETAILKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITCHARGEKEY), '0'), 38, 10) is null and 
                    src:DEPOSITCHARGEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FIXEDCHGCHARGEDAYS), '0'), 38, 10) is null and 
                    src:FIXEDCHGCHARGEDAYS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FIXEDCHGFROMDATE), '1900-01-01')) is null and 
                    src:FIXEDCHGFROMDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FIXEDCHGNUMBERPERIODS), '0'), 38, 10) is null and 
                    src:FIXEDCHGNUMBERPERIODS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FIXEDCHGPERIODDAYS), '0'), 38, 10) is null and 
                    src:FIXEDCHGPERIODDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FIXEDCHGPERIODSINYEAR), '0'), 38, 10) is null and 
                    src:FIXEDCHGPERIODSINYEAR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FIXEDCHGRATEPERPERIOD), '0'), 38, 10) is null and 
                    src:FIXEDCHGRATEPERPERIOD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FIXEDCHGTODATE), '1900-01-01')) is null and 
                    src:FIXEDCHGTODATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMRATEKEY), '0'), 38, 10) is null and 
                    src:LINEITEMRATEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ONEOFFCHARGEKEY), '0'), 38, 10) is null and 
                    src:ONEOFFCHARGEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)