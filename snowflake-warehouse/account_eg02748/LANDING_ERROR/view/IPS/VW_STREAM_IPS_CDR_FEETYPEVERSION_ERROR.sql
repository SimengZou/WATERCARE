CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_FEETYPEVERSION_ERROR AS SELECT src, 'IPS_CDR_FEETYPEVERSION' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEETYPEKEY), '0'), 38, 10) is null and 
                    src:FEETYPEKEY is not null) THEN
                    'FEETYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:FEETYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEETYPEVERSIONKEY), '0'), 38, 10) is null and 
                    src:FEETYPEVERSIONKEY is not null) THEN
                    'FEETYPEVERSIONKEY contains a non-numeric value : \'' || AS_VARCHAR(src:FEETYPEVERSIONKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXFEEAMT), '0'), 38, 10) is null and 
                    src:MAXFEEAMT is not null) THEN
                    'MAXFEEAMT contains a non-numeric value : \'' || AS_VARCHAR(src:MAXFEEAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINBALANCE), '0'), 38, 10) is null and 
                    src:MINBALANCE is not null) THEN
                    'MINBALANCE contains a non-numeric value : \'' || AS_VARCHAR(src:MINBALANCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINBALANCECALCTYPE), '0'), 38, 10) is null and 
                    src:MINBALANCECALCTYPE is not null) THEN
                    'MINBALANCECALCTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:MINBALANCECALCTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINBALANCEFRMLA), '0'), 38, 10) is null and 
                    src:MINBALANCEFRMLA is not null) THEN
                    'MINBALANCEFRMLA contains a non-numeric value : \'' || AS_VARCHAR(src:MINBALANCEFRMLA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINFEEAMT), '0'), 38, 10) is null and 
                    src:MINFEEAMT is not null) THEN
                    'MINFEEAMT contains a non-numeric value : \'' || AS_VARCHAR(src:MINFEEAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QTY), '0'), 38, 10) is null and 
                    src:QTY is not null) THEN
                    'QTY contains a non-numeric value : \'' || AS_VARCHAR(src:QTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QTYFRMLA), '0'), 38, 10) is null and 
                    src:QTYFRMLA is not null) THEN
                    'QTYFRMLA contains a non-numeric value : \'' || AS_VARCHAR(src:QTYFRMLA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITYCALCTYPE), '0'), 38, 10) is null and 
                    src:QUANTITYCALCTYPE is not null) THEN
                    'QUANTITYCALCTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:QUANTITYCALCTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE), '0'), 38, 10) is null and 
                    src:VALUE is not null) THEN
                    'VALUE contains a non-numeric value : \'' || AS_VARCHAR(src:VALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUECALCTYPE), '0'), 38, 10) is null and 
                    src:VALUECALCTYPE is not null) THEN
                    'VALUECALCTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:VALUECALCTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUEFRMLA), '0'), 38, 10) is null and 
                    src:VALUEFRMLA is not null) THEN
                    'VALUEFRMLA contains a non-numeric value : \'' || AS_VARCHAR(src:VALUEFRMLA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUERATECODEKEY), '0'), 38, 10) is null and 
                    src:VALUERATECODEKEY is not null) THEN
                    'VALUERATECODEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:VALUERATECODEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUERATETBLFRMLAKEY), '0'), 38, 10) is null and 
                    src:VALUERATETBLFRMLAKEY is not null) THEN
                    'VALUERATETBLFRMLAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:VALUERATETBLFRMLAKEY) || '\'' WHEN 
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
                                    
                src:FEETYPEVERSIONKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_FEETYPEVERSION as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEETYPEKEY), '0'), 38, 10) is null and 
                    src:FEETYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEETYPEVERSIONKEY), '0'), 38, 10) is null and 
                    src:FEETYPEVERSIONKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXFEEAMT), '0'), 38, 10) is null and 
                    src:MAXFEEAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINBALANCE), '0'), 38, 10) is null and 
                    src:MINBALANCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINBALANCECALCTYPE), '0'), 38, 10) is null and 
                    src:MINBALANCECALCTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINBALANCEFRMLA), '0'), 38, 10) is null and 
                    src:MINBALANCEFRMLA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINFEEAMT), '0'), 38, 10) is null and 
                    src:MINFEEAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QTY), '0'), 38, 10) is null and 
                    src:QTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QTYFRMLA), '0'), 38, 10) is null and 
                    src:QTYFRMLA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITYCALCTYPE), '0'), 38, 10) is null and 
                    src:QUANTITYCALCTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE), '0'), 38, 10) is null and 
                    src:VALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUECALCTYPE), '0'), 38, 10) is null and 
                    src:VALUECALCTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUEFRMLA), '0'), 38, 10) is null and 
                    src:VALUEFRMLA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUERATECODEKEY), '0'), 38, 10) is null and 
                    src:VALUERATECODEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUERATETBLFRMLAKEY), '0'), 38, 10) is null and 
                    src:VALUERATETBLFRMLAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)