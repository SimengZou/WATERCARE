CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_BUILDING_APPDETAILTYPECONSTRAINT_ERROR AS SELECT src, 'IPS_CDR_BUILDING_APPDETAILTYPECONSTRAINT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPDETAILTYPECONSTRAINTKEY), '0'), 38, 10) is null and 
                    src:APPDETAILTYPECONSTRAINTKEY is not null) THEN
                    'APPDETAILTYPECONSTRAINTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPDETAILTYPECONSTRAINTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLICATIONDETAILTYPE), '0'), 38, 10) is null and 
                    src:APPLICATIONDETAILTYPE is not null) THEN
                    'APPLICATIONDETAILTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:APPLICATIONDETAILTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGAPPLPROCESSSTATE), '0'), 38, 10) is null and 
                    src:BLDGAPPLPROCESSSTATE is not null) THEN
                    'BLDGAPPLPROCESSSTATE contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGAPPLPROCESSSTATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PORTALCUSTOMERCONSTRAINT), '0'), 38, 10) is null and 
                    src:PORTALCUSTOMERCONSTRAINT is not null) THEN
                    'PORTALCUSTOMERCONSTRAINT contains a non-numeric value : \'' || AS_VARCHAR(src:PORTALCUSTOMERCONSTRAINT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PORTALPUBLICCONSTRAINT), '0'), 38, 10) is null and 
                    src:PORTALPUBLICCONSTRAINT is not null) THEN
                    'PORTALPUBLICCONSTRAINT contains a non-numeric value : \'' || AS_VARCHAR(src:PORTALPUBLICCONSTRAINT) || '\'' WHEN 
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
                                    
                src:APPDETAILTYPECONSTRAINTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_BUILDING_APPDETAILTYPECONSTRAINT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPDETAILTYPECONSTRAINTKEY), '0'), 38, 10) is null and 
                    src:APPDETAILTYPECONSTRAINTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLICATIONDETAILTYPE), '0'), 38, 10) is null and 
                    src:APPLICATIONDETAILTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGAPPLPROCESSSTATE), '0'), 38, 10) is null and 
                    src:BLDGAPPLPROCESSSTATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PORTALCUSTOMERCONSTRAINT), '0'), 38, 10) is null and 
                    src:PORTALCUSTOMERCONSTRAINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PORTALPUBLICCONSTRAINT), '0'), 38, 10) is null and 
                    src:PORTALPUBLICCONSTRAINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)