CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CRM_DETAILPAGE_ERROR AS SELECT src, 'IPS_CRM_DETAILPAGE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCESSIDADD), '0'), 38, 10) is null and 
                    src:ACCESSIDADD is not null) THEN
                    'ACCESSIDADD contains a non-numeric value : \'' || AS_VARCHAR(src:ACCESSIDADD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCESSIDUPDATE), '0'), 38, 10) is null and 
                    src:ACCESSIDUPDATE is not null) THEN
                    'ACCESSIDUPDATE contains a non-numeric value : \'' || AS_VARCHAR(src:ACCESSIDUPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCESSIDVIEW), '0'), 38, 10) is null and 
                    src:ACCESSIDVIEW is not null) THEN
                    'ACCESSIDVIEW contains a non-numeric value : \'' || AS_VARCHAR(src:ACCESSIDVIEW) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DETAILPAGEKEY), '0'), 38, 10) is null and 
                    src:DETAILPAGEKEY is not null) THEN
                    'DETAILPAGEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DETAILPAGEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAY), '0'), 38, 10) is null and 
                    src:DISPLAY is not null) THEN
                    'DISPLAY contains a non-numeric value : \'' || AS_VARCHAR(src:DISPLAY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAYORDER), '0'), 38, 10) is null and 
                    src:DISPLAYORDER is not null) THEN
                    'DISPLAYORDER contains a non-numeric value : \'' || AS_VARCHAR(src:DISPLAYORDER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDATE), '1900-01-01')) is null and 
                    src:EFFECTIVEDATE is not null) THEN
                    'EFFECTIVEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFECTIVEDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVENTHANDLER), '0'), 38, 10) is null and 
                    src:EVENTHANDLER is not null) THEN
                    'EVENTHANDLER contains a non-numeric value : \'' || AS_VARCHAR(src:EVENTHANDLER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPIREDATE), '1900-01-01')) is null and 
                    src:EXPIREDATE is not null) THEN
                    'EXPIREDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPIREDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PORTALCUSTOMERCONSTRAINT), '0'), 38, 10) is null and 
                    src:PORTALCUSTOMERCONSTRAINT is not null) THEN
                    'PORTALCUSTOMERCONSTRAINT contains a non-numeric value : \'' || AS_VARCHAR(src:PORTALCUSTOMERCONSTRAINT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PORTALDETAILPAGEAREA), '0'), 38, 10) is null and 
                    src:PORTALDETAILPAGEAREA is not null) THEN
                    'PORTALDETAILPAGEAREA contains a non-numeric value : \'' || AS_VARCHAR(src:PORTALDETAILPAGEAREA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PORTALDETAILPAGEPLACEMENT), '0'), 38, 10) is null and 
                    src:PORTALDETAILPAGEPLACEMENT is not null) THEN
                    'PORTALDETAILPAGEPLACEMENT contains a non-numeric value : \'' || AS_VARCHAR(src:PORTALDETAILPAGEPLACEMENT) || '\'' WHEN 
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
                                    
                src:DETAILPAGEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CRM_DETAILPAGE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCESSIDADD), '0'), 38, 10) is null and 
                    src:ACCESSIDADD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCESSIDUPDATE), '0'), 38, 10) is null and 
                    src:ACCESSIDUPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCESSIDVIEW), '0'), 38, 10) is null and 
                    src:ACCESSIDVIEW is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DETAILPAGEKEY), '0'), 38, 10) is null and 
                    src:DETAILPAGEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAY), '0'), 38, 10) is null and 
                    src:DISPLAY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAYORDER), '0'), 38, 10) is null and 
                    src:DISPLAYORDER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDATE), '1900-01-01')) is null and 
                    src:EFFECTIVEDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVENTHANDLER), '0'), 38, 10) is null and 
                    src:EVENTHANDLER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPIREDATE), '1900-01-01')) is null and 
                    src:EXPIREDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PORTALCUSTOMERCONSTRAINT), '0'), 38, 10) is null and 
                    src:PORTALCUSTOMERCONSTRAINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PORTALDETAILPAGEAREA), '0'), 38, 10) is null and 
                    src:PORTALDETAILPAGEAREA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PORTALDETAILPAGEPLACEMENT), '0'), 38, 10) is null and 
                    src:PORTALDETAILPAGEPLACEMENT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PORTALPUBLICCONSTRAINT), '0'), 38, 10) is null and 
                    src:PORTALPUBLICCONSTRAINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)