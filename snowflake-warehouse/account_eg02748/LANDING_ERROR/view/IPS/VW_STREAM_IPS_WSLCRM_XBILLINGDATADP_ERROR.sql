CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLCRM_XBILLINGDATADP_ERROR AS SELECT src, 'IPS_WSLCRM_XBILLINGDATADP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACCOUNTCREATEDATE), '1900-01-01')) is null and 
                    src:ACCOUNTCREATEDATE is not null) THEN
                    'ACCOUNTCREATEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ACCOUNTCREATEDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTACTKEY), '0'), 38, 10) is null and 
                    src:CONTACTKEY is not null) THEN
                    'CONTACTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CONTACTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISCONNECTIONDATE), '1900-01-01')) is null and 
                    src:DISCONNECTIONDATE is not null) THEN
                    'DISCONNECTIONDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DISCONNECTIONDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTDAILYWATERDEMAND), '0'), 38, 10) is null and 
                    src:ESTDAILYWATERDEMAND is not null) THEN
                    'ESTDAILYWATERDEMAND contains a non-numeric value : \'' || AS_VARCHAR(src:ESTDAILYWATERDEMAND) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FLOWATTHEMETER), '0'), 38, 10) is null and 
                    src:FLOWATTHEMETER is not null) THEN
                    'FLOWATTHEMETER contains a non-numeric value : \'' || AS_VARCHAR(src:FLOWATTHEMETER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GISXCOORD), '0'), 38, 10) is null and 
                    src:GISXCOORD is not null) THEN
                    'GISXCOORD contains a non-numeric value : \'' || AS_VARCHAR(src:GISXCOORD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GISYCOORD), '0'), 38, 10) is null and 
                    src:GISYCOORD is not null) THEN
                    'GISYCOORD contains a non-numeric value : \'' || AS_VARCHAR(src:GISYCOORD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GISZCOORD), '0'), 38, 10) is null and 
                    src:GISZCOORD is not null) THEN
                    'GISZCOORD contains a non-numeric value : \'' || AS_VARCHAR(src:GISZCOORD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INITIALMETERREADING), '0'), 38, 10) is null and 
                    src:INITIALMETERREADING is not null) THEN
                    'INITIALMETERREADING contains a non-numeric value : \'' || AS_VARCHAR(src:INITIALMETERREADING) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEWMETERINSTALLDATE), '1900-01-01')) is null and 
                    src:NEWMETERINSTALLDATE is not null) THEN
                    'NEWMETERINSTALLDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:NEWMETERINSTALLDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLDMETERREADING), '0'), 38, 10) is null and 
                    src:OLDMETERREADING is not null) THEN
                    'OLDMETERREADING contains a non-numeric value : \'' || AS_VARCHAR(src:OLDMETERREADING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PIPEDAIMETER), '0'), 38, 10) is null and 
                    src:PIPEDAIMETER is not null) THEN
                    'PIPEDAIMETER contains a non-numeric value : \'' || AS_VARCHAR(src:PIPEDAIMETER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PIPELENGTH), '0'), 38, 10) is null and 
                    src:PIPELENGTH is not null) THEN
                    'PIPELENGTH contains a non-numeric value : \'' || AS_VARCHAR(src:PIPELENGTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVNO), '0'), 38, 10) is null and 
                    src:SERVNO is not null) THEN
                    'SERVNO contains a non-numeric value : \'' || AS_VARCHAR(src:SERVNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XBILLINGDATADPKEY), '0'), 38, 10) is null and 
                    src:XBILLINGDATADPKEY is not null) THEN
                    'XBILLINGDATADPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XBILLINGDATADPKEY) || '\'' WHEN 
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
                                    
                src:XBILLINGDATADPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCRM_XBILLINGDATADP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACCOUNTCREATEDATE), '1900-01-01')) is null and 
                    src:ACCOUNTCREATEDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTACTKEY), '0'), 38, 10) is null and 
                    src:CONTACTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISCONNECTIONDATE), '1900-01-01')) is null and 
                    src:DISCONNECTIONDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTDAILYWATERDEMAND), '0'), 38, 10) is null and 
                    src:ESTDAILYWATERDEMAND is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FLOWATTHEMETER), '0'), 38, 10) is null and 
                    src:FLOWATTHEMETER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GISXCOORD), '0'), 38, 10) is null and 
                    src:GISXCOORD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GISYCOORD), '0'), 38, 10) is null and 
                    src:GISYCOORD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GISZCOORD), '0'), 38, 10) is null and 
                    src:GISZCOORD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INITIALMETERREADING), '0'), 38, 10) is null and 
                    src:INITIALMETERREADING is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEWMETERINSTALLDATE), '1900-01-01')) is null and 
                    src:NEWMETERINSTALLDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLDMETERREADING), '0'), 38, 10) is null and 
                    src:OLDMETERREADING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PIPEDAIMETER), '0'), 38, 10) is null and 
                    src:PIPEDAIMETER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PIPELENGTH), '0'), 38, 10) is null and 
                    src:PIPELENGTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVNO), '0'), 38, 10) is null and 
                    src:SERVNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XBILLINGDATADPKEY), '0'), 38, 10) is null and 
                    src:XBILLINGDATADPKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)