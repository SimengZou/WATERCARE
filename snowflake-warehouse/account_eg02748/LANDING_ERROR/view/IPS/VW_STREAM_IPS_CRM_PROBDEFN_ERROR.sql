CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CRM_PROBDEFN_ERROR AS SELECT src, 'IPS_CRM_PROBDEFN' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETINSPTYPE), '0'), 38, 10) is null and 
                    src:ASSETINSPTYPE is not null) THEN
                    'ASSETINSPTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:ASSETINSPTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETTYPE), '0'), 38, 10) is null and 
                    src:ASSETTYPE is not null) THEN
                    'ASSETTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:ASSETTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTORES), '0'), 38, 10) is null and 
                    src:AUTORES is not null) THEN
                    'AUTORES contains a non-numeric value : \'' || AS_VARCHAR(src:AUTORES) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASETYPE), '0'), 38, 10) is null and 
                    src:CASETYPE is not null) THEN
                    'CASETYPE contains a non-numeric value : \'' || AS_VARCHAR(src:CASETYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CATEGORY), '0'), 38, 10) is null and 
                    src:CATEGORY is not null) THEN
                    'CATEGORY contains a non-numeric value : \'' || AS_VARCHAR(src:CATEGORY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CORRPROCESSSETUP), '0'), 38, 10) is null and 
                    src:CORRPROCESSSETUP is not null) THEN
                    'CORRPROCESSSETUP contains a non-numeric value : \'' || AS_VARCHAR(src:CORRPROCESSSETUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CREATEDINLASTXMIN), '0'), 38, 10) is null and 
                    src:CREATEDINLASTXMIN is not null) THEN
                    'CREATEDINLASTXMIN contains a non-numeric value : \'' || AS_VARCHAR(src:CREATEDINLASTXMIN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPDAYS), '0'), 38, 10) is null and 
                    src:INSPDAYS is not null) THEN
                    'INSPDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:INSPDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPHRS), '0'), 38, 10) is null and 
                    src:INSPHRS is not null) THEN
                    'INSPHRS contains a non-numeric value : \'' || AS_VARCHAR(src:INSPHRS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPMINS), '0'), 38, 10) is null and 
                    src:INSPMINS is not null) THEN
                    'INSPMINS contains a non-numeric value : \'' || AS_VARCHAR(src:INSPMINS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OOCTYPE), '0'), 38, 10) is null and 
                    src:OOCTYPE is not null) THEN
                    'OOCTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:OOCTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POOLKEY), '0'), 38, 10) is null and 
                    src:POOLKEY is not null) THEN
                    'POOLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:POOLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROBKEY), '0'), 38, 10) is null and 
                    src:PROBKEY is not null) THEN
                    'PROBKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PROBKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESDAYS), '0'), 38, 10) is null and 
                    src:RESDAYS is not null) THEN
                    'RESDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:RESDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESHRS), '0'), 38, 10) is null and 
                    src:RESHRS is not null) THEN
                    'RESHRS contains a non-numeric value : \'' || AS_VARCHAR(src:RESHRS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDDAYS), '0'), 38, 10) is null and 
                    src:SCHEDDAYS is not null) THEN
                    'SCHEDDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:SCHEDDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDHRS), '0'), 38, 10) is null and 
                    src:SCHEDHRS is not null) THEN
                    'SCHEDHRS contains a non-numeric value : \'' || AS_VARCHAR(src:SCHEDHRS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHRADIUS), '0'), 38, 10) is null and 
                    src:SEARCHRADIUS is not null) THEN
                    'SEARCHRADIUS contains a non-numeric value : \'' || AS_VARCHAR(src:SEARCHRADIUS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVREQASSETTYPE), '0'), 38, 10) is null and 
                    src:SERVREQASSETTYPE is not null) THEN
                    'SERVREQASSETTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:SERVREQASSETTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STANDARDWORKORDERKEY), '0'), 38, 10) is null and 
                    src:STANDARDWORKORDERKEY is not null) THEN
                    'STANDARDWORKORDERKEY contains a non-numeric value : \'' || AS_VARCHAR(src:STANDARDWORKORDERKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTDAYS), '0'), 38, 10) is null and 
                    src:STARTDAYS is not null) THEN
                    'STARTDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:STARTDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTHRS), '0'), 38, 10) is null and 
                    src:STARTHRS is not null) THEN
                    'STARTHRS contains a non-numeric value : \'' || AS_VARCHAR(src:STARTHRS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUSPDAYS), '0'), 38, 10) is null and 
                    src:SUSPDAYS is not null) THEN
                    'SUSPDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:SUSPDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUSPHRS), '0'), 38, 10) is null and 
                    src:SUSPHRS is not null) THEN
                    'SUSPHRS contains a non-numeric value : \'' || AS_VARCHAR(src:SUSPHRS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WOACTIVITY), '0'), 38, 10) is null and 
                    src:WOACTIVITY is not null) THEN
                    'WOACTIVITY contains a non-numeric value : \'' || AS_VARCHAR(src:WOACTIVITY) || '\'' WHEN 
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
                                    
                src:PROBKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CRM_PROBDEFN as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETINSPTYPE), '0'), 38, 10) is null and 
                    src:ASSETINSPTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETTYPE), '0'), 38, 10) is null and 
                    src:ASSETTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTORES), '0'), 38, 10) is null and 
                    src:AUTORES is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASETYPE), '0'), 38, 10) is null and 
                    src:CASETYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CATEGORY), '0'), 38, 10) is null and 
                    src:CATEGORY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CORRPROCESSSETUP), '0'), 38, 10) is null and 
                    src:CORRPROCESSSETUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CREATEDINLASTXMIN), '0'), 38, 10) is null and 
                    src:CREATEDINLASTXMIN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPDAYS), '0'), 38, 10) is null and 
                    src:INSPDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPHRS), '0'), 38, 10) is null and 
                    src:INSPHRS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPMINS), '0'), 38, 10) is null and 
                    src:INSPMINS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OOCTYPE), '0'), 38, 10) is null and 
                    src:OOCTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POOLKEY), '0'), 38, 10) is null and 
                    src:POOLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROBKEY), '0'), 38, 10) is null and 
                    src:PROBKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESDAYS), '0'), 38, 10) is null and 
                    src:RESDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESHRS), '0'), 38, 10) is null and 
                    src:RESHRS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDDAYS), '0'), 38, 10) is null and 
                    src:SCHEDDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDHRS), '0'), 38, 10) is null and 
                    src:SCHEDHRS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHRADIUS), '0'), 38, 10) is null and 
                    src:SEARCHRADIUS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVREQASSETTYPE), '0'), 38, 10) is null and 
                    src:SERVREQASSETTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STANDARDWORKORDERKEY), '0'), 38, 10) is null and 
                    src:STANDARDWORKORDERKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTDAYS), '0'), 38, 10) is null and 
                    src:STARTDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTHRS), '0'), 38, 10) is null and 
                    src:STARTHRS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUSPDAYS), '0'), 38, 10) is null and 
                    src:SUSPDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUSPHRS), '0'), 38, 10) is null and 
                    src:SUSPHRS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WOACTIVITY), '0'), 38, 10) is null and 
                    src:WOACTIVITY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)