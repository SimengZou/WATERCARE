CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_PLANNING_PLANREVIEW_ERROR AS SELECT src, 'IPS_CDR_PLANNING_PLANREVIEW' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACTLTM), '0'), 38, 10) is null and 
                    src:ACTLTM is not null) THEN
                    'ACTLTM contains a non-numeric value : \'' || AS_VARCHAR(src:ACTLTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANKEY), '0'), 38, 10) is null and 
                    src:APPLANKEY is not null) THEN
                    'APPLANKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANREVIEWKEY), '0'), 38, 10) is null and 
                    src:APPLANREVIEWKEY is not null) THEN
                    'APPLANREVIEWKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANREVIEWKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANREVIEWTYPEKEY), '0'), 38, 10) is null and 
                    src:APPLANREVIEWTYPEKEY is not null) THEN
                    'APPLANREVIEWTYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANREVIEWTYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSIGNTOPROVIDER), '0'), 38, 10) is null and 
                    src:ASSIGNTOPROVIDER is not null) THEN
                    'ASSIGNTOPROVIDER contains a non-numeric value : \'' || AS_VARCHAR(src:ASSIGNTOPROVIDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPBYPROVIDER), '0'), 38, 10) is null and 
                    src:COMPBYPROVIDER is not null) THEN
                    'COMPBYPROVIDER contains a non-numeric value : \'' || AS_VARCHAR(src:COMPBYPROVIDER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COMPDTTM), '1900-01-01')) is null and 
                    src:COMPDTTM is not null) THEN
                    'COMPDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:COMPDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ISSDTTM), '1900-01-01')) is null and 
                    src:ISSDTTM is not null) THEN
                    'ISSDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ISSDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESULTBYPROVIDER), '0'), 38, 10) is null and 
                    src:RESULTBYPROVIDER is not null) THEN
                    'RESULTBYPROVIDER contains a non-numeric value : \'' || AS_VARCHAR(src:RESULTBYPROVIDER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RESULTDTTM), '1900-01-01')) is null and 
                    src:RESULTDTTM is not null) THEN
                    'RESULTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:RESULTDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCHEDDTTM), '1900-01-01')) is null and 
                    src:SCHEDDTTM is not null) THEN
                    'SCHEDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:SCHEDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTBYPROVIDER), '0'), 38, 10) is null and 
                    src:STARTBYPROVIDER is not null) THEN
                    'STARTBYPROVIDER contains a non-numeric value : \'' || AS_VARCHAR(src:STARTBYPROVIDER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) THEN
                    'STARTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUSPDT), '1900-01-01')) is null and 
                    src:SUSPDT is not null) THEN
                    'SUSPDT contains a non-timestamp value : \'' || AS_VARCHAR(src:SUSPDT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TYPENO), '0'), 38, 10) is null and 
                    src:TYPENO is not null) THEN
                    'TYPENO contains a non-numeric value : \'' || AS_VARCHAR(src:TYPENO) || '\'' WHEN 
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
                                    
                src:APPLANREVIEWKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_PLANNING_PLANREVIEW as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACTLTM), '0'), 38, 10) is null and 
                    src:ACTLTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANKEY), '0'), 38, 10) is null and 
                    src:APPLANKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANREVIEWKEY), '0'), 38, 10) is null and 
                    src:APPLANREVIEWKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANREVIEWTYPEKEY), '0'), 38, 10) is null and 
                    src:APPLANREVIEWTYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSIGNTOPROVIDER), '0'), 38, 10) is null and 
                    src:ASSIGNTOPROVIDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPBYPROVIDER), '0'), 38, 10) is null and 
                    src:COMPBYPROVIDER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COMPDTTM), '1900-01-01')) is null and 
                    src:COMPDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ISSDTTM), '1900-01-01')) is null and 
                    src:ISSDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESULTBYPROVIDER), '0'), 38, 10) is null and 
                    src:RESULTBYPROVIDER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RESULTDTTM), '1900-01-01')) is null and 
                    src:RESULTDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCHEDDTTM), '1900-01-01')) is null and 
                    src:SCHEDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTBYPROVIDER), '0'), 38, 10) is null and 
                    src:STARTBYPROVIDER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUSPDT), '1900-01-01')) is null and 
                    src:SUSPDT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TYPENO), '0'), 38, 10) is null and 
                    src:TYPENO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)