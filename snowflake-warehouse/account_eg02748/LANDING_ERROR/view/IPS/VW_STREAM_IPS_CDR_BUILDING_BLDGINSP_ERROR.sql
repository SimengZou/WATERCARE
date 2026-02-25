CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_BUILDING_BLDGINSP_ERROR AS SELECT src, 'IPS_CDR_BUILDING_BLDGINSP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACTVDDTTM), '1900-01-01')) is null and 
                    src:ACTVDDTTM is not null) THEN
                    'ACTVDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ACTVDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGINSPKEY), '0'), 38, 10) is null and 
                    src:APBLDGINSPKEY is not null) THEN
                    'APBLDGINSPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBLDGINSPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGINSPTYPEKEY), '0'), 38, 10) is null and 
                    src:APBLDGINSPTYPEKEY is not null) THEN
                    'APBLDGINSPTYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBLDGINSPTYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGKEY), '0'), 38, 10) is null and 
                    src:APBLDGKEY is not null) THEN
                    'APBLDGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBLDGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSIGNTOPROVIDER), '0'), 38, 10) is null and 
                    src:ASSIGNTOPROVIDER is not null) THEN
                    'ASSIGNTOPROVIDER contains a non-numeric value : \'' || AS_VARCHAR(src:ASSIGNTOPROVIDER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CALLDTTM), '1900-01-01')) is null and 
                    src:CALLDTTM is not null) THEN
                    'CALLDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:CALLDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COMPDTTM), '1900-01-01')) is null and 
                    src:COMPDTTM is not null) THEN
                    'COMPDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:COMPDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPBYPROVIDER), '0'), 38, 10) is null and 
                    src:INSPBYPROVIDER is not null) THEN
                    'INSPBYPROVIDER contains a non-numeric value : \'' || AS_VARCHAR(src:INSPBYPROVIDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPHOURS), '0'), 38, 10) is null and 
                    src:INSPHOURS is not null) THEN
                    'INSPHOURS contains a non-numeric value : \'' || AS_VARCHAR(src:INSPHOURS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSTRIPKEY), '0'), 38, 10) is null and 
                    src:INSTRIPKEY is not null) THEN
                    'INSTRIPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:INSTRIPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MOBILEDTTM), '1900-01-01')) is null and 
                    src:MOBILEDTTM is not null) THEN
                    'MOBILEDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MOBILEDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ODOMSTART), '0'), 38, 10) is null and 
                    src:ODOMSTART is not null) THEN
                    'ODOMSTART contains a non-numeric value : \'' || AS_VARCHAR(src:ODOMSTART) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ODOMSTOP), '0'), 38, 10) is null and 
                    src:ODOMSTOP is not null) THEN
                    'ODOMSTOP contains a non-numeric value : \'' || AS_VARCHAR(src:ODOMSTOP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD), '0'), 38, 10) is null and 
                    src:ORD is not null) THEN
                    'ORD contains a non-numeric value : \'' || AS_VARCHAR(src:ORD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REQUESTEDBY), '0'), 38, 10) is null and 
                    src:REQUESTEDBY is not null) THEN
                    'REQUESTEDBY contains a non-numeric value : \'' || AS_VARCHAR(src:REQUESTEDBY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESULTBYPROVIDER), '0'), 38, 10) is null and 
                    src:RESULTBYPROVIDER is not null) THEN
                    'RESULTBYPROVIDER contains a non-numeric value : \'' || AS_VARCHAR(src:RESULTBYPROVIDER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RESULTDTTM), '1900-01-01')) is null and 
                    src:RESULTDTTM is not null) THEN
                    'RESULTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:RESULTDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCHEDDTTM), '1900-01-01')) is null and 
                    src:SCHEDDTTM is not null) THEN
                    'SCHEDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:SCHEDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) THEN
                    'STARTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STAT), '0'), 38, 10) is null and 
                    src:STAT is not null) THEN
                    'STAT contains a non-numeric value : \'' || AS_VARCHAR(src:STAT) || '\'' WHEN 
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
                                    
                src:APBLDGINSPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_BUILDING_BLDGINSP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACTVDDTTM), '1900-01-01')) is null and 
                    src:ACTVDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGINSPKEY), '0'), 38, 10) is null and 
                    src:APBLDGINSPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGINSPTYPEKEY), '0'), 38, 10) is null and 
                    src:APBLDGINSPTYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGKEY), '0'), 38, 10) is null and 
                    src:APBLDGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSIGNTOPROVIDER), '0'), 38, 10) is null and 
                    src:ASSIGNTOPROVIDER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CALLDTTM), '1900-01-01')) is null and 
                    src:CALLDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COMPDTTM), '1900-01-01')) is null and 
                    src:COMPDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPBYPROVIDER), '0'), 38, 10) is null and 
                    src:INSPBYPROVIDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPHOURS), '0'), 38, 10) is null and 
                    src:INSPHOURS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSTRIPKEY), '0'), 38, 10) is null and 
                    src:INSTRIPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MOBILEDTTM), '1900-01-01')) is null and 
                    src:MOBILEDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ODOMSTART), '0'), 38, 10) is null and 
                    src:ODOMSTART is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ODOMSTOP), '0'), 38, 10) is null and 
                    src:ODOMSTOP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD), '0'), 38, 10) is null and 
                    src:ORD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REQUESTEDBY), '0'), 38, 10) is null and 
                    src:REQUESTEDBY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESULTBYPROVIDER), '0'), 38, 10) is null and 
                    src:RESULTBYPROVIDER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RESULTDTTM), '1900-01-01')) is null and 
                    src:RESULTDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCHEDDTTM), '1900-01-01')) is null and 
                    src:SCHEDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STAT), '0'), 38, 10) is null and 
                    src:STAT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TYPENO), '0'), 38, 10) is null and 
                    src:TYPENO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)