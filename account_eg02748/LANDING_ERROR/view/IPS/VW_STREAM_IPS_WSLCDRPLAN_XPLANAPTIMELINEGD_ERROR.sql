CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLCDRPLAN_XPLANAPTIMELINEGD_ERROR AS SELECT src, 'IPS_WSLCDRPLAN_XPLANAPTIMELINEGD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AREAHECTARES), '0'), 38, 10) is null and 
                    src:AREAHECTARES is not null) THEN
                    'AREAHECTARES contains a non-numeric value : \'' || AS_VARCHAR(src:AREAHECTARES) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPLETEMONTH), '0'), 38, 10) is null and 
                    src:COMPLETEMONTH is not null) THEN
                    'COMPLETEMONTH contains a non-numeric value : \'' || AS_VARCHAR(src:COMPLETEMONTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPLETEYEAR1), '0'), 38, 10) is null and 
                    src:COMPLETEYEAR1 is not null) THEN
                    'COMPLETEYEAR1 contains a non-numeric value : \'' || AS_VARCHAR(src:COMPLETEYEAR1) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOOFUNITS), '0'), 38, 10) is null and 
                    src:NOOFUNITS is not null) THEN
                    'NOOFUNITS contains a non-numeric value : \'' || AS_VARCHAR(src:NOOFUNITS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTMONTH), '0'), 38, 10) is null and 
                    src:STARTMONTH is not null) THEN
                    'STARTMONTH contains a non-numeric value : \'' || AS_VARCHAR(src:STARTMONTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTYEAR1), '0'), 38, 10) is null and 
                    src:STARTYEAR1 is not null) THEN
                    'STARTYEAR1 contains a non-numeric value : \'' || AS_VARCHAR(src:STARTYEAR1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XPLANAPTIMELINEDPKEY), '0'), 38, 10) is null and 
                    src:XPLANAPTIMELINEDPKEY is not null) THEN
                    'XPLANAPTIMELINEDPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XPLANAPTIMELINEDPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XPLANAPTIMELINEGDKEY), '0'), 38, 10) is null and 
                    src:XPLANAPTIMELINEGDKEY is not null) THEN
                    'XPLANAPTIMELINEGDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XPLANAPTIMELINEGDKEY) || '\'' WHEN 
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
                                    
                src:XPLANAPTIMELINEGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRPLAN_XPLANAPTIMELINEGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AREAHECTARES), '0'), 38, 10) is null and 
                    src:AREAHECTARES is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPLETEMONTH), '0'), 38, 10) is null and 
                    src:COMPLETEMONTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPLETEYEAR1), '0'), 38, 10) is null and 
                    src:COMPLETEYEAR1 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOOFUNITS), '0'), 38, 10) is null and 
                    src:NOOFUNITS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTMONTH), '0'), 38, 10) is null and 
                    src:STARTMONTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTYEAR1), '0'), 38, 10) is null and 
                    src:STARTYEAR1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XPLANAPTIMELINEDPKEY), '0'), 38, 10) is null and 
                    src:XPLANAPTIMELINEDPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XPLANAPTIMELINEGDKEY), '0'), 38, 10) is null and 
                    src:XPLANAPTIMELINEGDKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)