CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLCDRBUILD_XBUILDINSTALLDP_ERROR AS SELECT src, 'IPS_WSLCDRBUILD_XBUILDINSTALLDP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGAPPLDTLKEY), '0'), 38, 10) is null and 
                    src:APBLDGAPPLDTLKEY is not null) THEN
                    'APBLDGAPPLDTLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBLDGAPPLDTLKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INSTALLCOMPLETE), '1900-01-01')) is null and 
                    src:INSTALLCOMPLETE is not null) THEN
                    'INSTALLCOMPLETE contains a non-timestamp value : \'' || AS_VARCHAR(src:INSTALLCOMPLETE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:REINSTATEMENTCOMPLETE), '1900-01-01')) is null and 
                    src:REINSTATEMENTCOMPLETE is not null) THEN
                    'REINSTATEMENTCOMPLETE contains a non-timestamp value : \'' || AS_VARCHAR(src:REINSTATEMENTCOMPLETE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XBUILDINSTALLDPKEY), '0'), 38, 10) is null and 
                    src:XBUILDINSTALLDPKEY is not null) THEN
                    'XBUILDINSTALLDPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XBUILDINSTALLDPKEY) || '\'' WHEN 
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
                                    
                src:XBUILDINSTALLDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRBUILD_XBUILDINSTALLDP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGAPPLDTLKEY), '0'), 38, 10) is null and 
                    src:APBLDGAPPLDTLKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INSTALLCOMPLETE), '1900-01-01')) is null and 
                    src:INSTALLCOMPLETE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:REINSTATEMENTCOMPLETE), '1900-01-01')) is null and 
                    src:REINSTATEMENTCOMPLETE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XBUILDINSTALLDPKEY), '0'), 38, 10) is null and 
                    src:XBUILDINSTALLDPKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)