CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALRUNLINKWO_ERROR AS SELECT src, 'IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALRUNLINKWO' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALKEY), '0'), 38, 10) is null and 
                    src:ASSVALKEY is not null) THEN
                    'ASSVALKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALRUNKEY), '0'), 38, 10) is null and 
                    src:ASSVALRUNKEY is not null) THEN
                    'ASSVALRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALRUNKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COMPLETEDDATETIME), '1900-01-01')) is null and 
                    src:COMPLETEDDATETIME is not null) THEN
                    'COMPLETEDDATETIME contains a non-timestamp value : \'' || AS_VARCHAR(src:COMPLETEDDATETIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HISTKEY), '0'), 38, 10) is null and 
                    src:HISTKEY is not null) THEN
                    'HISTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:HISTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINKASSVALWOKEY), '0'), 38, 10) is null and 
                    src:LINKASSVALWOKEY is not null) THEN
                    'LINKASSVALWOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LINKASSVALWOKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTCOST), '0'), 38, 10) is null and 
                    src:TOTCOST is not null) THEN
                    'TOTCOST contains a non-numeric value : \'' || AS_VARCHAR(src:TOTCOST) || '\'' WHEN 
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
                                    
                src:LINKASSVALWOKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALRUNLINKWO as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALKEY), '0'), 38, 10) is null and 
                    src:ASSVALKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALRUNKEY), '0'), 38, 10) is null and 
                    src:ASSVALRUNKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COMPLETEDDATETIME), '1900-01-01')) is null and 
                    src:COMPLETEDDATETIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HISTKEY), '0'), 38, 10) is null and 
                    src:HISTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINKASSVALWOKEY), '0'), 38, 10) is null and 
                    src:LINKASSVALWOKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTCOST), '0'), 38, 10) is null and 
                    src:TOTCOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)