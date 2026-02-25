CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_METERMANAGEMENT_WATER_ROUTE_ERROR AS SELECT src, 'IPS_METERMANAGEMENT_WATER_ROUTE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVERAGECONSUMPTION), '0'), 38, 10) is null and 
                    src:AVERAGECONSUMPTION is not null) THEN
                    'AVERAGECONSUMPTION contains a non-numeric value : \'' || AS_VARCHAR(src:AVERAGECONSUMPTION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTDTTM), '1900-01-01')) is null and 
                    src:LASTDTTM is not null) THEN
                    'LASTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:LASTDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTREADINGEXSCHDKEY), '0'), 38, 10) is null and 
                    src:LASTREADINGEXSCHDKEY is not null) THEN
                    'LASTREADINGEXSCHDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LASTREADINGEXSCHDKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTREADINGIMSCHDKEY), '0'), 38, 10) is null and 
                    src:LASTREADINGIMSCHDKEY is not null) THEN
                    'LASTREADINGIMSCHDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LASTREADINGIMSCHDKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEXTDTTM), '1900-01-01')) is null and 
                    src:NEXTDTTM is not null) THEN
                    'NEXTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:NEXTDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROUTEKEY), '0'), 38, 10) is null and 
                    src:ROUTEKEY is not null) THEN
                    'ROUTEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ROUTEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TIMEINT), '0'), 38, 10) is null and 
                    src:TIMEINT is not null) THEN
                    'TIMEINT contains a non-numeric value : \'' || AS_VARCHAR(src:TIMEINT) || '\'' WHEN 
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
                                    
                src:ROUTEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_ROUTE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVERAGECONSUMPTION), '0'), 38, 10) is null and 
                    src:AVERAGECONSUMPTION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTDTTM), '1900-01-01')) is null and 
                    src:LASTDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTREADINGEXSCHDKEY), '0'), 38, 10) is null and 
                    src:LASTREADINGEXSCHDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTREADINGIMSCHDKEY), '0'), 38, 10) is null and 
                    src:LASTREADINGIMSCHDKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEXTDTTM), '1900-01-01')) is null and 
                    src:NEXTDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROUTEKEY), '0'), 38, 10) is null and 
                    src:ROUTEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TIMEINT), '0'), 38, 10) is null and 
                    src:TIMEINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)