CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_INF_VALUATION_VALUATION_ASSVALRESERVEGD_ERROR AS SELECT src, 'IPS_INF_VALUATION_VALUATION_ASSVALRESERVEGD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACUMDEPB4REVAL), '0'), 38, 10) is null and 
                    src:ACUMDEPB4REVAL is not null) THEN
                    'ACUMDEPB4REVAL contains a non-numeric value : \'' || AS_VARCHAR(src:ACUMDEPB4REVAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALRESERVEGDKEY), '0'), 38, 10) is null and 
                    src:ASSVALRESERVEGDKEY is not null) THEN
                    'ASSVALRESERVEGDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALRESERVEGDKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALRESERVEKEY), '0'), 38, 10) is null and 
                    src:ASSVALRESERVEKEY is not null) THEN
                    'ASSVALRESERVEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALRESERVEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BALSHTBOOKVAL), '0'), 38, 10) is null and 
                    src:BALSHTBOOKVAL is not null) THEN
                    'BALSHTBOOKVAL contains a non-numeric value : \'' || AS_VARCHAR(src:BALSHTBOOKVAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAPEXP), '0'), 38, 10) is null and 
                    src:CAPEXP is not null) THEN
                    'CAPEXP contains a non-numeric value : \'' || AS_VARCHAR(src:CAPEXP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CLOSERESERVE), '0'), 38, 10) is null and 
                    src:CLOSERESERVE is not null) THEN
                    'CLOSERESERVE contains a non-numeric value : \'' || AS_VARCHAR(src:CLOSERESERVE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISPDATE), '1900-01-01')) is null and 
                    src:DISPDATE is not null) THEN
                    'DISPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DISPDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEMENT), '0'), 38, 10) is null and 
                    src:MOVEMENT is not null) THEN
                    'MOVEMENT contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEMENT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENEXPLIFE), '0'), 38, 10) is null and 
                    src:OPENEXPLIFE is not null) THEN
                    'OPENEXPLIFE contains a non-numeric value : \'' || AS_VARCHAR(src:OPENEXPLIFE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OPENFYDATE), '1900-01-01')) is null and 
                    src:OPENFYDATE is not null) THEN
                    'OPENFYDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:OPENFYDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENREPLCOST), '0'), 38, 10) is null and 
                    src:OPENREPLCOST is not null) THEN
                    'OPENREPLCOST contains a non-numeric value : \'' || AS_VARCHAR(src:OPENREPLCOST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENRESERVE), '0'), 38, 10) is null and 
                    src:OPENRESERVE is not null) THEN
                    'OPENRESERVE contains a non-numeric value : \'' || AS_VARCHAR(src:OPENRESERVE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENRESIDLIFE), '0'), 38, 10) is null and 
                    src:OPENRESIDLIFE is not null) THEN
                    'OPENRESIDLIFE contains a non-numeric value : \'' || AS_VARCHAR(src:OPENRESIDLIFE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENWDV), '0'), 38, 10) is null and 
                    src:OPENWDV is not null) THEN
                    'OPENWDV contains a non-numeric value : \'' || AS_VARCHAR(src:OPENWDV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REMLIFE), '0'), 38, 10) is null and 
                    src:REMLIFE is not null) THEN
                    'REMLIFE contains a non-numeric value : \'' || AS_VARCHAR(src:REMLIFE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RESERVEDATE), '1900-01-01')) is null and 
                    src:RESERVEDATE is not null) THEN
                    'RESERVEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:RESERVEDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALREPLCOST), '0'), 38, 10) is null and 
                    src:REVALREPLCOST is not null) THEN
                    'REVALREPLCOST contains a non-numeric value : \'' || AS_VARCHAR(src:REVALREPLCOST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RUNNO), '0'), 38, 10) is null and 
                    src:RUNNO is not null) THEN
                    'RUNNO contains a non-numeric value : \'' || AS_VARCHAR(src:RUNNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALKEY), '0'), 38, 10) is null and 
                    src:VALKEY is not null) THEN
                    'VALKEY contains a non-numeric value : \'' || AS_VARCHAR(src:VALKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUETODISPOSE), '0'), 38, 10) is null and 
                    src:VALUETODISPOSE is not null) THEN
                    'VALUETODISPOSE contains a non-numeric value : \'' || AS_VARCHAR(src:VALUETODISPOSE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WDV), '0'), 38, 10) is null and 
                    src:WDV is not null) THEN
                    'WDV contains a non-numeric value : \'' || AS_VARCHAR(src:WDV) || '\'' WHEN 
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
                                    
                src:ASSVALRESERVEGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_INF_VALUATION_VALUATION_ASSVALRESERVEGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACUMDEPB4REVAL), '0'), 38, 10) is null and 
                    src:ACUMDEPB4REVAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALRESERVEGDKEY), '0'), 38, 10) is null and 
                    src:ASSVALRESERVEGDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALRESERVEKEY), '0'), 38, 10) is null and 
                    src:ASSVALRESERVEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BALSHTBOOKVAL), '0'), 38, 10) is null and 
                    src:BALSHTBOOKVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAPEXP), '0'), 38, 10) is null and 
                    src:CAPEXP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CLOSERESERVE), '0'), 38, 10) is null and 
                    src:CLOSERESERVE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISPDATE), '1900-01-01')) is null and 
                    src:DISPDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEMENT), '0'), 38, 10) is null and 
                    src:MOVEMENT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENEXPLIFE), '0'), 38, 10) is null and 
                    src:OPENEXPLIFE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OPENFYDATE), '1900-01-01')) is null and 
                    src:OPENFYDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENREPLCOST), '0'), 38, 10) is null and 
                    src:OPENREPLCOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENRESERVE), '0'), 38, 10) is null and 
                    src:OPENRESERVE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENRESIDLIFE), '0'), 38, 10) is null and 
                    src:OPENRESIDLIFE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENWDV), '0'), 38, 10) is null and 
                    src:OPENWDV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REMLIFE), '0'), 38, 10) is null and 
                    src:REMLIFE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RESERVEDATE), '1900-01-01')) is null and 
                    src:RESERVEDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALREPLCOST), '0'), 38, 10) is null and 
                    src:REVALREPLCOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RUNNO), '0'), 38, 10) is null and 
                    src:RUNNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALKEY), '0'), 38, 10) is null and 
                    src:VALKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUETODISPOSE), '0'), 38, 10) is null and 
                    src:VALUETODISPOSE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WDV), '0'), 38, 10) is null and 
                    src:WDV is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)