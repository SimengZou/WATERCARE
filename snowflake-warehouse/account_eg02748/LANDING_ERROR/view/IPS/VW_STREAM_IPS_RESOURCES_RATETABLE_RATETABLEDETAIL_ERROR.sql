CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_RESOURCES_RATETABLE_RATETABLEDETAIL_ERROR AS SELECT src, 'IPS_RESOURCES_RATETABLE_RATETABLEDETAIL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BRANCHKEY), '0'), 38, 10) is null and 
                    src:BRANCHKEY is not null) THEN
                    'BRANCHKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BRANCHKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONVERSIONFACTOR), '0'), 38, 10) is null and 
                    src:CONVERSIONFACTOR is not null) THEN
                    'CONVERSIONFACTOR contains a non-numeric value : \'' || AS_VARCHAR(src:CONVERSIONFACTOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CYCLEDAYS), '0'), 38, 10) is null and 
                    src:CYCLEDAYS is not null) THEN
                    'CYCLEDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:CYCLEDAYS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDATE), '1900-01-01')) is null and 
                    src:EFFECTIVEDATE is not null) THEN
                    'EFFECTIVEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFECTIVEDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVETODATE), '1900-01-01')) is null and 
                    src:EFFECTIVETODATE is not null) THEN
                    'EFFECTIVETODATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFECTIVETODATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FINALCYCLEDAYS), '0'), 38, 10) is null and 
                    src:FINALCYCLEDAYS is not null) THEN
                    'FINALCYCLEDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:FINALCYCLEDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FORMULAKEY), '0'), 38, 10) is null and 
                    src:FORMULAKEY is not null) THEN
                    'FORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:FORMULAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXIMUMCHARGE), '0'), 38, 10) is null and 
                    src:MAXIMUMCHARGE is not null) THEN
                    'MAXIMUMCHARGE contains a non-numeric value : \'' || AS_VARCHAR(src:MAXIMUMCHARGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINIMUMCHARGE), '0'), 38, 10) is null and 
                    src:MINIMUMCHARGE is not null) THEN
                    'MINIMUMCHARGE contains a non-numeric value : \'' || AS_VARCHAR(src:MINIMUMCHARGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFSTEPS), '0'), 38, 10) is null and 
                    src:NUMBEROFSTEPS is not null) THEN
                    'NUMBEROFSTEPS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFSTEPS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTVALUE), '0'), 38, 10) is null and 
                    src:PARENTVALUE is not null) THEN
                    'PARENTVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:PARENTVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP1), '0'), 38, 10) is null and 
                    src:RATEFORSTEP1 is not null) THEN
                    'RATEFORSTEP1 contains a non-numeric value : \'' || AS_VARCHAR(src:RATEFORSTEP1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP10), '0'), 38, 10) is null and 
                    src:RATEFORSTEP10 is not null) THEN
                    'RATEFORSTEP10 contains a non-numeric value : \'' || AS_VARCHAR(src:RATEFORSTEP10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP2), '0'), 38, 10) is null and 
                    src:RATEFORSTEP2 is not null) THEN
                    'RATEFORSTEP2 contains a non-numeric value : \'' || AS_VARCHAR(src:RATEFORSTEP2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP3), '0'), 38, 10) is null and 
                    src:RATEFORSTEP3 is not null) THEN
                    'RATEFORSTEP3 contains a non-numeric value : \'' || AS_VARCHAR(src:RATEFORSTEP3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP4), '0'), 38, 10) is null and 
                    src:RATEFORSTEP4 is not null) THEN
                    'RATEFORSTEP4 contains a non-numeric value : \'' || AS_VARCHAR(src:RATEFORSTEP4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP5), '0'), 38, 10) is null and 
                    src:RATEFORSTEP5 is not null) THEN
                    'RATEFORSTEP5 contains a non-numeric value : \'' || AS_VARCHAR(src:RATEFORSTEP5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP6), '0'), 38, 10) is null and 
                    src:RATEFORSTEP6 is not null) THEN
                    'RATEFORSTEP6 contains a non-numeric value : \'' || AS_VARCHAR(src:RATEFORSTEP6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP7), '0'), 38, 10) is null and 
                    src:RATEFORSTEP7 is not null) THEN
                    'RATEFORSTEP7 contains a non-numeric value : \'' || AS_VARCHAR(src:RATEFORSTEP7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP8), '0'), 38, 10) is null and 
                    src:RATEFORSTEP8 is not null) THEN
                    'RATEFORSTEP8 contains a non-numeric value : \'' || AS_VARCHAR(src:RATEFORSTEP8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP9), '0'), 38, 10) is null and 
                    src:RATEFORSTEP9 is not null) THEN
                    'RATEFORSTEP9 contains a non-numeric value : \'' || AS_VARCHAR(src:RATEFORSTEP9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEPERUNIT), '0'), 38, 10) is null and 
                    src:RATEPERUNIT is not null) THEN
                    'RATEPERUNIT contains a non-numeric value : \'' || AS_VARCHAR(src:RATEPERUNIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP1), '0'), 38, 10) is null and 
                    src:RATESTEP1 is not null) THEN
                    'RATESTEP1 contains a non-numeric value : \'' || AS_VARCHAR(src:RATESTEP1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP10), '0'), 38, 10) is null and 
                    src:RATESTEP10 is not null) THEN
                    'RATESTEP10 contains a non-numeric value : \'' || AS_VARCHAR(src:RATESTEP10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP2), '0'), 38, 10) is null and 
                    src:RATESTEP2 is not null) THEN
                    'RATESTEP2 contains a non-numeric value : \'' || AS_VARCHAR(src:RATESTEP2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP3), '0'), 38, 10) is null and 
                    src:RATESTEP3 is not null) THEN
                    'RATESTEP3 contains a non-numeric value : \'' || AS_VARCHAR(src:RATESTEP3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP4), '0'), 38, 10) is null and 
                    src:RATESTEP4 is not null) THEN
                    'RATESTEP4 contains a non-numeric value : \'' || AS_VARCHAR(src:RATESTEP4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP5), '0'), 38, 10) is null and 
                    src:RATESTEP5 is not null) THEN
                    'RATESTEP5 contains a non-numeric value : \'' || AS_VARCHAR(src:RATESTEP5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP6), '0'), 38, 10) is null and 
                    src:RATESTEP6 is not null) THEN
                    'RATESTEP6 contains a non-numeric value : \'' || AS_VARCHAR(src:RATESTEP6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP7), '0'), 38, 10) is null and 
                    src:RATESTEP7 is not null) THEN
                    'RATESTEP7 contains a non-numeric value : \'' || AS_VARCHAR(src:RATESTEP7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP8), '0'), 38, 10) is null and 
                    src:RATESTEP8 is not null) THEN
                    'RATESTEP8 contains a non-numeric value : \'' || AS_VARCHAR(src:RATESTEP8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP9), '0'), 38, 10) is null and 
                    src:RATESTEP9 is not null) THEN
                    'RATESTEP9 contains a non-numeric value : \'' || AS_VARCHAR(src:RATESTEP9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATETABLEDETAILKEY), '0'), 38, 10) is null and 
                    src:RATETABLEDETAILKEY is not null) THEN
                    'RATETABLEDETAILKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RATETABLEDETAILKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATETABLESETUPKEY), '0'), 38, 10) is null and 
                    src:RATETABLESETUPKEY is not null) THEN
                    'RATETABLESETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RATETABLESETUPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATETYPE), '0'), 38, 10) is null and 
                    src:RATETYPE is not null) THEN
                    'RATETYPE contains a non-numeric value : \'' || AS_VARCHAR(src:RATETYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SORTORDER), '0'), 38, 10) is null and 
                    src:SORTORDER is not null) THEN
                    'SORTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:SORTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USEFORPRORATING), '0'), 38, 10) is null and 
                    src:USEFORPRORATING is not null) THEN
                    'USEFORPRORATING contains a non-numeric value : \'' || AS_VARCHAR(src:USEFORPRORATING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE), '0'), 38, 10) is null and 
                    src:VALUE is not null) THEN
                    'VALUE contains a non-numeric value : \'' || AS_VARCHAR(src:VALUE) || '\'' WHEN 
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
                                    
                src:RATETABLEDETAILKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_RATETABLE_RATETABLEDETAIL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BRANCHKEY), '0'), 38, 10) is null and 
                    src:BRANCHKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONVERSIONFACTOR), '0'), 38, 10) is null and 
                    src:CONVERSIONFACTOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CYCLEDAYS), '0'), 38, 10) is null and 
                    src:CYCLEDAYS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDATE), '1900-01-01')) is null and 
                    src:EFFECTIVEDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVETODATE), '1900-01-01')) is null and 
                    src:EFFECTIVETODATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FINALCYCLEDAYS), '0'), 38, 10) is null and 
                    src:FINALCYCLEDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FORMULAKEY), '0'), 38, 10) is null and 
                    src:FORMULAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXIMUMCHARGE), '0'), 38, 10) is null and 
                    src:MAXIMUMCHARGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINIMUMCHARGE), '0'), 38, 10) is null and 
                    src:MINIMUMCHARGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFSTEPS), '0'), 38, 10) is null and 
                    src:NUMBEROFSTEPS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTVALUE), '0'), 38, 10) is null and 
                    src:PARENTVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP1), '0'), 38, 10) is null and 
                    src:RATEFORSTEP1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP10), '0'), 38, 10) is null and 
                    src:RATEFORSTEP10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP2), '0'), 38, 10) is null and 
                    src:RATEFORSTEP2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP3), '0'), 38, 10) is null and 
                    src:RATEFORSTEP3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP4), '0'), 38, 10) is null and 
                    src:RATEFORSTEP4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP5), '0'), 38, 10) is null and 
                    src:RATEFORSTEP5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP6), '0'), 38, 10) is null and 
                    src:RATEFORSTEP6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP7), '0'), 38, 10) is null and 
                    src:RATEFORSTEP7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP8), '0'), 38, 10) is null and 
                    src:RATEFORSTEP8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEFORSTEP9), '0'), 38, 10) is null and 
                    src:RATEFORSTEP9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATEPERUNIT), '0'), 38, 10) is null and 
                    src:RATEPERUNIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP1), '0'), 38, 10) is null and 
                    src:RATESTEP1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP10), '0'), 38, 10) is null and 
                    src:RATESTEP10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP2), '0'), 38, 10) is null and 
                    src:RATESTEP2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP3), '0'), 38, 10) is null and 
                    src:RATESTEP3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP4), '0'), 38, 10) is null and 
                    src:RATESTEP4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP5), '0'), 38, 10) is null and 
                    src:RATESTEP5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP6), '0'), 38, 10) is null and 
                    src:RATESTEP6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP7), '0'), 38, 10) is null and 
                    src:RATESTEP7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP8), '0'), 38, 10) is null and 
                    src:RATESTEP8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATESTEP9), '0'), 38, 10) is null and 
                    src:RATESTEP9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATETABLEDETAILKEY), '0'), 38, 10) is null and 
                    src:RATETABLEDETAILKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATETABLESETUPKEY), '0'), 38, 10) is null and 
                    src:RATETABLESETUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATETYPE), '0'), 38, 10) is null and 
                    src:RATETYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SORTORDER), '0'), 38, 10) is null and 
                    src:SORTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USEFORPRORATING), '0'), 38, 10) is null and 
                    src:USEFORPRORATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE), '0'), 38, 10) is null and 
                    src:VALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)