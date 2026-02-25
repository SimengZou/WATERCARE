CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_DELINQUENCYMILESTONE_ERROR AS SELECT src, 'IPS_BILLING_DELINQUENCYMILESTONE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTODECREMENTBELOWAMT), '0'), 38, 10) is null and 
                    src:AUTODECREMENTBELOWAMT is not null) THEN
                    'AUTODECREMENTBELOWAMT contains a non-numeric value : \'' || AS_VARCHAR(src:AUTODECREMENTBELOWAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTODECREMENTMILESTONEKEY), '0'), 38, 10) is null and 
                    src:AUTODECREMENTMILESTONEKEY is not null) THEN
                    'AUTODECREMENTMILESTONEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:AUTODECREMENTMILESTONEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTODECREMENTMSFORMULA), '0'), 38, 10) is null and 
                    src:AUTODECREMENTMSFORMULA is not null) THEN
                    'AUTODECREMENTMSFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:AUTODECREMENTMSFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COLLRESCODEFORMULAKEY), '0'), 38, 10) is null and 
                    src:COLLRESCODEFORMULAKEY is not null) THEN
                    'COLLRESCODEFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COLLRESCODEFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CREDITRATINGPOINTS), '0'), 38, 10) is null and 
                    src:CREDITRATINGPOINTS is not null) THEN
                    'CREDITRATINGPOINTS contains a non-numeric value : \'' || AS_VARCHAR(src:CREDITRATINGPOINTS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CREDITRATINGPOINTSFORMULA), '0'), 38, 10) is null and 
                    src:CREDITRATINGPOINTSFORMULA is not null) THEN
                    'CREDITRATINGPOINTSFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:CREDITRATINGPOINTSFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DECREMENTMILESTONEFORMULA), '0'), 38, 10) is null and 
                    src:DECREMENTMILESTONEFORMULA is not null) THEN
                    'DECREMENTMILESTONEFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:DECREMENTMILESTONEFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DECREMENTMILESTONKEY), '0'), 38, 10) is null and 
                    src:DECREMENTMILESTONKEY is not null) THEN
                    'DECREMENTMILESTONKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DECREMENTMILESTONKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAYORDER), '0'), 38, 10) is null and 
                    src:DISPLAYORDER is not null) THEN
                    'DISPLAYORDER contains a non-numeric value : \'' || AS_VARCHAR(src:DISPLAYORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DUEAFTER), '0'), 38, 10) is null and 
                    src:DUEAFTER is not null) THEN
                    'DUEAFTER contains a non-numeric value : \'' || AS_VARCHAR(src:DUEAFTER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DURATION), '0'), 38, 10) is null and 
                    src:DURATION is not null) THEN
                    'DURATION contains a non-numeric value : \'' || AS_VARCHAR(src:DURATION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DURATIONFORMULA), '0'), 38, 10) is null and 
                    src:DURATIONFORMULA is not null) THEN
                    'DURATIONFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:DURATIONFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LIENRESCODEFORMULAKEY), '0'), 38, 10) is null and 
                    src:LIENRESCODEFORMULAKEY is not null) THEN
                    'LIENRESCODEFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LIENRESCODEFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MILESTONEKEY), '0'), 38, 10) is null and 
                    src:MILESTONEKEY is not null) THEN
                    'MILESTONEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MILESTONEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEXTMILESTONEFORMULA), '0'), 38, 10) is null and 
                    src:NEXTMILESTONEFORMULA is not null) THEN
                    'NEXTMILESTONEFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:NEXTMILESTONEFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEXTMILESTONEKEY), '0'), 38, 10) is null and 
                    src:NEXTMILESTONEKEY is not null) THEN
                    'NEXTMILESTONEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:NEXTMILESTONEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESOLUTIONCODEFORMULA), '0'), 38, 10) is null and 
                    src:RESOLUTIONCODEFORMULA is not null) THEN
                    'RESOLUTIONCODEFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:RESOLUTIONCODEFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESOLVEBELOWAMT), '0'), 38, 10) is null and 
                    src:RESOLVEBELOWAMT is not null) THEN
                    'RESOLVEBELOWAMT contains a non-numeric value : \'' || AS_VARCHAR(src:RESOLVEBELOWAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESOLVEFORMULA), '0'), 38, 10) is null and 
                    src:RESOLVEFORMULA is not null) THEN
                    'RESOLVEFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:RESOLVEFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSEPAYMILESTONEFORMULA), '0'), 38, 10) is null and 
                    src:REVERSEPAYMILESTONEFORMULA is not null) THEN
                    'REVERSEPAYMILESTONEFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:REVERSEPAYMILESTONEFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSEPAYMILESTONEKEY), '0'), 38, 10) is null and 
                    src:REVERSEPAYMILESTONEKEY is not null) THEN
                    'REVERSEPAYMILESTONEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REVERSEPAYMILESTONEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEMEKEY), '0'), 38, 10) is null and 
                    src:SCHEMEKEY is not null) THEN
                    'SCHEMEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SCHEMEKEY) || '\'' WHEN 
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
                                    
                src:MILESTONEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_DELINQUENCYMILESTONE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTODECREMENTBELOWAMT), '0'), 38, 10) is null and 
                    src:AUTODECREMENTBELOWAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTODECREMENTMILESTONEKEY), '0'), 38, 10) is null and 
                    src:AUTODECREMENTMILESTONEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTODECREMENTMSFORMULA), '0'), 38, 10) is null and 
                    src:AUTODECREMENTMSFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COLLRESCODEFORMULAKEY), '0'), 38, 10) is null and 
                    src:COLLRESCODEFORMULAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CREDITRATINGPOINTS), '0'), 38, 10) is null and 
                    src:CREDITRATINGPOINTS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CREDITRATINGPOINTSFORMULA), '0'), 38, 10) is null and 
                    src:CREDITRATINGPOINTSFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DECREMENTMILESTONEFORMULA), '0'), 38, 10) is null and 
                    src:DECREMENTMILESTONEFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DECREMENTMILESTONKEY), '0'), 38, 10) is null and 
                    src:DECREMENTMILESTONKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAYORDER), '0'), 38, 10) is null and 
                    src:DISPLAYORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DUEAFTER), '0'), 38, 10) is null and 
                    src:DUEAFTER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DURATION), '0'), 38, 10) is null and 
                    src:DURATION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DURATIONFORMULA), '0'), 38, 10) is null and 
                    src:DURATIONFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LIENRESCODEFORMULAKEY), '0'), 38, 10) is null and 
                    src:LIENRESCODEFORMULAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MILESTONEKEY), '0'), 38, 10) is null and 
                    src:MILESTONEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEXTMILESTONEFORMULA), '0'), 38, 10) is null and 
                    src:NEXTMILESTONEFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEXTMILESTONEKEY), '0'), 38, 10) is null and 
                    src:NEXTMILESTONEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESOLUTIONCODEFORMULA), '0'), 38, 10) is null and 
                    src:RESOLUTIONCODEFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESOLVEBELOWAMT), '0'), 38, 10) is null and 
                    src:RESOLVEBELOWAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESOLVEFORMULA), '0'), 38, 10) is null and 
                    src:RESOLVEFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSEPAYMILESTONEFORMULA), '0'), 38, 10) is null and 
                    src:REVERSEPAYMILESTONEFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSEPAYMILESTONEKEY), '0'), 38, 10) is null and 
                    src:REVERSEPAYMILESTONEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEMEKEY), '0'), 38, 10) is null and 
                    src:SCHEMEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)