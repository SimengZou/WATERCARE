CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_RESOURCES_RATETABLE_CALCULATIONDETAIL_ERROR AS SELECT src, 'IPS_RESOURCES_RATETABLE_CALCULATIONDETAIL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BASEBILLABLEUSAGE), '0'), 38, 10) is null and 
                    src:BASEBILLABLEUSAGE is not null) THEN
                    'BASEBILLABLEUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:BASEBILLABLEUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCULATIONDETAILKEY), '0'), 38, 10) is null and 
                    src:CALCULATIONDETAILKEY is not null) THEN
                    'CALCULATIONDETAILKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CALCULATIONDETAILKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCULATIONTABLE), '0'), 38, 10) is null and 
                    src:CALCULATIONTABLE is not null) THEN
                    'CALCULATIONTABLE contains a non-numeric value : \'' || AS_VARCHAR(src:CALCULATIONTABLE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONVERTEDUNITS), '0'), 38, 10) is null and 
                    src:CONVERTEDUNITS is not null) THEN
                    'CONVERTEDUNITS contains a non-numeric value : \'' || AS_VARCHAR(src:CONVERTEDUNITS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYSBETWEENREADS), '0'), 38, 10) is null and 
                    src:DAYSBETWEENREADS is not null) THEN
                    'DAYSBETWEENREADS contains a non-numeric value : \'' || AS_VARCHAR(src:DAYSBETWEENREADS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFTODATE), '1900-01-01')) is null and 
                    src:EFFTODATE is not null) THEN
                    'EFFTODATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFTODATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FULLAMT), '0'), 38, 10) is null and 
                    src:FULLAMT is not null) THEN
                    'FULLAMT contains a non-numeric value : \'' || AS_VARCHAR(src:FULLAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFSTEPS), '0'), 38, 10) is null and 
                    src:NUMBEROFSTEPS is not null) THEN
                    'NUMBEROFSTEPS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFSTEPS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRORATEAMT), '0'), 38, 10) is null and 
                    src:PRORATEAMT is not null) THEN
                    'PRORATEAMT contains a non-numeric value : \'' || AS_VARCHAR(src:PRORATEAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATETABLEDETAILKEY), '0'), 38, 10) is null and 
                    src:RATETABLEDETAILKEY is not null) THEN
                    'RATETABLEDETAILKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RATETABLEDETAILKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG1), '0'), 38, 10) is null and 
                    src:RTCHG1 is not null) THEN
                    'RTCHG1 contains a non-numeric value : \'' || AS_VARCHAR(src:RTCHG1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG10), '0'), 38, 10) is null and 
                    src:RTCHG10 is not null) THEN
                    'RTCHG10 contains a non-numeric value : \'' || AS_VARCHAR(src:RTCHG10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG2), '0'), 38, 10) is null and 
                    src:RTCHG2 is not null) THEN
                    'RTCHG2 contains a non-numeric value : \'' || AS_VARCHAR(src:RTCHG2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG3), '0'), 38, 10) is null and 
                    src:RTCHG3 is not null) THEN
                    'RTCHG3 contains a non-numeric value : \'' || AS_VARCHAR(src:RTCHG3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG4), '0'), 38, 10) is null and 
                    src:RTCHG4 is not null) THEN
                    'RTCHG4 contains a non-numeric value : \'' || AS_VARCHAR(src:RTCHG4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG5), '0'), 38, 10) is null and 
                    src:RTCHG5 is not null) THEN
                    'RTCHG5 contains a non-numeric value : \'' || AS_VARCHAR(src:RTCHG5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG6), '0'), 38, 10) is null and 
                    src:RTCHG6 is not null) THEN
                    'RTCHG6 contains a non-numeric value : \'' || AS_VARCHAR(src:RTCHG6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG7), '0'), 38, 10) is null and 
                    src:RTCHG7 is not null) THEN
                    'RTCHG7 contains a non-numeric value : \'' || AS_VARCHAR(src:RTCHG7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG8), '0'), 38, 10) is null and 
                    src:RTCHG8 is not null) THEN
                    'RTCHG8 contains a non-numeric value : \'' || AS_VARCHAR(src:RTCHG8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG9), '0'), 38, 10) is null and 
                    src:RTCHG9 is not null) THEN
                    'RTCHG9 contains a non-numeric value : \'' || AS_VARCHAR(src:RTCHG9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTMAXCHG), '0'), 38, 10) is null and 
                    src:RTMAXCHG is not null) THEN
                    'RTMAXCHG contains a non-numeric value : \'' || AS_VARCHAR(src:RTMAXCHG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTMINCHG), '0'), 38, 10) is null and 
                    src:RTMINCHG is not null) THEN
                    'RTMINCHG contains a non-numeric value : \'' || AS_VARCHAR(src:RTMINCHG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP1), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP1 is not null) THEN
                    'RTPRORATESTEP1 contains a non-numeric value : \'' || AS_VARCHAR(src:RTPRORATESTEP1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP10), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP10 is not null) THEN
                    'RTPRORATESTEP10 contains a non-numeric value : \'' || AS_VARCHAR(src:RTPRORATESTEP10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP2), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP2 is not null) THEN
                    'RTPRORATESTEP2 contains a non-numeric value : \'' || AS_VARCHAR(src:RTPRORATESTEP2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP3), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP3 is not null) THEN
                    'RTPRORATESTEP3 contains a non-numeric value : \'' || AS_VARCHAR(src:RTPRORATESTEP3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP4), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP4 is not null) THEN
                    'RTPRORATESTEP4 contains a non-numeric value : \'' || AS_VARCHAR(src:RTPRORATESTEP4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP5), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP5 is not null) THEN
                    'RTPRORATESTEP5 contains a non-numeric value : \'' || AS_VARCHAR(src:RTPRORATESTEP5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP6), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP6 is not null) THEN
                    'RTPRORATESTEP6 contains a non-numeric value : \'' || AS_VARCHAR(src:RTPRORATESTEP6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP7), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP7 is not null) THEN
                    'RTPRORATESTEP7 contains a non-numeric value : \'' || AS_VARCHAR(src:RTPRORATESTEP7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP8), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP8 is not null) THEN
                    'RTPRORATESTEP8 contains a non-numeric value : \'' || AS_VARCHAR(src:RTPRORATESTEP8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP9), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP9 is not null) THEN
                    'RTPRORATESTEP9 contains a non-numeric value : \'' || AS_VARCHAR(src:RTPRORATESTEP9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTSTDDAYS), '0'), 38, 10) is null and 
                    src:RTSTDDAYS is not null) THEN
                    'RTSTDDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:RTSTDDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUNIT), '0'), 38, 10) is null and 
                    src:RTUNIT is not null) THEN
                    'RTUNIT contains a non-numeric value : \'' || AS_VARCHAR(src:RTUNIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG1), '0'), 38, 10) is null and 
                    src:RTUSG1 is not null) THEN
                    'RTUSG1 contains a non-numeric value : \'' || AS_VARCHAR(src:RTUSG1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG10), '0'), 38, 10) is null and 
                    src:RTUSG10 is not null) THEN
                    'RTUSG10 contains a non-numeric value : \'' || AS_VARCHAR(src:RTUSG10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG2), '0'), 38, 10) is null and 
                    src:RTUSG2 is not null) THEN
                    'RTUSG2 contains a non-numeric value : \'' || AS_VARCHAR(src:RTUSG2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG3), '0'), 38, 10) is null and 
                    src:RTUSG3 is not null) THEN
                    'RTUSG3 contains a non-numeric value : \'' || AS_VARCHAR(src:RTUSG3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG4), '0'), 38, 10) is null and 
                    src:RTUSG4 is not null) THEN
                    'RTUSG4 contains a non-numeric value : \'' || AS_VARCHAR(src:RTUSG4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG5), '0'), 38, 10) is null and 
                    src:RTUSG5 is not null) THEN
                    'RTUSG5 contains a non-numeric value : \'' || AS_VARCHAR(src:RTUSG5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG6), '0'), 38, 10) is null and 
                    src:RTUSG6 is not null) THEN
                    'RTUSG6 contains a non-numeric value : \'' || AS_VARCHAR(src:RTUSG6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG7), '0'), 38, 10) is null and 
                    src:RTUSG7 is not null) THEN
                    'RTUSG7 contains a non-numeric value : \'' || AS_VARCHAR(src:RTUSG7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG8), '0'), 38, 10) is null and 
                    src:RTUSG8 is not null) THEN
                    'RTUSG8 contains a non-numeric value : \'' || AS_VARCHAR(src:RTUSG8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG9), '0'), 38, 10) is null and 
                    src:RTUSG9 is not null) THEN
                    'RTUSG9 contains a non-numeric value : \'' || AS_VARCHAR(src:RTUSG9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSGDAYS), '0'), 38, 10) is null and 
                    src:RTUSGDAYS is not null) THEN
                    'RTUSGDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:RTUSGDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SINGLERATE), '0'), 38, 10) is null and 
                    src:SINGLERATE is not null) THEN
                    'SINGLERATE contains a non-numeric value : \'' || AS_VARCHAR(src:SINGLERATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRVUSGDAYS), '0'), 38, 10) is null and 
                    src:SRVUSGDAYS is not null) THEN
                    'SRVUSGDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:SRVUSGDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALBILLABLEUSAGE), '0'), 38, 10) is null and 
                    src:TOTALBILLABLEUSAGE is not null) THEN
                    'TOTALBILLABLEUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:TOTALBILLABLEUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALUSAGEDAYS), '0'), 38, 10) is null and 
                    src:TOTALUSAGEDAYS is not null) THEN
                    'TOTALUSAGEDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:TOTALUSAGEDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USEFORPRORATING), '0'), 38, 10) is null and 
                    src:USEFORPRORATING is not null) THEN
                    'USEFORPRORATING contains a non-numeric value : \'' || AS_VARCHAR(src:USEFORPRORATING) || '\'' WHEN 
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
                                    
                src:CALCULATIONDETAILKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_RATETABLE_CALCULATIONDETAIL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BASEBILLABLEUSAGE), '0'), 38, 10) is null and 
                    src:BASEBILLABLEUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCULATIONDETAILKEY), '0'), 38, 10) is null and 
                    src:CALCULATIONDETAILKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCULATIONTABLE), '0'), 38, 10) is null and 
                    src:CALCULATIONTABLE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONVERTEDUNITS), '0'), 38, 10) is null and 
                    src:CONVERTEDUNITS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYSBETWEENREADS), '0'), 38, 10) is null and 
                    src:DAYSBETWEENREADS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFTODATE), '1900-01-01')) is null and 
                    src:EFFTODATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FULLAMT), '0'), 38, 10) is null and 
                    src:FULLAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFSTEPS), '0'), 38, 10) is null and 
                    src:NUMBEROFSTEPS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRORATEAMT), '0'), 38, 10) is null and 
                    src:PRORATEAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATETABLEDETAILKEY), '0'), 38, 10) is null and 
                    src:RATETABLEDETAILKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG1), '0'), 38, 10) is null and 
                    src:RTCHG1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG10), '0'), 38, 10) is null and 
                    src:RTCHG10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG2), '0'), 38, 10) is null and 
                    src:RTCHG2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG3), '0'), 38, 10) is null and 
                    src:RTCHG3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG4), '0'), 38, 10) is null and 
                    src:RTCHG4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG5), '0'), 38, 10) is null and 
                    src:RTCHG5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG6), '0'), 38, 10) is null and 
                    src:RTCHG6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG7), '0'), 38, 10) is null and 
                    src:RTCHG7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG8), '0'), 38, 10) is null and 
                    src:RTCHG8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTCHG9), '0'), 38, 10) is null and 
                    src:RTCHG9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTMAXCHG), '0'), 38, 10) is null and 
                    src:RTMAXCHG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTMINCHG), '0'), 38, 10) is null and 
                    src:RTMINCHG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP1), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP10), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP2), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP3), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP4), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP5), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP6), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP7), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP8), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTPRORATESTEP9), '0'), 38, 10) is null and 
                    src:RTPRORATESTEP9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTSTDDAYS), '0'), 38, 10) is null and 
                    src:RTSTDDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUNIT), '0'), 38, 10) is null and 
                    src:RTUNIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG1), '0'), 38, 10) is null and 
                    src:RTUSG1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG10), '0'), 38, 10) is null and 
                    src:RTUSG10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG2), '0'), 38, 10) is null and 
                    src:RTUSG2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG3), '0'), 38, 10) is null and 
                    src:RTUSG3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG4), '0'), 38, 10) is null and 
                    src:RTUSG4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG5), '0'), 38, 10) is null and 
                    src:RTUSG5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG6), '0'), 38, 10) is null and 
                    src:RTUSG6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG7), '0'), 38, 10) is null and 
                    src:RTUSG7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG8), '0'), 38, 10) is null and 
                    src:RTUSG8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSG9), '0'), 38, 10) is null and 
                    src:RTUSG9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTUSGDAYS), '0'), 38, 10) is null and 
                    src:RTUSGDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SINGLERATE), '0'), 38, 10) is null and 
                    src:SINGLERATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRVUSGDAYS), '0'), 38, 10) is null and 
                    src:SRVUSGDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALBILLABLEUSAGE), '0'), 38, 10) is null and 
                    src:TOTALBILLABLEUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALUSAGEDAYS), '0'), 38, 10) is null and 
                    src:TOTALUSAGEDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USEFORPRORATING), '0'), 38, 10) is null and 
                    src:USEFORPRORATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)