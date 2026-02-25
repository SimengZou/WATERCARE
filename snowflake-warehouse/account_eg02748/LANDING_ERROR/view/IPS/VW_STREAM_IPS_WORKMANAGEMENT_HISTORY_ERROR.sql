CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WORKMANAGEMENT_HISTORY_ERROR AS SELECT src, 'IPS_WORKMANAGEMENT_HISTORY' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACTKEY), '0'), 38, 10) is null and 
                    src:ACTKEY is not null) THEN
                    'ACTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACTUALQTY), '0'), 38, 10) is null and 
                    src:ACTUALQTY is not null) THEN
                    'ACTUALQTY contains a non-numeric value : \'' || AS_VARCHAR(src:ACTUALQTY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) THEN
                    'ADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETINSPECTIONKEY), '0'), 38, 10) is null and 
                    src:ASSETINSPECTIONKEY is not null) THEN
                    'ASSETINSPECTIONKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSETINSPECTIONKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is null and 
                    src:BLDGFLOOR is not null) THEN
                    'BLDGFLOOR contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGFLOOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is null and 
                    src:BLDGROOM is not null) THEN
                    'BLDGROOM contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGROOM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BUDGETKEY), '0'), 38, 10) is null and 
                    src:BUDGETKEY is not null) THEN
                    'BUDGETKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BUDGETKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CMPLKEY), '0'), 38, 10) is null and 
                    src:CMPLKEY is not null) THEN
                    'CMPLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CMPLKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COMPDTTM), '1900-01-01')) is null and 
                    src:COMPDTTM is not null) THEN
                    'COMPDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:COMPDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPTYPE), '0'), 38, 10) is null and 
                    src:COMPTYPE is not null) THEN
                    'COMPTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:COMPTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIST), '0'), 38, 10) is null and 
                    src:DIST is not null) THEN
                    'DIST contains a non-numeric value : \'' || AS_VARCHAR(src:DIST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOWNTIME), '0'), 38, 10) is null and 
                    src:DOWNTIME is not null) THEN
                    'DOWNTIME contains a non-numeric value : \'' || AS_VARCHAR(src:DOWNTIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTIMATEDCOST), '0'), 38, 10) is null and 
                    src:ESTIMATEDCOST is not null) THEN
                    'ESTIMATEDCOST contains a non-numeric value : \'' || AS_VARCHAR(src:ESTIMATEDCOST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FLOWDPTH), '0'), 38, 10) is null and 
                    src:FLOWDPTH is not null) THEN
                    'FLOWDPTH contains a non-numeric value : \'' || AS_VARCHAR(src:FLOWDPTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FRREF), '0'), 38, 10) is null and 
                    src:FRREF is not null) THEN
                    'FRREF contains a non-numeric value : \'' || AS_VARCHAR(src:FRREF) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRPMAINTSCHD), '0'), 38, 10) is null and 
                    src:GRPMAINTSCHD is not null) THEN
                    'GRPMAINTSCHD contains a non-numeric value : \'' || AS_VARCHAR(src:GRPMAINTSCHD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRPPROJKEY), '0'), 38, 10) is null and 
                    src:GRPPROJKEY is not null) THEN
                    'GRPPROJKEY contains a non-numeric value : \'' || AS_VARCHAR(src:GRPPROJKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HISTKEY), '0'), 38, 10) is null and 
                    src:HISTKEY is not null) THEN
                    'HISTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:HISTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HRS), '0'), 38, 10) is null and 
                    src:HRS is not null) THEN
                    'HRS contains a non-numeric value : \'' || AS_VARCHAR(src:HRS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INCIDENT), '0'), 38, 10) is null and 
                    src:INCIDENT is not null) THEN
                    'INCIDENT contains a non-numeric value : \'' || AS_VARCHAR(src:INCIDENT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITDTTM), '1900-01-01')) is null and 
                    src:INITDTTM is not null) THEN
                    'INITDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:INITDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARFROMFT), '0'), 38, 10) is null and 
                    src:LINEARFROMFT is not null) THEN
                    'LINEARFROMFT contains a non-numeric value : \'' || AS_VARCHAR(src:LINEARFROMFT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARFROMM), '0'), 38, 10) is null and 
                    src:LINEARFROMM is not null) THEN
                    'LINEARFROMM contains a non-numeric value : \'' || AS_VARCHAR(src:LINEARFROMM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARTOFT), '0'), 38, 10) is null and 
                    src:LINEARTOFT is not null) THEN
                    'LINEARTOFT contains a non-numeric value : \'' || AS_VARCHAR(src:LINEARTOFT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARTOM), '0'), 38, 10) is null and 
                    src:LINEARTOM is not null) THEN
                    'LINEARTOM contains a non-numeric value : \'' || AS_VARCHAR(src:LINEARTOM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINTSCHED), '0'), 38, 10) is null and 
                    src:MAINTSCHED is not null) THEN
                    'MAINTSCHED contains a non-numeric value : \'' || AS_VARCHAR(src:MAINTSCHED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MEASFLOW), '0'), 38, 10) is null and 
                    src:MEASFLOW is not null) THEN
                    'MEASFLOW contains a non-numeric value : \'' || AS_VARCHAR(src:MEASFLOW) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MILESTONE), '0'), 38, 10) is null and 
                    src:MILESTONE is not null) THEN
                    'MILESTONE contains a non-numeric value : \'' || AS_VARCHAR(src:MILESTONE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MOBILEDTTM), '1900-01-01')) is null and 
                    src:MOBILEDTTM is not null) THEN
                    'MOBILEDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MOBILEDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OWNKEY), '0'), 38, 10) is null and 
                    src:OWNKEY is not null) THEN
                    'OWNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:OWNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANKEY), '0'), 38, 10) is null and 
                    src:PLANKEY is not null) THEN
                    'PLANKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PLANKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANNEDQTY), '0'), 38, 10) is null and 
                    src:PLANNEDQTY is not null) THEN
                    'PLANNEDQTY contains a non-numeric value : \'' || AS_VARCHAR(src:PLANNEDQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is null and 
                    src:PRCLKEY is not null) THEN
                    'PRCLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PRCLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRES), '0'), 38, 10) is null and 
                    src:PRES is not null) THEN
                    'PRES contains a non-numeric value : \'' || AS_VARCHAR(src:PRES) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RWATTRKEY), '0'), 38, 10) is null and 
                    src:RWATTRKEY is not null) THEN
                    'RWATTRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RWATTRKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCHEDDTTM), '1900-01-01')) is null and 
                    src:SCHEDDTTM is not null) THEN
                    'SCHEDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:SCHEDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCHEDFINISH), '1900-01-01')) is null and 
                    src:SCHEDFINISH is not null) THEN
                    'SCHEDFINISH contains a non-timestamp value : \'' || AS_VARCHAR(src:SCHEDFINISH) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) THEN
                    'STARTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUSPDTTM), '1900-01-01')) is null and 
                    src:SUSPDTTM is not null) THEN
                    'SUSPDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:SUSPDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOREF), '0'), 38, 10) is null and 
                    src:TOREF is not null) THEN
                    'TOREF contains a non-numeric value : \'' || AS_VARCHAR(src:TOREF) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USG), '0'), 38, 10) is null and 
                    src:USG is not null) THEN
                    'USG contains a non-numeric value : \'' || AS_VARCHAR(src:USG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WOLINKKEY), '0'), 38, 10) is null and 
                    src:WOLINKKEY is not null) THEN
                    'WOLINKKEY contains a non-numeric value : \'' || AS_VARCHAR(src:WOLINKKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCOORDINATE), '0'), 38, 10) is null and 
                    src:XCOORDINATE is not null) THEN
                    'XCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:XCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:YCOORDINATE), '0'), 38, 10) is null and 
                    src:YCOORDINATE is not null) THEN
                    'YCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:YCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ZCOORDINATE), '0'), 38, 10) is null and 
                    src:ZCOORDINATE is not null) THEN
                    'ZCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:ZCOORDINATE) || '\'' WHEN 
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
                                    
                src:HISTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WORKMANAGEMENT_HISTORY as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACTKEY), '0'), 38, 10) is null and 
                    src:ACTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACTUALQTY), '0'), 38, 10) is null and 
                    src:ACTUALQTY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETINSPECTIONKEY), '0'), 38, 10) is null and 
                    src:ASSETINSPECTIONKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is null and 
                    src:BLDGFLOOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is null and 
                    src:BLDGROOM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BUDGETKEY), '0'), 38, 10) is null and 
                    src:BUDGETKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CMPLKEY), '0'), 38, 10) is null and 
                    src:CMPLKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COMPDTTM), '1900-01-01')) is null and 
                    src:COMPDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPTYPE), '0'), 38, 10) is null and 
                    src:COMPTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIST), '0'), 38, 10) is null and 
                    src:DIST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOWNTIME), '0'), 38, 10) is null and 
                    src:DOWNTIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTIMATEDCOST), '0'), 38, 10) is null and 
                    src:ESTIMATEDCOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FLOWDPTH), '0'), 38, 10) is null and 
                    src:FLOWDPTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FRREF), '0'), 38, 10) is null and 
                    src:FRREF is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRPMAINTSCHD), '0'), 38, 10) is null and 
                    src:GRPMAINTSCHD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRPPROJKEY), '0'), 38, 10) is null and 
                    src:GRPPROJKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HISTKEY), '0'), 38, 10) is null and 
                    src:HISTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HRS), '0'), 38, 10) is null and 
                    src:HRS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INCIDENT), '0'), 38, 10) is null and 
                    src:INCIDENT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITDTTM), '1900-01-01')) is null and 
                    src:INITDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARFROMFT), '0'), 38, 10) is null and 
                    src:LINEARFROMFT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARFROMM), '0'), 38, 10) is null and 
                    src:LINEARFROMM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARTOFT), '0'), 38, 10) is null and 
                    src:LINEARTOFT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARTOM), '0'), 38, 10) is null and 
                    src:LINEARTOM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINTSCHED), '0'), 38, 10) is null and 
                    src:MAINTSCHED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MEASFLOW), '0'), 38, 10) is null and 
                    src:MEASFLOW is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MILESTONE), '0'), 38, 10) is null and 
                    src:MILESTONE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MOBILEDTTM), '1900-01-01')) is null and 
                    src:MOBILEDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OWNKEY), '0'), 38, 10) is null and 
                    src:OWNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANKEY), '0'), 38, 10) is null and 
                    src:PLANKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANNEDQTY), '0'), 38, 10) is null and 
                    src:PLANNEDQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is null and 
                    src:PRCLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRES), '0'), 38, 10) is null and 
                    src:PRES is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RWATTRKEY), '0'), 38, 10) is null and 
                    src:RWATTRKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCHEDDTTM), '1900-01-01')) is null and 
                    src:SCHEDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCHEDFINISH), '1900-01-01')) is null and 
                    src:SCHEDFINISH is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUSPDTTM), '1900-01-01')) is null and 
                    src:SUSPDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOREF), '0'), 38, 10) is null and 
                    src:TOREF is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USG), '0'), 38, 10) is null and 
                    src:USG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WOLINKKEY), '0'), 38, 10) is null and 
                    src:WOLINKKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCOORDINATE), '0'), 38, 10) is null and 
                    src:XCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:YCOORDINATE), '0'), 38, 10) is null and 
                    src:YCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ZCOORDINATE), '0'), 38, 10) is null and 
                    src:ZCOORDINATE is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)