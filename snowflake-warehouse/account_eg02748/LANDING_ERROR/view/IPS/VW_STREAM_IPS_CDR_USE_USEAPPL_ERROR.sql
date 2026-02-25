CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_USE_USEAPPL_ERROR AS SELECT src, 'IPS_CDR_USE_USEAPPL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACTLVLTN), '0'), 38, 10) is null and 
                    src:ACTLVLTN is not null) THEN
                    'ACTLVLTN contains a non-numeric value : \'' || AS_VARCHAR(src:ACTLVLTN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) THEN
                    'ADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APDTTM), '1900-01-01')) is null and 
                    src:APDTTM is not null) THEN
                    'APDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:APDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APKEY), '0'), 38, 10) is null and 
                    src:APKEY is not null) THEN
                    'APKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APMODLKEY), '0'), 38, 10) is null and 
                    src:APMODLKEY is not null) THEN
                    'APMODLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APMODLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPRJKEY), '0'), 38, 10) is null and 
                    src:APPRJKEY is not null) THEN
                    'APPRJKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPRJKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEDEFNKEY), '0'), 38, 10) is null and 
                    src:APUSEDEFNKEY is not null) THEN
                    'APUSEDEFNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APUSEDEFNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEKEY), '0'), 38, 10) is null and 
                    src:APUSEKEY is not null) THEN
                    'APUSEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APUSEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEPROCESSSTATEKEY), '0'), 38, 10) is null and 
                    src:APUSEPROCESSSTATEKEY is not null) THEN
                    'APUSEPROCESSSTATEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APUSEPROCESSSTATEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHKEY), '0'), 38, 10) is null and 
                    src:BATCHKEY is not null) THEN
                    'BATCHKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BATCHKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is null and 
                    src:BLDGFLOOR is not null) THEN
                    'BLDGFLOOR contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGFLOOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is null and 
                    src:BLDGROOM is not null) THEN
                    'BLDGROOM contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGROOM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCVLTN), '0'), 38, 10) is null and 
                    src:CALCVLTN is not null) THEN
                    'CALCVLTN contains a non-numeric value : \'' || AS_VARCHAR(src:CALCVLTN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COODTTM), '1900-01-01')) is null and 
                    src:COODTTM is not null) THEN
                    'COODTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:COODTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DECLVLTN), '0'), 38, 10) is null and 
                    src:DECLVLTN is not null) THEN
                    'DECLVLTN contains a non-numeric value : \'' || AS_VARCHAR(src:DECLVLTN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENDDTTM), '1900-01-01')) is null and 
                    src:ENDDTTM is not null) THEN
                    'ENDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ENDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDTTM), '1900-01-01')) is null and 
                    src:EXPDTTM is not null) THEN
                    'EXPDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FINDTTM), '1900-01-01')) is null and 
                    src:FINDTTM is not null) THEN
                    'FINDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:FINDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GROUPKEY), '0'), 38, 10) is null and 
                    src:GROUPKEY is not null) THEN
                    'GROUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:GROUPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ISSDTTM), '1900-01-01')) is null and 
                    src:ISSDTTM is not null) THEN
                    'ISSDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ISSDTTM) || '\'' WHEN 
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LSTACTDTTM), '1900-01-01')) is null and 
                    src:LSTACTDTTM is not null) THEN
                    'LSTACTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:LSTACTDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOPAGES), '0'), 38, 10) is null and 
                    src:NOPAGES is not null) THEN
                    'NOPAGES contains a non-numeric value : \'' || AS_VARCHAR(src:NOPAGES) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOPLANS), '0'), 38, 10) is null and 
                    src:NOPLANS is not null) THEN
                    'NOPLANS contains a non-numeric value : \'' || AS_VARCHAR(src:NOPLANS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is null and 
                    src:PRCLKEY is not null) THEN
                    'PRCLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PRCLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYCONVEYANCE), '0'), 38, 10) is null and 
                    src:PROPERTYCONVEYANCE is not null) THEN
                    'PROPERTYCONVEYANCE contains a non-numeric value : \'' || AS_VARCHAR(src:PROPERTYCONVEYANCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYCRANEDERRICK), '0'), 38, 10) is null and 
                    src:PROPERTYCRANEDERRICK is not null) THEN
                    'PROPERTYCRANEDERRICK contains a non-numeric value : \'' || AS_VARCHAR(src:PROPERTYCRANEDERRICK) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYEQUIPMENT), '0'), 38, 10) is null and 
                    src:PROPERTYEQUIPMENT is not null) THEN
                    'PROPERTYEQUIPMENT contains a non-numeric value : \'' || AS_VARCHAR(src:PROPERTYEQUIPMENT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYFACADE), '0'), 38, 10) is null and 
                    src:PROPERTYFACADE is not null) THEN
                    'PROPERTYFACADE contains a non-numeric value : \'' || AS_VARCHAR(src:PROPERTYFACADE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYMATERIAL), '0'), 38, 10) is null and 
                    src:PROPERTYMATERIAL is not null) THEN
                    'PROPERTYMATERIAL contains a non-numeric value : \'' || AS_VARCHAR(src:PROPERTYMATERIAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYVESSEL), '0'), 38, 10) is null and 
                    src:PROPERTYVESSEL is not null) THEN
                    'PROPERTYVESSEL contains a non-numeric value : \'' || AS_VARCHAR(src:PROPERTYVESSEL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPKEY), '0'), 38, 10) is null and 
                    src:PROPKEY is not null) THEN
                    'PROPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PROPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPOCCUPKEY), '0'), 38, 10) is null and 
                    src:PROPOCCUPKEY is not null) THEN
                    'PROPOCCUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PROPOCCUPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) THEN
                    'STARTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TMPCOODTTM), '1900-01-01')) is null and 
                    src:TMPCOODTTM is not null) THEN
                    'TMPCOODTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:TMPCOODTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USEAREA), '0'), 38, 10) is null and 
                    src:USEAREA is not null) THEN
                    'USEAREA contains a non-numeric value : \'' || AS_VARCHAR(src:USEAREA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
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
                                    
                src:APUSEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_USE_USEAPPL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACTLVLTN), '0'), 38, 10) is null and 
                    src:ACTLVLTN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APDTTM), '1900-01-01')) is null and 
                    src:APDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APKEY), '0'), 38, 10) is null and 
                    src:APKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APMODLKEY), '0'), 38, 10) is null and 
                    src:APMODLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPRJKEY), '0'), 38, 10) is null and 
                    src:APPRJKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEDEFNKEY), '0'), 38, 10) is null and 
                    src:APUSEDEFNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEKEY), '0'), 38, 10) is null and 
                    src:APUSEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEPROCESSSTATEKEY), '0'), 38, 10) is null and 
                    src:APUSEPROCESSSTATEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHKEY), '0'), 38, 10) is null and 
                    src:BATCHKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is null and 
                    src:BLDGFLOOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is null and 
                    src:BLDGROOM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCVLTN), '0'), 38, 10) is null and 
                    src:CALCVLTN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COODTTM), '1900-01-01')) is null and 
                    src:COODTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DECLVLTN), '0'), 38, 10) is null and 
                    src:DECLVLTN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENDDTTM), '1900-01-01')) is null and 
                    src:ENDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDTTM), '1900-01-01')) is null and 
                    src:EXPDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FINDTTM), '1900-01-01')) is null and 
                    src:FINDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GROUPKEY), '0'), 38, 10) is null and 
                    src:GROUPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ISSDTTM), '1900-01-01')) is null and 
                    src:ISSDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARFROMFT), '0'), 38, 10) is null and 
                    src:LINEARFROMFT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARFROMM), '0'), 38, 10) is null and 
                    src:LINEARFROMM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARTOFT), '0'), 38, 10) is null and 
                    src:LINEARTOFT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARTOM), '0'), 38, 10) is null and 
                    src:LINEARTOM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LSTACTDTTM), '1900-01-01')) is null and 
                    src:LSTACTDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOPAGES), '0'), 38, 10) is null and 
                    src:NOPAGES is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOPLANS), '0'), 38, 10) is null and 
                    src:NOPLANS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is null and 
                    src:PRCLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYCONVEYANCE), '0'), 38, 10) is null and 
                    src:PROPERTYCONVEYANCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYCRANEDERRICK), '0'), 38, 10) is null and 
                    src:PROPERTYCRANEDERRICK is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYEQUIPMENT), '0'), 38, 10) is null and 
                    src:PROPERTYEQUIPMENT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYFACADE), '0'), 38, 10) is null and 
                    src:PROPERTYFACADE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYMATERIAL), '0'), 38, 10) is null and 
                    src:PROPERTYMATERIAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYVESSEL), '0'), 38, 10) is null and 
                    src:PROPERTYVESSEL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPKEY), '0'), 38, 10) is null and 
                    src:PROPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPOCCUPKEY), '0'), 38, 10) is null and 
                    src:PROPOCCUPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TMPCOODTTM), '1900-01-01')) is null and 
                    src:TMPCOODTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USEAREA), '0'), 38, 10) is null and 
                    src:USEAREA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCOORDINATE), '0'), 38, 10) is null and 
                    src:XCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:YCOORDINATE), '0'), 38, 10) is null and 
                    src:YCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ZCOORDINATE), '0'), 38, 10) is null and 
                    src:ZCOORDINATE is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)