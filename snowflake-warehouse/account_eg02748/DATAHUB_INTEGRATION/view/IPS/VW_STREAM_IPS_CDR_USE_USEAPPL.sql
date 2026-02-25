CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_USE_USEAPPL AS SELECT
                        src:ACTLVLTN::numeric(38, 10) AS ACTLVLTN, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:APBY::varchar AS APBY, 
                        src:APDTTM::datetime AS APDTTM, 
                        src:APKEY::integer AS APKEY, 
                        src:APMODLKEY::integer AS APMODLKEY, 
                        src:APNAME::varchar AS APNAME, 
                        src:APNO::varchar AS APNO, 
                        src:APPRJKEY::integer AS APPRJKEY, 
                        src:APUSEDEFNKEY::integer AS APUSEDEFNKEY, 
                        src:APUSEKEY::integer AS APUSEKEY, 
                        src:APUSEPROCESSSTATEKEY::integer AS APUSEPROCESSSTATEKEY, 
                        src:BATCHKEY::integer AS BATCHKEY, 
                        src:BLDGFLOOR::integer AS BLDGFLOOR, 
                        src:BLDGROOM::integer AS BLDGROOM, 
                        src:CALCVLTN::numeric(38, 10) AS CALCVLTN, 
                        src:CAPACITY::varchar AS CAPACITY, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:COOBY::varchar AS COOBY, 
                        src:COODTTM::datetime AS COODTTM, 
                        src:DECLVLTN::numeric(38, 10) AS DECLVLTN, 
                        src:DELETED::boolean AS DELETED, 
                        src:ENDDTTM::datetime AS ENDDTTM, 
                        src:EXPDTTM::datetime AS EXPDTTM, 
                        src:FINBY::varchar AS FINBY, 
                        src:FINDTTM::datetime AS FINDTTM, 
                        src:GROUPKEY::integer AS GROUPKEY, 
                        src:ISSBY::varchar AS ISSBY, 
                        src:ISSDTTM::datetime AS ISSDTTM, 
                        src:LINEARFROMFT::numeric(38, 10) AS LINEARFROMFT, 
                        src:LINEARFROMM::numeric(38, 10) AS LINEARFROMM, 
                        src:LINEARTOFT::numeric(38, 10) AS LINEARTOFT, 
                        src:LINEARTOM::numeric(38, 10) AS LINEARTOM, 
                        src:LOC::varchar AS LOC, 
                        src:LSTACTDTTM::datetime AS LSTACTDTTM, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOPAGES::integer AS NOPAGES, 
                        src:NOPLANS::integer AS NOPLANS, 
                        src:OCCUPANCYTYPE::varchar AS OCCUPANCYTYPE, 
                        src:PLANREV::varchar AS PLANREV, 
                        src:PRCLKEY::integer AS PRCLKEY, 
                        src:PRI::varchar AS PRI, 
                        src:PROPERTYCONVEYANCE::integer AS PROPERTYCONVEYANCE, 
                        src:PROPERTYCRANEDERRICK::integer AS PROPERTYCRANEDERRICK, 
                        src:PROPERTYEQUIPMENT::integer AS PROPERTYEQUIPMENT, 
                        src:PROPERTYFACADE::integer AS PROPERTYFACADE, 
                        src:PROPERTYMATERIAL::integer AS PROPERTYMATERIAL, 
                        src:PROPERTYVESSEL::integer AS PROPERTYVESSEL, 
                        src:PROPKEY::integer AS PROPKEY, 
                        src:PROPOCCUPKEY::integer AS PROPOCCUPKEY, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
                        src:STAT::varchar AS STAT, 
                        src:TMPCOOBY::varchar AS TMPCOOBY, 
                        src:TMPCOODTTM::datetime AS TMPCOODTTM, 
                        src:USEAPPLSTATUS::varchar AS USEAPPLSTATUS, 
                        src:USEAREA::numeric(38, 10) AS USEAREA, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WORKTYPE::varchar AS WORKTYPE, 
                        src:XCOORDINATE::numeric(38, 10) AS XCOORDINATE, 
                        src:YCOORDINATE::numeric(38, 10) AS YCOORDINATE, 
                        src:ZCOORDINATE::numeric(38, 10) AS ZCOORDINATE, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
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
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACTLVLTN), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:APDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APMODLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APPRJKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APUSEDEFNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APUSEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APUSEPROCESSSTATEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BATCHKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CALCVLTN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:COODTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DECLVLTN), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ENDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:FINDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GROUPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ISSDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARFROMFT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARFROMM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARTOFT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARTOM), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LSTACTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NOPAGES), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NOPLANS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROPERTYCONVEYANCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROPERTYCRANEDERRICK), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROPERTYEQUIPMENT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROPERTYFACADE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROPERTYMATERIAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROPERTYVESSEL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROPOCCUPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TMPCOODTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USEAREA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:YCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ZCOORDINATE), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null