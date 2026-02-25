CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_BUILDING_BLDGAPPL AS SELECT
                        src:ACTLVLTN::numeric(38, 10) AS ACTLVLTN, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:APBLDGBATCHKEY::integer AS APBLDGBATCHKEY, 
                        src:APBLDGDEFNKEY::integer AS APBLDGDEFNKEY, 
                        src:APBLDGKEY::integer AS APBLDGKEY, 
                        src:APBLDGPROCESSSTATEKEY::integer AS APBLDGPROCESSSTATEKEY, 
                        src:APBY::varchar AS APBY, 
                        src:APDTTM::datetime AS APDTTM, 
                        src:APKEY::integer AS APKEY, 
                        src:APMODELPROJKEY::integer AS APMODELPROJKEY, 
                        src:APNAME::varchar AS APNAME, 
                        src:APNO::varchar AS APNO, 
                        src:BLDGAPPLSTATUS::varchar AS BLDGAPPLSTATUS, 
                        src:BLDGAREA::numeric(38, 10) AS BLDGAREA, 
                        src:BLDGFLOOR::integer AS BLDGFLOOR, 
                        src:BLDGROOM::integer AS BLDGROOM, 
                        src:CALCVLTN::numeric(38, 10) AS CALCVLTN, 
                        src:CAPACITY::varchar AS CAPACITY, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:COOBY::varchar AS COOBY, 
                        src:COODTTM::datetime AS COODTTM, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DECLVLTN::numeric(38, 10) AS DECLVLTN, 
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
                        src:LOCNOTES::varchar AS LOCNOTES, 
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
                        src:STAT::varchar AS STAT, 
                        src:TMPCOOBY::varchar AS TMPCOOBY, 
                        src:TMPCOODTTM::datetime AS TMPCOODTTM, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WORKTYPE::varchar AS WORKTYPE, 
                        src:XCOORDINATE::numeric(38, 10) AS XCOORDINATE, 
                        src:YCOORDINATE::numeric(38, 10) AS YCOORDINATE, 
                        src:ZCOORDINATE::numeric(38, 10) AS ZCOORDINATE, 
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:APBLDGKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APBLDGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_BUILDING_BLDGAPPL as strm))