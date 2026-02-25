CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_PLANNING_PLANAPPL AS SELECT
                        src:ACTLVLTN::numeric(38, 10) AS ACTLVLTN, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:APBY::varchar AS APBY, 
                        src:APDTTM::datetime AS APDTTM, 
                        src:APKEY::integer AS APKEY, 
                        src:APNO::varchar AS APNO, 
                        src:APPLANDEFNKEY::integer AS APPLANDEFNKEY, 
                        src:APPLANKEY::integer AS APPLANKEY, 
                        src:APPLANPROCESSSTATEKEY::integer AS APPLANPROCESSSTATEKEY, 
                        src:APRVBY::varchar AS APRVBY, 
                        src:APRVDTTM::datetime AS APRVDTTM, 
                        src:AREAUM::varchar AS AREAUM, 
                        src:BILLGRP::varchar AS BILLGRP, 
                        src:BLDGAREA::numeric(38, 10) AS BLDGAREA, 
                        src:BLDGFLOOR::integer AS BLDGFLOOR, 
                        src:BLDGROOM::integer AS BLDGROOM, 
                        src:CALCVLTN::numeric(38, 10) AS CALCVLTN, 
                        src:CAPACITY::varchar AS CAPACITY, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:COMPVAL::numeric(38, 10) AS COMPVAL, 
                        src:COOBY::varchar AS COOBY, 
                        src:COODTTM::datetime AS COODTTM, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DECLVLTN::numeric(38, 10) AS DECLVLTN, 
                        src:EXPDTTM::datetime AS EXPDTTM, 
                        src:FINBY::varchar AS FINBY, 
                        src:FINDTTM::datetime AS FINDTTM, 
                        src:GROUPKEY::integer AS GROUPKEY, 
                        src:LINEARFROMFT::numeric(38, 10) AS LINEARFROMFT, 
                        src:LINEARFROMM::numeric(38, 10) AS LINEARFROMM, 
                        src:LINEARTOFT::numeric(38, 10) AS LINEARTOFT, 
                        src:LINEARTOM::numeric(38, 10) AS LINEARTOM, 
                        src:LOC::varchar AS LOC, 
                        src:LOCNOTES::varchar AS LOCNOTES, 
                        src:LSTACTDTTM::datetime AS LSTACTDTTM, 
                        src:LSTPRNDTTM::datetime AS LSTPRNDTTM, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOPAGES::integer AS NOPAGES, 
                        src:NOPLANS::integer AS NOPLANS, 
                        src:OCCUPANCYTYPE::varchar AS OCCUPANCYTYPE, 
                        src:PHASENO::varchar AS PHASENO, 
                        src:PLANAPPLSTATUS::varchar AS PLANAPPLSTATUS, 
                        src:PLANREV::varchar AS PLANREV, 
                        src:PRCLKEY::integer AS PRCLKEY, 
                        src:PRI::varchar AS PRI, 
                        src:PROJNAME::varchar AS PROJNAME, 
                        src:PROPKEY::integer AS PROPKEY, 
                        src:PROPOCCUPKEY::integer AS PROPOCCUPKEY, 
                        src:PRPSTOPDT::datetime AS PRPSTOPDT, 
                        src:PRPSTRTDT::datetime AS PRPSTRTDT, 
                        src:STAT::varchar AS STAT, 
                        src:STATBY::varchar AS STATBY, 
                        src:STATDTTM::datetime AS STATDTTM, 
                        src:STATREAS::varchar AS STATREAS, 
                        src:SUBDIVCODE::varchar AS SUBDIVCODE, 
                        src:SZDESC::varchar AS SZDESC, 
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
    
                        
                src:APPLANKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APPLANKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_PLANNING_PLANAPPL as strm))