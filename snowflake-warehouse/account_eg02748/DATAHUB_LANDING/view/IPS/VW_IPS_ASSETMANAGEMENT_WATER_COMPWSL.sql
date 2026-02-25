CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_ASSETMANAGEMENT_WATER_COMPWSL AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:ADDRQUAL::varchar AS ADDRQUAL, 
                        src:AREA::varchar AS AREA, 
                        src:ASBLT::varchar AS ASBLT, 
                        src:BGTNO::varchar AS BGTNO, 
                        src:BLDGFLOOR::integer AS BLDGFLOOR, 
                        src:BLDGKEY::integer AS BLDGKEY, 
                        src:BLDGROOM::integer AS BLDGROOM, 
                        src:BUDGETKEY::integer AS BUDGETKEY, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:COMPLEXKEY::integer AS COMPLEXKEY, 
                        src:CRITSRV::varchar AS CRITSRV, 
                        src:CURBSTOP::varchar AS CURBSTOP, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DBNO::varchar AS DBNO, 
                        src:DIAM::numeric(38, 10) AS DIAM, 
                        src:DIAMUOM::varchar AS DIAMUOM, 
                        src:DISTRICT::varchar AS DISTRICT, 
                        src:EPAID::varchar AS EPAID, 
                        src:EXPBY::varchar AS EXPBY, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:FIRELINE::varchar AS FIRELINE, 
                        src:GISSTATIC::integer AS GISSTATIC, 
                        src:INSTDATE::datetime AS INSTDATE, 
                        src:LOC::varchar AS LOC, 
                        src:MAINKEY::integer AS MAINKEY, 
                        src:MAPNO::varchar AS MAPNO, 
                        src:MFGKEY::integer AS MFGKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NPDESID::varchar AS NPDESID, 
                        src:OWN::varchar AS OWN, 
                        src:PIPELEN::numeric(38, 10) AS PIPELEN, 
                        src:PIPELENUOM::varchar AS PIPELENUOM, 
                        src:PIPETYPE::varchar AS PIPETYPE, 
                        src:POSITION::integer AS POSITION, 
                        src:PRCLKEY::integer AS PRCLKEY, 
                        src:PRESZONE::varchar AS PRESZONE, 
                        src:SEGKEY::integer AS SEGKEY, 
                        src:SERVSTAT::varchar AS SERVSTAT, 
                        src:SIC::varchar AS SIC, 
                        src:SITEKEY::integer AS SITEKEY, 
                        src:SRVTYPE::varchar AS SRVTYPE, 
                        src:SUBAREA::varchar AS SUBAREA, 
                        src:SURF::varchar AS SURF, 
                        src:TAPADDRKEY::integer AS TAPADDRKEY, 
                        src:TAPLOC::varchar AS TAPLOC, 
                        src:UICID::varchar AS UICID, 
                        src:UNITDESC::varchar AS UNITDESC, 
                        src:UNITID::varchar AS UNITID, 
                        src:UNITTYPE::varchar AS UNITTYPE, 
                        src:USGAREAKEY::integer AS USGAREAKEY, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XCOORD::numeric(38, 10) AS XCOORD, 
                        src:YCOORD::numeric(38, 10) AS YCOORD, 
                        src:ZCOORD::numeric(38, 10) AS ZCOORD, 
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
    
                        
                src:COMPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:COMPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_ASSETMANAGEMENT_WATER_COMPWSL as strm))