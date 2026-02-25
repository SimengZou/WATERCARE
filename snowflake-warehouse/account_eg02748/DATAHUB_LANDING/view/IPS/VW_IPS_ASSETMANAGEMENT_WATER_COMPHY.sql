CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_ASSETMANAGEMENT_WATER_COMPHY AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:ADDRQUAL::varchar AS ADDRQUAL, 
                        src:AREA::varchar AS AREA, 
                        src:ASBLT::varchar AS ASBLT, 
                        src:AUXVALVE::varchar AS AUXVALVE, 
                        src:BARRELSIZE::numeric(38, 10) AS BARRELSIZE, 
                        src:BARRELSIZEUOM::varchar AS BARRELSIZEUOM, 
                        src:BGTNO::varchar AS BGTNO, 
                        src:BLDGFLOOR::integer AS BLDGFLOOR, 
                        src:BLDGROOM::integer AS BLDGROOM, 
                        src:BUDGETKEY::integer AS BUDGETKEY, 
                        src:COLOR::varchar AS COLOR, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:COMPLEXKEY::integer AS COMPLEXKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DISTRICT::varchar AS DISTRICT, 
                        src:EXPBY::varchar AS EXPBY, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:FEEDERDIAM::numeric(38, 10) AS FEEDERDIAM, 
                        src:FEEDERDIAMUOM::varchar AS FEEDERDIAMUOM, 
                        src:FEEDERLEN::numeric(38, 10) AS FEEDERLEN, 
                        src:FEEDERLENUOM::varchar AS FEEDERLENUOM, 
                        src:FEEDERTYPE::varchar AS FEEDERTYPE, 
                        src:GISSTATIC::integer AS GISSTATIC, 
                        src:HT::numeric(38, 10) AS HT, 
                        src:HTUOM::varchar AS HTUOM, 
                        src:INSTDATE::datetime AS INSTDATE, 
                        src:INTKEY::integer AS INTKEY, 
                        src:LOC::varchar AS LOC, 
                        src:MAINKEY::integer AS MAINKEY, 
                        src:MAPNO::varchar AS MAPNO, 
                        src:MFGKEY::integer AS MFGKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODELNO::varchar AS MODELNO, 
                        src:OBST::varchar AS OBST, 
                        src:OUTLSZ1::numeric(38, 10) AS OUTLSZ1, 
                        src:OUTLSZ1UOM::varchar AS OUTLSZ1UOM, 
                        src:OUTLSZ2::numeric(38, 10) AS OUTLSZ2, 
                        src:OUTLSZ3::numeric(38, 10) AS OUTLSZ3, 
                        src:OUTLSZ4::numeric(38, 10) AS OUTLSZ4, 
                        src:OWN::varchar AS OWN, 
                        src:PACKING::varchar AS PACKING, 
                        src:PAINTTYPE::varchar AS PAINTTYPE, 
                        src:POSITION::integer AS POSITION, 
                        src:PRCLKEY::integer AS PRCLKEY, 
                        src:PRESZONE::varchar AS PRESZONE, 
                        src:SEGKEY::integer AS SEGKEY, 
                        src:SERNO::varchar AS SERNO, 
                        src:SERVSTAT::varchar AS SERVSTAT, 
                        src:SITEKEY::integer AS SITEKEY, 
                        src:SLKEY::integer AS SLKEY, 
                        src:SUBAREA::varchar AS SUBAREA, 
                        src:UNITDESC::varchar AS UNITDESC, 
                        src:UNITID::varchar AS UNITID, 
                        src:UNITTYPE::varchar AS UNITTYPE, 
                        src:USGAREAKEY::integer AS USGAREAKEY, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WVKEY::integer AS WVKEY, 
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
                FROM DATAHUB_LANDING.IPS_ASSETMANAGEMENT_WATER_COMPHY as strm))