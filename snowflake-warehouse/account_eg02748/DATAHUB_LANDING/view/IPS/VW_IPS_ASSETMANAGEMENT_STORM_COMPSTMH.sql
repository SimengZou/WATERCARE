CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_ASSETMANAGEMENT_STORM_COMPSTMH AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:ADDRQUAL::varchar AS ADDRQUAL, 
                        src:AREA::varchar AS AREA, 
                        src:ASBLT::varchar AS ASBLT, 
                        src:BARLDIAM::numeric(38, 10) AS BARLDIAM, 
                        src:BARLDIAMUOM::varchar AS BARLDIAMUOM, 
                        src:BASETYPE::varchar AS BASETYPE, 
                        src:BENCHTYPE::varchar AS BENCHTYPE, 
                        src:BGTNO::varchar AS BGTNO, 
                        src:BLDGFLOOR::integer AS BLDGFLOOR, 
                        src:BLDGROOM::integer AS BLDGROOM, 
                        src:BUDGETKEY::integer AS BUDGETKEY, 
                        src:CHNLTYPE::varchar AS CHNLTYPE, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:COMPLEXKEY::integer AS COMPLEXKEY, 
                        src:CONETYPE::varchar AS CONETYPE, 
                        src:CVRDIAM::numeric(38, 10) AS CVRDIAM, 
                        src:CVRDIAMUOM::varchar AS CVRDIAMUOM, 
                        src:CVRTYPE::varchar AS CVRTYPE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DISTRICT::varchar AS DISTRICT, 
                        src:DROPMH::varchar AS DROPMH, 
                        src:EXPBY::varchar AS EXPBY, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:FRAMETYPE::varchar AS FRAMETYPE, 
                        src:GISSTATIC::integer AS GISSTATIC, 
                        src:HYDIST::numeric(38, 10) AS HYDIST, 
                        src:HYDISTUOM::varchar AS HYDISTUOM, 
                        src:INSTDATE::datetime AS INSTDATE, 
                        src:INTKEY::integer AS INTKEY, 
                        src:LOC::varchar AS LOC, 
                        src:MAPNO::varchar AS MAPNO, 
                        src:METERED::varchar AS METERED, 
                        src:MHDPTH::numeric(38, 10) AS MHDPTH, 
                        src:MHDPTHUOM::varchar AS MHDPTHUOM, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OWN::varchar AS OWN, 
                        src:POSITION::integer AS POSITION, 
                        src:PRCLKEY::integer AS PRCLKEY, 
                        src:RINGSTYPE::varchar AS RINGSTYPE, 
                        src:SEGKEY::integer AS SEGKEY, 
                        src:SERVSTAT::varchar AS SERVSTAT, 
                        src:SITEKEY::integer AS SITEKEY, 
                        src:STEPSTYPE::varchar AS STEPSTYPE, 
                        src:SUBAREA::varchar AS SUBAREA, 
                        src:SURF::varchar AS SURF, 
                        src:UNITDESC::varchar AS UNITDESC, 
                        src:UNITID::varchar AS UNITID, 
                        src:UNITTYPE::varchar AS UNITTYPE, 
                        src:USGAREAKEY::integer AS USGAREAKEY, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WALLTYPE::varchar AS WALLTYPE, 
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
                FROM DATAHUB_LANDING.IPS_ASSETMANAGEMENT_STORM_COMPSTMH as strm))