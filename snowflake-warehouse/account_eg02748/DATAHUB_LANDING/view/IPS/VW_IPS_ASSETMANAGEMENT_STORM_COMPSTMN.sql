CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_ASSETMANAGEMENT_STORM_COMPSTMN AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:ADDRQUAL::varchar AS ADDRQUAL, 
                        src:AREA::varchar AS AREA, 
                        src:ASBLT::varchar AS ASBLT, 
                        src:BGTNO::varchar AS BGTNO, 
                        src:BLDGFLOOR::integer AS BLDGFLOOR, 
                        src:BLDGROOM::integer AS BLDGROOM, 
                        src:BUDGETKEY::integer AS BUDGETKEY, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:COMPLEXKEY::integer AS COMPLEXKEY, 
                        src:CRIT::varchar AS CRIT, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DIRFRDWN::varchar AS DIRFRDWN, 
                        src:DIRFRUPS::varchar AS DIRFRUPS, 
                        src:DISTRICT::varchar AS DISTRICT, 
                        src:DSGNFLOW::numeric(38, 10) AS DSGNFLOW, 
                        src:DWNDPTH::numeric(38, 10) AS DWNDPTH, 
                        src:DWNDPTHUOM::varchar AS DWNDPTHUOM, 
                        src:DWNELEV::numeric(38, 10) AS DWNELEV, 
                        src:DWNELEVUOM::varchar AS DWNELEVUOM, 
                        src:EXPBY::varchar AS EXPBY, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:FFACTOR::numeric(38, 10) AS FFACTOR, 
                        src:GISSTATIC::integer AS GISSTATIC, 
                        src:GROUNDWAT::numeric(38, 10) AS GROUNDWAT, 
                        src:GROUNDWATUOM::varchar AS GROUNDWATUOM, 
                        src:INSTDATE::datetime AS INSTDATE, 
                        src:JTLEN::numeric(38, 10) AS JTLEN, 
                        src:JTLENUOM::varchar AS JTLENUOM, 
                        src:JTTYPE::varchar AS JTTYPE, 
                        src:LOC::varchar AS LOC, 
                        src:MAINCOMP1::integer AS MAINCOMP1, 
                        src:MAINCOMP2::integer AS MAINCOMP2, 
                        src:MAINKEY1::integer AS MAINKEY1, 
                        src:MAINKEY2::integer AS MAINKEY2, 
                        src:MAPNO::varchar AS MAPNO, 
                        src:MFGKEY::integer AS MFGKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OWN::varchar AS OWN, 
                        src:PARLINENO::varchar AS PARLINENO, 
                        src:PIPEDIAM::numeric(38, 10) AS PIPEDIAM, 
                        src:PIPEDIAMUOM::varchar AS PIPEDIAMUOM, 
                        src:PIPEHT::numeric(38, 10) AS PIPEHT, 
                        src:PIPEHTUOM::varchar AS PIPEHTUOM, 
                        src:PIPELEN::numeric(38, 10) AS PIPELEN, 
                        src:PIPELENUOM::varchar AS PIPELENUOM, 
                        src:PIPESHP::varchar AS PIPESHP, 
                        src:PIPETYPE::varchar AS PIPETYPE, 
                        src:POSITION::integer AS POSITION, 
                        src:PRCLKEY::integer AS PRCLKEY, 
                        src:SEGKEY::integer AS SEGKEY, 
                        src:SEGMENT::varchar AS SEGMENT, 
                        src:SERVSTAT::varchar AS SERVSTAT, 
                        src:SITEKEY::integer AS SITEKEY, 
                        src:SLP::numeric(38, 10) AS SLP, 
                        src:SLPUOM::varchar AS SLPUOM, 
                        src:SUBAREA::varchar AS SUBAREA, 
                        src:SURF::varchar AS SURF, 
                        src:UNITDESC::varchar AS UNITDESC, 
                        src:UNITID::varchar AS UNITID, 
                        src:UNITID2::varchar AS UNITID2, 
                        src:UNITTYPE::varchar AS UNITTYPE, 
                        src:UPSDPTH::numeric(38, 10) AS UPSDPTH, 
                        src:UPSDPTHUOM::varchar AS UPSDPTHUOM, 
                        src:UPSELEV::numeric(38, 10) AS UPSELEV, 
                        src:UPSELEVUOM::varchar AS UPSELEVUOM, 
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
                FROM DATAHUB_LANDING.IPS_ASSETMANAGEMENT_STORM_COMPSTMN as strm))