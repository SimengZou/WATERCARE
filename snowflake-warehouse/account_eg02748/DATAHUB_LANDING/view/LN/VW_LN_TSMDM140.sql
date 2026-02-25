CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TSMDM140 AS SELECT
                        src:asmc::varchar AS asmc, 
                        src:asmc_ref_compnr::integer AS asmc_ref_compnr, 
                        src:ccar::varchar AS ccar, 
                        src:ccar_ref_compnr::integer AS ccar_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:csar::varchar AS csar, 
                        src:csar_ref_compnr::integer AS csar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:emno::varchar AS emno, 
                        src:emno_ref_compnr::integer AS emno_ref_compnr, 
                        src:frmn::varchar AS frmn, 
                        src:frmn_ref_compnr::integer AS frmn_ref_compnr, 
                        src:lsdt::datetime AS lsdt, 
                        src:mopd::numeric(38, 17) AS mopd, 
                        src:nots::object AS nots, 
                        src:pgen::integer AS pgen, 
                        src:pgen_kw::varchar AS pgen_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:ucts::integer AS ucts, 
                        src:ucts_kw::varchar AS ucts_kw, 
                        src:username::varchar AS username, 
            CASE
                WHEN 'LN' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'LN' = 'IPS'
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
    
                        
                src:compnr,
                src:emno,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:emno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TSMDM140 as strm))