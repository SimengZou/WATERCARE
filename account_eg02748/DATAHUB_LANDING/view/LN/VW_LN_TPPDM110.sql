CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPDM110 AS SELECT
                        src:afrt::integer AS afrt, 
                        src:afrt_kw::varchar AS afrt_kw, 
                        src:blbl::integer AS blbl, 
                        src:blbl_kw::varchar AS blbl_kw, 
                        src:cact::varchar AS cact, 
                        src:capt::integer AS capt, 
                        src:capt_kw::varchar AS capt_kw, 
                        src:compnr::integer AS compnr, 
                        src:cspt::integer AS cspt, 
                        src:cspt_kw::varchar AS cspt_kw, 
                        src:cuni::varchar AS cuni, 
                        src:cuni_ref_compnr::integer AS cuni_ref_compnr, 
                        src:cuti::varchar AS cuti, 
                        src:cuti_ref_compnr::integer AS cuti_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:eoty::integer AS eoty, 
                        src:eoty_kw::varchar AS eoty_kw, 
                        src:freq::numeric(38, 17) AS freq, 
                        src:lvps::integer AS lvps, 
                        src:lvps_kw::varchar AS lvps_kw, 
                        src:mevl::integer AS mevl, 
                        src:mevl_kw::varchar AS mevl_kw, 
                        src:plmc::integer AS plmc, 
                        src:plmc_ref_compnr::integer AS plmc_ref_compnr, 
                        src:plmp::varchar AS plmp, 
                        src:prin::integer AS prin, 
                        src:prin_kw::varchar AS prin_kw, 
                        src:prms::numeric(38, 17) AS prms, 
                        src:prnd::numeric(38, 17) AS prnd, 
                        src:prsp::numeric(38, 17) AS prsp, 
                        src:prst::numeric(38, 17) AS prst, 
                        src:rent::integer AS rent, 
                        src:rent_kw::varchar AS rent_kw, 
                        src:rfac::varchar AS rfac, 
                        src:rout::varchar AS rout, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:setl::integer AS setl, 
                        src:setl_kw::varchar AS setl_kw, 
                        src:tact::integer AS tact, 
                        src:tact_kw::varchar AS tact_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
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
    
                        
                src:cact,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cact,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPDM110 as strm))