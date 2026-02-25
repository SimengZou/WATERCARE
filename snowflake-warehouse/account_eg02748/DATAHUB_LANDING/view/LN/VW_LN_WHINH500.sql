CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_WHINH500 AS SELECT
                        src:ccst::integer AS ccst, 
                        src:ccst_kw::varchar AS ccst_kw, 
                        src:cdf_rdte::datetime AS cdf_rdte, 
                        src:cdf_rfin::integer AS cdf_rfin, 
                        src:cdf_rfin_kw::varchar AS cdf_rfin_kw, 
                        src:cdf_rusr::varchar AS cdf_rusr, 
                        src:cntn::integer AS cntn, 
                        src:compnr::integer AS compnr, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:emno::varchar AS emno, 
                        src:emno_ref_compnr::integer AS emno_ref_compnr, 
                        src:odat::datetime AS odat, 
                        src:orno::varchar AS orno, 
                        src:prcc::integer AS prcc, 
                        src:prcc_kw::varchar AS prcc_kw, 
                        src:prnt::integer AS prnt, 
                        src:prnt_kw::varchar AS prnt_kw, 
                        src:rcyn::integer AS rcyn, 
                        src:rcyn_kw::varchar AS rcyn_kw, 
                        src:redt::datetime AS redt, 
                        src:refe::object AS refe, 
                        src:sequencenumber::integer AS sequencenumber, 
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
    
                        
                src:cntn,
                src:orno,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cntn,
                src:orno,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_WHINH500 as strm))