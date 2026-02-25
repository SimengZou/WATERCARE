CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_WHINH521 AS SELECT
                        src:adat::datetime AS adat, 
                        src:adpr::numeric(38, 17) AS adpr, 
                        src:adrn::varchar AS adrn, 
                        src:adrn_ref_compnr::integer AS adrn_ref_compnr, 
                        src:amnt::numeric(38, 17) AS amnt, 
                        src:appr::integer AS appr, 
                        src:appr_kw::varchar AS appr_kw, 
                        src:atse::varchar AS atse, 
                        src:atse_ref_compnr::integer AS atse_ref_compnr, 
                        src:clot::varchar AS clot, 
                        src:compnr::integer AS compnr, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_loca_ref_compnr::integer AS cwar_loca_ref_compnr, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dpby::integer AS dpby, 
                        src:dpby_kw::varchar AS dpby_kw, 
                        src:huid::varchar AS huid, 
                        src:huid_ref_compnr::integer AS huid_ref_compnr, 
                        src:hupr::integer AS hupr, 
                        src:hupr_kw::varchar AS hupr_kw, 
                        src:idat::datetime AS idat, 
                        src:ifbp::varchar AS ifbp, 
                        src:ifbp_ref_compnr::integer AS ifbp_ref_compnr, 
                        src:iown::integer AS iown, 
                        src:iown_kw::varchar AS iown_kw, 
                        src:istr::integer AS istr, 
                        src:istr_kw::varchar AS istr_kw, 
                        src:itag::varchar AS itag, 
                        src:item::varchar AS item, 
                        src:item_atse_ref_compnr::integer AS item_atse_ref_compnr, 
                        src:item_clot_ref_compnr::integer AS item_clot_ref_compnr, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:loca::varchar AS loca, 
                        src:orno::varchar AS orno, 
                        src:orno_ref_compnr::integer AS orno_ref_compnr, 
                        src:ownr::varchar AS ownr, 
                        src:ownr_ref_compnr::integer AS ownr_ref_compnr, 
                        src:owns::integer AS owns, 
                        src:owns_kw::varchar AS owns_kw, 
                        src:pkdf::varchar AS pkdf, 
                        src:pkdf_ref_compnr::integer AS pkdf_ref_compnr, 
                        src:pono::integer AS pono, 
                        src:prcd::integer AS prcd, 
                        src:prcd_kw::varchar AS prcd_kw, 
                        src:pric::numeric(38, 17) AS pric, 
                        src:qadj::numeric(38, 17) AS qadj, 
                        src:qadr::numeric(38, 17) AS qadr, 
                        src:qapu::numeric(38, 17) AS qapu, 
                        src:qspu::numeric(38, 17) AS qspu, 
                        src:qstp::numeric(38, 17) AS qstp, 
                        src:qstr::numeric(38, 17) AS qstr, 
                        src:qvpu::numeric(38, 17) AS qvpu, 
                        src:qvrc::numeric(38, 17) AS qvrc, 
                        src:qvrr::numeric(38, 17) AS qvrr, 
                        src:rjin::integer AS rjin, 
                        src:rjin_kw::varchar AS rjin_kw, 
                        src:rowc::integer AS rowc, 
                        src:rowc_ref_compnr::integer AS rowc_ref_compnr, 
                        src:rown::varchar AS rown, 
                        src:rown_ref_compnr::integer AS rown_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:spcn::varchar AS spcn, 
                        src:stun::varchar AS stun, 
                        src:stun_ref_compnr::integer AS stun_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:uapr::integer AS uapr, 
                        src:uapr_kw::varchar AS uapr_kw, 
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
    
                        
                src:pono,
                src:orno,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:pono,
                src:orno,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_WHINH521 as strm))