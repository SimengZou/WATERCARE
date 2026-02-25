CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP240 AS SELECT
                        src:amth_1::numeric(38, 17) AS amth_1, 
                        src:amth_2::numeric(38, 17) AS amth_2, 
                        src:amth_3::numeric(38, 17) AS amth_3, 
                        src:bppr::integer AS bppr, 
                        src:bppr_kw::varchar AS bppr_kw, 
                        src:bptc::varchar AS bptc, 
                        src:bptc_ref_compnr::integer AS bptc_ref_compnr, 
                        src:byer::varchar AS byer, 
                        src:cadr::varchar AS cadr, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cdec::varchar AS cdec, 
                        src:ceno::varchar AS ceno, 
                        src:ciko::integer AS ciko, 
                        src:ciko_kw::varchar AS ciko_kw, 
                        src:cipo::integer AS cipo, 
                        src:cish::varchar AS cish, 
                        src:cisq::integer AS cisq, 
                        src:citt::varchar AS citt, 
                        src:clas::varchar AS clas, 
                        src:clas_ref_compnr::integer AS clas_ref_compnr, 
                        src:clsb::integer AS clsb, 
                        src:clsb_kw::varchar AS clsb_kw, 
                        src:clsc::varchar AS clsc, 
                        src:cmnf::varchar AS cmnf, 
                        src:compnr::integer AS compnr, 
                        src:cpay::varchar AS cpay, 
                        src:cpay_ref_compnr::integer AS cpay_ref_compnr, 
                        src:crit::object AS crit, 
                        src:crrf::integer AS crrf, 
                        src:crrf_kw::varchar AS crrf_kw, 
                        src:cvat::varchar AS cvat, 
                        src:cvlo::numeric(38, 17) AS cvlo, 
                        src:cvyn::integer AS cvyn, 
                        src:cvyn_kw::varchar AS cvyn_kw, 
                        src:cwar::varchar AS cwar, 
                        src:damt::numeric(38, 17) AS damt, 
                        src:deleted::boolean AS deleted, 
                        src:fcpo::integer AS fcpo, 
                        src:fire::integer AS fire, 
                        src:fire_kw::varchar AS fire_kw, 
                        src:fsli::integer AS fsli, 
                        src:glco::varchar AS glco, 
                        src:ifbp::varchar AS ifbp, 
                        src:ifbp_ref_compnr::integer AS ifbp_ref_compnr, 
                        src:issp::integer AS issp, 
                        src:issp_kw::varchar AS issp_kw, 
                        src:item::varchar AS item, 
                        src:itgr::varchar AS itgr, 
                        src:itsc::integer AS itsc, 
                        src:itsc_kw::varchar AS itsc_kw, 
                        src:ityp::integer AS ityp, 
                        src:ityp_kw::varchar AS ityp_kw, 
                        src:iuni::varchar AS iuni, 
                        src:iuni_ref_compnr::integer AS iuni_ref_compnr, 
                        src:koor::integer AS koor, 
                        src:koor_kw::varchar AS koor_kw, 
                        src:ladt::datetime AS ladt, 
                        src:lmat::integer AS lmat, 
                        src:lmat_kw::varchar AS lmat_kw, 
                        src:loco::integer AS loco, 
                        src:maam::numeric(38, 17) AS maam, 
                        src:maqu::numeric(38, 17) AS maqu, 
                        src:mcfr::integer AS mcfr, 
                        src:mcfr_kw::varchar AS mcfr_kw, 
                        src:mcpr::integer AS mcpr, 
                        src:mcpr_kw::varchar AS mcpr_kw, 
                        src:mpnr::varchar AS mpnr, 
                        src:mqpu::numeric(38, 17) AS mqpu, 
                        src:mtch::integer AS mtch, 
                        src:mtch_kw::varchar AS mtch_kw, 
                        src:oamt::numeric(38, 17) AS oamt, 
                        src:odat::datetime AS odat, 
                        src:omti::integer AS omti, 
                        src:omti_kw::varchar AS omti_kw, 
                        src:oqan::numeric(38, 17) AS oqan, 
                        src:oref::varchar AS oref, 
                        src:orno::varchar AS orno, 
                        src:ortp::integer AS ortp, 
                        src:ortp_kw::varchar AS ortp_kw, 
                        src:oset::integer AS oset, 
                        src:otbp::varchar AS otbp, 
                        src:otyp::integer AS otyp, 
                        src:otyp_kw::varchar AS otyp_kw, 
                        src:paft::integer AS paft, 
                        src:paft_kw::varchar AS paft_kw, 
                        src:paya::varchar AS paya, 
                        src:paya_ref_compnr::integer AS paya_ref_compnr, 
                        src:poff::varchar AS poff, 
                        src:pono::integer AS pono, 
                        src:ppvp::integer AS ppvp, 
                        src:ppvp_kw::varchar AS ppvp_kw, 
                        src:proj::varchar AS proj, 
                        src:prrc::integer AS prrc, 
                        src:prrc_kw::varchar AS prrc_kw, 
                        src:ptyp::varchar AS ptyp, 
                        src:ptyp_ref_compnr::integer AS ptyp_ref_compnr, 
                        src:ratd::datetime AS ratd, 
                        src:rate_1::numeric(38, 17) AS rate_1, 
                        src:rate_2::numeric(38, 17) AS rate_2, 
                        src:rate_3::numeric(38, 17) AS rate_3, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:rcod::varchar AS rcod, 
                        src:rcod_ref_compnr::integer AS rcod_ref_compnr, 
                        src:rqty::numeric(38, 17) AS rqty, 
                        src:rtyp::varchar AS rtyp, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sfad::varchar AS sfad, 
                        src:sfbl::integer AS sfbl, 
                        src:sfbl_kw::varchar AS sfbl_kw, 
                        src:sfbp::varchar AS sfbp, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:slcp::integer AS slcp, 
                        src:slso::varchar AS slso, 
                        src:sorn::object AS sorn, 
                        src:sqnb::integer AS sqnb, 
                        src:srtp::integer AS srtp, 
                        src:srtp_kw::varchar AS srtp_kw, 
                        src:tapq::numeric(38, 17) AS tapq, 
                        src:timestamp::datetime AS timestamp, 
                        src:toma::integer AS toma, 
                        src:toma_kw::varchar AS toma_kw, 
                        src:tref::varchar AS tref, 
                        src:trgu::varchar AS trgu, 
                        src:trqu::numeric(38, 17) AS trqu, 
                        src:unit::varchar AS unit, 
                        src:unit_ref_compnr::integer AS unit_ref_compnr, 
                        src:uppr::integer AS uppr, 
                        src:uppr_kw::varchar AS uppr_kw, 
                        src:username::varchar AS username, 
                        src:vatc::varchar AS vatc, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'LN' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'LN' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
            etl_load_datetime,
            etl_load_metadatafilename
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename
                from
                (
                    SELECT
                        
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:pono,
                src:loco,
                src:orno,
                src:otyp,
                src:compnr,
                src:sqnb  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFACP240 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amth_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amth_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amth_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bppr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bptc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ciko), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cipo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cisq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clas_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clsb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crrf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvlo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:damt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcpo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fire), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fsli), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:issp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itsc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ityp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iuni_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:koor), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ladt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lmat), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:loco), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:maam), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:maqu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mcfr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mcpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mqpu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mtch), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oamt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:odat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:omti), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oqan), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ortp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oset), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:otyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:paft), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:paya_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pono), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ppvp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ratd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rqty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sfbl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:slcp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sqnb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:srtp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tapq), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:toma), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trqu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:unit_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uppr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null