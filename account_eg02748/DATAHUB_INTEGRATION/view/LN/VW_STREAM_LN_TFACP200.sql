CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP200 AS SELECT
                        src:adcd::varchar AS adcd, 
                        src:adty::integer AS adty, 
                        src:adty_kw::varchar AS adty_kw, 
                        src:afpy::integer AS afpy, 
                        src:afpy_kw::varchar AS afpy_kw, 
                        src:amnt::numeric(38, 17) AS amnt, 
                        src:amth_1::numeric(38, 17) AS amth_1, 
                        src:amth_2::numeric(38, 17) AS amth_2, 
                        src:amth_3::numeric(38, 17) AS amth_3, 
                        src:amth_dtwc::numeric(38, 17) AS amth_dtwc, 
                        src:amth_rfrc::numeric(38, 17) AS amth_rfrc, 
                        src:amti::numeric(38, 17) AS amti, 
                        src:appr::integer AS appr, 
                        src:appr_kw::varchar AS appr_kw, 
                        src:autm::integer AS autm, 
                        src:autm_kw::varchar AS autm_kw, 
                        src:baca_1::numeric(38, 17) AS baca_1, 
                        src:baca_2::numeric(38, 17) AS baca_2, 
                        src:baca_3::numeric(38, 17) AS baca_3, 
                        src:baca_dtwc::numeric(38, 17) AS baca_dtwc, 
                        src:baca_invc::numeric(38, 17) AS baca_invc, 
                        src:baca_rfrc::numeric(38, 17) AS baca_rfrc, 
                        src:baco::numeric(38, 17) AS baco, 
                        src:bahc_1::numeric(38, 17) AS bahc_1, 
                        src:bahc_2::numeric(38, 17) AS bahc_2, 
                        src:bahc_3::numeric(38, 17) AS bahc_3, 
                        src:bahc_dtwc::numeric(38, 17) AS bahc_dtwc, 
                        src:bahc_rfrc::numeric(38, 17) AS bahc_rfrc, 
                        src:bala::numeric(38, 17) AS bala, 
                        src:balc::numeric(38, 17) AS balc, 
                        src:balh_1::numeric(38, 17) AS balh_1, 
                        src:balh_2::numeric(38, 17) AS balh_2, 
                        src:balh_3::numeric(38, 17) AS balh_3, 
                        src:balh_dtwc::numeric(38, 17) AS balh_dtwc, 
                        src:balh_rfrc::numeric(38, 17) AS balh_rfrc, 
                        src:bank::varchar AS bank, 
                        src:basi::numeric(38, 17) AS basi, 
                        src:bdat::date AS bdat, 
                        src:bdsp::integer AS bdsp, 
                        src:bdsp_kw::varchar AS bdsp_kw, 
                        src:bkrn::varchar AS bkrn, 
                        src:blac::integer AS blac, 
                        src:blac_kw::varchar AS blac_kw, 
                        src:bloc::varchar AS bloc, 
                        src:bloc_ref_compnr::integer AS bloc_ref_compnr, 
                        src:bpri::varchar AS bpri, 
                        src:bref::varchar AS bref, 
                        src:bref_ref_compnr::integer AS bref_ref_compnr, 
                        src:btno::integer AS btno, 
                        src:bust::integer AS bust, 
                        src:bust_kw::varchar AS bust_kw, 
                        src:ccrs::varchar AS ccrs, 
                        src:ccrs_ref_compnr::integer AS ccrs_ref_compnr, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cdam_1::numeric(38, 17) AS cdam_1, 
                        src:cdam_2::numeric(38, 17) AS cdam_2, 
                        src:cdam_3::numeric(38, 17) AS cdam_3, 
                        src:cdam_dtwc::numeric(38, 17) AS cdam_dtwc, 
                        src:cdam_invc::numeric(38, 17) AS cdam_invc, 
                        src:cdam_rfrc::numeric(38, 17) AS cdam_rfrc, 
                        src:cfrs::varchar AS cfrs, 
                        src:cfrs_ref_compnr::integer AS cfrs_ref_compnr, 
                        src:clus::varchar AS clus, 
                        src:compnr::integer AS compnr, 
                        src:cpay::varchar AS cpay, 
                        src:cpay_ref_compnr::integer AS cpay_ref_compnr, 
                        src:cvat::varchar AS cvat, 
                        src:cvat_ref_compnr::integer AS cvat_ref_compnr, 
                        src:dapr::integer AS dapr, 
                        src:dc1a::numeric(38, 17) AS dc1a, 
                        src:dc1h_1::numeric(38, 17) AS dc1h_1, 
                        src:dc1h_2::numeric(38, 17) AS dc1h_2, 
                        src:dc1h_3::numeric(38, 17) AS dc1h_3, 
                        src:dc1h_dtwc::numeric(38, 17) AS dc1h_dtwc, 
                        src:dc1h_rfrc::numeric(38, 17) AS dc1h_rfrc, 
                        src:dc1i::numeric(38, 17) AS dc1i, 
                        src:dc2a::numeric(38, 17) AS dc2a, 
                        src:dc2h_1::numeric(38, 17) AS dc2h_1, 
                        src:dc2h_2::numeric(38, 17) AS dc2h_2, 
                        src:dc2h_3::numeric(38, 17) AS dc2h_3, 
                        src:dc2h_dtwc::numeric(38, 17) AS dc2h_dtwc, 
                        src:dc2h_rfrc::numeric(38, 17) AS dc2h_rfrc, 
                        src:dc2i::numeric(38, 17) AS dc2i, 
                        src:dc3a::numeric(38, 17) AS dc3a, 
                        src:dc3h_1::numeric(38, 17) AS dc3h_1, 
                        src:dc3h_2::numeric(38, 17) AS dc3h_2, 
                        src:dc3h_3::numeric(38, 17) AS dc3h_3, 
                        src:dc3h_dtwc::numeric(38, 17) AS dc3h_dtwc, 
                        src:dc3h_rfrc::numeric(38, 17) AS dc3h_rfrc, 
                        src:dc3i::numeric(38, 17) AS dc3i, 
                        src:deleted::boolean AS deleted, 
                        src:did1::date AS did1, 
                        src:did2::date AS did2, 
                        src:did3::date AS did3, 
                        src:dim1::varchar AS dim1, 
                        src:dim2::varchar AS dim2, 
                        src:dim3::varchar AS dim3, 
                        src:dim4::varchar AS dim4, 
                        src:dim5::varchar AS dim5, 
                        src:dim6::varchar AS dim6, 
                        src:dim7::varchar AS dim7, 
                        src:dim8::varchar AS dim8, 
                        src:dim9::varchar AS dim9, 
                        src:dm10::varchar AS dm10, 
                        src:dm11::varchar AS dm11, 
                        src:dm12::varchar AS dm12, 
                        src:doca::integer AS doca, 
                        src:docd::date AS docd, 
                        src:docn::integer AS docn, 
                        src:dued::date AS dued, 
                        src:edtp::integer AS edtp, 
                        src:edtp_kw::varchar AS edtp_kw, 
                        src:enia::integer AS enia, 
                        src:enia_kw::varchar AS enia_kw, 
                        src:iadr::varchar AS iadr, 
                        src:iadr_ref_compnr::integer AS iadr_ref_compnr, 
                        src:ifbp::varchar AS ifbp, 
                        src:ifbp_ref_compnr::integer AS ifbp_ref_compnr, 
                        src:ildp::integer AS ildp, 
                        src:ildp_kw::varchar AS ildp_kw, 
                        src:irdt::date AS irdt, 
                        src:isup::object AS isup, 
                        src:lapa::numeric(38, 17) AS lapa, 
                        src:laph_1::numeric(38, 17) AS laph_1, 
                        src:laph_2::numeric(38, 17) AS laph_2, 
                        src:laph_3::numeric(38, 17) AS laph_3, 
                        src:laph_dtwc::numeric(38, 17) AS laph_dtwc, 
                        src:laph_rfrc::numeric(38, 17) AS laph_rfrc, 
                        src:lapi::numeric(38, 17) AS lapi, 
                        src:leac::varchar AS leac, 
                        src:leac_ref_compnr::integer AS leac_ref_compnr, 
                        src:line::integer AS line, 
                        src:link::numeric(38, 17) AS link, 
                        src:lino::integer AS lino, 
                        src:liqd::date AS liqd, 
                        src:load::varchar AS load, 
                        src:loco::integer AS loco, 
                        src:lpdt::date AS lpdt, 
                        src:lvat::integer AS lvat, 
                        src:lvat_kw::varchar AS lvat_kw, 
                        src:ninv::integer AS ninv, 
                        src:oinv::integer AS oinv, 
                        src:optb::varchar AS optb, 
                        src:optb_ref_compnr::integer AS optb_ref_compnr, 
                        src:orno::varchar AS orno, 
                        src:osch::integer AS osch, 
                        src:osup::object AS osup, 
                        src:otbp::varchar AS otbp, 
                        src:otbp_ref_compnr::integer AS otbp_ref_compnr, 
                        src:otyp::varchar AS otyp, 
                        src:otyp_ref_compnr::integer AS otyp_ref_compnr, 
                        src:pada::numeric(38, 17) AS pada, 
                        src:padh_1::numeric(38, 17) AS padh_1, 
                        src:padh_2::numeric(38, 17) AS padh_2, 
                        src:padh_3::numeric(38, 17) AS padh_3, 
                        src:padh_dtwc::numeric(38, 17) AS padh_dtwc, 
                        src:padh_rfrc::numeric(38, 17) AS padh_rfrc, 
                        src:padi::numeric(38, 17) AS padi, 
                        src:papr::integer AS papr, 
                        src:papr_kw::varchar AS papr_kw, 
                        src:paya::varchar AS paya, 
                        src:paya_ref_compnr::integer AS paya_ref_compnr, 
                        src:payd::integer AS payd, 
                        src:payl::integer AS payl, 
                        src:paym::varchar AS paym, 
                        src:paym_ref_compnr::integer AS paym_ref_compnr, 
                        src:payt::varchar AS payt, 
                        src:pcom::integer AS pcom, 
                        src:pdat::date AS pdat, 
                        src:pdif::numeric(38, 17) AS pdif, 
                        src:pdoc::varchar AS pdoc, 
                        src:pfre::integer AS pfre, 
                        src:pfre_kw::varchar AS pfre_kw, 
                        src:post::integer AS post, 
                        src:prin::integer AS prin, 
                        src:prin_kw::varchar AS prin_kw, 
                        src:prod::integer AS prod, 
                        src:ptad::varchar AS ptad, 
                        src:ptad_ref_compnr::integer AS ptad_ref_compnr, 
                        src:ptbp::varchar AS ptbp, 
                        src:ptbp_bank_ref_compnr::integer AS ptbp_bank_ref_compnr, 
                        src:ptbp_ref_compnr::integer AS ptbp_ref_compnr, 
                        src:ptyp::varchar AS ptyp, 
                        src:ptyp_ref_compnr::integer AS ptyp_ref_compnr, 
                        src:pysh::integer AS pysh, 
                        src:pysh_kw::varchar AS pysh_kw, 
                        src:rade::integer AS rade, 
                        src:rade_kw::varchar AS rade_kw, 
                        src:ragr::varchar AS ragr, 
                        src:ratd::datetime AS ratd, 
                        src:rate_1::numeric(38, 17) AS rate_1, 
                        src:rate_2::numeric(38, 17) AS rate_2, 
                        src:rate_3::numeric(38, 17) AS rate_3, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:rcpt::integer AS rcpt, 
                        src:rcpt_kw::varchar AS rcpt_kw, 
                        src:reas::varchar AS reas, 
                        src:reas_ref_compnr::integer AS reas_ref_compnr, 
                        src:refr::object AS refr, 
                        src:regc::varchar AS regc, 
                        src:regc_ref_compnr::integer AS regc_ref_compnr, 
                        src:rtyp::varchar AS rtyp, 
                        src:schn::integer AS schn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sgtp::integer AS sgtp, 
                        src:sgtp_kw::varchar AS sgtp_kw, 
                        src:shpi::varchar AS shpi, 
                        src:shpm::object AS shpm, 
                        src:stap::integer AS stap, 
                        src:stap_kw::varchar AS stap_kw, 
                        src:stat::integer AS stat, 
                        src:stat_kw::varchar AS stat_kw, 
                        src:step::integer AS step, 
                        src:step_kw::varchar AS step_kw, 
                        src:subc::integer AS subc, 
                        src:subc_kw::varchar AS subc_kw, 
                        src:svah_1::numeric(38, 17) AS svah_1, 
                        src:svah_2::numeric(38, 17) AS svah_2, 
                        src:svah_3::numeric(38, 17) AS svah_3, 
                        src:svam::numeric(38, 17) AS svam, 
                        src:tapr::varchar AS tapr, 
                        src:tbrl::numeric(38, 17) AS tbrl, 
                        src:tdoc::varchar AS tdoc, 
                        src:tdoc_ref_compnr::integer AS tdoc_ref_compnr, 
                        src:text::integer AS text, 
                        src:text_ref_compnr::integer AS text_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:tnpn::varchar AS tnpn, 
                        src:tore_1::numeric(38, 17) AS tore_1, 
                        src:tore_2::numeric(38, 17) AS tore_2, 
                        src:tore_3::numeric(38, 17) AS tore_3, 
                        src:tore_dtwc::numeric(38, 17) AS tore_dtwc, 
                        src:tore_invc::numeric(38, 17) AS tore_invc, 
                        src:tore_rfrc::numeric(38, 17) AS tore_rfrc, 
                        src:tpay::integer AS tpay, 
                        src:tpay_kw::varchar AS tpay_kw, 
                        src:ttyp::varchar AS ttyp, 
                        src:ttyp_ref_compnr::integer AS ttyp_ref_compnr, 
                        src:txdt::date AS txdt, 
                        src:txtb::integer AS txtb, 
                        src:txtb_ref_compnr::integer AS txtb_ref_compnr, 
                        src:typa::varchar AS typa, 
                        src:typa_ref_compnr::integer AS typa_ref_compnr, 
                        src:user::varchar AS user, 
                        src:username::varchar AS username, 
                        src:vata::numeric(38, 17) AS vata, 
                        src:vatc::varchar AS vatc, 
                        src:vatc_cvat_ref_compnr::integer AS vatc_cvat_ref_compnr, 
                        src:vatc_ref_compnr::integer AS vatc_ref_compnr, 
                        src:vath_1::numeric(38, 17) AS vath_1, 
                        src:vath_2::numeric(38, 17) AS vath_2, 
                        src:vath_3::numeric(38, 17) AS vath_3, 
                        src:vath_dtwc::numeric(38, 17) AS vath_dtwc, 
                        src:vath_rfrc::numeric(38, 17) AS vath_rfrc, 
                        src:vati::numeric(38, 17) AS vati, 
                        src:vatp::integer AS vatp, 
                        src:vaty::integer AS vaty, 
                        src:vrsm::varchar AS vrsm, 
                        src:whti::integer AS whti, 
                        src:whti_kw::varchar AS whti_kw, 
                        src:wtph_1::numeric(38, 17) AS wtph_1, 
                        src:wtph_2::numeric(38, 17) AS wtph_2, 
                        src:wtph_3::numeric(38, 17) AS wtph_3, 
                        src:wtpi::numeric(38, 17) AS wtpi, 
                        src:year::integer AS year, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:ttyp,
                src:lino,
                src:docn,
                src:line,
                src:ninv,
                src:compnr,
                src:tdoc  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFACP200 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:afpy), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amth_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amth_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amth_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amth_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amth_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amti), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:appr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:autm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:baca_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:baca_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:baca_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:baca_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:baca_invc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:baca_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:baco), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bahc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bahc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bahc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bahc_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bahc_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bala), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:balc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:balh_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:balh_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:balh_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:balh_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:balh_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:basi), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:bdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bdsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bloc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bref_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:btno), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bust), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdam_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdam_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdam_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdam_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdam_invc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdam_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfrs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dapr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc1a), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc1h_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc1h_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc1h_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc1h_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc1h_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc1i), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc2a), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc2h_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc2h_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc2h_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc2h_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc2h_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc2i), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc3a), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc3h_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc3h_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc3h_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc3h_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc3h_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dc3i), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:did1), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:did2), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:did3), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:doca), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:docd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:docn), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:dued), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:edtp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:enia), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iadr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ildp), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:irdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lapa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:laph_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:laph_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:laph_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:laph_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:laph_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lapi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:leac_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:line), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:link), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lino), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:liqd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:loco), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:lpdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lvat), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ninv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oinv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:optb_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:osch), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:otyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pada), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:padh_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:padh_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:padh_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:padh_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:padh_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:padi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:papr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:paya_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:payd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:payl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:paym_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcom), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:pdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pdif), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pfre), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:post), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prod), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptad_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptbp_bank_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pysh), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rade), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ratd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcpt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:reas_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:regc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:schn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sgtp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stap), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stat), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:step), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:subc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:svah_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:svah_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:svah_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:svam), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tbrl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tdoc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tore_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tore_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tore_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tore_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tore_invc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tore_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tpay), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ttyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:txdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:typa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vata), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vatc_cvat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vatc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vath_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vath_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vath_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vath_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vath_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vati), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vatp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vaty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:whti), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wtph_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wtph_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wtph_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wtpi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null