CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TSMDM000 AS SELECT
                        src:alrm::integer AS alrm, 
                        src:alrm_kw::varchar AS alrm_kw, 
                        src:atws_1::integer AS atws_1, 
                        src:atws_10::integer AS atws_10, 
                        src:atws_2::integer AS atws_2, 
                        src:atws_3::integer AS atws_3, 
                        src:atws_4::integer AS atws_4, 
                        src:atws_5::integer AS atws_5, 
                        src:atws_6::integer AS atws_6, 
                        src:atws_7::integer AS atws_7, 
                        src:atws_8::integer AS atws_8, 
                        src:atws_9::integer AS atws_9, 
                        src:atws_kw_1::varchar AS atws_kw_1, 
                        src:atws_kw_10::varchar AS atws_kw_10, 
                        src:atws_kw_2::varchar AS atws_kw_2, 
                        src:atws_kw_3::varchar AS atws_kw_3, 
                        src:atws_kw_4::varchar AS atws_kw_4, 
                        src:atws_kw_5::varchar AS atws_kw_5, 
                        src:atws_kw_6::varchar AS atws_kw_6, 
                        src:atws_kw_7::varchar AS atws_kw_7, 
                        src:atws_kw_8::varchar AS atws_kw_8, 
                        src:atws_kw_9::varchar AS atws_kw_9, 
                        src:ccli::integer AS ccli, 
                        src:ccli_kw::varchar AS ccli_kw, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cibp::integer AS cibp, 
                        src:cibp_kw::varchar AS cibp_kw, 
                        src:clgr::varchar AS clgr, 
                        src:clgr_ref_compnr::integer AS clgr_ref_compnr, 
                        src:clyn::integer AS clyn, 
                        src:clyn_kw::varchar AS clyn_kw, 
                        src:compnr::integer AS compnr, 
                        src:cpdi_1::numeric(38, 17) AS cpdi_1, 
                        src:cpdi_2::numeric(38, 17) AS cpdi_2, 
                        src:cpdi_3::numeric(38, 17) AS cpdi_3, 
                        src:ctii::integer AS ctii, 
                        src:ctii_kw::varchar AS ctii_kw, 
                        src:deleted::boolean AS deleted, 
                        src:dfpb::integer AS dfpb, 
                        src:dfpb_kw::varchar AS dfpb_kw, 
                        src:dipy::integer AS dipy, 
                        src:dipy_kw::varchar AS dipy_kw, 
                        src:dist::integer AS dist, 
                        src:dist_kw::varchar AS dist_kw, 
                        src:dsca::object AS dsca, 
                        src:erac::integer AS erac, 
                        src:erac_kw::varchar AS erac_kw, 
                        src:erdc::integer AS erdc, 
                        src:erdc_kw::varchar AS erdc_kw, 
                        src:feyn::integer AS feyn, 
                        src:feyn_kw::varchar AS feyn_kw, 
                        src:flds::integer AS flds, 
                        src:flds_kw::varchar AS flds_kw, 
                        src:grpl::integer AS grpl, 
                        src:grpl_kw::varchar AS grpl_kw, 
                        src:icpr_1::integer AS icpr_1, 
                        src:icpr_2::integer AS icpr_2, 
                        src:icpr_3::integer AS icpr_3, 
                        src:icpr_4::integer AS icpr_4, 
                        src:icpr_5::integer AS icpr_5, 
                        src:icpr_kw_1::varchar AS icpr_kw_1, 
                        src:icpr_kw_2::varchar AS icpr_kw_2, 
                        src:icpr_kw_3::varchar AS icpr_kw_3, 
                        src:icpr_kw_4::varchar AS icpr_kw_4, 
                        src:icpr_kw_5::varchar AS icpr_kw_5, 
                        src:icrr::integer AS icrr, 
                        src:icrr_kw::varchar AS icrr_kw, 
                        src:icsc::varchar AS icsc, 
                        src:inby::integer AS inby, 
                        src:inby_kw::varchar AS inby_kw, 
                        src:indt::datetime AS indt, 
                        src:ingr::integer AS ingr, 
                        src:ingr_kw::varchar AS ingr_kw, 
                        src:ipsm::integer AS ipsm, 
                        src:ipsm_kw::varchar AS ipsm_kw, 
                        src:isdp::integer AS isdp, 
                        src:isdp_kw::varchar AS isdp_kw, 
                        src:isom::integer AS isom, 
                        src:isom_kw::varchar AS isom_kw, 
                        src:iwos::integer AS iwos, 
                        src:iwos_kw::varchar AS iwos_kw, 
                        src:kpad::integer AS kpad, 
                        src:kpad_kw::varchar AS kpad_kw, 
                        src:meyn::integer AS meyn, 
                        src:meyn_kw::varchar AS meyn_kw, 
                        src:ncri::integer AS ncri, 
                        src:ncri_kw::varchar AS ncri_kw, 
                        src:ngtb::varchar AS ngtb, 
                        src:ngtb_ref_compnr::integer AS ngtb_ref_compnr, 
                        src:ngtb_sntb_ref_compnr::integer AS ngtb_sntb_ref_compnr, 
                        src:npeg::integer AS npeg, 
                        src:npeg_kw::varchar AS npeg_kw, 
                        src:pmnt::integer AS pmnt, 
                        src:pmnt_kw::varchar AS pmnt_kw, 
                        src:prnt::integer AS prnt, 
                        src:prnt_kw::varchar AS prnt_kw, 
                        src:pvap::numeric(38, 17) AS pvap, 
                        src:ract::varchar AS ract, 
                        src:ract_ref_compnr::integer AS ract_ref_compnr, 
                        src:sbmt::integer AS sbmt, 
                        src:sbmt_kw::varchar AS sbmt_kw, 
                        src:scin::integer AS scin, 
                        src:scin_kw::varchar AS scin_kw, 
                        src:scli::integer AS scli, 
                        src:scli_kw::varchar AS scli_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:shpm::integer AS shpm, 
                        src:shpm_kw::varchar AS shpm_kw, 
                        src:siyn::integer AS siyn, 
                        src:siyn_kw::varchar AS siyn_kw, 
                        src:skla::integer AS skla, 
                        src:skla_kw::varchar AS skla_kw, 
                        src:sklb::integer AS sklb, 
                        src:sklb_kw::varchar AS sklb_kw, 
                        src:sklc::integer AS sklc, 
                        src:sklc_kw::varchar AS sklc_kw, 
                        src:skll::integer AS skll, 
                        src:skll_kw::varchar AS skll_kw, 
                        src:sntb::varchar AS sntb, 
                        src:spdi::numeric(38, 17) AS spdi, 
                        src:spsm::integer AS spsm, 
                        src:spsm_kw::varchar AS spsm_kw, 
                        src:text::integer AS text, 
                        src:text_ref_compnr::integer AS text_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:tlgi::integer AS tlgi, 
                        src:tlgi_kw::varchar AS tlgi_kw, 
                        src:trac_1::numeric(38, 17) AS trac_1, 
                        src:trac_2::numeric(38, 17) AS trac_2, 
                        src:trac_3::numeric(38, 17) AS trac_3, 
                        src:tras::numeric(38, 17) AS tras, 
                        src:trmd::integer AS trmd, 
                        src:trmd_kw::varchar AS trmd_kw, 
                        src:ttpl::integer AS ttpl, 
                        src:ttpl_kw::varchar AS ttpl_kw, 
                        src:ubpp::integer AS ubpp, 
                        src:ubpp_kw::varchar AS ubpp_kw, 
                        src:uday::varchar AS uday, 
                        src:uday_ref_compnr::integer AS uday_ref_compnr, 
                        src:udgn::integer AS udgn, 
                        src:udgn_kw::varchar AS udgn_kw, 
                        src:umon::varchar AS umon, 
                        src:umon_ref_compnr::integer AS umon_ref_compnr, 
                        src:undi::varchar AS undi, 
                        src:undi_ref_compnr::integer AS undi_ref_compnr, 
                        src:unlr::varchar AS unlr, 
                        src:unlr_ref_compnr::integer AS unlr_ref_compnr, 
                        src:untd::varchar AS untd, 
                        src:untd_ref_compnr::integer AS untd_ref_compnr, 
                        src:untm::varchar AS untm, 
                        src:untm_ref_compnr::integer AS untm_ref_compnr, 
                        src:usar::integer AS usar, 
                        src:usar_kw::varchar AS usar_kw, 
                        src:username::varchar AS username, 
                        src:uspg::integer AS uspg, 
                        src:uspg_kw::varchar AS uspg_kw, 
                        src:utpd::integer AS utpd, 
                        src:utpd_kw::varchar AS utpd_kw, 
                        src:uwev::integer AS uwev, 
                        src:uwev_kw::varchar AS uwev_kw, 
                        src:uwks::varchar AS uwks, 
                        src:uwks_ref_compnr::integer AS uwks_ref_compnr, 
                        src:uyrs::varchar AS uyrs, 
                        src:uyrs_ref_compnr::integer AS uyrs_ref_compnr, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:indt,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSMDM000 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alrm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atws_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atws_10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atws_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atws_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atws_4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atws_5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atws_6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atws_7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atws_8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atws_9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccli), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cibp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clgr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpdi_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpdi_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpdi_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctii), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dfpb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dipy), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dist), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:erac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:erdc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:feyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:flds), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:grpl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:icpr_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:icpr_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:icpr_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:icpr_4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:icpr_5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:icrr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inby), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:indt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ingr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ipsm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:isdp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:isom), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iwos), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:kpad), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:meyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ncri), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ngtb_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ngtb_sntb_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:npeg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pmnt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prnt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pvap), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ract_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sbmt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scli), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:shpm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:siyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:skla), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sklb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sklc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:skll), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spdi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spsm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tlgi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trac_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trac_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trac_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tras), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trmd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ttpl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ubpp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uday_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:udgn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:umon_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:undi_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:unlr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:untd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:untm_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:usar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uspg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:utpd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uwev), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uwks_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uyrs_ref_compnr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null