CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TTAAD200 AS SELECT
                        src:allp::integer AS allp, 
                        src:allp_kw::varchar AS allp_kw, 
                        src:apsv::varchar AS apsv, 
                        src:apud::integer AS apud, 
                        src:apud_kw::varchar AS apud_kw, 
                        src:clan::varchar AS clan, 
                        src:clan_ref_compnr::integer AS clan_ref_compnr, 
                        src:cmen::varchar AS cmen, 
                        src:cmod::varchar AS cmod, 
                        src:comp::integer AS comp, 
                        src:comp_ref_compnr::integer AS comp_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cpac::varchar AS cpac, 
                        src:cpac_ref_compnr::integer AS cpac_ref_compnr, 
                        src:crol_1::varchar AS crol_1, 
                        src:crol_2::varchar AS crol_2, 
                        src:crol_3::varchar AS crol_3, 
                        src:crol_4::varchar AS crol_4, 
                        src:crol_5::varchar AS crol_5, 
                        src:crol_6::varchar AS crol_6, 
                        src:csbh::integer AS csbh, 
                        src:csbh_kw::varchar AS csbh_kw, 
                        src:csbn::integer AS csbn, 
                        src:csbn_kw::varchar AS csbn_kw, 
                        src:defa::integer AS defa, 
                        src:defa_kw::varchar AS defa_kw, 
                        src:deleted::boolean AS deleted, 
                        src:delh::integer AS delh, 
                        src:dskt::varchar AS dskt, 
                        src:edtm::integer AS edtm, 
                        src:faxn::varchar AS faxn, 
                        src:hist::integer AS hist, 
                        src:hist_kw::varchar AS hist_kw, 
                        src:hmdi::object AS hmdi, 
                        src:ifsi::object AS ifsi, 
                        src:ifss::integer AS ifss, 
                        src:ifss_kw::varchar AS ifss_kw, 
                        src:ifsu::integer AS ifsu, 
                        src:ifsu_kw::varchar AS ifsu_kw, 
                        src:intt::integer AS intt, 
                        src:loca::varchar AS loca, 
                        src:loca_ref_compnr::integer AS loca_ref_compnr, 
                        src:mail::object AS mail, 
                        src:maxp::integer AS maxp, 
                        src:mtyp::integer AS mtyp, 
                        src:mtyp_kw::varchar AS mtyp_kw, 
                        src:name::object AS name, 
                        src:pacc::varchar AS pacc, 
                        src:pacc_ref_compnr::integer AS pacc_ref_compnr, 
                        src:palp::integer AS palp, 
                        src:palp_kw::varchar AS palp_kw, 
                        src:phlp::object AS phlp, 
                        src:prrt::integer AS prrt, 
                        src:pswd::varchar AS pswd, 
                        src:pwyn::integer AS pwyn, 
                        src:pwyn_kw::varchar AS pwyn_kw, 
                        src:role::varchar AS role, 
                        src:s132::integer AS s132, 
                        src:s132_kw::varchar AS s132_kw, 
                        src:scdh::integer AS scdh, 
                        src:scdh_kw::varchar AS scdh_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:shcm::varchar AS shcm, 
                        src:shtp::integer AS shtp, 
                        src:shtp_kw::varchar AS shtp_kw, 
                        src:sita::varchar AS sita, 
                        src:smsa::varchar AS smsa, 
                        src:stdp::integer AS stdp, 
                        src:stdp_kw::varchar AS stdp_kw, 
                        src:stpr::integer AS stpr, 
                        src:stpr_kw::varchar AS stpr_kw, 
                        src:sttm::integer AS sttm, 
                        src:syst::varchar AS syst, 
                        src:syst2::varchar AS syst2, 
                        src:tdir::integer AS tdir, 
                        src:tdir_kw::varchar AS tdir_kw, 
                        src:tdva::varchar AS tdva, 
                        src:tdvc::varchar AS tdvc, 
                        src:tdvp::varchar AS tdvp, 
                        src:tdvp_ref_compnr::integer AS tdvp_ref_compnr, 
                        src:tele::varchar AS tele, 
                        src:telx::varchar AS telx, 
                        src:tenant::varchar AS tenant, 
                        src:timestamp::datetime AS timestamp, 
                        src:tlb1::integer AS tlb1, 
                        src:tlb1_kw::varchar AS tlb1_kw, 
                        src:tlb2::integer AS tlb2, 
                        src:tlb2_kw::varchar AS tlb2_kw, 
                        src:tter::varchar AS tter, 
                        src:ttxa::varchar AS ttxa, 
                        src:ttxf::varchar AS ttxf, 
                        src:ttxg::varchar AS ttxg, 
                        src:tusd::varchar AS tusd, 
                        src:tusd_ref_compnr::integer AS tusd_ref_compnr, 
                        src:ucln::integer AS ucln, 
                        src:ucln_kw::varchar AS ucln_kw, 
                        src:uhmd::integer AS uhmd, 
                        src:uhmd_kw::varchar AS uhmd_kw, 
                        src:uico::integer AS uico, 
                        src:uico_kw::varchar AS uico_kw, 
                        src:uids::integer AS uids, 
                        src:uids_kw::varchar AS uids_kw, 
                        src:uoos::integer AS uoos, 
                        src:uoos_kw::varchar AS uoos_kw, 
                        src:user::varchar AS user, 
                        src:user_role_ref_compnr::integer AS user_role_ref_compnr, 
                        src:username::varchar AS username, 
                        src:uspr::varchar AS uspr, 
                        src:uspr_ref_compnr::integer AS uspr_ref_compnr, 
                        src:utyp::integer AS utyp, 
                        src:utyp_kw::varchar AS utyp_kw, 
                        src:uusr::varchar AS uusr, 
                        src:wait::integer AS wait, 
                        src:whlp::varchar AS whlp, 
                        src:wtit::object AS wtit, 
                        src:z_mmode::integer AS z_mmode, 
                        src:z_mmode_kw::varchar AS z_mmode_kw, 
                        src:z_pswd::varchar AS z_pswd, 
                        src:za_dlan::varchar AS za_dlan, 
                        src:zb_ssou::object AS zb_ssou, 
                        src:zb_ugsu::integer AS zb_ugsu, 
                        src:zb_ugsu_kw::varchar AS zb_ugsu_kw, 
                        src:zc_fcom::integer AS zc_fcom, 
                        src:zc_lcom::integer AS zc_lcom, 
                        src:zd_idnt::varchar AS zd_idnt, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:user,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TTAAD200 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:allp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:apud), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:comp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:comp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpac_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csbh), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csbn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:defa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:delh), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:edtm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hist), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ifss), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ifsu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:intt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:loca_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:maxp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mtyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pacc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:palp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prrt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pwyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:s132), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scdh), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:shtp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stdp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sttm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tdir), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tdvp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tlb1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tlb2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tusd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ucln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uhmd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uico), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uids), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uoos), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:user_role_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uspr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:utyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wait), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:z_mmode), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:zb_ugsu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:zc_fcom), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:zc_lcom), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null