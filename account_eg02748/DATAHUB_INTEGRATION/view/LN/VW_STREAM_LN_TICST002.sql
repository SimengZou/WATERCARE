CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TICST002 AS SELECT
                        src:ahma::numeric(38, 17) AS ahma, 
                        src:ahmc::numeric(38, 17) AS ahmc, 
                        src:ahmq::numeric(38, 17) AS ahmq, 
                        src:ahms::numeric(38, 17) AS ahms, 
                        src:ahwq::numeric(38, 17) AS ahwq, 
                        src:ahws::numeric(38, 17) AS ahws, 
                        src:amcc_1::numeric(38, 17) AS amcc_1, 
                        src:amcc_2::numeric(38, 17) AS amcc_2, 
                        src:amcc_3::numeric(38, 17) AS amcc_3, 
                        src:amcc_dwhc::numeric(38, 17) AS amcc_dwhc, 
                        src:amcc_lclc::numeric(38, 17) AS amcc_lclc, 
                        src:amcc_refc::numeric(38, 17) AS amcc_refc, 
                        src:amcc_rpc1::numeric(38, 17) AS amcc_rpc1, 
                        src:amcc_rpc2::numeric(38, 17) AS amcc_rpc2, 
                        src:aohc_1::numeric(38, 17) AS aohc_1, 
                        src:aohc_2::numeric(38, 17) AS aohc_2, 
                        src:aohc_3::numeric(38, 17) AS aohc_3, 
                        src:aohc_dwhc::numeric(38, 17) AS aohc_dwhc, 
                        src:aohc_lclc::numeric(38, 17) AS aohc_lclc, 
                        src:aohc_refc::numeric(38, 17) AS aohc_refc, 
                        src:aohc_rpc1::numeric(38, 17) AS aohc_rpc1, 
                        src:aohc_rpc2::numeric(38, 17) AS aohc_rpc2, 
                        src:ascc_1::numeric(38, 17) AS ascc_1, 
                        src:ascc_2::numeric(38, 17) AS ascc_2, 
                        src:ascc_3::numeric(38, 17) AS ascc_3, 
                        src:ascc_dwhc::numeric(38, 17) AS ascc_dwhc, 
                        src:ascc_lclc::numeric(38, 17) AS ascc_lclc, 
                        src:ascc_refc::numeric(38, 17) AS ascc_refc, 
                        src:ascc_rpc1::numeric(38, 17) AS ascc_rpc1, 
                        src:ascc_rpc2::numeric(38, 17) AS ascc_rpc2, 
                        src:ascq_1::numeric(38, 17) AS ascq_1, 
                        src:ascq_2::numeric(38, 17) AS ascq_2, 
                        src:ascq_3::numeric(38, 17) AS ascq_3, 
                        src:ascq_dwhc::numeric(38, 17) AS ascq_dwhc, 
                        src:ascq_lclc::numeric(38, 17) AS ascq_lclc, 
                        src:ascq_refc::numeric(38, 17) AS ascq_refc, 
                        src:ascq_rpc1::numeric(38, 17) AS ascq_rpc1, 
                        src:ascq_rpc2::numeric(38, 17) AS ascq_rpc2, 
                        src:ashc::numeric(38, 17) AS ashc, 
                        src:ashm::numeric(38, 17) AS ashm, 
                        src:ashq::numeric(38, 17) AS ashq, 
                        src:ashs::numeric(38, 17) AS ashs, 
                        src:asmq::numeric(38, 17) AS asmq, 
                        src:asms::numeric(38, 17) AS asms, 
                        src:awgc_1::numeric(38, 17) AS awgc_1, 
                        src:awgc_2::numeric(38, 17) AS awgc_2, 
                        src:awgc_3::numeric(38, 17) AS awgc_3, 
                        src:awgc_dwhc::numeric(38, 17) AS awgc_dwhc, 
                        src:awgc_lclc::numeric(38, 17) AS awgc_lclc, 
                        src:awgc_refc::numeric(38, 17) AS awgc_refc, 
                        src:awgc_rpc1::numeric(38, 17) AS awgc_rpc1, 
                        src:awgc_rpc2::numeric(38, 17) AS awgc_rpc2, 
                        src:bpid::varchar AS bpid, 
                        src:bpid_ref_compnr::integer AS bpid_ref_compnr, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cmdt::datetime AS cmdt, 
                        src:compnr::integer AS compnr, 
                        src:cowc::varchar AS cowc, 
                        src:cums::numeric(38, 17) AS cums, 
                        src:cwoc::varchar AS cwoc, 
                        src:cwoc_ref_compnr::integer AS cwoc_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dptm_fcmp::integer AS dptm_fcmp, 
                        src:eshc::numeric(38, 17) AS eshc, 
                        src:eshm::numeric(38, 17) AS eshm, 
                        src:fdur::integer AS fdur, 
                        src:fdur_kw::varchar AS fdur_kw, 
                        src:frso::integer AS frso, 
                        src:frso_kw::varchar AS frso_kw, 
                        src:hrem::numeric(38, 17) AS hrem, 
                        src:hrmc::numeric(38, 17) AS hrmc, 
                        src:mccs_1::numeric(38, 17) AS mccs_1, 
                        src:mccs_2::numeric(38, 17) AS mccs_2, 
                        src:mccs_3::numeric(38, 17) AS mccs_3, 
                        src:mccs_dwhc::numeric(38, 17) AS mccs_dwhc, 
                        src:mccs_lclc::numeric(38, 17) AS mccs_lclc, 
                        src:mccs_refc::numeric(38, 17) AS mccs_refc, 
                        src:mccs_rpc1::numeric(38, 17) AS mccs_rpc1, 
                        src:mccs_rpc2::numeric(38, 17) AS mccs_rpc2, 
                        src:mcno::varchar AS mcno, 
                        src:mcno_ref_compnr::integer AS mcno_ref_compnr, 
                        src:mcoc::numeric(38, 17) AS mcoc, 
                        src:mopr::numeric(38, 17) AS mopr, 
                        src:most::numeric(38, 17) AS most, 
                        src:nomc::integer AS nomc, 
                        src:ohcs_1::numeric(38, 17) AS ohcs_1, 
                        src:ohcs_2::numeric(38, 17) AS ohcs_2, 
                        src:ohcs_3::numeric(38, 17) AS ohcs_3, 
                        src:ohcs_dwhc::numeric(38, 17) AS ohcs_dwhc, 
                        src:ohcs_lclc::numeric(38, 17) AS ohcs_lclc, 
                        src:ohcs_refc::numeric(38, 17) AS ohcs_refc, 
                        src:ohcs_rpc1::numeric(38, 17) AS ohcs_rpc1, 
                        src:ohcs_rpc2::numeric(38, 17) AS ohcs_rpc2, 
                        src:opno::integer AS opno, 
                        src:oprc::varchar AS oprc, 
                        src:oprc_ref_compnr::integer AS oprc_ref_compnr, 
                        src:pcdt::datetime AS pcdt, 
                        src:pdno::varchar AS pdno, 
                        src:pdno_ref_compnr::integer AS pdno_ref_compnr, 
                        src:prte::numeric(38, 17) AS prte, 
                        src:qcmp::numeric(38, 17) AS qcmp, 
                        src:qpln::numeric(38, 17) AS qpln, 
                        src:qqar::numeric(38, 17) AS qqar, 
                        src:qqar_buar::numeric(38, 17) AS qqar_buar, 
                        src:qqar_buln::numeric(38, 17) AS qqar_buln, 
                        src:qqar_bupc::numeric(38, 17) AS qqar_bupc, 
                        src:qqar_butm::numeric(38, 17) AS qqar_butm, 
                        src:qqar_buvl::numeric(38, 17) AS qqar_buvl, 
                        src:qqar_buwg::numeric(38, 17) AS qqar_buwg, 
                        src:qrjc::numeric(38, 17) AS qrjc, 
                        src:qrjc_buar::numeric(38, 17) AS qrjc_buar, 
                        src:qrjc_buln::numeric(38, 17) AS qrjc_buln, 
                        src:qrjc_bupc::numeric(38, 17) AS qrjc_bupc, 
                        src:qrjc_butm::numeric(38, 17) AS qrjc_butm, 
                        src:qrjc_buvl::numeric(38, 17) AS qrjc_buvl, 
                        src:qrjc_buwg::numeric(38, 17) AS qrjc_buwg, 
                        src:runi::numeric(38, 17) AS runi, 
                        src:rutm::numeric(38, 17) AS rutm, 
                        src:sccs_1::numeric(38, 17) AS sccs_1, 
                        src:sccs_2::numeric(38, 17) AS sccs_2, 
                        src:sccs_3::numeric(38, 17) AS sccs_3, 
                        src:sccs_dwhc::numeric(38, 17) AS sccs_dwhc, 
                        src:sccs_lclc::numeric(38, 17) AS sccs_lclc, 
                        src:sccs_refc::numeric(38, 17) AS sccs_refc, 
                        src:sccs_rpc1::numeric(38, 17) AS sccs_rpc1, 
                        src:sccs_rpc2::numeric(38, 17) AS sccs_rpc2, 
                        src:scpq::numeric(38, 17) AS scpq, 
                        src:scpq_buar::numeric(38, 17) AS scpq_buar, 
                        src:scpq_buln::numeric(38, 17) AS scpq_buln, 
                        src:scpq_bupc::numeric(38, 17) AS scpq_bupc, 
                        src:scpq_butm::numeric(38, 17) AS scpq_butm, 
                        src:scpq_buvl::numeric(38, 17) AS scpq_buvl, 
                        src:scpq_buwg::numeric(38, 17) AS scpq_buwg, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:subr::numeric(38, 17) AS subr, 
                        src:sutm::numeric(38, 17) AS sutm, 
                        src:tano::varchar AS tano, 
                        src:timestamp::datetime AS timestamp, 
                        src:username::varchar AS username, 
                        src:wgcs_1::numeric(38, 17) AS wgcs_1, 
                        src:wgcs_2::numeric(38, 17) AS wgcs_2, 
                        src:wgcs_3::numeric(38, 17) AS wgcs_3, 
                        src:wgcs_dwhc::numeric(38, 17) AS wgcs_dwhc, 
                        src:wgcs_lclc::numeric(38, 17) AS wgcs_lclc, 
                        src:wgcs_refc::numeric(38, 17) AS wgcs_refc, 
                        src:wgcs_rpc1::numeric(38, 17) AS wgcs_rpc1, 
                        src:wgcs_rpc2::numeric(38, 17) AS wgcs_rpc2, 
                        src:yldp::numeric(38, 17) AS yldp, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:pdno,
                src:compnr,
                src:opno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TICST002 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ahma), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ahmc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ahmq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ahms), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ahwq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ahws), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amcc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amcc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amcc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amcc_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amcc_lclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amcc_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amcc_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amcc_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aohc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aohc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aohc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aohc_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aohc_lclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aohc_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aohc_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aohc_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascc_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascc_lclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascc_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascc_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascc_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascq_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascq_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascq_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascq_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascq_lclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascq_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascq_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascq_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ashc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ashm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ashq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ashs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:asmq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:asms), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:awgc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:awgc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:awgc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:awgc_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:awgc_lclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:awgc_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:awgc_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:awgc_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cmdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cums), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dptm_fcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eshc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eshm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fdur), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:frso), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hrem), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hrmc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mccs_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mccs_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mccs_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mccs_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mccs_lclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mccs_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mccs_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mccs_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mcno_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mcoc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mopr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:most), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nomc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ohcs_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ohcs_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ohcs_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ohcs_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ohcs_lclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ohcs_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ohcs_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ohcs_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:opno), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oprc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:pcdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pdno_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prte), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qpln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qqar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qqar_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qqar_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qqar_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qqar_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qqar_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qqar_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrjc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrjc_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrjc_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrjc_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrjc_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrjc_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qrjc_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:runi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rutm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sccs_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sccs_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sccs_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sccs_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sccs_lclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sccs_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sccs_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sccs_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpq_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpq_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpq_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpq_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpq_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpq_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:subr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sutm), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wgcs_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wgcs_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wgcs_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wgcs_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wgcs_lclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wgcs_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wgcs_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wgcs_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:yldp), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null