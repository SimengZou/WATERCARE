CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TISFC010 AS SELECT
                        src:ardt::datetime AS ardt, 
                        src:asdt::datetime AS asdt, 
                        src:bfls::integer AS bfls, 
                        src:bfls_kw::varchar AS bfls_kw, 
                        src:blcd::varchar AS blcd, 
                        src:blcd_ref_compnr::integer AS blcd_ref_compnr, 
                        src:bpid::varchar AS bpid, 
                        src:bpid_ref_compnr::integer AS bpid_ref_compnr, 
                        src:cctt::integer AS cctt, 
                        src:cctt_kw::varchar AS cctt_kw, 
                        src:cmdt::datetime AS cmdt, 
                        src:compnr::integer AS compnr, 
                        src:cont::varchar AS cont, 
                        src:copo::integer AS copo, 
                        src:copo_kw::varchar AS copo_kw, 
                        src:crem::varchar AS crem, 
                        src:cwoc::varchar AS cwoc, 
                        src:cwoc_ref_compnr::integer AS cwoc_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dmso::integer AS dmso, 
                        src:dmso_kw::varchar AS dmso_kw, 
                        src:esdt::datetime AS esdt, 
                        src:fdur::integer AS fdur, 
                        src:fdur_kw::varchar AS fdur_kw, 
                        src:fidt::datetime AS fidt, 
                        src:fxpd::integer AS fxpd, 
                        src:fxpd_kw::varchar AS fxpd_kw, 
                        src:fxsu::numeric(38, 17) AS fxsu, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:lfdt::datetime AS lfdt, 
                        src:maho::numeric(38, 17) AS maho, 
                        src:mcho::numeric(38, 17) AS mcho, 
                        src:mcno::varchar AS mcno, 
                        src:mcno_ref_compnr::integer AS mcno_ref_compnr, 
                        src:mcoc::numeric(38, 17) AS mcoc, 
                        src:mnrs::integer AS mnrs, 
                        src:mnrs_kw::varchar AS mnrs_kw, 
                        src:mopr::numeric(38, 17) AS mopr, 
                        src:most::numeric(38, 17) AS most, 
                        src:mtyp::varchar AS mtyp, 
                        src:mtyp_ref_compnr::integer AS mtyp_ref_compnr, 
                        src:mvrd::datetime AS mvrd, 
                        src:mvtm::numeric(38, 17) AS mvtm, 
                        src:nnts::integer AS nnts, 
                        src:nomc::integer AS nomc, 
                        src:nopr::integer AS nopr, 
                        src:oopn::integer AS oopn, 
                        src:oopr::varchar AS oopr, 
                        src:opno::integer AS opno, 
                        src:opst::integer AS opst, 
                        src:opst_kw::varchar AS opst_kw, 
                        src:orcd::varchar AS orcd, 
                        src:orrv::varchar AS orrv, 
                        src:oset::integer AS oset, 
                        src:pdno::varchar AS pdno, 
                        src:pdno_ref_compnr::integer AS pdno_ref_compnr, 
                        src:pfdt::datetime AS pfdt, 
                        src:phsp::numeric(38, 17) AS phsp, 
                        src:phsp_buar::numeric(38, 17) AS phsp_buar, 
                        src:phsp_buln::numeric(38, 17) AS phsp_buln, 
                        src:phsp_bupc::numeric(38, 17) AS phsp_bupc, 
                        src:phsp_butm::numeric(38, 17) AS phsp_butm, 
                        src:phsp_buvl::numeric(38, 17) AS phsp_buvl, 
                        src:phsp_buwg::numeric(38, 17) AS phsp_buwg, 
                        src:ploc::varchar AS ploc, 
                        src:prdt::datetime AS prdt, 
                        src:prrd::datetime AS prrd, 
                        src:prte::numeric(38, 17) AS prte, 
                        src:prtm::numeric(38, 17) AS prtm, 
                        src:psdt::datetime AS psdt, 
                        src:qbfd::numeric(38, 17) AS qbfd, 
                        src:qbfd_buar::numeric(38, 17) AS qbfd_buar, 
                        src:qbfd_buln::numeric(38, 17) AS qbfd_buln, 
                        src:qbfd_bupc::numeric(38, 17) AS qbfd_bupc, 
                        src:qbfd_butm::numeric(38, 17) AS qbfd_butm, 
                        src:qbfd_buvl::numeric(38, 17) AS qbfd_buvl, 
                        src:qbfd_buwg::numeric(38, 17) AS qbfd_buwg, 
                        src:qcmp::numeric(38, 17) AS qcmp, 
                        src:qpli::numeric(38, 17) AS qpli, 
                        src:qplo::numeric(38, 17) AS qplo, 
                        src:qpnt::numeric(38, 17) AS qpnt, 
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
                        src:qsbc::numeric(38, 17) AS qsbc, 
                        src:qsbc_buar::numeric(38, 17) AS qsbc_buar, 
                        src:qsbc_buln::numeric(38, 17) AS qsbc_buln, 
                        src:qsbc_bupc::numeric(38, 17) AS qsbc_bupc, 
                        src:qsbc_butm::numeric(38, 17) AS qsbc_butm, 
                        src:qsbc_buvl::numeric(38, 17) AS qsbc_buvl, 
                        src:qsbc_buwg::numeric(38, 17) AS qsbc_buwg, 
                        src:qtbf::numeric(38, 17) AS qtbf, 
                        src:qtbf_buar::numeric(38, 17) AS qtbf_buar, 
                        src:qtbf_buln::numeric(38, 17) AS qtbf_buln, 
                        src:qtbf_bupc::numeric(38, 17) AS qtbf_bupc, 
                        src:qtbf_butm::numeric(38, 17) AS qtbf_butm, 
                        src:qtbf_buvl::numeric(38, 17) AS qtbf_buvl, 
                        src:qtbf_buwg::numeric(38, 17) AS qtbf_buwg, 
                        src:qtbi::numeric(38, 17) AS qtbi, 
                        src:qtbi_buar::numeric(38, 17) AS qtbi_buar, 
                        src:qtbi_buln::numeric(38, 17) AS qtbi_buln, 
                        src:qtbi_bupc::numeric(38, 17) AS qtbi_bupc, 
                        src:qtbi_butm::numeric(38, 17) AS qtbi_butm, 
                        src:qtbi_buvl::numeric(38, 17) AS qtbi_buvl, 
                        src:qtbi_buwg::numeric(38, 17) AS qtbi_buwg, 
                        src:qtor::numeric(38, 17) AS qtor, 
                        src:qtor_buar::numeric(38, 17) AS qtor_buar, 
                        src:qtor_buln::numeric(38, 17) AS qtor_buln, 
                        src:qtor_bupc::numeric(38, 17) AS qtor_bupc, 
                        src:qtor_butm::numeric(38, 17) AS qtor_butm, 
                        src:qtor_buvl::numeric(38, 17) AS qtor_buvl, 
                        src:qtor_buwg::numeric(38, 17) AS qtor_buwg, 
                        src:qtrt::numeric(38, 17) AS qtrt, 
                        src:qutm::numeric(38, 17) AS qutm, 
                        src:qxac::numeric(38, 17) AS qxac, 
                        src:reco::varchar AS reco, 
                        src:reco_ref_compnr::integer AS reco_ref_compnr, 
                        src:refo::varchar AS refo, 
                        src:retm::numeric(38, 17) AS retm, 
                        src:rmch::integer AS rmch, 
                        src:rmch_kw::varchar AS rmch_kw, 
                        src:romt::varchar AS romt, 
                        src:rosi::varchar AS rosi, 
                        src:rowc::varchar AS rowc, 
                        src:rsdt::datetime AS rsdt, 
                        src:runi::numeric(38, 17) AS runi, 
                        src:rusd::datetime AS rusd, 
                        src:rutm::numeric(38, 17) AS rutm, 
                        src:scpq::numeric(38, 17) AS scpq, 
                        src:scpq_buar::numeric(38, 17) AS scpq_buar, 
                        src:scpq_buln::numeric(38, 17) AS scpq_buln, 
                        src:scpq_bupc::numeric(38, 17) AS scpq_bupc, 
                        src:scpq_butm::numeric(38, 17) AS scpq_butm, 
                        src:scpq_buvl::numeric(38, 17) AS scpq_buvl, 
                        src:scpq_buwg::numeric(38, 17) AS scpq_buwg, 
                        src:sctt::integer AS sctt, 
                        src:sctt_kw::varchar AS sctt_kw, 
                        src:sdoc::integer AS sdoc, 
                        src:sdoc_kw::varchar AS sdoc_kw, 
                        src:seqn::integer AS seqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sltm::numeric(38, 17) AS sltm, 
                        src:sopr::integer AS sopr, 
                        src:sopr_kw::varchar AS sopr_kw, 
                        src:sptm::numeric(38, 17) AS sptm, 
                        src:srit::varchar AS srit, 
                        src:sset::integer AS sset, 
                        src:ssmd::integer AS ssmd, 
                        src:ssmd_kw::varchar AS ssmd_kw, 
                        src:sstm::numeric(38, 17) AS sstm, 
                        src:suba::varchar AS suba, 
                        src:suba_ref_compnr::integer AS suba_ref_compnr, 
                        src:subr::numeric(38, 17) AS subr, 
                        src:subs::varchar AS subs, 
                        src:subs_ref_compnr::integer AS subs_ref_compnr, 
                        src:sutm::numeric(38, 17) AS sutm, 
                        src:tano::varchar AS tano, 
                        src:tano_ref_compnr::integer AS tano_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:tlyn::integer AS tlyn, 
                        src:tlyn_kw::varchar AS tlyn_kw, 
                        src:trdf::datetime AS trdf, 
                        src:trdl::numeric(38, 17) AS trdl, 
                        src:trdt::datetime AS trdt, 
                        src:trls::numeric(38, 17) AS trls, 
                        src:trno::datetime AS trno, 
                        src:tuni::integer AS tuni, 
                        src:tuni_kw::varchar AS tuni_kw, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:username::varchar AS username, 
                        src:wcst::varchar AS wcst, 
                        src:wcst_cwoc_mtyp_ref_compnr::integer AS wcst_cwoc_mtyp_ref_compnr, 
                        src:whsa::varchar AS whsa, 
                        src:whsa_ref_compnr::integer AS whsa_ref_compnr, 
                        src:ydtp::integer AS ydtp, 
                        src:ydtp_kw::varchar AS ydtp_kw, 
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
                                        
                src:compnr,
                src:pdno,
                src:opno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TISFC010 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ardt), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:asdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bfls), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blcd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cctt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cmdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmso), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:esdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fdur), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:fidt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fxpd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fxsu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:lfdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:maho), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mcho), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mcno_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mcoc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mnrs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mopr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:most), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mtyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:mvrd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mvtm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nnts), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nomc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nopr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oopn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:opno), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:opst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oset), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pdno_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:pfdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:phsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:phsp_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:phsp_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:phsp_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:phsp_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:phsp_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:phsp_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:prdt), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:prrd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prte), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prtm), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:psdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qbfd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qbfd_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qbfd_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qbfd_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qbfd_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qbfd_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qbfd_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qpli), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qplo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qpnt), '0'), 38, 10) is not null and 
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
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qsbc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qsbc_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qsbc_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qsbc_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qsbc_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qsbc_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qsbc_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbf_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbf_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbf_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbf_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbf_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbf_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbi_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbi_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbi_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbi_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbi_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtbi_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtor_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtor_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtor_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtor_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtor_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtor_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qtrt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qutm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qxac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:reco_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:retm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rmch), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rsdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:runi), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rusd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rutm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpq_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpq_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpq_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpq_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpq_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpq_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sctt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sdoc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:seqn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sltm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sopr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sptm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sset), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ssmd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sstm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:suba_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:subr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:subs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sutm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tano_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tlyn), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:trdf), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trdl), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:trdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trls), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:trno), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tuni), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wcst_cwoc_mtyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:whsa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ydtp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:yldp), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null