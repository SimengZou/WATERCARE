CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TCCOM110 AS SELECT
                        src:aalg::integer AS aalg, 
                        src:aalg_kw::varchar AS aalg_kw, 
                        src:aapr::integer AS aapr, 
                        src:aapr_kw::varchar AS aapr_kw, 
                        src:aaps::integer AS aaps, 
                        src:aaps_kw::varchar AS aaps_kw, 
                        src:ackx::integer AS ackx, 
                        src:ackx_kw::varchar AS ackx_kw, 
                        src:aprc::integer AS aprc, 
                        src:aprc_kw::varchar AS aprc_kw, 
                        src:apsr::integer AS apsr, 
                        src:apsr_kw::varchar AS apsr_kw, 
                        src:arcu::varchar AS arcu, 
                        src:arcu_ref_compnr::integer AS arcu_ref_compnr, 
                        src:arev::numeric(38, 17) AS arev, 
                        src:asma::integer AS asma, 
                        src:asma_kw::varchar AS asma_kw, 
                        src:bppr::varchar AS bppr, 
                        src:bppr_ref_compnr::integer AS bppr_ref_compnr, 
                        src:bpst::integer AS bpst, 
                        src:bpst_kw::varchar AS bpst_kw, 
                        src:bpus::varchar AS bpus, 
                        src:bpus_ref_compnr::integer AS bpus_ref_compnr, 
                        src:cacf::integer AS cacf, 
                        src:cacf_kw::varchar AS cacf_kw, 
                        src:cacs::integer AS cacs, 
                        src:cacs_kw::varchar AS cacs_kw, 
                        src:cadr::varchar AS cadr, 
                        src:cadr_ref_compnr::integer AS cadr_ref_compnr, 
                        src:cbps::varchar AS cbps, 
                        src:cbps_ref_compnr::integer AS cbps_ref_compnr, 
                        src:cbrn::varchar AS cbrn, 
                        src:cbrn_ref_compnr::integer AS cbrn_ref_compnr, 
                        src:cbtp::varchar AS cbtp, 
                        src:cbtp_ref_compnr::integer AS cbtp_ref_compnr, 
                        src:ccal::varchar AS ccal, 
                        src:ccal_ref_compnr::integer AS ccal_ref_compnr, 
                        src:ccnt::varchar AS ccnt, 
                        src:ccnt_ref_compnr::integer AS ccnt_ref_compnr, 
                        src:cdec::varchar AS cdec, 
                        src:cdec_ref_compnr::integer AS cdec_ref_compnr, 
                        src:chak::integer AS chak, 
                        src:chak_kw::varchar AS chak_kw, 
                        src:chan::varchar AS chan, 
                        src:chan_ref_compnr::integer AS chan_ref_compnr, 
                        src:clan::varchar AS clan, 
                        src:clan_ref_compnr::integer AS clan_ref_compnr, 
                        src:clgr::varchar AS clgr, 
                        src:clgr_ref_compnr::integer AS clgr_ref_compnr, 
                        src:cofc::varchar AS cofc, 
                        src:compnr::integer AS compnr, 
                        src:cpls::varchar AS cpls, 
                        src:cpls_ref_compnr::integer AS cpls_ref_compnr, 
                        src:crdt::datetime AS crdt, 
                        src:creg::varchar AS creg, 
                        src:creg_ref_compnr::integer AS creg_ref_compnr, 
                        src:crep::varchar AS crep, 
                        src:crep_ref_compnr::integer AS crep_ref_compnr, 
                        src:crtc::varchar AS crtc, 
                        src:cspr::integer AS cspr, 
                        src:cspr_ref_compnr::integer AS cspr_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:endt::datetime AS endt, 
                        src:frin::integer AS frin, 
                        src:frin_kw::varchar AS frin_kw, 
                        src:incd::varchar AS incd, 
                        src:incd_ref_compnr::integer AS incd_ref_compnr, 
                        src:itbp::varchar AS itbp, 
                        src:itbp_ref_compnr::integer AS itbp_ref_compnr, 
                        src:lcmp::integer AS lcmp, 
                        src:lcmp_ref_compnr::integer AS lcmp_ref_compnr, 
                        src:lmdt::datetime AS lmdt, 
                        src:lmsp::numeric(38, 17) AS lmsp, 
                        src:lmus::varchar AS lmus, 
                        src:macc::integer AS macc, 
                        src:macc_kw::varchar AS macc_kw, 
                        src:mask::varchar AS mask, 
                        src:mask_ref_compnr::integer AS mask_ref_compnr, 
                        src:mcfr::integer AS mcfr, 
                        src:mcfr_kw::varchar AS mcfr_kw, 
                        src:noem::integer AS noem, 
                        src:odis::numeric(38, 17) AS odis, 
                        src:ofbp::varchar AS ofbp, 
                        src:ofbp_ref_compnr::integer AS ofbp_ref_compnr, 
                        src:ofbp_stbp_ref_compnr::integer AS ofbp_stbp_ref_compnr, 
                        src:osno::varchar AS osno, 
                        src:osrp::varchar AS osrp, 
                        src:osrp_ref_compnr::integer AS osrp_ref_compnr, 
                        src:owsh::object AS owsh, 
                        src:pcmp::integer AS pcmp, 
                        src:pcmp_kw::varchar AS pcmp_kw, 
                        src:pldd::varchar AS pldd, 
                        src:pldd_ref_compnr::integer AS pldd_ref_compnr, 
                        src:prio::integer AS prio, 
                        src:prio_ref_compnr::integer AS prio_ref_compnr, 
                        src:prms::integer AS prms, 
                        src:prms_kw::varchar AS prms_kw, 
                        src:ptpa::varchar AS ptpa, 
                        src:ptpa_ref_compnr::integer AS ptpa_ref_compnr, 
                        src:rdec::varchar AS rdec, 
                        src:rdec_ref_compnr::integer AS rdec_ref_compnr, 
                        src:rinv::integer AS rinv, 
                        src:rinv_kw::varchar AS rinv_kw, 
                        src:rtng::integer AS rtng, 
                        src:rtng_kw::varchar AS rtng_kw, 
                        src:sbil::integer AS sbil, 
                        src:sbil_kw::varchar AS sbil_kw, 
                        src:scon::integer AS scon, 
                        src:scon_kw::varchar AS scon_kw, 
                        src:scqq::integer AS scqq, 
                        src:scqq_kw::varchar AS scqq_kw, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sotp::varchar AS sotp, 
                        src:sscc::integer AS sscc, 
                        src:sscc_kw::varchar AS sscc_kw, 
                        src:stbp::varchar AS stbp, 
                        src:stbp_ref_compnr::integer AS stbp_ref_compnr, 
                        src:stdt::datetime AS stdt, 
                        src:ster::varchar AS ster, 
                        src:ster_ref_compnr::integer AS ster_ref_compnr, 
                        src:tefx::varchar AS tefx, 
                        src:telp::varchar AS telp, 
                        src:tfci::varchar AS tfci, 
                        src:tfcy::varchar AS tfcy, 
                        src:tfex::varchar AS tfex, 
                        src:tfln::varchar AS tfln, 
                        src:timestamp::datetime AS timestamp, 
                        src:tlci::varchar AS tlci, 
                        src:tlcy::varchar AS tlcy, 
                        src:tlex::varchar AS tlex, 
                        src:tlln::varchar AS tlln, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:ucnd::integer AS ucnd, 
                        src:ucnd_kw::varchar AS ucnd_kw, 
                        src:umsp::numeric(38, 17) AS umsp, 
                        src:user::varchar AS user, 
                        src:username::varchar AS username, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                src:ofbp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCCOM110 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aalg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aapr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aaps), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ackx), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aprc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:apsr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:arcu_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:arev), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:asma), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bppr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bpst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bpus_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cacf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cacs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cbps_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cbtp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccnt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:chak), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:chan_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clgr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpls_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:crdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cspr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cspr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:endt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:frin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:incd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:lmdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lmsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:macc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mask_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mcfr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:noem), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:odis), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ofbp_stbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pldd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prio), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prio_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prms), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rdec_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rinv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtng), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sbil), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scon), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scqq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sscc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:stdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ster_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ucnd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:umsp), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null