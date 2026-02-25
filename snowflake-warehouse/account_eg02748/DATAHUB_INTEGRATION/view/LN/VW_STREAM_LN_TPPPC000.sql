CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TPPPC000 AS SELECT
                        src:amoc::integer AS amoc, 
                        src:amoc_kw::varchar AS amoc_kw, 
                        src:amor::integer AS amor, 
                        src:amor_kw::varchar AS amor_kw, 
                        src:amos::integer AS amos, 
                        src:amos_kw::varchar AS amos_kw, 
                        src:antp::integer AS antp, 
                        src:antp_kw::varchar AS antp_kw, 
                        src:awpp::integer AS awpp, 
                        src:awpp_kw::varchar AS awpp_kw, 
                        src:casc::varchar AS casc, 
                        src:casc_ref_compnr::integer AS casc_ref_compnr, 
                        src:casd::varchar AS casd, 
                        src:casd_ref_compnr::integer AS casd_ref_compnr, 
                        src:ccap::integer AS ccap, 
                        src:ccap_kw::varchar AS ccap_kw, 
                        src:cint::integer AS cint, 
                        src:cint_kw::varchar AS cint_kw, 
                        src:cmif::integer AS cmif, 
                        src:cmif_kw::varchar AS cmif_kw, 
                        src:compnr::integer AS compnr, 
                        src:copr::integer AS copr, 
                        src:copr_kw::varchar AS copr_kw, 
                        src:cprp::integer AS cprp, 
                        src:cprp_kw::varchar AS cprp_kw, 
                        src:cptc::varchar AS cptc, 
                        src:cptc_ref_compnr::integer AS cptc_ref_compnr, 
                        src:cptc_year_peri_ref_compnr::integer AS cptc_year_peri_ref_compnr, 
                        src:cptf::varchar AS cptf, 
                        src:cptf_ref_compnr::integer AS cptf_ref_compnr, 
                        src:cpth::varchar AS cpth, 
                        src:cpth_hyea_hper_ref_compnr::integer AS cpth_hyea_hper_ref_compnr, 
                        src:cpth_ref_compnr::integer AS cpth_ref_compnr, 
                        src:dcic::varchar AS dcic, 
                        src:dcic_ref_compnr::integer AS dcic_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dequ::varchar AS dequ, 
                        src:dequ_ref_compnr::integer AS dequ_ref_compnr, 
                        src:dfrt::varchar AS dfrt, 
                        src:dfrt_ref_compnr::integer AS dfrt_ref_compnr, 
                        src:dhlp::varchar AS dhlp, 
                        src:dhlp_ref_compnr::integer AS dhlp_ref_compnr, 
                        src:ditm::varchar AS ditm, 
                        src:ditm_ref_compnr::integer AS ditm_ref_compnr, 
                        src:dsca::object AS dsca, 
                        src:dsub::varchar AS dsub, 
                        src:dsub_ref_compnr::integer AS dsub_ref_compnr, 
                        src:dtrl::varchar AS dtrl, 
                        src:dtrl_ref_compnr::integer AS dtrl_ref_compnr, 
                        src:expc::integer AS expc, 
                        src:expc_kw::varchar AS expc_kw, 
                        src:hper::integer AS hper, 
                        src:hyea::integer AS hyea, 
                        src:indt::datetime AS indt, 
                        src:ipcs::integer AS ipcs, 
                        src:ipcs_kw::varchar AS ipcs_kw, 
                        src:ipiw::integer AS ipiw, 
                        src:ipiw_kw::varchar AS ipiw_kw, 
                        src:lfcm::integer AS lfcm, 
                        src:lfcm_kw::varchar AS lfcm_kw, 
                        src:mcpr::integer AS mcpr, 
                        src:mcpr_kw::varchar AS mcpr_kw, 
                        src:mfcr::integer AS mfcr, 
                        src:mfcr_kw::varchar AS mfcr_kw, 
                        src:pacc::integer AS pacc, 
                        src:pacc_kw::varchar AS pacc_kw, 
                        src:peri::integer AS peri, 
                        src:pfpp::integer AS pfpp, 
                        src:pfpp_kw::varchar AS pfpp_kw, 
                        src:pgth::integer AS pgth, 
                        src:pgth_kw::varchar AS pgth_kw, 
                        src:ppht::integer AS ppht, 
                        src:ppht_kw::varchar AS ppht_kw, 
                        src:prhr::integer AS prhr, 
                        src:prhr_kw::varchar AS prhr_kw, 
                        src:qacc::integer AS qacc, 
                        src:qacc_kw::varchar AS qacc_kw, 
                        src:revr::integer AS revr, 
                        src:revr_kw::varchar AS revr_kw, 
                        src:scev::integer AS scev, 
                        src:scev_kw::varchar AS scev_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:text::integer AS text, 
                        src:text_ref_compnr::integer AS text_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:tohc::integer AS tohc, 
                        src:tohc_kw::varchar AS tohc_kw, 
                        src:tsem::varchar AS tsem, 
                        src:tsem_ref_compnr::integer AS tsem_ref_compnr, 
                        src:tstk::varchar AS tstk, 
                        src:tstk_ref_compnr::integer AS tstk_ref_compnr, 
                        src:upsa::integer AS upsa, 
                        src:upsa_kw::varchar AS upsa_kw, 
                        src:urcc::integer AS urcc, 
                        src:urcc_kw::varchar AS urcc_kw, 
                        src:username::varchar AS username, 
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
                                        
                src:compnr,
                src:indt  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPPC000 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amoc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amos), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:antp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:awpp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:casc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:casd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccap), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cint), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmif), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cptc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cptc_year_peri_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cptf_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpth_hyea_hper_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpth_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dcic_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dequ_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dfrt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dhlp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ditm_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dsub_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dtrl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:expc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hper), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hyea), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:indt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ipcs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ipiw), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lfcm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mcpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mfcr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pacc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:peri), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pfpp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pgth), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ppht), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prhr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qacc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:revr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scev), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tohc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tsem_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tstk_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:upsa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:urcc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null