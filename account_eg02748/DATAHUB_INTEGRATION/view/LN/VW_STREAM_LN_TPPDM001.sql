CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TPPDM001 AS SELECT
                        src:aagt::integer AS aagt, 
                        src:aagt_kw::varchar AS aagt_kw, 
                        src:asct::integer AS asct, 
                        src:asct_kw::varchar AS asct_kw, 
                        src:cact::varchar AS cact, 
                        src:cofc::varchar AS cofc, 
                        src:cofc_ref_compnr::integer AS cofc_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cpla_cact_ref_compnr::integer AS cprj_cpla_cact_ref_compnr, 
                        src:cprj_cpla_ref_compnr::integer AS cprj_cpla_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cprj_vers_ref_compnr::integer AS cprj_vers_ref_compnr, 
                        src:cspa::varchar AS cspa, 
                        src:cspa_ref_compnr::integer AS cspa_ref_compnr, 
                        src:ctng::varchar AS ctng, 
                        src:ctng_ctsr_ref_compnr::integer AS ctng_ctsr_ref_compnr, 
                        src:ctng_ref_compnr::integer AS ctng_ref_compnr, 
                        src:ctsr::varchar AS ctsr, 
                        src:dcty::integer AS dcty, 
                        src:dcty_kw::varchar AS dcty_kw, 
                        src:deleted::boolean AS deleted, 
                        src:delt::integer AS delt, 
                        src:delt_kw::varchar AS delt_kw, 
                        src:dfet::integer AS dfet, 
                        src:dfet_kw::varchar AS dfet_kw, 
                        src:dflv::integer AS dflv, 
                        src:dflv_kw::varchar AS dflv_kw, 
                        src:dsct::integer AS dsct, 
                        src:dsct_kw::varchar AS dsct_kw, 
                        src:erng::varchar AS erng, 
                        src:erng_ersr_ref_compnr::integer AS erng_ersr_ref_compnr, 
                        src:erng_ref_compnr::integer AS erng_ref_compnr, 
                        src:ersr::varchar AS ersr, 
                        src:ests::varchar AS ests, 
                        src:loco::varchar AS loco, 
                        src:mnvf::integer AS mnvf, 
                        src:mnvf_kw::varchar AS mnvf_kw, 
                        src:mpvw::integer AS mpvw, 
                        src:mpvw_kw::varchar AS mpvw_kw, 
                        src:ngrp::varchar AS ngrp, 
                        src:ngrp_ests_ref_compnr::integer AS ngrp_ests_ref_compnr, 
                        src:ngrp_prsr_ref_compnr::integer AS ngrp_prsr_ref_compnr, 
                        src:ngrp_ref_compnr::integer AS ngrp_ref_compnr, 
                        src:olvl::integer AS olvl, 
                        src:potp::varchar AS potp, 
                        src:potp_ref_compnr::integer AS potp_ref_compnr, 
                        src:prim::integer AS prim, 
                        src:prim_kw::varchar AS prim_kw, 
                        src:prsr::varchar AS prsr, 
                        src:pung::varchar AS pung, 
                        src:pung_pusr_ref_compnr::integer AS pung_pusr_ref_compnr, 
                        src:pung_ref_compnr::integer AS pung_ref_compnr, 
                        src:pusr::varchar AS pusr, 
                        src:rlcr::integer AS rlcr, 
                        src:rlcr_kw::varchar AS rlcr_kw, 
                        src:rlgn::integer AS rlgn, 
                        src:rlgn_kw::varchar AS rlgn_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:serr::varchar AS serr, 
                        src:song::varchar AS song, 
                        src:song_ref_compnr::integer AS song_ref_compnr, 
                        src:song_sosr_ref_compnr::integer AS song_sosr_ref_compnr, 
                        src:sosr::varchar AS sosr, 
                        src:tgrp::varchar AS tgrp, 
                        src:tgrp_ref_compnr::integer AS tgrp_ref_compnr, 
                        src:tgrp_tpsr_ref_compnr::integer AS tgrp_tpsr_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:tprf::integer AS tprf, 
                        src:tprf_kw::varchar AS tprf_kw, 
                        src:tpsr::varchar AS tpsr, 
                        src:tpte::varchar AS tpte, 
                        src:tpte_ref_compnr::integer AS tpte_ref_compnr, 
                        src:udac::integer AS udac, 
                        src:udac_kw::varchar AS udac_kw, 
                        src:udev::integer AS udev, 
                        src:udev_kw::varchar AS udev_kw, 
                        src:udpl::integer AS udpl, 
                        src:udpl_kw::varchar AS udpl_kw, 
                        src:udpr::integer AS udpr, 
                        src:udpr_kw::varchar AS udpr_kw, 
                        src:udsp::integer AS udsp, 
                        src:udsp_kw::varchar AS udsp_kw, 
                        src:username::varchar AS username, 
                        src:vers::varchar AS vers, 
                        src:whng::varchar AS whng, 
                        src:whng_ref_compnr::integer AS whng_ref_compnr, 
                        src:whng_whsr_ref_compnr::integer AS whng_whsr_ref_compnr, 
                        src:whsr::varchar AS whsr, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                src:loco  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM001 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aagt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:asct), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_vers_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cspa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctng_ctsr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctng_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dcty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:delt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dfet), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dflv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dsct), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:erng_ersr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:erng_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mnvf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mpvw), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ngrp_ests_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ngrp_prsr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ngrp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:olvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:potp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prim), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pung_pusr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pung_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rlcr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rlgn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:song_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:song_sosr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tgrp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tgrp_tpsr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tprf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tpte_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:udac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:udev), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:udpl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:udpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:udsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:whng_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:whng_whsr_ref_compnr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null