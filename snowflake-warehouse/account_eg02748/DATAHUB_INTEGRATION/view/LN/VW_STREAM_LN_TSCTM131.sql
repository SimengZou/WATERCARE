CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM131 AS SELECT
                        src:alco_1::numeric(38, 17) AS alco_1, 
                        src:alco_2::numeric(38, 17) AS alco_2, 
                        src:alco_3::numeric(38, 17) AS alco_3, 
                        src:alco_cntc::numeric(38, 17) AS alco_cntc, 
                        src:alco_dwhc::numeric(38, 17) AS alco_dwhc, 
                        src:alco_refc::numeric(38, 17) AS alco_refc, 
                        src:alqu::numeric(38, 17) AS alqu, 
                        src:alqu_buar::numeric(38, 17) AS alqu_buar, 
                        src:alqu_buln::numeric(38, 17) AS alqu_buln, 
                        src:alqu_bupc::numeric(38, 17) AS alqu_bupc, 
                        src:alqu_buvl::numeric(38, 17) AS alqu_buvl, 
                        src:alqu_buwg::numeric(38, 17) AS alqu_buwg, 
                        src:alqu_invu::numeric(38, 17) AS alqu_invu, 
                        src:alsa::numeric(38, 17) AS alsa, 
                        src:alsa_dwhc::numeric(38, 17) AS alsa_dwhc, 
                        src:alsa_homc::numeric(38, 17) AS alsa_homc, 
                        src:alsa_refc::numeric(38, 17) AS alsa_refc, 
                        src:alsa_rpc1::numeric(38, 17) AS alsa_rpc1, 
                        src:alsa_rpc2::numeric(38, 17) AS alsa_rpc2, 
                        src:amnt::numeric(38, 17) AS amnt, 
                        src:amnt_dwhc::numeric(38, 17) AS amnt_dwhc, 
                        src:amnt_homc::numeric(38, 17) AS amnt_homc, 
                        src:amnt_refc::numeric(38, 17) AS amnt_refc, 
                        src:amnt_rpc1::numeric(38, 17) AS amnt_rpc1, 
                        src:amnt_rpc2::numeric(38, 17) AS amnt_rpc2, 
                        src:camt_1::numeric(38, 17) AS camt_1, 
                        src:camt_2::numeric(38, 17) AS camt_2, 
                        src:camt_3::numeric(38, 17) AS camt_3, 
                        src:camt_cntc::numeric(38, 17) AS camt_cntc, 
                        src:camt_dwhc::numeric(38, 17) AS camt_dwhc, 
                        src:camt_refc::numeric(38, 17) AS camt_refc, 
                        src:ccmp::varchar AS ccmp, 
                        src:ccmp_ref_compnr::integer AS ccmp_ref_compnr, 
                        src:cctp::varchar AS cctp, 
                        src:cctp_ref_compnr::integer AS cctp_ref_compnr, 
                        src:cdis_1::varchar AS cdis_1, 
                        src:cdis_10::varchar AS cdis_10, 
                        src:cdis_11::varchar AS cdis_11, 
                        src:cdis_2::varchar AS cdis_2, 
                        src:cdis_3::varchar AS cdis_3, 
                        src:cdis_4::varchar AS cdis_4, 
                        src:cdis_5::varchar AS cdis_5, 
                        src:cdis_6::varchar AS cdis_6, 
                        src:cdis_7::varchar AS cdis_7, 
                        src:cdis_8::varchar AS cdis_8, 
                        src:cdis_9::varchar AS cdis_9, 
                        src:cfln::integer AS cfln, 
                        src:compnr::integer AS compnr, 
                        src:cotp::integer AS cotp, 
                        src:cotp_kw::varchar AS cotp_kw, 
                        src:cseq::integer AS cseq, 
                        src:csgr::varchar AS csgr, 
                        src:csgr_ref_compnr::integer AS csgr_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:disc_1::numeric(38, 17) AS disc_1, 
                        src:disc_10::numeric(38, 17) AS disc_10, 
                        src:disc_11::numeric(38, 17) AS disc_11, 
                        src:disc_2::numeric(38, 17) AS disc_2, 
                        src:disc_3::numeric(38, 17) AS disc_3, 
                        src:disc_4::numeric(38, 17) AS disc_4, 
                        src:disc_5::numeric(38, 17) AS disc_5, 
                        src:disc_6::numeric(38, 17) AS disc_6, 
                        src:disc_7::numeric(38, 17) AS disc_7, 
                        src:disc_8::numeric(38, 17) AS disc_8, 
                        src:disc_9::numeric(38, 17) AS disc_9, 
                        src:dmde_1::varchar AS dmde_1, 
                        src:dmde_10::varchar AS dmde_10, 
                        src:dmde_11::varchar AS dmde_11, 
                        src:dmde_2::varchar AS dmde_2, 
                        src:dmde_3::varchar AS dmde_3, 
                        src:dmde_4::varchar AS dmde_4, 
                        src:dmde_5::varchar AS dmde_5, 
                        src:dmde_6::varchar AS dmde_6, 
                        src:dmde_7::varchar AS dmde_7, 
                        src:dmde_8::varchar AS dmde_8, 
                        src:dmde_9::varchar AS dmde_9, 
                        src:dmse_1::integer AS dmse_1, 
                        src:dmse_10::integer AS dmse_10, 
                        src:dmse_11::integer AS dmse_11, 
                        src:dmse_2::integer AS dmse_2, 
                        src:dmse_3::integer AS dmse_3, 
                        src:dmse_4::integer AS dmse_4, 
                        src:dmse_5::integer AS dmse_5, 
                        src:dmse_6::integer AS dmse_6, 
                        src:dmse_7::integer AS dmse_7, 
                        src:dmse_8::integer AS dmse_8, 
                        src:dmse_9::integer AS dmse_9, 
                        src:dmth_1::integer AS dmth_1, 
                        src:dmth_10::integer AS dmth_10, 
                        src:dmth_11::integer AS dmth_11, 
                        src:dmth_2::integer AS dmth_2, 
                        src:dmth_3::integer AS dmth_3, 
                        src:dmth_4::integer AS dmth_4, 
                        src:dmth_5::integer AS dmth_5, 
                        src:dmth_6::integer AS dmth_6, 
                        src:dmth_7::integer AS dmth_7, 
                        src:dmth_8::integer AS dmth_8, 
                        src:dmth_9::integer AS dmth_9, 
                        src:dmth_kw_1::varchar AS dmth_kw_1, 
                        src:dmth_kw_10::varchar AS dmth_kw_10, 
                        src:dmth_kw_11::varchar AS dmth_kw_11, 
                        src:dmth_kw_2::varchar AS dmth_kw_2, 
                        src:dmth_kw_3::varchar AS dmth_kw_3, 
                        src:dmth_kw_4::varchar AS dmth_kw_4, 
                        src:dmth_kw_5::varchar AS dmth_kw_5, 
                        src:dmth_kw_6::varchar AS dmth_kw_6, 
                        src:dmth_kw_7::varchar AS dmth_kw_7, 
                        src:dmth_kw_8::varchar AS dmth_kw_8, 
                        src:dmth_kw_9::varchar AS dmth_kw_9, 
                        src:dmty_1::integer AS dmty_1, 
                        src:dmty_10::integer AS dmty_10, 
                        src:dmty_11::integer AS dmty_11, 
                        src:dmty_2::integer AS dmty_2, 
                        src:dmty_3::integer AS dmty_3, 
                        src:dmty_4::integer AS dmty_4, 
                        src:dmty_5::integer AS dmty_5, 
                        src:dmty_6::integer AS dmty_6, 
                        src:dmty_7::integer AS dmty_7, 
                        src:dmty_8::integer AS dmty_8, 
                        src:dmty_9::integer AS dmty_9, 
                        src:dmty_kw_1::varchar AS dmty_kw_1, 
                        src:dmty_kw_10::varchar AS dmty_kw_10, 
                        src:dmty_kw_11::varchar AS dmty_kw_11, 
                        src:dmty_kw_2::varchar AS dmty_kw_2, 
                        src:dmty_kw_3::varchar AS dmty_kw_3, 
                        src:dmty_kw_4::varchar AS dmty_kw_4, 
                        src:dmty_kw_5::varchar AS dmty_kw_5, 
                        src:dmty_kw_6::varchar AS dmty_kw_6, 
                        src:dmty_kw_7::varchar AS dmty_kw_7, 
                        src:dmty_kw_8::varchar AS dmty_kw_8, 
                        src:dmty_kw_9::varchar AS dmty_kw_9, 
                        src:dorg_1::integer AS dorg_1, 
                        src:dorg_10::integer AS dorg_10, 
                        src:dorg_11::integer AS dorg_11, 
                        src:dorg_2::integer AS dorg_2, 
                        src:dorg_3::integer AS dorg_3, 
                        src:dorg_4::integer AS dorg_4, 
                        src:dorg_5::integer AS dorg_5, 
                        src:dorg_6::integer AS dorg_6, 
                        src:dorg_7::integer AS dorg_7, 
                        src:dorg_8::integer AS dorg_8, 
                        src:dorg_9::integer AS dorg_9, 
                        src:dorg_kw_1::varchar AS dorg_kw_1, 
                        src:dorg_kw_10::varchar AS dorg_kw_10, 
                        src:dorg_kw_11::varchar AS dorg_kw_11, 
                        src:dorg_kw_2::varchar AS dorg_kw_2, 
                        src:dorg_kw_3::varchar AS dorg_kw_3, 
                        src:dorg_kw_4::varchar AS dorg_kw_4, 
                        src:dorg_kw_5::varchar AS dorg_kw_5, 
                        src:dorg_kw_6::varchar AS dorg_kw_6, 
                        src:dorg_kw_7::varchar AS dorg_kw_7, 
                        src:dorg_kw_8::varchar AS dorg_kw_8, 
                        src:dorg_kw_9::varchar AS dorg_kw_9, 
                        src:dspc::integer AS dspc, 
                        src:dspc_kw::varchar AS dspc_kw, 
                        src:dtrm::integer AS dtrm, 
                        src:dtrm_kw::varchar AS dtrm_kw, 
                        src:ealc_1::numeric(38, 17) AS ealc_1, 
                        src:ealc_2::numeric(38, 17) AS ealc_2, 
                        src:ealc_3::numeric(38, 17) AS ealc_3, 
                        src:ealc_cntc::numeric(38, 17) AS ealc_cntc, 
                        src:ealc_dwhc::numeric(38, 17) AS ealc_dwhc, 
                        src:ealc_refc::numeric(38, 17) AS ealc_refc, 
                        src:eals::numeric(38, 17) AS eals, 
                        src:eals_dwhc::numeric(38, 17) AS eals_dwhc, 
                        src:eals_homc::numeric(38, 17) AS eals_homc, 
                        src:eals_refc::numeric(38, 17) AS eals_refc, 
                        src:eals_rpc1::numeric(38, 17) AS eals_rpc1, 
                        src:eals_rpc2::numeric(38, 17) AS eals_rpc2, 
                        src:eaqu::numeric(38, 17) AS eaqu, 
                        src:eaqu_buar::numeric(38, 17) AS eaqu_buar, 
                        src:eaqu_buln::numeric(38, 17) AS eaqu_buln, 
                        src:eaqu_bupc::numeric(38, 17) AS eaqu_bupc, 
                        src:eaqu_butm::numeric(38, 17) AS eaqu_butm, 
                        src:eaqu_buvl::numeric(38, 17) AS eaqu_buvl, 
                        src:eaqu_buwg::numeric(38, 17) AS eaqu_buwg, 
                        src:eaqu_invu::numeric(38, 17) AS eaqu_invu, 
                        src:elgb::integer AS elgb, 
                        src:elgb_kw::varchar AS elgb_kw, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:ldam_1::numeric(38, 17) AS ldam_1, 
                        src:ldam_10::numeric(38, 17) AS ldam_10, 
                        src:ldam_11::numeric(38, 17) AS ldam_11, 
                        src:ldam_2::numeric(38, 17) AS ldam_2, 
                        src:ldam_3::numeric(38, 17) AS ldam_3, 
                        src:ldam_4::numeric(38, 17) AS ldam_4, 
                        src:ldam_5::numeric(38, 17) AS ldam_5, 
                        src:ldam_6::numeric(38, 17) AS ldam_6, 
                        src:ldam_7::numeric(38, 17) AS ldam_7, 
                        src:ldam_8::numeric(38, 17) AS ldam_8, 
                        src:ldam_9::numeric(38, 17) AS ldam_9, 
                        src:nrbt::integer AS nrbt, 
                        src:pmde::varchar AS pmde, 
                        src:pmse::integer AS pmse, 
                        src:porg::integer AS porg, 
                        src:porg_kw::varchar AS porg_kw, 
                        src:pric_1::numeric(38, 17) AS pric_1, 
                        src:pric_2::numeric(38, 17) AS pric_2, 
                        src:pric_3::numeric(38, 17) AS pric_3, 
                        src:pris::numeric(38, 17) AS pris, 
                        src:quan::numeric(38, 17) AS quan, 
                        src:quan_buar::numeric(38, 17) AS quan_buar, 
                        src:quan_buln::numeric(38, 17) AS quan_buln, 
                        src:quan_bupc::numeric(38, 17) AS quan_bupc, 
                        src:quan_buvl::numeric(38, 17) AS quan_buvl, 
                        src:quan_buwg::numeric(38, 17) AS quan_buwg, 
                        src:quan_invu::numeric(38, 17) AS quan_invu, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:spco_1::numeric(38, 17) AS spco_1, 
                        src:spco_2::numeric(38, 17) AS spco_2, 
                        src:spco_3::numeric(38, 17) AS spco_3, 
                        src:spco_cntc::numeric(38, 17) AS spco_cntc, 
                        src:spco_dwhc::numeric(38, 17) AS spco_dwhc, 
                        src:spco_refc::numeric(38, 17) AS spco_refc, 
                        src:spqu::numeric(38, 17) AS spqu, 
                        src:spqu_buar::numeric(38, 17) AS spqu_buar, 
                        src:spqu_buln::numeric(38, 17) AS spqu_buln, 
                        src:spqu_bupc::numeric(38, 17) AS spqu_bupc, 
                        src:spqu_buvl::numeric(38, 17) AS spqu_buvl, 
                        src:spqu_buwg::numeric(38, 17) AS spqu_buwg, 
                        src:spqu_invu::numeric(38, 17) AS spqu_invu, 
                        src:spsa::numeric(38, 17) AS spsa, 
                        src:spsa_dwhc::numeric(38, 17) AS spsa_dwhc, 
                        src:spsa_homc::numeric(38, 17) AS spsa_homc, 
                        src:spsa_refc::numeric(38, 17) AS spsa_refc, 
                        src:spsa_rpc1::numeric(38, 17) AS spsa_rpc1, 
                        src:spsa_rpc2::numeric(38, 17) AS spsa_rpc2, 
                        src:tcam::numeric(38, 17) AS tcam, 
                        src:tcqa::numeric(38, 17) AS tcqa, 
                        src:term::integer AS term, 
                        src:term_cfln_cctp_cotp_nrbt_ref_compnr::integer AS term_cfln_cctp_cotp_nrbt_ref_compnr, 
                        src:term_cfln_ref_compnr::integer AS term_cfln_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
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
                                        
                src:cseq,
                src:cfln,
                src:compnr,
                src:cotp,
                src:term,
                src:cctp,
                src:nrbt  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCTM131 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alco_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alco_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alco_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alco_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alco_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alco_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alqu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alqu_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alqu_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alqu_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alqu_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alqu_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alqu_invu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alsa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alsa_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alsa_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alsa_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alsa_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alsa_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camt_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camt_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camt_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camt_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camt_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:camt_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccmp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cctp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cotp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cseq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csgr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_11), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disc_9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_11), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmse_9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_11), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmth_9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_11), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmty_9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_11), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dorg_9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dspc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dtrm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ealc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ealc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ealc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ealc_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ealc_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ealc_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eals), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eals_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eals_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eals_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eals_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eals_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eaqu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eaqu_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eaqu_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eaqu_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eaqu_butm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eaqu_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eaqu_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eaqu_invu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:elgb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_11), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ldam_9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nrbt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pmse), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:porg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pric_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pric_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pric_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pris), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan_invu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spco_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spco_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spco_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spco_cntc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spco_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spco_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spqu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spqu_buar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spqu_buln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spqu_bupc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spqu_buvl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spqu_buwg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spqu_invu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spsa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spsa_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spsa_homc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spsa_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spsa_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spsa_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcam), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcqa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:term), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:term_cfln_cctp_cotp_nrbt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:term_cfln_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null