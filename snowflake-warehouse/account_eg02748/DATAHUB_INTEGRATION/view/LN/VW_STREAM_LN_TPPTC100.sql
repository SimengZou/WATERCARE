CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TPPTC100 AS SELECT
                        src:aext::varchar AS aext, 
                        src:afrt::integer AS afrt, 
                        src:afrt_kw::varchar AS afrt_kw, 
                        src:anbr::varchar AS anbr, 
                        src:blbl::integer AS blbl, 
                        src:blbl_kw::varchar AS blbl_kw, 
                        src:cape::integer AS cape, 
                        src:cape_kw::varchar AS cape_kw, 
                        src:ccat::varchar AS ccat, 
                        src:ccat_ref_compnr::integer AS ccat_ref_compnr, 
                        src:ccst::integer AS ccst, 
                        src:ccst_kw::varchar AS ccst_kw, 
                        src:cctf::integer AS cctf, 
                        src:cctf_kw::varchar AS cctf_kw, 
                        src:cctr::integer AS cctr, 
                        src:cctr_kw::varchar AS cctr_kw, 
                        src:compnr::integer AS compnr, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cstl_ref_compnr::integer AS cprj_cstl_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:csec::varchar AS csec, 
                        src:csec_ref_compnr::integer AS csec_ref_compnr, 
                        src:cspa::varchar AS cspa, 
                        src:cspt::integer AS cspt, 
                        src:cspt_kw::varchar AS cspt_kw, 
                        src:cstg::varchar AS cstg, 
                        src:cstg_ref_compnr::integer AS cstg_ref_compnr, 
                        src:cstl::varchar AS cstl, 
                        src:cstv::numeric(38, 17) AS cstv, 
                        src:cuni::varchar AS cuni, 
                        src:cuni_ref_compnr::integer AS cuni_ref_compnr, 
                        src:cuti::varchar AS cuti, 
                        src:cuti_ref_compnr::integer AS cuti_ref_compnr, 
                        src:cuvc::varchar AS cuvc, 
                        src:cuvc_ref_compnr::integer AS cuvc_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:dldt::datetime AS dldt, 
                        src:dwar::varchar AS dwar, 
                        src:dwar_ref_compnr::integer AS dwar_ref_compnr, 
                        src:freq::numeric(38, 17) AS freq, 
                        src:gris::integer AS gris, 
                        src:hbyn::integer AS hbyn, 
                        src:hbyn_kw::varchar AS hbyn_kw, 
                        src:llnr::integer AS llnr, 
                        src:ltpr::datetime AS ltpr, 
                        src:lvps::integer AS lvps, 
                        src:lvps_kw::varchar AS lvps_kw, 
                        src:ohdt::datetime AS ohdt, 
                        src:prin::integer AS prin, 
                        src:prin_kw::varchar AS prin_kw, 
                        src:pris::numeric(38, 17) AS pris, 
                        src:prpf::integer AS prpf, 
                        src:prpf_kw::varchar AS prpf_kw, 
                        src:prps::integer AS prps, 
                        src:prps_kw::varchar AS prps_kw, 
                        src:prsp::numeric(38, 17) AS prsp, 
                        src:prss::numeric(38, 17) AS prss, 
                        src:pwar::varchar AS pwar, 
                        src:pwar_ref_compnr::integer AS pwar_ref_compnr, 
                        src:qanc::numeric(38, 17) AS qanc, 
                        src:qans::numeric(38, 17) AS qans, 
                        src:qust::integer AS qust, 
                        src:qutm::numeric(38, 17) AS qutm, 
                        src:rcod::varchar AS rcod, 
                        src:rcod_ref_compnr::integer AS rcod_ref_compnr, 
                        src:rdas::datetime AS rdas, 
                        src:rdse::datetime AS rdse, 
                        src:rfac::varchar AS rfac, 
                        src:rfsa_1::integer AS rfsa_1, 
                        src:rfsa_2::integer AS rfsa_2, 
                        src:rfsa_3::integer AS rfsa_3, 
                        src:rfse_1::integer AS rfse_1, 
                        src:rfse_2::integer AS rfse_2, 
                        src:rfse_3::integer AS rfse_3, 
                        src:rtsa_1::numeric(38, 17) AS rtsa_1, 
                        src:rtsa_2::numeric(38, 17) AS rtsa_2, 
                        src:rtsa_3::numeric(38, 17) AS rtsa_3, 
                        src:rtse_1::numeric(38, 17) AS rtse_1, 
                        src:rtse_2::numeric(38, 17) AS rtse_2, 
                        src:rtse_3::numeric(38, 17) AS rtse_3, 
                        src:sacu::varchar AS sacu, 
                        src:sacu_ref_compnr::integer AS sacu_ref_compnr, 
                        src:seak::object AS seak, 
                        src:secu::varchar AS secu, 
                        src:secu_ref_compnr::integer AS secu_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:setl::integer AS setl, 
                        src:setl_kw::varchar AS setl_kw, 
                        src:sort::object AS sort, 
                        src:stat::integer AS stat, 
                        src:stat_kw::varchar AS stat_kw, 
                        src:stdt::datetime AS stdt, 
                        src:timestamp::datetime AS timestamp, 
                        src:txsp::integer AS txsp, 
                        src:txsp_kw::varchar AS txsp_kw, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:username::varchar AS username, 
                        src:wast::integer AS wast, 
                        src:wast_kw::varchar AS wast_kw, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                src:cprj,
                src:cspa  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPTC100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:afrt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blbl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cape), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cctf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cctr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cspt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:dldt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:freq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gris), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hbyn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:llnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ltpr), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lvps), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ohdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pris), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prpf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prps), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prss), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qanc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qans), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qust), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qutm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdas), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdse), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfsa_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfsa_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfsa_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfse_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfse_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfse_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtsa_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtsa_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtsa_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtse_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtse_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtse_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sacu_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:secu_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:setl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stat), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:stdt), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wast), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null