CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TDCMS050 AS SELECT
                        src:amic::numeric(38, 17) AS amic, 
                        src:amnt::numeric(38, 17) AS amnt, 
                        src:amoc::numeric(38, 17) AS amoc, 
                        src:amta::numeric(38, 17) AS amta, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cmam_1::numeric(38, 17) AS cmam_1, 
                        src:cmam_2::numeric(38, 17) AS cmam_2, 
                        src:cmam_3::numeric(38, 17) AS cmam_3, 
                        src:cmam_dtwc::numeric(38, 17) AS cmam_dtwc, 
                        src:cmam_rfrc::numeric(38, 17) AS cmam_rfrc, 
                        src:cmam_rpc1::numeric(38, 17) AS cmam_rpc1, 
                        src:cmam_rpc2::numeric(38, 17) AS cmam_rpc2, 
                        src:cmam_trnc::numeric(38, 17) AS cmam_trnc, 
                        src:cmdt::datetime AS cmdt, 
                        src:cmpr::numeric(38, 17) AS cmpr, 
                        src:cmst::integer AS cmst, 
                        src:cmst_kw::varchar AS cmst_kw, 
                        src:cofc::varchar AS cofc, 
                        src:cofc_ref_compnr::integer AS cofc_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cpdt::varchar AS cpdt, 
                        src:cpdt_ref_compnr::integer AS cpdt_ref_compnr, 
                        src:cpdt_year_perd_ref_compnr::integer AS cpdt_year_perd_ref_compnr, 
                        src:crin::integer AS crin, 
                        src:crtp::varchar AS crtp, 
                        src:crtp_ref_compnr::integer AS crtp_ref_compnr, 
                        src:crtt::varchar AS crtt, 
                        src:deleted::boolean AS deleted, 
                        src:dsqn::integer AS dsqn, 
                        src:fdpt::varchar AS fdpt, 
                        src:fdpt_ref_compnr::integer AS fdpt_ref_compnr, 
                        src:grpc::numeric(38, 17) AS grpc, 
                        src:icur::varchar AS icur, 
                        src:icur_ref_compnr::integer AS icur_ref_compnr, 
                        src:invd::datetime AS invd, 
                        src:invl::integer AS invl, 
                        src:invn::integer AS invn, 
                        src:iraf_1::integer AS iraf_1, 
                        src:iraf_2::integer AS iraf_2, 
                        src:iraf_3::integer AS iraf_3, 
                        src:irat_1::numeric(38, 17) AS irat_1, 
                        src:irat_2::numeric(38, 17) AS irat_2, 
                        src:irat_3::numeric(38, 17) AS irat_3, 
                        src:irdt::datetime AS irdt, 
                        src:irtp::varchar AS irtp, 
                        src:irtp_ref_compnr::integer AS irtp_ref_compnr, 
                        src:oraf_1::integer AS oraf_1, 
                        src:oraf_2::integer AS oraf_2, 
                        src:oraf_3::integer AS oraf_3, 
                        src:orat_1::numeric(38, 17) AS orat_1, 
                        src:orat_2::numeric(38, 17) AS orat_2, 
                        src:orat_3::numeric(38, 17) AS orat_3, 
                        src:ordt::datetime AS ordt, 
                        src:orno::varchar AS orno, 
                        src:orno_ref_compnr::integer AS orno_ref_compnr, 
                        src:ortp::varchar AS ortp, 
                        src:ortp_ref_compnr::integer AS ortp_ref_compnr, 
                        src:perd::integer AS perd, 
                        src:pono::integer AS pono, 
                        src:prdt::datetime AS prdt, 
                        src:prty::varchar AS prty, 
                        src:reld::integer AS reld, 
                        src:reld_kw::varchar AS reld_kw, 
                        src:reln::varchar AS reln, 
                        src:reln_ref_compnr::integer AS reln_ref_compnr, 
                        src:resv::integer AS resv, 
                        src:resv_kw::varchar AS resv_kw, 
                        src:rlsq::integer AS rlsq, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sern::integer AS sern, 
                        src:sqnb::integer AS sqnb, 
                        src:timestamp::datetime AS timestamp, 
                        src:trdt::date AS trdt, 
                        src:ttyp::varchar AS ttyp, 
                        src:type::integer AS type, 
                        src:type_kw::varchar AS type_kw, 
                        src:upid::integer AS upid, 
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
                src:orno,
                src:invl,
                src:pono,
                src:reln,
                src:type,
                src:sqnb,
                src:sern,
                src:dsqn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDCMS050 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amic), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amoc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmam_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmam_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmam_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmam_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmam_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmam_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmam_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmam_trnc), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cmdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpdt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpdt_year_perd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crtp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dsqn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fdpt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:grpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:icur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:invd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:invl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:invn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iraf_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iraf_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iraf_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:irat_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:irat_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:irat_3), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:irdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:irtp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oraf_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oraf_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oraf_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:orat_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:orat_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:orat_3), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ordt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:orno_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ortp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:perd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pono), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:prdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:reld), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:reln_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:resv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rlsq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sern), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sqnb), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:trdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:type), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:upid), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null