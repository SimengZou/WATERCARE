CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TDPUR456 AS SELECT
                        src:afrw::varchar AS afrw, 
                        src:amch::numeric(38, 17) AS amch, 
                        src:amld::numeric(38, 17) AS amld, 
                        src:amod::numeric(38, 17) AS amod, 
                        src:arej::integer AS arej, 
                        src:arej_kw::varchar AS arej_kw, 
                        src:asnl::object AS asnl, 
                        src:asno::object AS asno, 
                        src:bgxc::integer AS bgxc, 
                        src:bgxc_kw::varchar AS bgxc_kw, 
                        src:citr::object AS citr, 
                        src:cmnf::varchar AS cmnf, 
                        src:cmnf_ref_compnr::integer AS cmnf_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:coop_1::numeric(38, 17) AS coop_1, 
                        src:coop_2::numeric(38, 17) AS coop_2, 
                        src:coop_3::numeric(38, 17) AS coop_3, 
                        src:copr_1::numeric(38, 17) AS copr_1, 
                        src:copr_2::numeric(38, 17) AS copr_2, 
                        src:copr_3::numeric(38, 17) AS copr_3, 
                        src:crej::varchar AS crej, 
                        src:crej_ref_compnr::integer AS crej_ref_compnr, 
                        src:cuva::numeric(38, 17) AS cuva, 
                        src:cuvc::varchar AS cuvc, 
                        src:cuvc_ref_compnr::integer AS cuvc_ref_compnr, 
                        src:damt::numeric(38, 17) AS damt, 
                        src:ddte::datetime AS ddte, 
                        src:deleted::boolean AS deleted, 
                        src:deln::varchar AS deln, 
                        src:dino::object AS dino, 
                        src:fire::integer AS fire, 
                        src:fire_kw::varchar AS fire_kw, 
                        src:ftdt::datetime AS ftdt, 
                        src:hisa::integer AS hisa, 
                        src:hisa_kw::varchar AS hisa_kw, 
                        src:ldat::datetime AS ldat, 
                        src:load::varchar AS load, 
                        src:logn::varchar AS logn, 
                        src:lssn::varchar AS lssn, 
                        src:lssn_ref_compnr::integer AS lssn_ref_compnr, 
                        src:mpnr::varchar AS mpnr, 
                        src:mpnr_cmnf_ref_compnr::integer AS mpnr_cmnf_ref_compnr, 
                        src:onpr::numeric(38, 17) AS onpr, 
                        src:orno::varchar AS orno, 
                        src:pono::integer AS pono, 
                        src:qiap::numeric(38, 17) AS qiap, 
                        src:qidl::numeric(38, 17) AS qidl, 
                        src:qips::numeric(38, 17) AS qips, 
                        src:qirj::numeric(38, 17) AS qirj, 
                        src:qual::integer AS qual, 
                        src:qual_kw::varchar AS qual_kw, 
                        src:rarc::integer AS rarc, 
                        src:rarc_kw::varchar AS rarc_kw, 
                        src:rcld::datetime AS rcld, 
                        src:rcno::varchar AS rcno, 
                        src:refa::object AS refa, 
                        src:revi::varchar AS revi, 
                        src:rseq::integer AS rseq, 
                        src:rsqn::integer AS rsqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sern::integer AS sern, 
                        src:shdt::datetime AS shdt, 
                        src:shpm::varchar AS shpm, 
                        src:spcn::varchar AS spcn, 
                        src:sqnb::integer AS sqnb, 
                        src:timestamp::datetime AS timestamp, 
                        src:trdt::datetime AS trdt, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:username::varchar AS username, 
                        src:wght::numeric(38, 17) AS wght, 
                        src:wtun::varchar AS wtun, 
                        src:wtun_ref_compnr::integer AS wtun_ref_compnr, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:sern,
                src:pono,
                src:orno,
                src:rsqn,
                src:compnr,
                src:trdt,
                src:sqnb  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR456 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amch), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amld), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amod), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:arej), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bgxc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:coop_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:coop_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:coop_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:copr_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crej_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuva), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:damt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ddte), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fire), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ftdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hisa), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ldat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lssn_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mpnr_cmnf_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:onpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pono), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qiap), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qidl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qips), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qirj), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qual), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rarc), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rcld), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rseq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rsqn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sern), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:shdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sqnb), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:trdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wght), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wtun_ref_compnr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null