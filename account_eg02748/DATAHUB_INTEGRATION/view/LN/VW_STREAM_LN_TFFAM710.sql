CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM710 AS SELECT
                        src:acnp::integer AS acnp, 
                        src:acnp_kw::varchar AS acnp_kw, 
                        src:bdad::integer AS bdad, 
                        src:bdad_kw::varchar AS bdad_kw, 
                        src:bnbv::integer AS bnbv, 
                        src:bnbv_kw::varchar AS bnbv_kw, 
                        src:calb::integer AS calb, 
                        src:calb_kw::varchar AS calb_kw, 
                        src:capp::integer AS capp, 
                        src:capp_kw::varchar AS capp_kw, 
                        src:compnr::integer AS compnr, 
                        src:conv::integer AS conv, 
                        src:conv_kw::varchar AS conv_kw, 
                        src:cper::numeric(38, 17) AS cper, 
                        src:dbpt::numeric(38, 17) AS dbpt, 
                        src:deleted::boolean AS deleted, 
                        src:depl::integer AS depl, 
                        src:depl_kw::varchar AS depl_kw, 
                        src:desc::object AS desc, 
                        src:didt::integer AS didt, 
                        src:didt_kw::varchar AS didt_kw, 
                        src:disp::integer AS disp, 
                        src:disp_kw::varchar AS disp_kw, 
                        src:dprc::integer AS dprc, 
                        src:dprc_kw::varchar AS dprc_kw, 
                        src:face::integer AS face, 
                        src:face_kw::varchar AS face_kw, 
                        src:famt::integer AS famt, 
                        src:famt_kw::varchar AS famt_kw, 
                        src:fcal::integer AS fcal, 
                        src:fcal_kw::varchar AS fcal_kw, 
                        src:fcom::integer AS fcom, 
                        src:fcom_kw::varchar AS fcom_kw, 
                        src:ffin::integer AS ffin, 
                        src:ffin_kw::varchar AS ffin_kw, 
                        src:fixa_1::numeric(38, 17) AS fixa_1, 
                        src:fixa_2::numeric(38, 17) AS fixa_2, 
                        src:fixa_3::numeric(38, 17) AS fixa_3, 
                        src:foth::integer AS foth, 
                        src:foth_kw::varchar AS foth_kw, 
                        src:fspe::integer AS fspe, 
                        src:fspe_kw::varchar AS fspe_kw, 
                        src:fsta::integer AS fsta, 
                        src:fsta_kw::varchar AS fsta_kw, 
                        src:fstd::integer AS fstd, 
                        src:fstd_kw::varchar AS fstd_kw, 
                        src:gfac::numeric(38, 17) AS gfac, 
                        src:intr::numeric(38, 17) AS intr, 
                        src:life::integer AS life, 
                        src:meth::varchar AS meth, 
                        src:nygf::integer AS nygf, 
                        src:perc::numeric(38, 17) AS perc, 
                        src:post::integer AS post, 
                        src:post_kw::varchar AS post_kw, 
                        src:rdgf::numeric(38, 17) AS rdgf, 
                        src:rndy::integer AS rndy, 
                        src:rndy_kw::varchar AS rndy_kw, 
                        src:rtty::integer AS rtty, 
                        src:rtty_kw::varchar AS rtty_kw, 
                        src:sbsv::integer AS sbsv, 
                        src:sbsv_kw::varchar AS sbsv_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:silp::integer AS silp, 
                        src:silp_kw::varchar AS silp_kw, 
                        src:stcp::integer AS stcp, 
                        src:stcp_kw::varchar AS stcp_kw, 
                        src:swsl::integer AS swsl, 
                        src:swsl_kw::varchar AS swsl_kw, 
                        src:taxt::integer AS taxt, 
                        src:taxt_kw::varchar AS taxt_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:type::integer AS type, 
                        src:type_kw::varchar AS type_kw, 
                        src:updt::integer AS updt, 
                        src:updt_kw::varchar AS updt_kw, 
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
                src:meth  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM710 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acnp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bdad), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bnbv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:calb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:capp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:conv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cper), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dbpt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:depl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:didt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dprc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:face), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:famt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcal), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcom), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ffin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fixa_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fixa_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fixa_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:foth), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fspe), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fsta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fstd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gfac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:intr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:life), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nygf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:perc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:post), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rdgf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rndy), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sbsv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:silp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stcp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:swsl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:taxt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:type), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:updt), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null