CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TDPUR430 AS SELECT
                        src:amld::numeric(38, 17) AS amld, 
                        src:amod::numeric(38, 17) AS amod, 
                        src:ccof::varchar AS ccof, 
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
                        src:cnig::integer AS cnig, 
                        src:cnig_kw::varchar AS cnig_kw, 
                        src:compnr::integer AS compnr, 
                        src:cono::varchar AS cono, 
                        src:cpon::integer AS cpon, 
                        src:csqn::integer AS csqn, 
                        src:cupp::varchar AS cupp, 
                        src:cupp_ref_compnr::integer AS cupp_ref_compnr, 
                        src:cuva::numeric(38, 17) AS cuva, 
                        src:cuvc::varchar AS cuvc, 
                        src:cuvc_ref_compnr::integer AS cuvc_ref_compnr, 
                        src:cvpp::numeric(38, 17) AS cvpp, 
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
                        src:dtrm::integer AS dtrm, 
                        src:dtrm_kw::varchar AS dtrm_kw, 
                        src:elgb::integer AS elgb, 
                        src:elgb_kw::varchar AS elgb_kw, 
                        src:ftdt::datetime AS ftdt, 
                        src:iamt::numeric(38, 17) AS iamt, 
                        src:invd::datetime AS invd, 
                        src:invn::varchar AS invn, 
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
                        src:opsq::integer AS opsq, 
                        src:orno::varchar AS orno, 
                        src:pamt::numeric(38, 17) AS pamt, 
                        src:pmde::varchar AS pmde, 
                        src:pmse::integer AS pmse, 
                        src:pono::integer AS pono, 
                        src:porg::integer AS porg, 
                        src:porg_kw::varchar AS porg_kw, 
                        src:pric::numeric(38, 17) AS pric, 
                        src:proc::integer AS proc, 
                        src:proc_kw::varchar AS proc_kw, 
                        src:prsg::varchar AS prsg, 
                        src:prsg_ref_compnr::integer AS prsg_ref_compnr, 
                        src:psqn::integer AS psqn, 
                        src:ptyp::integer AS ptyp, 
                        src:ptyp_kw::varchar AS ptyp_kw, 
                        src:qiiv::numeric(38, 17) AS qiiv, 
                        src:qipr::numeric(38, 17) AS qipr, 
                        src:qpiv::numeric(38, 17) AS qpiv, 
                        src:qppr::numeric(38, 17) AS qppr, 
                        src:ract::integer AS ract, 
                        src:ract_kw::varchar AS ract_kw, 
                        src:rarc::integer AS rarc, 
                        src:rarc_kw::varchar AS rarc_kw, 
                        src:rsqn::integer AS rsqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sqnb::integer AS sqnb, 
                        src:stsc::integer AS stsc, 
                        src:stsc_kw::varchar AS stsc_kw, 
                        src:stsd::integer AS stsd, 
                        src:stsd_kw::varchar AS stsd_kw, 
                        src:styp::integer AS styp, 
                        src:styp_kw::varchar AS styp_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:uldt::datetime AS uldt, 
                        src:usdt::datetime AS usdt, 
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
                                        
                src:psqn,
                src:compnr,
                src:orno,
                src:rsqn,
                src:pono,
                src:sqnb  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR430 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amld), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amod), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnig), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpon), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csqn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cupp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuva), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvpp), '0'), 38, 10) is not null and 
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
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dtrm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:elgb), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ftdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iamt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:invd), '1900-01-01')) is not null and 
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
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:opsq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pamt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pmse), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pono), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:porg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pric), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:proc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prsg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:psqn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qiiv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qipr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qpiv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qppr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ract), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rarc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rsqn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sqnb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stsc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stsd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:styp), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:uldt), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:usdt), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null