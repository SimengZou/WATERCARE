CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP245 AS SELECT
                        src:boid::varchar AS boid, 
                        src:bonm::varchar AS bonm, 
                        src:borf::varchar AS borf, 
                        src:cmnf::varchar AS cmnf, 
                        src:compnr::integer AS compnr, 
                        src:cval::numeric(38, 17) AS cval, 
                        src:cvlc::varchar AS cvlc, 
                        src:cvlc_ref_compnr::integer AS cvlc_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:fcpo::integer AS fcpo, 
                        src:imrf::object AS imrf, 
                        src:lmat::integer AS lmat, 
                        src:lmat_kw::varchar AS lmat_kw, 
                        src:loco::integer AS loco, 
                        src:loco_otyp_orno_pono_sqnb_ref_compnr::integer AS loco_otyp_orno_pono_sqnb_ref_compnr, 
                        src:maam::numeric(38, 17) AS maam, 
                        src:maqu::numeric(38, 17) AS maqu, 
                        src:mpnr::varchar AS mpnr, 
                        src:mqpu::numeric(38, 17) AS mqpu, 
                        src:mtch::integer AS mtch, 
                        src:mtch_kw::varchar AS mtch_kw, 
                        src:omti::integer AS omti, 
                        src:omti_kw::varchar AS omti_kw, 
                        src:orno::varchar AS orno, 
                        src:otbp::varchar AS otbp, 
                        src:otyp::integer AS otyp, 
                        src:otyp_kw::varchar AS otyp_kw, 
                        src:pono::integer AS pono, 
                        src:quan::numeric(38, 17) AS quan, 
                        src:rcno::varchar AS rcno, 
                        src:rdat::datetime AS rdat, 
                        src:rgui::varchar AS rgui, 
                        src:rqty::numeric(38, 17) AS rqty, 
                        src:rseq::integer AS rseq, 
                        src:sbdt::integer AS sbdt, 
                        src:sbdt_kw::varchar AS sbdt_kw, 
                        src:sdat::datetime AS sdat, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sfbl::integer AS sfbl, 
                        src:sfbl_kw::varchar AS sfbl_kw, 
                        src:shpm::varchar AS shpm, 
                        src:sqnb::integer AS sqnb, 
                        src:timestamp::datetime AS timestamp, 
                        src:toma::integer AS toma, 
                        src:toma_kw::varchar AS toma_kw, 
                        src:trqn::numeric(38, 17) AS trqn, 
                        src:uppr::integer AS uppr, 
                        src:uppr_kw::varchar AS uppr_kw, 
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
                                        
                src:shpm,
                src:sqnb,
                src:rcno,
                src:pono,
                src:rseq,
                src:loco,
                src:otyp,
                src:compnr,
                src:orno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFACP245 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cval), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvlc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcpo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lmat), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:loco), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:loco_otyp_orno_pono_sqnb_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:maam), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:maqu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mqpu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mtch), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:omti), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:otyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pono), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:quan), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rqty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rseq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sbdt), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sfbl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sqnb), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:toma), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trqn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uppr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null