CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFACP251 AS SELECT
                        src:amth_1::numeric(38, 17) AS amth_1, 
                        src:amth_2::numeric(38, 17) AS amth_2, 
                        src:amth_3::numeric(38, 17) AS amth_3, 
                        src:aprp::integer AS aprp, 
                        src:apry::integer AS apry, 
                        src:buex::integer AS buex, 
                        src:buex_kw::varchar AS buex_kw, 
                        src:ccur::varchar AS ccur, 
                        src:compnr::integer AS compnr, 
                        src:data::datetime AS data, 
                        src:dbmo::integer AS dbmo, 
                        src:dbmo_kw::varchar AS dbmo_kw, 
                        src:deleted::boolean AS deleted, 
                        src:finl::integer AS finl, 
                        src:finl_kw::varchar AS finl_kw, 
                        src:iamt::numeric(38, 17) AS iamt, 
                        src:icom::integer AS icom, 
                        src:idoc::integer AS idoc, 
                        src:ifbp::varchar AS ifbp, 
                        src:ifbp_ref_compnr::integer AS ifbp_ref_compnr, 
                        src:imrf::object AS imrf, 
                        src:iqan::numeric(38, 17) AS iqan, 
                        src:ityp::varchar AS ityp, 
                        src:loco::integer AS loco, 
                        src:orno::varchar AS orno, 
                        src:otyp::integer AS otyp, 
                        src:otyp_kw::varchar AS otyp_kw, 
                        src:pdfd::integer AS pdfd, 
                        src:pdfd_kw::varchar AS pdfd_kw, 
                        src:pdif::numeric(38, 17) AS pdif, 
                        src:pono::integer AS pono, 
                        src:pric::integer AS pric, 
                        src:pric_kw::varchar AS pric_kw, 
                        src:ratd::datetime AS ratd, 
                        src:rate_1::numeric(38, 17) AS rate_1, 
                        src:rate_2::numeric(38, 17) AS rate_2, 
                        src:rate_3::numeric(38, 17) AS rate_3, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:rcno::varchar AS rcno, 
                        src:rseq::integer AS rseq, 
                        src:rtyp::varchar AS rtyp, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:shpm::varchar AS shpm, 
                        src:shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr::integer AS shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr, 
                        src:sqnb::integer AS sqnb, 
                        src:timestamp::datetime AS timestamp, 
                        src:username::varchar AS username, 
            CASE
                WHEN 'LN' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'LN' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:compnr,
                src:loco,
                src:pono,
                src:icom,
                src:idoc,
                src:ityp,
                src:rseq,
                src:otyp,
                src:rcno,
                src:shpm,
                src:orno,
                src:sqnb,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:loco,
                src:pono,
                src:icom,
                src:idoc,
                src:ityp,
                src:rseq,
                src:otyp,
                src:rcno,
                src:shpm,
                src:orno,
                src:sqnb  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFACP251 as strm))