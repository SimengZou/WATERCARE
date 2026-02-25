CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP256 AS SELECT
                        src:afpn::varchar AS afpn, 
                        src:aprp::integer AS aprp, 
                        src:apry::integer AS apry, 
                        src:bobj::varchar AS bobj, 
                        src:boor::integer AS boor, 
                        src:boor_kw::varchar AS boor_kw, 
                        src:borf::varchar AS borf, 
                        src:boty::integer AS boty, 
                        src:boty_kw::varchar AS boty_kw, 
                        src:cisq::integer AS cisq, 
                        src:clin::integer AS clin, 
                        src:clln::integer AS clln, 
                        src:clus::varchar AS clus, 
                        src:compnr::integer AS compnr, 
                        src:cseq::integer AS cseq, 
                        src:cvat::varchar AS cvat, 
                        src:cvat_ref_compnr::integer AS cvat_ref_compnr, 
                        src:cvli::numeric(38, 17) AS cvli, 
                        src:data::datetime AS data, 
                        src:dbcr::integer AS dbcr, 
                        src:dbcr_kw::varchar AS dbcr_kw, 
                        src:deleted::boolean AS deleted, 
                        src:iamt::numeric(38, 17) AS iamt, 
                        src:icom::integer AS icom, 
                        src:icur::varchar AS icur, 
                        src:idcn::integer AS idcn, 
                        src:idln::integer AS idln, 
                        src:idoc::integer AS idoc, 
                        src:ilin::integer AS ilin, 
                        src:ilno::integer AS ilno, 
                        src:iqan::numeric(38, 17) AS iqan, 
                        src:iseq::integer AS iseq, 
                        src:itdo::varchar AS itdo, 
                        src:item::varchar AS item, 
                        src:ityp::varchar AS ityp, 
                        src:iuni::varchar AS iuni, 
                        src:lcln::integer AS lcln, 
                        src:lmat::integer AS lmat, 
                        src:lmat_kw::varchar AS lmat_kw, 
                        src:load::varchar AS load, 
                        src:loco::integer AS loco, 
                        src:maam::numeric(38, 17) AS maam, 
                        src:mtrc::integer AS mtrc, 
                        src:mtrc_kw::varchar AS mtrc_kw, 
                        src:mtyp::integer AS mtyp, 
                        src:mtyp_kw::varchar AS mtyp_kw, 
                        src:oamt::numeric(38, 17) AS oamt, 
                        src:ocur::varchar AS ocur, 
                        src:ocvt::varchar AS ocvt, 
                        src:ocvt_ref_compnr::integer AS ocvt_ref_compnr, 
                        src:oqan::numeric(38, 17) AS oqan, 
                        src:orno::varchar AS orno, 
                        src:otyp::integer AS otyp, 
                        src:otyp_kw::varchar AS otyp_kw, 
                        src:ovtc::varchar AS ovtc, 
                        src:ovtc_ocvt_ref_compnr::integer AS ovtc_ocvt_ref_compnr, 
                        src:ovtc_ref_compnr::integer AS ovtc_ref_compnr, 
                        src:pono::integer AS pono, 
                        src:pric::integer AS pric, 
                        src:pric_kw::varchar AS pric_kw, 
                        src:rcno::varchar AS rcno, 
                        src:rdam::numeric(38, 17) AS rdam, 
                        src:rlin::integer AS rlin, 
                        src:rseq::integer AS rseq, 
                        src:rttp::integer AS rttp, 
                        src:rttp_kw::varchar AS rttp_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sern::integer AS sern, 
                        src:shln::integer AS shln, 
                        src:shpi::varchar AS shpi, 
                        src:shpm::object AS shpm, 
                        src:sort::integer AS sort, 
                        src:spln::integer AS spln, 
                        src:sqnb::integer AS sqnb, 
                        src:tbai::numeric(38, 17) AS tbai, 
                        src:tcna::integer AS tcna, 
                        src:tcna_kw::varchar AS tcna_kw, 
                        src:tcrd::integer AS tcrd, 
                        src:tcrd_kw::varchar AS tcrd_kw, 
                        src:tctb::integer AS tctb, 
                        src:tctb_kw::varchar AS tctb_kw, 
                        src:tctc::integer AS tctc, 
                        src:tctc_kw::varchar AS tctc_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:trgu::varchar AS trgu, 
                        src:username::varchar AS username, 
                        src:vatc::varchar AS vatc, 
                        src:vatc_cvat_ref_compnr::integer AS vatc_cvat_ref_compnr, 
                        src:vatc_ref_compnr::integer AS vatc_ref_compnr, 
                        src:vrln::integer AS vrln, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:icom,
                src:compnr,
                src:ilin,
                src:iseq,
                src:idoc,
                src:idln,
                src:ityp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFACP256 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aprp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:apry), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:boor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:boty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cisq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cseq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvli), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:data), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dbcr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iamt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:icom), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:idcn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:idln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:idoc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ilin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ilno), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iqan), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iseq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lmat), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:loco), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:maam), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mtrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mtyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oamt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ocvt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oqan), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:otyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ovtc_ocvt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ovtc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pono), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pric), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rdam), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rlin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rseq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rttp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sern), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:shln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sort), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:spln), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sqnb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tbai), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcna), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tcrd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tctb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tctc), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vatc_cvat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vatc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vrln), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null