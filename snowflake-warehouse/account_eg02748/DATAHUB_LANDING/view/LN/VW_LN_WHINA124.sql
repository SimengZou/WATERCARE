CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_WHINA124 AS SELECT
                        src:amnt_dtwc::numeric(38, 17) AS amnt_dtwc, 
                        src:amnt_lclc::numeric(38, 17) AS amnt_lclc, 
                        src:amnt_rfrc::numeric(38, 17) AS amnt_rfrc, 
                        src:amnt_rpc1::numeric(38, 17) AS amnt_rpc1, 
                        src:amnt_rpc2::numeric(38, 17) AS amnt_rpc2, 
                        src:amtf_1::numeric(38, 17) AS amtf_1, 
                        src:amtf_2::numeric(38, 17) AS amtf_2, 
                        src:amtf_3::numeric(38, 17) AS amtf_3, 
                        src:amtt_1::numeric(38, 17) AS amtt_1, 
                        src:amtt_2::numeric(38, 17) AS amtt_2, 
                        src:amtt_3::numeric(38, 17) AS amtt_3, 
                        src:atsf::varchar AS atsf, 
                        src:atsf_ref_compnr::integer AS atsf_ref_compnr, 
                        src:atst::varchar AS atst, 
                        src:atst_ref_compnr::integer AS atst_ref_compnr, 
                        src:boml::integer AS boml, 
                        src:ccpf::varchar AS ccpf, 
                        src:ccpf_cpcp::varchar AS ccpf_cpcp, 
                        src:ccpf_ref_compnr::integer AS ccpf_ref_compnr, 
                        src:ccpt::varchar AS ccpt, 
                        src:ccpt_ref_compnr::integer AS ccpt_ref_compnr, 
                        src:codf::varchar AS codf, 
                        src:codf_cwar::varchar AS codf_cwar, 
                        src:codt::varchar AS codt, 
                        src:compnr::integer AS compnr, 
                        src:crdt::datetime AS crdt, 
                        src:cuso::integer AS cuso, 
                        src:cuso_kw::varchar AS cuso_kw, 
                        src:deleted::boolean AS deleted, 
                        src:dlin::integer AS dlin, 
                        src:expt::integer AS expt, 
                        src:expt_kw::varchar AS expt_kw, 
                        src:fitr::integer AS fitr, 
                        src:fitr_kw::varchar AS fitr_kw, 
                        src:guid::varchar AS guid, 
                        src:houf::numeric(38, 17) AS houf, 
                        src:houf_hour::numeric(38, 17) AS houf_hour, 
                        src:hout::numeric(38, 17) AS hout, 
                        src:itid::varchar AS itid, 
                        src:itmf::varchar AS itmf, 
                        src:itmf_item::varchar AS itmf_item, 
                        src:itmf_ref_compnr::integer AS itmf_ref_compnr, 
                        src:itmt::varchar AS itmt, 
                        src:itmt_ref_compnr::integer AS itmt_ref_compnr, 
                        src:itse::integer AS itse, 
                        src:ivsq::integer AS ivsq, 
                        src:koor::integer AS koor, 
                        src:koor_kw::varchar AS koor_kw, 
                        src:lcln::integer AS lcln, 
                        src:logf::integer AS logf, 
                        src:logt::integer AS logt, 
                        src:ocmp::integer AS ocmp, 
                        src:ocmp_ref_compnr::integer AS ocmp_ref_compnr, 
                        src:orno::varchar AS orno, 
                        src:pono::integer AS pono, 
                        src:quaf::numeric(38, 17) AS quaf, 
                        src:quan_buar::numeric(38, 17) AS quan_buar, 
                        src:quan_buln::numeric(38, 17) AS quan_buln, 
                        src:quan_buvl::numeric(38, 17) AS quan_buvl, 
                        src:quan_buwg::numeric(38, 17) AS quan_buwg, 
                        src:quan_invu::numeric(38, 17) AS quan_invu, 
                        src:quat::numeric(38, 17) AS quat, 
                        src:rcln::integer AS rcln, 
                        src:rcno::varchar AS rcno, 
                        src:rorg::integer AS rorg, 
                        src:rorg_kw::varchar AS rorg_kw, 
                        src:rorn::varchar AS rorn, 
                        src:rseq::integer AS rseq, 
                        src:seqn::integer AS seqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:trdt::datetime AS trdt, 
                        src:tror::integer AS tror, 
                        src:tror_kw::varchar AS tror_kw, 
                        src:typf::integer AS typf, 
                        src:typf_kw::varchar AS typf_kw, 
                        src:typt::integer AS typt, 
                        src:typt_kw::varchar AS typt_kw, 
                        src:unif::varchar AS unif, 
                        src:unif_ref_compnr::integer AS unif_ref_compnr, 
                        src:unit::varchar AS unit, 
                        src:unit_ref_compnr::integer AS unit_ref_compnr, 
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
    
                        
                src:guid,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:guid,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_WHINA124 as strm))