CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPIN011 AS SELECT
                        src:adva::integer AS adva, 
                        src:amnt::numeric(38, 17) AS amnt, 
                        src:amnt_cntc::numeric(38, 17) AS amnt_cntc, 
                        src:amnt_dwhc::numeric(38, 17) AS amnt_dwhc, 
                        src:amnt_homc::numeric(38, 17) AS amnt_homc, 
                        src:amnt_prjc::numeric(38, 17) AS amnt_prjc, 
                        src:amnt_refc::numeric(38, 17) AS amnt_refc, 
                        src:amnt_rpc1::numeric(38, 17) AS amnt_rpc1, 
                        src:amnt_rpc2::numeric(38, 17) AS amnt_rpc2, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cnln::varchar AS cnln, 
                        src:cnpr::varchar AS cnpr, 
                        src:cnpr_ref_compnr::integer AS cnpr_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cono::varchar AS cono, 
                        src:cono_cnln_ref_compnr::integer AS cono_cnln_ref_compnr, 
                        src:cono_cnln_sern_cprj_ofbp_ref_compnr::integer AS cono_cnln_sern_cprj_ofbp_ref_compnr, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dlvr::integer AS dlvr, 
                        src:fcmp::integer AS fcmp, 
                        src:idln::integer AS idln, 
                        src:idoc::integer AS idoc, 
                        src:iseq::integer AS iseq, 
                        src:ityp::varchar AS ityp, 
                        src:nins::integer AS nins, 
                        src:ofbp::varchar AS ofbp, 
                        src:ofbp_ref_compnr::integer AS ofbp_ref_compnr, 
                        src:pamt::numeric(38, 17) AS pamt, 
                        src:prdt::datetime AS prdt, 
                        src:rate_1::numeric(38, 17) AS rate_1, 
                        src:rate_2::numeric(38, 17) AS rate_2, 
                        src:rate_3::numeric(38, 17) AS rate_3, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:rdat::datetime AS rdat, 
                        src:rtyp::varchar AS rtyp, 
                        src:rtyp_ref_compnr::integer AS rtyp_ref_compnr, 
                        src:schd::integer AS schd, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:serl::integer AS serl, 
                        src:sern::integer AS sern, 
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
    
                        
                src:cnln,
                src:sern,
                src:serl,
                src:compnr,
                src:cprj,
                src:ofbp,
                src:cono,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cnln,
                src:sern,
                src:serl,
                src:compnr,
                src:cprj,
                src:ofbp,
                src:cono  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPIN011 as strm))