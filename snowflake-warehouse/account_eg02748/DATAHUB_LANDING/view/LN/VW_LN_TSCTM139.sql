CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TSCTM139 AS SELECT
                        src:amnt::numeric(38, 17) AS amnt, 
                        src:amnt_dwhc::numeric(38, 17) AS amnt_dwhc, 
                        src:amnt_homc::numeric(38, 17) AS amnt_homc, 
                        src:amnt_refc::numeric(38, 17) AS amnt_refc, 
                        src:amnt_rpc1::numeric(38, 17) AS amnt_rpc1, 
                        src:amnt_rpc2::numeric(38, 17) AS amnt_rpc2, 
                        src:avtm::numeric(38, 17) AS avtm, 
                        src:avtm_butm::numeric(38, 17) AS avtm_butm, 
                        src:camt_1::numeric(38, 17) AS camt_1, 
                        src:camt_2::numeric(38, 17) AS camt_2, 
                        src:camt_3::numeric(38, 17) AS camt_3, 
                        src:camt_cntc::numeric(38, 17) AS camt_cntc, 
                        src:camt_dwhc::numeric(38, 17) AS camt_dwhc, 
                        src:camt_refc::numeric(38, 17) AS camt_refc, 
                        src:cctp::varchar AS cctp, 
                        src:cctp_ref_compnr::integer AS cctp_ref_compnr, 
                        src:cfln::integer AS cfln, 
                        src:compnr::integer AS compnr, 
                        src:cotp::integer AS cotp, 
                        src:cotp_kw::varchar AS cotp_kw, 
                        src:crmt::numeric(38, 17) AS crmt, 
                        src:cseq::integer AS cseq, 
                        src:cstm::numeric(38, 17) AS cstm, 
                        src:cstm_butm::numeric(38, 17) AS cstm_butm, 
                        src:deleted::boolean AS deleted, 
                        src:nrbt::integer AS nrbt, 
                        src:proc::integer AS proc, 
                        src:proc_kw::varchar AS proc_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sptm::numeric(38, 17) AS sptm, 
                        src:sptm_butm::numeric(38, 17) AS sptm_butm, 
                        src:term::integer AS term, 
                        src:term_cfln_cctp_cotp_nrbt_ref_compnr::integer AS term_cfln_cctp_cotp_nrbt_ref_compnr, 
                        src:term_cfln_ref_compnr::integer AS term_cfln_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:username::varchar AS username, 
                        src:utpc::numeric(38, 17) AS utpc, 
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
    
                        
                src:term,
                src:cotp,
                src:cfln,
                src:nrbt,
                src:cseq,
                src:cctp,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:term,
                src:cotp,
                src:cfln,
                src:nrbt,
                src:cseq,
                src:cctp,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TSCTM139 as strm))