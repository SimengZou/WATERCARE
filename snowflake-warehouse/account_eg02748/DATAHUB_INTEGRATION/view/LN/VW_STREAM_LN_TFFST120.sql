CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFFST120 AS SELECT
                        src:acca::varchar AS acca, 
                        src:accn::varchar AS accn, 
                        src:accs::integer AS accs, 
                        src:accs_kw::varchar AS accs_kw, 
                        src:acct::integer AS acct, 
                        src:acct_kw::varchar AS acct_kw, 
                        src:acrd::varchar AS acrd, 
                        src:atxt::integer AS atxt, 
                        src:atxt_ref_compnr::integer AS atxt_ref_compnr, 
                        src:cfsa::integer AS cfsa, 
                        src:cfsa_kw::varchar AS cfsa_kw, 
                        src:cgla::varchar AS cgla, 
                        src:compnr::integer AS compnr, 
                        src:dbcr::integer AS dbcr, 
                        src:dbcr_kw::varchar AS dbcr_kw, 
                        src:dcgl::integer AS dcgl, 
                        src:dcgl_kw::varchar AS dcgl_kw, 
                        src:dcsw::integer AS dcsw, 
                        src:dcsw_kw::varchar AS dcsw_kw, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:facc::varchar AS facc, 
                        src:fstm::varchar AS fstm, 
                        src:fstm_acca_ref_compnr::integer AS fstm_acca_ref_compnr, 
                        src:fstm_acrd_ref_compnr::integer AS fstm_acrd_ref_compnr, 
                        src:fstm_cgla_ref_compnr::integer AS fstm_cgla_ref_compnr, 
                        src:fstm_facc_ref_compnr::integer AS fstm_facc_ref_compnr, 
                        src:fstm_pacc_ref_compnr::integer AS fstm_pacc_ref_compnr, 
                        src:fstm_ref_compnr::integer AS fstm_ref_compnr, 
                        src:muaa::integer AS muaa, 
                        src:muaa_kw::varchar AS muaa_kw, 
                        src:pacc::varchar AS pacc, 
                        src:prta::integer AS prta, 
                        src:prta_kw::varchar AS prta_kw, 
                        src:pseq::integer AS pseq, 
                        src:rhis::integer AS rhis, 
                        src:rhis_kw::varchar AS rhis_kw, 
                        src:rtyp::varchar AS rtyp, 
                        src:rtyp_ref_compnr::integer AS rtyp_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sign::integer AS sign, 
                        src:sign_kw::varchar AS sign_kw, 
                        src:subl::integer AS subl, 
                        src:timestamp::datetime AS timestamp, 
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
                src:fstm,
                src:accn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFST120 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:accs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acct), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atxt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atxt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfsa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dbcr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dcgl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dcsw), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fstm_acca_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fstm_acrd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fstm_cgla_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fstm_facc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fstm_pacc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fstm_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:muaa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pseq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rhis), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sign), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:subl), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null