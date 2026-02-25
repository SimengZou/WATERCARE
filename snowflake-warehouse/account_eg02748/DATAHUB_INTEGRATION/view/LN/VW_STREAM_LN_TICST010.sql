CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TICST010 AS SELECT
                        src:aamt_1::numeric(38, 17) AS aamt_1, 
                        src:aamt_2::numeric(38, 17) AS aamt_2, 
                        src:aamt_3::numeric(38, 17) AS aamt_3, 
                        src:aamt_dwhc::numeric(38, 17) AS aamt_dwhc, 
                        src:aamt_lclc::numeric(38, 17) AS aamt_lclc, 
                        src:aamt_refc::numeric(38, 17) AS aamt_refc, 
                        src:aamt_rpc1::numeric(38, 17) AS aamt_rpc1, 
                        src:aamt_rpc2::numeric(38, 17) AS aamt_rpc2, 
                        src:addc::integer AS addc, 
                        src:addc_kw::varchar AS addc_kw, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cpcp::varchar AS cpcp, 
                        src:cpcp_ref_compnr::integer AS cpcp_ref_compnr, 
                        src:csto::integer AS csto, 
                        src:csto_kw::varchar AS csto_kw, 
                        src:cstv::integer AS cstv, 
                        src:cstv_kw::varchar AS cstv_kw, 
                        src:cwoc::varchar AS cwoc, 
                        src:cwoc_ref_compnr::integer AS cwoc_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dptm_fcmp::integer AS dptm_fcmp, 
                        src:eamt_1::numeric(38, 17) AS eamt_1, 
                        src:eamt_2::numeric(38, 17) AS eamt_2, 
                        src:eamt_3::numeric(38, 17) AS eamt_3, 
                        src:eamt_dwhc::numeric(38, 17) AS eamt_dwhc, 
                        src:eamt_lclc::numeric(38, 17) AS eamt_lclc, 
                        src:eamt_refc::numeric(38, 17) AS eamt_refc, 
                        src:eamt_rpc1::numeric(38, 17) AS eamt_rpc1, 
                        src:eamt_rpc2::numeric(38, 17) AS eamt_rpc2, 
                        src:nuna::numeric(38, 17) AS nuna, 
                        src:nune::numeric(38, 17) AS nune, 
                        src:pdno::varchar AS pdno, 
                        src:pdno_ref_compnr::integer AS pdno_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
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
                                        
                src:pdno,
                src:cpcp,
                src:cstv,
                src:addc,
                src:cwoc,
                src:csto,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TICST010 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aamt_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aamt_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aamt_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aamt_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aamt_lclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aamt_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aamt_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aamt_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:addc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpcp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csto), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cstv), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dptm_fcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eamt_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eamt_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eamt_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eamt_dwhc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eamt_lclc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eamt_refc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eamt_rpc1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eamt_rpc2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nuna), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nune), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pdno_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null