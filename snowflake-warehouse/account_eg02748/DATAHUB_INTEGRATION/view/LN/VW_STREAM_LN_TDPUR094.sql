CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TDPUR094 AS SELECT
                        src:cbor::integer AS cbor, 
                        src:cbor_kw::varchar AS cbor_kw, 
                        src:cfnm::integer AS cfnm, 
                        src:cfnm_kw::varchar AS cfnm_kw, 
                        src:cnsp::integer AS cnsp, 
                        src:cnsp_kw::varchar AS cnsp_kw, 
                        src:cnsr::integer AS cnsr, 
                        src:cnsr_kw::varchar AS cnsr_kw, 
                        src:compnr::integer AS compnr, 
                        src:coun::integer AS coun, 
                        src:coun_kw::varchar AS coun_kw, 
                        src:deleted::boolean AS deleted, 
                        src:drct::integer AS drct, 
                        src:drct_kw::varchar AS drct_kw, 
                        src:dsca::object AS dsca, 
                        src:efdt::datetime AS efdt, 
                        src:etpc::integer AS etpc, 
                        src:etpc_kw::varchar AS etpc_kw, 
                        src:exdt::datetime AS exdt, 
                        src:ngpc::varchar AS ngpc, 
                        src:ngpc_ref_compnr::integer AS ngpc_ref_compnr, 
                        src:ngpc_sepc_ref_compnr::integer AS ngpc_sepc_ref_compnr, 
                        src:ngpo::varchar AS ngpo, 
                        src:ngpo_ref_compnr::integer AS ngpo_ref_compnr, 
                        src:ngpo_sepo_ref_compnr::integer AS ngpo_sepo_ref_compnr, 
                        src:ngpq::varchar AS ngpq, 
                        src:ngpq_ref_compnr::integer AS ngpq_ref_compnr, 
                        src:ngpq_sepq_ref_compnr::integer AS ngpq_sepq_ref_compnr, 
                        src:pmsk::varchar AS pmsk, 
                        src:potp::varchar AS potp, 
                        src:proc::varchar AS proc, 
                        src:reto::integer AS reto, 
                        src:reto_kw::varchar AS reto_kw, 
                        src:sepc::varchar AS sepc, 
                        src:sepo::varchar AS sepo, 
                        src:sepq::varchar AS sepq, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:slcp::integer AS slcp, 
                        src:slcp_kw::varchar AS slcp_kw, 
                        src:subc::integer AS subc, 
                        src:subc_kw::varchar AS subc_kw, 
                        src:sund::integer AS sund, 
                        src:sund_kw::varchar AS sund_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:username::varchar AS username, 
                        src:wrhp::varchar AS wrhp, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                src:potp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR094 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cbor), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfnm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnsp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cnsr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:coun), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:drct), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:efdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:etpc), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:exdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ngpc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ngpc_sepc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ngpo_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ngpo_sepo_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ngpq_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ngpq_sepq_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:reto), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:slcp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:subc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sund), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null