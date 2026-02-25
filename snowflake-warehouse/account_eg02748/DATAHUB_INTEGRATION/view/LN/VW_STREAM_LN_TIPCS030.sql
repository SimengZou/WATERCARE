CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TIPCS030 AS SELECT
                        src:acdt::datetime AS acdt, 
                        src:aman::numeric(38, 17) AS aman, 
                        src:amch::numeric(38, 17) AS amch, 
                        src:ascp::numeric(38, 17) AS ascp, 
                        src:bkcs::integer AS bkcs, 
                        src:bkcs_kw::varchar AS bkcs_kw, 
                        src:bkdt::datetime AS bkdt, 
                        src:cbdg::varchar AS cbdg, 
                        src:cbdg_ref_compnr::integer AS cbdg_ref_compnr, 
                        src:cldt::datetime AS cldt, 
                        src:compnr::integer AS compnr, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:crfc::integer AS crfc, 
                        src:crfc_kw::varchar AS crfc_kw, 
                        src:ddat::datetime AS ddat, 
                        src:defc::integer AS defc, 
                        src:defc_kw::varchar AS defc_kw, 
                        src:deleted::boolean AS deleted, 
                        src:dstr::integer AS dstr, 
                        src:dstr_kw::varchar AS dstr_kw, 
                        src:ecpa::numeric(38, 17) AS ecpa, 
                        src:ecpr::numeric(38, 17) AS ecpr, 
                        src:erev::integer AS erev, 
                        src:erev_kw::varchar AS erev_kw, 
                        src:incp::varchar AS incp, 
                        src:incp_ref_compnr::integer AS incp_ref_compnr, 
                        src:intc::integer AS intc, 
                        src:intc_kw::varchar AS intc_kw, 
                        src:intp::integer AS intp, 
                        src:intp_kw::varchar AS intp_kw, 
                        src:mib4::integer AS mib4, 
                        src:mib4_kw::varchar AS mib4_kw, 
                        src:ncac::integer AS ncac, 
                        src:ncac_kw::varchar AS ncac_kw, 
                        src:otac::integer AS otac, 
                        src:otac_kw::varchar AS otac_kw, 
                        src:plcd::integer AS plcd, 
                        src:plcd_kw::varchar AS plcd_kw, 
                        src:pocm::integer AS pocm, 
                        src:pocm_kw::varchar AS pocm_kw, 
                        src:psta::varchar AS psta, 
                        src:psta_ref_compnr::integer AS psta_ref_compnr, 
                        src:psts::integer AS psts, 
                        src:psts_kw::varchar AS psts_kw, 
                        src:rrtp::integer AS rrtp, 
                        src:rrtp_kw::varchar AS rrtp_kw, 
                        src:sdat::datetime AS sdat, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:soco::numeric(38, 17) AS soco, 
                        src:timestamp::datetime AS timestamp, 
                        src:tinv_1::numeric(38, 17) AS tinv_1, 
                        src:tinv_2::numeric(38, 17) AS tinv_2, 
                        src:tinv_3::numeric(38, 17) AS tinv_3, 
                        src:tpsr::integer AS tpsr, 
                        src:tpsr_kw::varchar AS tpsr_kw, 
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
                                        
                src:cprj,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TIPCS030 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:acdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aman), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amch), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ascp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bkcs), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:bkdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cbdg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cldt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crfc), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ddat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:defc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dstr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ecpa), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ecpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:erev), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:incp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:intc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:intp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mib4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ncac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:otac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:plcd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pocm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:psta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:psts), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rrtp), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:soco), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tinv_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tinv_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tinv_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tpsr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null