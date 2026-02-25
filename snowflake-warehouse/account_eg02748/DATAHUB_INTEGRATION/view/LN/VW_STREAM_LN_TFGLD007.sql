CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD007 AS SELECT
                        src:compnr::integer AS compnr, 
                        src:dacp::datetime AS dacp, 
                        src:dacr::datetime AS dacr, 
                        src:dcmg::datetime AS dcmg, 
                        src:deleted::boolean AS deleted, 
                        src:dgld::datetime AS dgld, 
                        src:dint::datetime AS dint, 
                        src:prno::integer AS prno, 
                        src:ptyp::integer AS ptyp, 
                        src:ptyp_kw::varchar AS ptyp_kw, 
                        src:ptyp_year_prno_ref_compnr::integer AS ptyp_year_prno_ref_compnr, 
                        src:sacp::integer AS sacp, 
                        src:sacp_kw::varchar AS sacp_kw, 
                        src:sacr::integer AS sacr, 
                        src:sacr_kw::varchar AS sacr_kw, 
                        src:scmg::integer AS scmg, 
                        src:scmg_kw::varchar AS scmg_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sgld::integer AS sgld, 
                        src:sgld_kw::varchar AS sgld_kw, 
                        src:sint::integer AS sint, 
                        src:sint_kw::varchar AS sint_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:uacp::varchar AS uacp, 
                        src:uacr::varchar AS uacr, 
                        src:ucmg::varchar AS ucmg, 
                        src:ugld::varchar AS ugld, 
                        src:uint::varchar AS uint, 
                        src:username::varchar AS username, 
                        src:year::integer AS year, 
                        src:year_ref_compnr::integer AS year_ref_compnr, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:prno,
                src:compnr,
                src:year,
                src:ptyp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFGLD007 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:dacp), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:dacr), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:dcmg), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:dgld), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:dint), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prno), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptyp_year_prno_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sacp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sacr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scmg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sgld), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sint), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year_ref_compnr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null