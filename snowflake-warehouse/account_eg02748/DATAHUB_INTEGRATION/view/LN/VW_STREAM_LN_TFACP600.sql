CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP600 AS SELECT
                        src:amnt::numeric(38, 17) AS amnt, 
                        src:amtl::numeric(38, 17) AS amtl, 
                        src:bank::varchar AS bank, 
                        src:basu::varchar AS basu, 
                        src:compnr::integer AS compnr, 
                        src:curr::varchar AS curr, 
                        src:ddat::date AS ddat, 
                        src:deleted::boolean AS deleted, 
                        src:gcom::integer AS gcom, 
                        src:gdoc::integer AS gdoc, 
                        src:glin::integer AS glin, 
                        src:gtyp::varchar AS gtyp, 
                        src:ipco::integer AS ipco, 
                        src:ipdo::integer AS ipdo, 
                        src:ipli::integer AS ipli, 
                        src:ipty::varchar AS ipty, 
                        src:payd::integer AS payd, 
                        src:payl::integer AS payl, 
                        src:paym::varchar AS paym, 
                        src:payt::varchar AS payt, 
                        src:pbcp::integer AS pbcp, 
                        src:pbtn::integer AS pbtn, 
                        src:pcom::integer AS pcom, 
                        src:ptbp::varchar AS ptbp, 
                        src:ptbp_ref_compnr::integer AS ptbp_ref_compnr, 
                        src:pyid::varchar AS pyid, 
                        src:reas::varchar AS reas, 
                        src:sdat::datetime AS sdat, 
                        src:seqn::integer AS seqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:step::integer AS step, 
                        src:step_kw::varchar AS step_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:tpay::integer AS tpay, 
                        src:tpay_kw::varchar AS tpay_kw, 
                        src:tpbp::varchar AS tpbp, 
                        src:tpbp_ref_compnr::integer AS tpbp_ref_compnr, 
                        src:username::varchar AS username, 
                        src:usrc::varchar AS usrc, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:payd,
                src:payt,
                src:compnr,
                src:payl,
                src:seqn,
                src:pcom  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFACP600 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amtl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ddat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gcom), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:gdoc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:glin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ipco), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ipdo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ipli), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:payd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:payl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pbcp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pbtn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcom), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:seqn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:step), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tpay), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tpbp_ref_compnr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null