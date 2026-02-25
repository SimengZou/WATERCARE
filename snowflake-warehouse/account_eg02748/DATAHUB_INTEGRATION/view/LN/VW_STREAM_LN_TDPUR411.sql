CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TDPUR411 AS SELECT
                        src:atse::varchar AS atse, 
                        src:atse_ref_compnr::integer AS atse_ref_compnr, 
                        src:atsg::varchar AS atsg, 
                        src:atsg_ref_compnr::integer AS atsg_ref_compnr, 
                        src:citg::varchar AS citg, 
                        src:citg_ref_compnr::integer AS citg_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cpcl::varchar AS cpcl, 
                        src:cpcl_ref_compnr::integer AS cpcl_ref_compnr, 
                        src:cpgp::varchar AS cpgp, 
                        src:cpgp_ref_compnr::integer AS cpgp_ref_compnr, 
                        src:cpln::varchar AS cpln, 
                        src:cpln_ref_compnr::integer AS cpln_ref_compnr, 
                        src:csgp::varchar AS csgp, 
                        src:csgp_ref_compnr::integer AS csgp_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:iwgt::numeric(38, 17) AS iwgt, 
                        src:iwun::varchar AS iwun, 
                        src:iwun_ref_compnr::integer AS iwun_ref_compnr, 
                        src:kitm::integer AS kitm, 
                        src:kitm_kw::varchar AS kitm_kw, 
                        src:orno::varchar AS orno, 
                        src:orno_pono_sqnb_ref_compnr::integer AS orno_pono_sqnb_ref_compnr, 
                        src:orno_ref_compnr::integer AS orno_ref_compnr, 
                        src:pgmd::integer AS pgmd, 
                        src:pgmd_kw::varchar AS pgmd_kw, 
                        src:pono::integer AS pono, 
                        src:prtp::varchar AS prtp, 
                        src:prtp_ref_compnr::integer AS prtp_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:sqnb::integer AS sqnb, 
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
                src:sqnb,
                src:orno,
                src:pono  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR411 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atsg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpcl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpgp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpln_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csgp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iwgt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iwun_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:kitm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:orno_pono_sqnb_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:orno_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pgmd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pono), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prtp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sqnb), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null