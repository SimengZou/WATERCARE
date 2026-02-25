CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM820 AS SELECT
                        src:acmc_1::numeric(38, 17) AS acmc_1, 
                        src:acmc_2::numeric(38, 17) AS acmc_2, 
                        src:acmc_3::numeric(38, 17) AS acmc_3, 
                        src:acmz::integer AS acmz, 
                        src:acmz_kw::varchar AS acmz_kw, 
                        src:apam_1::numeric(38, 17) AS apam_1, 
                        src:apam_2::numeric(38, 17) AS apam_2, 
                        src:apam_3::numeric(38, 17) AS apam_3, 
                        src:apaz::integer AS apaz, 
                        src:apaz_kw::varchar AS apaz_kw, 
                        src:capi_1::numeric(38, 17) AS capi_1, 
                        src:capi_2::numeric(38, 17) AS capi_2, 
                        src:capi_3::numeric(38, 17) AS capi_3, 
                        src:capz::integer AS capz, 
                        src:capz_kw::varchar AS capz_kw, 
                        src:compnr::integer AS compnr, 
                        src:cost_1::numeric(38, 17) AS cost_1, 
                        src:cost_2::numeric(38, 17) AS cost_2, 
                        src:cost_3::numeric(38, 17) AS cost_3, 
                        src:cosz::integer AS cosz, 
                        src:cosz_kw::varchar AS cosz_kw, 
                        src:curr::varchar AS curr, 
                        src:deleted::boolean AS deleted, 
                        src:itca_1::numeric(38, 17) AS itca_1, 
                        src:itca_2::numeric(38, 17) AS itca_2, 
                        src:itca_3::numeric(38, 17) AS itca_3, 
                        src:itcz::integer AS itcz, 
                        src:itcz_kw::varchar AS itcz_kw, 
                        src:life::integer AS life, 
                        src:lifz::integer AS lifz, 
                        src:lifz_kw::varchar AS lifz_kw, 
                        src:ltdd_1::numeric(38, 17) AS ltdd_1, 
                        src:ltdd_2::numeric(38, 17) AS ltdd_2, 
                        src:ltdd_3::numeric(38, 17) AS ltdd_3, 
                        src:ltdz::integer AS ltdz, 
                        src:ltdz_kw::varchar AS ltdz_kw, 
                        src:ratd::datetime AS ratd, 
                        src:rate_1::numeric(38, 17) AS rate_1, 
                        src:rate_2::numeric(38, 17) AS rate_2, 
                        src:rate_3::numeric(38, 17) AS rate_3, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:reas::varchar AS reas, 
                        src:reas_ref_compnr::integer AS reas_ref_compnr, 
                        src:revl::integer AS revl, 
                        src:revl_kw::varchar AS revl_kw, 
                        src:rtyp::varchar AS rtyp, 
                        src:s179_1::numeric(38, 17) AS s179_1, 
                        src:s179_2::numeric(38, 17) AS s179_2, 
                        src:s179_3::numeric(38, 17) AS s179_3, 
                        src:s17z::integer AS s17z, 
                        src:s17z_kw::varchar AS s17z_kw, 
                        src:salv_1::numeric(38, 17) AS salv_1, 
                        src:salv_2::numeric(38, 17) AS salv_2, 
                        src:salv_3::numeric(38, 17) AS salv_3, 
                        src:salz::integer AS salz, 
                        src:salz_kw::varchar AS salz_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:tkey::integer AS tkey, 
                        src:tkey_ref_compnr::integer AS tkey_ref_compnr, 
                        src:traf::integer AS traf, 
                        src:traf_kw::varchar AS traf_kw, 
                        src:user::varchar AS user, 
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
                src:tkey  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM820 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acmc_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acmc_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acmc_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acmz), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:apam_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:apam_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:apam_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:apaz), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:capi_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:capi_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:capi_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:capz), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cost_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cost_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cost_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cosz), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itca_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itca_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itca_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itcz), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:life), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lifz), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ltdd_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ltdd_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ltdd_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ltdz), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ratd), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rate_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:reas_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:revl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:s179_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:s179_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:s179_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:s17z), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:salv_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:salv_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:salv_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:salz), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tkey), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tkey_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:traf), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null