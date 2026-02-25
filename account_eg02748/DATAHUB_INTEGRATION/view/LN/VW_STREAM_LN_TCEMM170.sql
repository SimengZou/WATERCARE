CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TCEMM170 AS SELECT
                        src:ccal::varchar AS ccal, 
                        src:ccal_ref_compnr::integer AS ccal_ref_compnr, 
                        src:clcu::varchar AS clcu, 
                        src:clcu_ref_compnr::integer AS clcu_ref_compnr, 
                        src:comp::integer AS comp, 
                        src:compnr::integer AS compnr, 
                        src:csys::integer AS csys, 
                        src:csys_kw::varchar AS csys_kw, 
                        src:ctya::integer AS ctya, 
                        src:ctya_kw::varchar AS ctya_kw, 
                        src:ctyb::integer AS ctyb, 
                        src:ctyb_kw::varchar AS ctyb_kw, 
                        src:ctyc::integer AS ctyc, 
                        src:ctyc_kw::varchar AS ctyc_kw, 
                        src:ctyp::integer AS ctyp, 
                        src:ctyp_kw::varchar AS ctyp_kw, 
                        src:dcur_comp::varchar AS dcur_comp, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:erub::varchar AS erub, 
                        src:erub_ref_compnr::integer AS erub_ref_compnr, 
                        src:eruc::varchar AS eruc, 
                        src:eruc_ref_compnr::integer AS eruc_ref_compnr, 
                        src:euro::varchar AS euro, 
                        src:euro_ref_compnr::integer AS euro_ref_compnr, 
                        src:exeu::varchar AS exeu, 
                        src:exex::varchar AS exex, 
                        src:expu::varchar AS expu, 
                        src:exsa::varchar AS exsa, 
                        src:fcua::varchar AS fcua, 
                        src:fcua_ref_compnr::integer AS fcua_ref_compnr, 
                        src:fcub::varchar AS fcub, 
                        src:fcub_ref_compnr::integer AS fcub_ref_compnr, 
                        src:fcuc::varchar AS fcuc, 
                        src:fcuc_ref_compnr::integer AS fcuc_ref_compnr, 
                        src:lcur::varchar AS lcur, 
                        src:lcur_ref_compnr::integer AS lcur_ref_compnr, 
                        src:ract::varchar AS ract, 
                        src:ract_ref_compnr::integer AS ract_ref_compnr, 
                        src:rdub::integer AS rdub, 
                        src:rdub_kw::varchar AS rdub_kw, 
                        src:rduc::integer AS rduc, 
                        src:rduc_kw::varchar AS rduc_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:taxo_rcmp::integer AS taxo_rcmp, 
                        src:timestamp::datetime AS timestamp, 
                        src:tmub::integer AS tmub, 
                        src:tmub_kw::varchar AS tmub_kw, 
                        src:tmuc::integer AS tmuc, 
                        src:tmuc_kw::varchar AS tmuc_kw, 
                        src:tzid::varchar AS tzid, 
                        src:tzid_ref_compnr::integer AS tzid_ref_compnr, 
                        src:umfc::integer AS umfc, 
                        src:umfc_kw::varchar AS umfc_kw, 
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
                src:comp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCEMM170 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clcu_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:comp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:csys), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctya), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctyb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctyc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:erub_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:eruc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:euro_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcua_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcub_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcuc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ract_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rdub), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rduc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:taxo_rcmp), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tmub), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tmuc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tzid_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:umfc), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null