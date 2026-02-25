CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TDPUR100 AS SELECT
                        src:adin::varchar AS adin, 
                        src:cadr::varchar AS cadr, 
                        src:cadr_ref_compnr::integer AS cadr_ref_compnr, 
                        src:ccon::varchar AS ccon, 
                        src:ccon_ref_compnr::integer AS ccon_ref_compnr, 
                        src:cdec::varchar AS cdec, 
                        src:cdec_ref_compnr::integer AS cdec_ref_compnr, 
                        src:cneg::integer AS cneg, 
                        src:cneg_kw::varchar AS cneg_kw, 
                        src:cofc::varchar AS cofc, 
                        src:cofc_ref_compnr::integer AS cofc_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cotp::varchar AS cotp, 
                        src:cpay::varchar AS cpay, 
                        src:cpay_ref_compnr::integer AS cpay_ref_compnr, 
                        src:crcd::varchar AS crcd, 
                        src:crcd_ref_compnr::integer AS crcd_ref_compnr, 
                        src:cset::varchar AS cset, 
                        src:cset_ref_compnr::integer AS cset_ref_compnr, 
                        src:ctcd::varchar AS ctcd, 
                        src:ctcd_ref_compnr::integer AS ctcd_ref_compnr, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:ddat::datetime AS ddat, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:edat::datetime AS edat, 
                        src:enlh::integer AS enlh, 
                        src:enlh_kw::varchar AS enlh_kw, 
                        src:etpc::integer AS etpc, 
                        src:etpc_kw::varchar AS etpc_kw, 
                        src:lcmp::integer AS lcmp, 
                        src:lcmp_ref_compnr::integer AS lcmp_ref_compnr, 
                        src:negt::integer AS negt, 
                        src:negt_kw::varchar AS negt_kw, 
                        src:ptpa::varchar AS ptpa, 
                        src:ptpa_ref_compnr::integer AS ptpa_ref_compnr, 
                        src:qdat::datetime AS qdat, 
                        src:qono::varchar AS qono, 
                        src:qspa::integer AS qspa, 
                        src:qspa_kw::varchar AS qspa_kw, 
                        src:rdat::datetime AS rdat, 
                        src:refa::object AS refa, 
                        src:refb::object AS refb, 
                        src:rfqt::varchar AS rfqt, 
                        src:rfqt_ref_compnr::integer AS rfqt_ref_compnr, 
                        src:rtdt::datetime AS rtdt, 
                        src:sdat::datetime AS sdat, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:tpbk::varchar AS tpbk, 
                        src:tpbk_ref_compnr::integer AS tpbk_ref_compnr, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:txtb::integer AS txtb, 
                        src:txtb_ref_compnr::integer AS txtb_ref_compnr, 
                        src:ulcr::integer AS ulcr, 
                        src:ulcr_kw::varchar AS ulcr_kw, 
                        src:uptc::integer AS uptc, 
                        src:uptc_kw::varchar AS uptc_kw, 
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
                                        
                src:qono,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccon_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cneg), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:crcd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cset_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctcd_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ddat), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:edat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:enlh), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:etpc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:negt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:qdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qspa), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rfqt_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:rtdt), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tpbk_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ulcr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uptc), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null