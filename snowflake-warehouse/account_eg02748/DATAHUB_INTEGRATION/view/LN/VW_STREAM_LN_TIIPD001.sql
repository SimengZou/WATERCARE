CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TIIPD001 AS SELECT
                        src:bfcp::integer AS bfcp, 
                        src:bfcp_kw::varchar AS bfcp_kw, 
                        src:bfep::integer AS bfep, 
                        src:bfep_kw::varchar AS bfep_kw, 
                        src:bfhr::integer AS bfhr, 
                        src:bfhr_kw::varchar AS bfhr_kw, 
                        src:bomu::integer AS bomu, 
                        src:bomu_kw::varchar AS bomu_kw, 
                        src:cick::integer AS cick, 
                        src:cick_kw::varchar AS cick_kw, 
                        src:cncd::varchar AS cncd, 
                        src:coac::integer AS coac, 
                        src:coac_kw::varchar AS coac_kw, 
                        src:compnr::integer AS compnr, 
                        src:cpha::integer AS cpha, 
                        src:cpha_kw::varchar AS cpha_kw, 
                        src:deleted::boolean AS deleted, 
                        src:drin::integer AS drin, 
                        src:drin_kw::varchar AS drin_kw, 
                        src:dris::integer AS dris, 
                        src:dris_kw::varchar AS dris_kw, 
                        src:iima::integer AS iima, 
                        src:iima_kw::varchar AS iima_kw, 
                        src:iimf::integer AS iimf, 
                        src:iimf_kw::varchar AS iimf_kw, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:iuma::integer AS iuma, 
                        src:iuma_kw::varchar AS iuma_kw, 
                        src:jsst::varchar AS jsst, 
                        src:nsfc::integer AS nsfc, 
                        src:nsfc_kw::varchar AS nsfc_kw, 
                        src:oltm::numeric(38, 17) AS oltm, 
                        src:oltu::integer AS oltu, 
                        src:oltu_kw::varchar AS oltu_kw, 
                        src:oqdr::integer AS oqdr, 
                        src:oqdr_kw::varchar AS oqdr_kw, 
                        src:pcrp::integer AS pcrp, 
                        src:phst::integer AS phst, 
                        src:phst_kw::varchar AS phst_kw, 
                        src:repi::integer AS repi, 
                        src:repi_kw::varchar AS repi_kw, 
                        src:rgrp::varchar AS rgrp, 
                        src:rgrp_ref_compnr::integer AS rgrp_ref_compnr, 
                        src:runi::numeric(38, 17) AS runi, 
                        src:scdl::varchar AS scdl, 
                        src:scdl_ref_compnr::integer AS scdl_ref_compnr, 
                        src:scpf::numeric(38, 17) AS scpf, 
                        src:scpq::numeric(38, 17) AS scpq, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sfpl::varchar AS sfpl, 
                        src:sfpl_ref_compnr::integer AS sfpl_ref_compnr, 
                        src:smfs::integer AS smfs, 
                        src:smfs_kw::varchar AS smfs_kw, 
                        src:stoi::integer AS stoi, 
                        src:stoi_kw::varchar AS stoi_kw, 
                        src:swoc::integer AS swoc, 
                        src:swoc_kw::varchar AS swoc_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:unom::numeric(38, 17) AS unom, 
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
                src:item  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TIIPD001 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bfcp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bfep), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bfhr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bomu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cick), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:coac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cpha), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:drin), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dris), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iima), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iimf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iuma), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nsfc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oltm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oltu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oqdr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcrp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:phst), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:repi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rgrp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:runi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scdl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:scpq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sfpl_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:smfs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:stoi), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:swoc), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:unom), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null