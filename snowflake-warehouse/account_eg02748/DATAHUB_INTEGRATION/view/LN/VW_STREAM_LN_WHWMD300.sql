CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_WHWMD300 AS SELECT
                        src:aoit::integer AS aoit, 
                        src:aoit_kw::varchar AS aoit_kw, 
                        src:aslo::integer AS aslo, 
                        src:aslo_kw::varchar AS aslo_kw, 
                        src:ball::integer AS ball, 
                        src:ball_kw::varchar AS ball_kw, 
                        src:bass::integer AS bass, 
                        src:bass_kw::varchar AS bass_kw, 
                        src:binb::integer AS binb, 
                        src:binb_kw::varchar AS binb_kw, 
                        src:bout::integer AS bout, 
                        src:bout_kw::varchar AS bout_kw, 
                        src:btri::integer AS btri, 
                        src:btri_kw::varchar AS btri_kw, 
                        src:btrr::integer AS btrr, 
                        src:btrr_kw::varchar AS btrr_kw, 
                        src:cadr::varchar AS cadr, 
                        src:cadr_ref_compnr::integer AS cadr_ref_compnr, 
                        src:cmes::varchar AS cmes, 
                        src:cmes_ref_compnr::integer AS cmes_ref_compnr, 
                        src:coln::varchar AS coln, 
                        src:compnr::integer AS compnr, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:cwar_zone_ref_compnr::integer AS cwar_zone_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:filo::integer AS filo, 
                        src:filo_kw::varchar AS filo_kw, 
                        src:"full"::integer AS "full", 
                        src:full_kw::varchar AS full_kw, 
                        src:inca::integer AS inca, 
                        src:inca_kw::varchar AS inca_kw, 
                        src:inlo::integer AS inlo, 
                        src:inlo_kw::varchar AS inlo_kw, 
                        src:itlo::integer AS itlo, 
                        src:itlo_kw::varchar AS itlo_kw, 
                        src:loca::varchar AS loca, 
                        src:loct::integer AS loct, 
                        src:loct_kw::varchar AS loct_kw, 
                        src:lolo::integer AS lolo, 
                        src:lolo_kw::varchar AS lolo_kw, 
                        src:oclo::integer AS oclo, 
                        src:oclo_kw::varchar AS oclo_kw, 
                        src:outl::integer AS outl, 
                        src:outl_kw::varchar AS outl_kw, 
                        src:ownr::varchar AS ownr, 
                        src:ownr_ref_compnr::integer AS ownr_ref_compnr, 
                        src:prio::integer AS prio, 
                        src:proo::integer AS proo, 
                        src:pseq::integer AS pseq, 
                        src:rack::varchar AS rack, 
                        src:rntl::integer AS rntl, 
                        src:rntl_kw::varchar AS rntl_kw, 
                        src:rowc::integer AS rowc, 
                        src:rowc_ref_compnr::integer AS rowc_ref_compnr, 
                        src:rown::varchar AS rown, 
                        src:rown_ref_compnr::integer AS rown_ref_compnr, 
                        src:seak::object AS seak, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sseq::integer AS sseq, 
                        src:strt::varchar AS strt, 
                        src:timestamp::datetime AS timestamp, 
                        src:tran::datetime AS tran, 
                        src:trfr::integer AS trfr, 
                        src:trfr_kw::varchar AS trfr_kw, 
                        src:trto::integer AS trto, 
                        src:trto_kw::varchar AS trto_kw, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:username::varchar AS username, 
                        src:zone::varchar AS zone, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                src:loca,
                src:cwar  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHWMD300 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aoit), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aslo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ball), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bass), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:binb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bout), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:btri), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:btrr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cmes_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cwar_zone_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:filo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:"full"), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inca), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inlo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:itlo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:loct), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lolo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:oclo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:outl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ownr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prio), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:proo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pseq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rntl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rowc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sseq), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:tran), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trfr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trto), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null