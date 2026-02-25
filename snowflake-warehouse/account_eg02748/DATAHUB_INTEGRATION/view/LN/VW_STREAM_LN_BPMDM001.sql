CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_BPMDM001 AS SELECT
                        src:bano::varchar AS bano, 
                        src:cadr::varchar AS cadr, 
                        src:cadr_ref_compnr::integer AS cadr_ref_compnr, 
                        src:ccty::varchar AS ccty, 
                        src:ccty_ref_compnr::integer AS ccty_ref_compnr, 
                        src:cedt::date AS cedt, 
                        src:cist::varchar AS cist, 
                        src:compnr::integer AS compnr, 
                        src:ctrg::varchar AS ctrg, 
                        src:daob::date AS daob, 
                        src:deleted::boolean AS deleted, 
                        src:edte::date AS edte, 
                        src:emno::varchar AS emno, 
                        src:emno_ref_compnr::integer AS emno_ref_compnr, 
                        src:emtp::integer AS emtp, 
                        src:emtp_kw::varchar AS emtp_kw, 
                        src:finr::varchar AS finr, 
                        src:hwem::numeric(38, 17) AS hwem, 
                        src:jobt::object AS jobt, 
                        src:mail::object AS mail, 
                        src:msad::object AS msad, 
                        src:msty::integer AS msty, 
                        src:msty_kw::varchar AS msty_kw, 
                        src:otbp::varchar AS otbp, 
                        src:otbp_ref_compnr::integer AS otbp_ref_compnr, 
                        src:prtn::object AS prtn, 
                        src:ptbp::varchar AS ptbp, 
                        src:ptbp_ref_compnr::integer AS ptbp_ref_compnr, 
                        src:rgno::object AS rgno, 
                        src:sdte::date AS sdte, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sexe::integer AS sexe, 
                        src:sexe_kw::varchar AS sexe_kw, 
                        src:supv::varchar AS supv, 
                        src:supv_ref_compnr::integer AS supv_ref_compnr, 
                        src:tctr::object AS tctr, 
                        src:tefw::varchar AS tefw, 
                        src:telm::varchar AS telm, 
                        src:telw::varchar AS telw, 
                        src:timestamp::datetime AS timestamp, 
                        src:tlw1::varchar AS tlw1, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:ucrm::integer AS ucrm, 
                        src:ucrm_kw::varchar AS ucrm_kw, 
                        src:username::varchar AS username, 
                        src:wtsc::varchar AS wtsc, 
                        src:wtsc_ref_compnr::integer AS wtsc_ref_compnr, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:emno,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_BPMDM001 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:cedt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:daob), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:edte), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:emno_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:emtp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:hwem), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:msty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sdte), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sexe), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:supv_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ucrm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:wtsc_ref_compnr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null