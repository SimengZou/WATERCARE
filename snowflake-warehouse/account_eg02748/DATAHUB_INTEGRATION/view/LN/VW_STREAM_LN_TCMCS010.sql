CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TCMCS010 AS SELECT
                        src:afal::varchar AS afal, 
                        src:afal_ref_compnr::integer AS afal_ref_compnr, 
                        src:awtn::integer AS awtn, 
                        src:awtn_kw::varchar AS awtn_kw, 
                        src:bnch::integer AS bnch, 
                        src:bnch_kw::varchar AS bnch_kw, 
                        src:ccty::varchar AS ccty, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:cgp1::varchar AS cgp1, 
                        src:cgp1_ref_compnr::integer AS cgp1_ref_compnr, 
                        src:cgp2::varchar AS cgp2, 
                        src:cgp2_ref_compnr::integer AS cgp2_ref_compnr, 
                        src:clan::varchar AS clan, 
                        src:clan_ref_compnr::integer AS clan_ref_compnr, 
                        src:coaf::varchar AS coaf, 
                        src:coaf_ref_compnr::integer AS coaf_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:esgn::varchar AS esgn, 
                        src:fxcd::varchar AS fxcd, 
                        src:geoc::varchar AS geoc, 
                        src:ict2::varchar AS ict2, 
                        src:ictc::varchar AS ictc, 
                        src:ivrc::varchar AS ivrc, 
                        src:meec::integer AS meec, 
                        src:meec_kw::varchar AS meec_kw, 
                        src:natl::object AS natl, 
                        src:pltx::integer AS pltx, 
                        src:pltx_kw::varchar AS pltx_kw, 
                        src:ppcd::integer AS ppcd, 
                        src:ptta::integer AS ptta, 
                        src:ptta_kw::varchar AS ptta_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:ssgn::varchar AS ssgn, 
                        src:tfcd::varchar AS tfcd, 
                        src:timestamp::datetime AS timestamp, 
                        src:txcd::varchar AS txcd, 
                        src:txmp::integer AS txmp, 
                        src:txmp_kw::varchar AS txmp_kw, 
                        src:tzid::varchar AS tzid, 
                        src:tzid_ref_compnr::integer AS tzid_ref_compnr, 
                        src:uarc::integer AS uarc, 
                        src:uarc_kw::varchar AS uarc_kw, 
                        src:username::varchar AS username, 
                        src:vnch::integer AS vnch, 
                        src:vnch_kw::varchar AS vnch_kw, 
                        src:xsgn::varchar AS xsgn, 
                        src:zmsk::varchar AS zmsk, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                src:ccty  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCMCS010 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:afal_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:awtn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bnch), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cgp1_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cgp2_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:coaf_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:meec), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pltx), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ppcd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tzid_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uarc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:vnch), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null