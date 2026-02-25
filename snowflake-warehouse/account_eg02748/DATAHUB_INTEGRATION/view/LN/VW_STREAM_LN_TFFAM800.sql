CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM800 AS SELECT
                        src:aext::varchar AS aext, 
                        src:amnt_1::numeric(38, 17) AS amnt_1, 
                        src:amnt_2::numeric(38, 17) AS amnt_2, 
                        src:amnt_3::numeric(38, 17) AS amnt_3, 
                        src:anbr::varchar AS anbr, 
                        src:anbr_aext_book_ref_compnr::integer AS anbr_aext_book_ref_compnr, 
                        src:anbr_aext_ref_compnr::integer AS anbr_aext_ref_compnr, 
                        src:atty::integer AS atty, 
                        src:atty_kw::varchar AS atty_kw, 
                        src:book::varchar AS book, 
                        src:book_ref_compnr::integer AS book_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:docn::integer AS docn, 
                        src:line::integer AS line, 
                        src:post::integer AS post, 
                        src:post_kw::varchar AS post_kw, 
                        src:prod::integer AS prod, 
                        src:rort::integer AS rort, 
                        src:rort_kw::varchar AS rort_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:tdat::date AS tdat, 
                        src:text::integer AS text, 
                        src:text_ref_compnr::integer AS text_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:tkey::integer AS tkey, 
                        src:ttyp::varchar AS ttyp, 
                        src:username::varchar AS username, 
                        src:year::integer AS year, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM800 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:anbr_aext_book_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:anbr_aext_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atty), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:book_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:docn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:line), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:post), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prod), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rort), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:tdat), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tkey), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null