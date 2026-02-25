CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFFAM800 AS SELECT
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
                        src:year::integer AS year, 
            CASE
                WHEN 'LN' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'LN' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:compnr,
                src:tkey,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:tkey  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFFAM800 as strm))