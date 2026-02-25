CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_WHINH439 AS SELECT
                        src:cdat::datetime AS cdat, 
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:fshp::integer AS fshp, 
                        src:fshp_kw::varchar AS fshp_kw, 
                        src:ldat::datetime AS ldat, 
                        src:mess::object AS mess, 
                        src:oorg::integer AS oorg, 
                        src:oorg_kw::varchar AS oorg_kw, 
                        src:orno::varchar AS orno, 
                        src:pono::integer AS pono, 
                        src:seqn::integer AS seqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:shln::integer AS shln, 
                        src:shpm::varchar AS shpm, 
                        src:shpm_shln_ref_compnr::integer AS shpm_shln_ref_compnr, 
                        src:sqnb::integer AS sqnb, 
                        src:timestamp::datetime AS timestamp, 
                        src:username::varchar AS username, 
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
    
                        
                src:orno,
                src:shpm,
                src:shln,
                src:compnr,
                src:pono,
                src:oorg,
                src:sqnb,
                src:seqn,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:orno,
                src:shpm,
                src:shln,
                src:compnr,
                src:pono,
                src:oorg,
                src:sqnb,
                src:seqn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_WHINH439 as strm))