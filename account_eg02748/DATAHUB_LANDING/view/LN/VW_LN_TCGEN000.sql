CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCGEN000 AS SELECT
                        src:awdt::integer AS awdt, 
                        src:awdt_kw::varchar AS awdt_kw, 
                        src:awfa::integer AS awfa, 
                        src:awfa_kw::varchar AS awfa_kw, 
                        src:clcd::datetime AS clcd, 
                        src:compnr::integer AS compnr, 
                        src:cpie::varchar AS cpie, 
                        src:cpie_ref_compnr::integer AS cpie_ref_compnr, 
                        src:crpd::integer AS crpd, 
                        src:cvfd::datetime AS cvfd, 
                        src:dcur::varchar AS dcur, 
                        src:dcur_ref_compnr::integer AS dcur_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:indt::datetime AS indt, 
                        src:rtyp::varchar AS rtyp, 
                        src:rtyp_ref_compnr::integer AS rtyp_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:time::integer AS time, 
                        src:timestamp::datetime AS timestamp, 
                        src:username::varchar AS username, 
                        src:wdte::object AS wdte, 
                        src:wdti::object AS wdti, 
                        src:wdto::object AS wdto, 
                        src:wdtr::object AS wdtr, 
                        src:wfae::object AS wfae, 
                        src:wfai::object AS wfai, 
                        src:wfao::object AS wfao, 
                        src:wfar::object AS wfar, 
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
    
                        
                src:indt,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:indt,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCGEN000 as strm))