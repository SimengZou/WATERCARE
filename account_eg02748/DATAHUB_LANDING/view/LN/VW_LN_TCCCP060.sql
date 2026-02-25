CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCCCP060 AS SELECT
                        src:compnr::integer AS compnr, 
                        src:cpdt::varchar AS cpdt, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:dtyp::integer AS dtyp, 
                        src:dtyp_kw::varchar AS dtyp_kw, 
                        src:dura::integer AS dura, 
                        src:gchk::integer AS gchk, 
                        src:gchk_kw::varchar AS gchk_kw, 
                        src:mchk::integer AS mchk, 
                        src:mchk_kw::varchar AS mchk_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sttm::integer AS sttm, 
                        src:timestamp::datetime AS timestamp, 
                        src:tzid::varchar AS tzid, 
                        src:username::varchar AS username, 
                        src:ychk::integer AS ychk, 
                        src:ychk_kw::varchar AS ychk_kw, 
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
                src:cpdt,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:cpdt  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCCCP060 as strm))