CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFGLD170 AS SELECT
                        src:acom::integer AS acom, 
                        src:acom_kw::varchar AS acom_kw, 
                        src:cldt::datetime AS cldt, 
                        src:compnr::integer AS compnr, 
                        src:crdt::datetime AS crdt, 
                        src:datm::datetime AS datm, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:dimm::integer AS dimm, 
                        src:dimm_kw::varchar AS dimm_kw, 
                        src:duac::integer AS duac, 
                        src:duac_kw::varchar AS duac_kw, 
                        src:rldt::datetime AS rldt, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:stat::integer AS stat, 
                        src:stat_kw::varchar AS stat_kw, 
                        src:taxo::varchar AS taxo, 
                        src:text::integer AS text, 
                        src:text_ref_compnr::integer AS text_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:username::varchar AS username, 
                        src:usrc::varchar AS usrc, 
                        src:usre::varchar AS usre, 
                        src:usrm::varchar AS usrm, 
                        src:usrr::varchar AS usrr, 
                        src:vers::integer AS vers, 
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
                src:taxo,
                src:vers,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:taxo,
                src:vers  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFGLD170 as strm))