CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TTDPM515 AS SELECT
                        src:cmod::varchar AS cmod, 
                        src:compnr::integer AS compnr, 
                        src:cpac::varchar AS cpac, 
                        src:cpac_ref_compnr::integer AS cpac_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dset::varchar AS dset, 
                        src:dset_ref_compnr::integer AS dset_ref_compnr, 
                        src:flno::varchar AS flno, 
                        src:pacc::varchar AS pacc, 
                        src:pacc_dset_ref_compnr::integer AS pacc_dset_ref_compnr, 
                        src:pacc_ref_compnr::integer AS pacc_ref_compnr, 
                        src:pubd::integer AS pubd, 
                        src:pubd_kw::varchar AS pubd_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
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
    
                        
                src:pacc,
                src:cmod,
                src:cpac,
                src:compnr,
                src:dset,
                src:flno,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:pacc,
                src:cmod,
                src:cpac,
                src:compnr,
                src:dset,
                src:flno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TTDPM515 as strm))