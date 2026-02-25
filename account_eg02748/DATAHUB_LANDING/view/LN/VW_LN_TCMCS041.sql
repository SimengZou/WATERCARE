CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCMCS041 AS SELECT
                        src:cdec::varchar AS cdec, 
                        src:cdyn::integer AS cdyn, 
                        src:cdyn_kw::varchar AS cdyn_kw, 
                        src:compnr::integer AS compnr, 
                        src:cptp::integer AS cptp, 
                        src:cptp_kw::varchar AS cptp_kw, 
                        src:crpd::integer AS crpd, 
                        src:crpd_kw::varchar AS crpd_kw, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:fcba::varchar AS fcba, 
                        src:fcba_ref_compnr::integer AS fcba_ref_compnr, 
                        src:potp::integer AS potp, 
                        src:potp_kw::varchar AS potp_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:spec::integer AS spec, 
                        src:spec_kw::varchar AS spec_kw, 
                        src:tdgp::integer AS tdgp, 
                        src:tdgp_kw::varchar AS tdgp_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
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
    
                        
                src:cdec,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cdec,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCMCS041 as strm))