CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFFST100 AS SELECT
                        src:accs::integer AS accs, 
                        src:accs_kw::varchar AS accs_kw, 
                        src:altp::integer AS altp, 
                        src:altp_kw::varchar AS altp_kw, 
                        src:altp_salt_ref_compnr::integer AS altp_salt_ref_compnr, 
                        src:anhe::integer AS anhe, 
                        src:anhe_ref_compnr::integer AS anhe_ref_compnr, 
                        src:cagr::varchar AS cagr, 
                        src:cagr_ref_compnr::integer AS cagr_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cugl::integer AS cugl, 
                        src:cugl_kw::varchar AS cugl_kw, 
                        src:datc::datetime AS datc, 
                        src:datm::datetime AS datm, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:fstm::varchar AS fstm, 
                        src:mdfs::datetime AS mdfs, 
                        src:nstm::varchar AS nstm, 
                        src:nstm_ref_compnr::integer AS nstm_ref_compnr, 
                        src:ouer::integer AS ouer, 
                        src:ouer_kw::varchar AS ouer_kw, 
                        src:pann::integer AS pann, 
                        src:pann_kw::varchar AS pann_kw, 
                        src:ptra::integer AS ptra, 
                        src:ptra_kw::varchar AS ptra_kw, 
                        src:rcur::varchar AS rcur, 
                        src:rcur_ref_compnr::integer AS rcur_ref_compnr, 
                        src:rhis::integer AS rhis, 
                        src:rhis_kw::varchar AS rhis_kw, 
                        src:rtyp::varchar AS rtyp, 
                        src:rtyp_ref_compnr::integer AS rtyp_ref_compnr, 
                        src:salt::varchar AS salt, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sltp::integer AS sltp, 
                        src:sltp_kw::varchar AS sltp_kw, 
                        src:sltp_stlt_ref_compnr::integer AS sltp_stlt_ref_compnr, 
                        src:stat::integer AS stat, 
                        src:stat_kw::varchar AS stat_kw, 
                        src:sthe::integer AS sthe, 
                        src:sthe_ref_compnr::integer AS sthe_ref_compnr, 
                        src:stlt::varchar AS stlt, 
                        src:styp::integer AS styp, 
                        src:styp_kw::varchar AS styp_kw, 
                        src:taxo::varchar AS taxo, 
                        src:timestamp::datetime AS timestamp, 
                        src:txdt::datetime AS txdt, 
                        src:username::varchar AS username, 
                        src:usrc::varchar AS usrc, 
                        src:usrm::varchar AS usrm, 
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
                src:fstm,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:fstm  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFFST100 as strm))