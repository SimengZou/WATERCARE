CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPTC300 AS SELECT
                        src:actl::integer AS actl, 
                        src:actl_kw::varchar AS actl_kw, 
                        src:bdtp::integer AS bdtp, 
                        src:bdtp_kw::varchar AS bdtp_kw, 
                        src:ccal::varchar AS ccal, 
                        src:cexc::varchar AS cexc, 
                        src:cexc_ref_compnr::integer AS cexc_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cpla_ref_compnr::integer AS cprj_cpla_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:crdt::datetime AS crdt, 
                        src:defn::integer AS defn, 
                        src:defn_kw::varchar AS defn_kw, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:exdt::datetime AS exdt, 
                        src:free::integer AS free, 
                        src:free_kw::varchar AS free_kw, 
                        src:ibud::integer AS ibud, 
                        src:ibud_kw::varchar AS ibud_kw, 
                        src:icon::integer AS icon, 
                        src:icon_kw::varchar AS icon_kw, 
                        src:iexc::integer AS iexc, 
                        src:iexc_kw::varchar AS iexc_kw, 
                        src:lcdt::datetime AS lcdt, 
                        src:lclg::varchar AS lclg, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:upmd::integer AS upmd, 
                        src:upmd_kw::varchar AS upmd_kw, 
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
    
                        
                src:compnr,
                src:ccal,
                src:cprj,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:ccal,
                src:cprj  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPTC300 as strm))