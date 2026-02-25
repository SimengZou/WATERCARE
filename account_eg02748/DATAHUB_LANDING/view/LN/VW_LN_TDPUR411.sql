CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TDPUR411 AS SELECT
                        src:atse::varchar AS atse, 
                        src:atse_ref_compnr::integer AS atse_ref_compnr, 
                        src:atsg::varchar AS atsg, 
                        src:atsg_ref_compnr::integer AS atsg_ref_compnr, 
                        src:citg::varchar AS citg, 
                        src:citg_ref_compnr::integer AS citg_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cpcl::varchar AS cpcl, 
                        src:cpcl_ref_compnr::integer AS cpcl_ref_compnr, 
                        src:cpgp::varchar AS cpgp, 
                        src:cpgp_ref_compnr::integer AS cpgp_ref_compnr, 
                        src:cpln::varchar AS cpln, 
                        src:cpln_ref_compnr::integer AS cpln_ref_compnr, 
                        src:csgp::varchar AS csgp, 
                        src:csgp_ref_compnr::integer AS csgp_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:item::varchar AS item, 
                        src:item_ref_compnr::integer AS item_ref_compnr, 
                        src:iwgt::numeric(38, 17) AS iwgt, 
                        src:iwun::varchar AS iwun, 
                        src:iwun_ref_compnr::integer AS iwun_ref_compnr, 
                        src:kitm::integer AS kitm, 
                        src:kitm_kw::varchar AS kitm_kw, 
                        src:orno::varchar AS orno, 
                        src:orno_pono_sqnb_ref_compnr::integer AS orno_pono_sqnb_ref_compnr, 
                        src:orno_ref_compnr::integer AS orno_ref_compnr, 
                        src:pgmd::integer AS pgmd, 
                        src:pgmd_kw::varchar AS pgmd_kw, 
                        src:pono::integer AS pono, 
                        src:prtp::varchar AS prtp, 
                        src:prtp_ref_compnr::integer AS prtp_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:site::varchar AS site, 
                        src:site_ref_compnr::integer AS site_ref_compnr, 
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
    
                        
                src:compnr,
                src:sqnb,
                src:orno,
                src:pono,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:sqnb,
                src:orno,
                src:pono  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TDPUR411 as strm))