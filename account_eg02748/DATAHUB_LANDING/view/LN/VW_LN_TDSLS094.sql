CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TDSLS094 AS SELECT
                        src:aaps::integer AS aaps, 
                        src:aaps_kw::varchar AS aaps_kw, 
                        src:aprd::integer AS aprd, 
                        src:aprd_kw::varchar AS aprd_kw, 
                        src:apur::integer AS apur, 
                        src:apur_kw::varchar AS apur_kw, 
                        src:blcs::varchar AS blcs, 
                        src:cnsi::integer AS cnsi, 
                        src:cnsi_kw::varchar AS cnsi_kw, 
                        src:cnsr::integer AS cnsr, 
                        src:cnsr_kw::varchar AS cnsr_kw, 
                        src:compnr::integer AS compnr, 
                        src:coun::integer AS coun, 
                        src:coun_kw::varchar AS coun_kw, 
                        src:cphl::integer AS cphl, 
                        src:cphl_kw::varchar AS cphl_kw, 
                        src:crdc::integer AS crdc, 
                        src:crdc_kw::varchar AS crdc_kw, 
                        src:deleted::boolean AS deleted, 
                        src:dsca::object AS dsca, 
                        src:efdt::datetime AS efdt, 
                        src:einc::integer AS einc, 
                        src:einc_kw::varchar AS einc_kw, 
                        src:exdt::datetime AS exdt, 
                        src:gmrc::integer AS gmrc, 
                        src:gmrc_kw::varchar AS gmrc_kw, 
                        src:incm::integer AS incm, 
                        src:incm_kw::varchar AS incm_kw, 
                        src:mwdl::integer AS mwdl, 
                        src:mwdl_kw::varchar AS mwdl_kw, 
                        src:ngsc::varchar AS ngsc, 
                        src:ngsc_ref_compnr::integer AS ngsc_ref_compnr, 
                        src:ngsc_sesc_ref_compnr::integer AS ngsc_sesc_ref_compnr, 
                        src:ngso::varchar AS ngso, 
                        src:ngso_ref_compnr::integer AS ngso_ref_compnr, 
                        src:ngso_seso_ref_compnr::integer AS ngso_seso_ref_compnr, 
                        src:ngsq::varchar AS ngsq, 
                        src:ngsq_ref_compnr::integer AS ngsq_ref_compnr, 
                        src:ngsq_sesq_ref_compnr::integer AS ngsq_sesq_ref_compnr, 
                        src:pmsk::varchar AS pmsk, 
                        src:proc::varchar AS proc, 
                        src:retb::integer AS retb, 
                        src:retb_kw::varchar AS retb_kw, 
                        src:reto::integer AS reto, 
                        src:reto_kw::varchar AS reto_kw, 
                        src:scon::integer AS scon, 
                        src:scon_kw::varchar AS scon_kw, 
                        src:sdec::varchar AS sdec, 
                        src:sdec_ref_compnr::integer AS sdec_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sesc::varchar AS sesc, 
                        src:seso::varchar AS seso, 
                        src:sesq::varchar AS sesq, 
                        src:sotp::varchar AS sotp, 
                        src:spay::varchar AS spay, 
                        src:spay_ref_compnr::integer AS spay_ref_compnr, 
                        src:ssoa::integer AS ssoa, 
                        src:ssoa_kw::varchar AS ssoa_kw, 
                        src:ssob::integer AS ssob, 
                        src:ssob_kw::varchar AS ssob_kw, 
                        src:ssoc::integer AS ssoc, 
                        src:ssoc_kw::varchar AS ssoc_kw, 
                        src:ssod::integer AS ssod, 
                        src:ssod_kw::varchar AS ssod_kw, 
                        src:ssoe::integer AS ssoe, 
                        src:ssoe_kw::varchar AS ssoe_kw, 
                        src:ssof::integer AS ssof, 
                        src:ssof_kw::varchar AS ssof_kw, 
                        src:sund::integer AS sund, 
                        src:sund_kw::varchar AS sund_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:udps::integer AS udps, 
                        src:udps_kw::varchar AS udps_kw, 
                        src:upls::integer AS upls, 
                        src:upls_kw::varchar AS upls_kw, 
                        src:upsh::integer AS upsh, 
                        src:upsh_kw::varchar AS upsh_kw, 
                        src:username::varchar AS username, 
                        src:wpal::integer AS wpal, 
                        src:wpal_kw::varchar AS wpal_kw, 
                        src:wrhp::varchar AS wrhp, 
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
                src:sotp,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:sotp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TDSLS094 as strm))