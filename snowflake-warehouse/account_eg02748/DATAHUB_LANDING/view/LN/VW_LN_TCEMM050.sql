CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TCEMM050 AS SELECT
                        src:admo::integer AS admo, 
                        src:admo_kw::varchar AS admo_kw, 
                        src:bpid::varchar AS bpid, 
                        src:bpid_ref_compnr::integer AS bpid_ref_compnr, 
                        src:cadr::varchar AS cadr, 
                        src:cadr_ref_compnr::integer AS cadr_ref_compnr, 
                        src:ccal::varchar AS ccal, 
                        src:ccal_ref_compnr::integer AS ccal_ref_compnr, 
                        src:clus::varchar AS clus, 
                        src:clus_ref_compnr::integer AS clus_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:crby::varchar AS crby, 
                        src:crdt::datetime AS crdt, 
                        src:cust::integer AS cust, 
                        src:cust_kw::varchar AS cust_kw, 
                        src:cwar::varchar AS cwar, 
                        src:cwar_ref_compnr::integer AS cwar_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:eunt::varchar AS eunt, 
                        src:eunt_ref_compnr::integer AS eunt_ref_compnr, 
                        src:expl::integer AS expl, 
                        src:expl_kw::varchar AS expl_kw, 
                        src:imag::varchar AS imag, 
                        src:inf1::object AS inf1, 
                        src:inf2::object AS inf2, 
                        src:lcmp::integer AS lcmp, 
                        src:lcmp_ref_compnr::integer AS lcmp_ref_compnr, 
                        src:ofbp::varchar AS ofbp, 
                        src:ofbp_ref_compnr::integer AS ofbp_ref_compnr, 
                        src:psit::varchar AS psit, 
                        src:psit_ref_compnr::integer AS psit_ref_compnr, 
                        src:ract::varchar AS ract, 
                        src:ract_ref_compnr::integer AS ract_ref_compnr, 
                        src:scat::varchar AS scat, 
                        src:scat_ref_compnr::integer AS scat_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sfbp::varchar AS sfbp, 
                        src:sfbp_ref_compnr::integer AS sfbp_ref_compnr, 
                        src:site::varchar AS site, 
                        src:stat::integer AS stat, 
                        src:stat_kw::varchar AS stat_kw, 
                        src:stbp::varchar AS stbp, 
                        src:stbp_ref_compnr::integer AS stbp_ref_compnr, 
                        src:subs::integer AS subs, 
                        src:subs_kw::varchar AS subs_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:username::varchar AS username, 
                        src:xsit::integer AS xsit, 
                        src:xsit_kw::varchar AS xsit_kw, 
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
                src:site,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:compnr,
                src:site  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TCEMM050 as strm))