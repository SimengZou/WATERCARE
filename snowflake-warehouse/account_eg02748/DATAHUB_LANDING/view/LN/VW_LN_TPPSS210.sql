CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TPPSS210 AS SELECT
                        src:compnr::integer AS compnr, 
                        src:cpla::varchar AS cpla, 
                        src:cprj::varchar AS cprj, 
                        src:cprj_cpla_pact_ref_compnr::integer AS cprj_cpla_pact_ref_compnr, 
                        src:cprj_cpla_ref_compnr::integer AS cprj_cpla_ref_compnr, 
                        src:cprj_cpla_sact_ref_compnr::integer AS cprj_cpla_sact_ref_compnr, 
                        src:cprj_ref_compnr::integer AS cprj_ref_compnr, 
                        src:cuni::varchar AS cuni, 
                        src:cuni_ref_compnr::integer AS cuni_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:ffno::varchar AS ffno, 
                        src:lead::numeric(38, 17) AS lead, 
                        src:llpc::integer AS llpc, 
                        src:pact::varchar AS pact, 
                        src:pcot::integer AS pcot, 
                        src:pcot_kw::varchar AS pcot_kw, 
                        src:psen::integer AS psen, 
                        src:rest::integer AS rest, 
                        src:rest_kw::varchar AS rest_kw, 
                        src:rltp::integer AS rltp, 
                        src:rltp_kw::varchar AS rltp_kw, 
                        src:sact::varchar AS sact, 
                        src:scot::integer AS scot, 
                        src:scot_kw::varchar AS scot_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:ssen::integer AS ssen, 
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
    
                        
                src:cprj,
                src:ffno,
                src:cpla,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cprj,
                src:ffno,
                src:cpla,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TPPSS210 as strm))