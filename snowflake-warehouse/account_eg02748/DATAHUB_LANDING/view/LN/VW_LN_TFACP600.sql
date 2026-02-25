CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFACP600 AS SELECT
                        src:amnt::numeric(38, 17) AS amnt, 
                        src:amtl::numeric(38, 17) AS amtl, 
                        src:bank::varchar AS bank, 
                        src:basu::varchar AS basu, 
                        src:compnr::integer AS compnr, 
                        src:curr::varchar AS curr, 
                        src:ddat::date AS ddat, 
                        src:deleted::boolean AS deleted, 
                        src:gcom::integer AS gcom, 
                        src:gdoc::integer AS gdoc, 
                        src:glin::integer AS glin, 
                        src:gtyp::varchar AS gtyp, 
                        src:ipco::integer AS ipco, 
                        src:ipdo::integer AS ipdo, 
                        src:ipli::integer AS ipli, 
                        src:ipty::varchar AS ipty, 
                        src:payd::integer AS payd, 
                        src:payl::integer AS payl, 
                        src:paym::varchar AS paym, 
                        src:payt::varchar AS payt, 
                        src:pbcp::integer AS pbcp, 
                        src:pbtn::integer AS pbtn, 
                        src:pcom::integer AS pcom, 
                        src:ptbp::varchar AS ptbp, 
                        src:ptbp_ref_compnr::integer AS ptbp_ref_compnr, 
                        src:pyid::varchar AS pyid, 
                        src:reas::varchar AS reas, 
                        src:sdat::datetime AS sdat, 
                        src:seqn::integer AS seqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:step::integer AS step, 
                        src:step_kw::varchar AS step_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:tpay::integer AS tpay, 
                        src:tpay_kw::varchar AS tpay_kw, 
                        src:tpbp::varchar AS tpbp, 
                        src:tpbp_ref_compnr::integer AS tpbp_ref_compnr, 
                        src:username::varchar AS username, 
                        src:usrc::varchar AS usrc, 
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
    
                        
                src:payd,
                src:payt,
                src:compnr,
                src:payl,
                src:seqn,
                src:pcom,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:payd,
                src:payt,
                src:compnr,
                src:payl,
                src:seqn,
                src:pcom  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFACP600 as strm))