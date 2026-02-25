CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFFAM840 AS SELECT
                        src:adrt::integer AS adrt, 
                        src:adrt_kw::varchar AS adrt_kw, 
                        src:compnr::integer AS compnr, 
                        src:cost_1::numeric(38, 17) AS cost_1, 
                        src:cost_2::numeric(38, 17) AS cost_2, 
                        src:cost_3::numeric(38, 17) AS cost_3, 
                        src:curr::varchar AS curr, 
                        src:deleted::boolean AS deleted, 
                        src:dqty::numeric(38, 17) AS dqty, 
                        src:jobn::integer AS jobn, 
                        src:life::integer AS life, 
                        src:ltdd_1::numeric(38, 17) AS ltdd_1, 
                        src:ltdd_2::numeric(38, 17) AS ltdd_2, 
                        src:ltdd_3::numeric(38, 17) AS ltdd_3, 
                        src:ltdr_1::numeric(38, 17) AS ltdr_1, 
                        src:ltdr_2::numeric(38, 17) AS ltdr_2, 
                        src:ltdr_3::numeric(38, 17) AS ltdr_3, 
                        src:proc_1::numeric(38, 17) AS proc_1, 
                        src:proc_2::numeric(38, 17) AS proc_2, 
                        src:proc_3::numeric(38, 17) AS proc_3, 
                        src:prod::integer AS prod, 
                        src:prot::numeric(38, 17) AS prot, 
                        src:ratd::datetime AS ratd, 
                        src:rate_1::numeric(38, 17) AS rate_1, 
                        src:rate_2::numeric(38, 17) AS rate_2, 
                        src:rate_3::numeric(38, 17) AS rate_3, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:rcst_1::numeric(38, 17) AS rcst_1, 
                        src:rcst_2::numeric(38, 17) AS rcst_2, 
                        src:rcst_3::numeric(38, 17) AS rcst_3, 
                        src:reas::varchar AS reas, 
                        src:reas_ref_compnr::integer AS reas_ref_compnr, 
                        src:refn::varchar AS refn, 
                        src:rtyp::varchar AS rtyp, 
                        src:salv_1::numeric(38, 17) AS salv_1, 
                        src:salv_2::numeric(38, 17) AS salv_2, 
                        src:salv_3::numeric(38, 17) AS salv_3, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:tagn::object AS tagn, 
                        src:timestamp::datetime AS timestamp, 
                        src:tkey::integer AS tkey, 
                        src:tkey_ref_compnr::integer AS tkey_ref_compnr, 
                        src:type::integer AS type, 
                        src:type_kw::varchar AS type_kw, 
                        src:user::varchar AS user, 
                        src:username::varchar AS username, 
                        src:year::integer AS year, 
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
    
                        
                src:tkey,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:tkey,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFFAM840 as strm))