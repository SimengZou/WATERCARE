CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFFAM830 AS SELECT
                        src:abdf::integer AS abdf, 
                        src:abdf_kw::varchar AS abdf_kw, 
                        src:adpc::numeric(38, 17) AS adpc, 
                        src:compnr::integer AS compnr, 
                        src:cost_1::numeric(38, 17) AS cost_1, 
                        src:cost_2::numeric(38, 17) AS cost_2, 
                        src:cost_3::numeric(38, 17) AS cost_3, 
                        src:curd::date AS curd, 
                        src:deleted::boolean AS deleted, 
                        src:depr_1::numeric(38, 17) AS depr_1, 
                        src:depr_2::numeric(38, 17) AS depr_2, 
                        src:depr_3::numeric(38, 17) AS depr_3, 
                        src:dtty::integer AS dtty, 
                        src:dtty_kw::varchar AS dtty_kw, 
                        src:jobn::integer AS jobn, 
                        src:last::date AS last, 
                        src:ltdd_1::numeric(38, 17) AS ltdd_1, 
                        src:ltdd_2::numeric(38, 17) AS ltdd_2, 
                        src:ltdd_3::numeric(38, 17) AS ltdd_3, 
                        src:ltdr_1::numeric(38, 17) AS ltdr_1, 
                        src:ltdr_2::numeric(38, 17) AS ltdr_2, 
                        src:ltdr_3::numeric(38, 17) AS ltdr_3, 
                        src:ltdu::integer AS ltdu, 
                        src:meth::varchar AS meth, 
                        src:meth_ref_compnr::integer AS meth_ref_compnr, 
                        src:prop::varchar AS prop, 
                        src:prop_ref_compnr::integer AS prop_ref_compnr, 
                        src:rcst_1::numeric(38, 17) AS rcst_1, 
                        src:rcst_2::numeric(38, 17) AS rcst_2, 
                        src:rcst_3::numeric(38, 17) AS rcst_3, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:susp::integer AS susp, 
                        src:susp_kw::varchar AS susp_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:tkey::integer AS tkey, 
                        src:tkey_ref_compnr::integer AS tkey_ref_compnr, 
                        src:unit::integer AS unit, 
                        src:user::varchar AS user, 
                        src:username::varchar AS username, 
                        src:ytdd_1::numeric(38, 17) AS ytdd_1, 
                        src:ytdd_2::numeric(38, 17) AS ytdd_2, 
                        src:ytdd_3::numeric(38, 17) AS ytdd_3, 
                        src:ytdr_1::numeric(38, 17) AS ytdr_1, 
                        src:ytdr_2::numeric(38, 17) AS ytdr_2, 
                        src:ytdr_3::numeric(38, 17) AS ytdr_3, 
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
                FROM DATAHUB_LANDING.LN_TFFAM830 as strm))