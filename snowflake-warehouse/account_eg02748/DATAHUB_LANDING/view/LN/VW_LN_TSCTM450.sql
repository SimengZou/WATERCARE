CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TSCTM450 AS SELECT
                        src:acpp_1::numeric(38, 17) AS acpp_1, 
                        src:acpp_2::numeric(38, 17) AS acpp_2, 
                        src:acpp_3::numeric(38, 17) AS acpp_3, 
                        src:cchn::integer AS cchn, 
                        src:cfln::integer AS cfln, 
                        src:compnr::integer AS compnr, 
                        src:csco::varchar AS csco, 
                        src:csco_cchn_ref_compnr::integer AS csco_cchn_ref_compnr, 
                        src:csco_ref_compnr::integer AS csco_ref_compnr, 
                        src:cvdy::integer AS cvdy, 
                        src:deleted::boolean AS deleted, 
                        src:esco_1::numeric(38, 17) AS esco_1, 
                        src:esco_2::numeric(38, 17) AS esco_2, 
                        src:esco_3::numeric(38, 17) AS esco_3, 
                        src:esrv::numeric(38, 17) AS esrv, 
                        src:esrv_dwhc::numeric(38, 17) AS esrv_dwhc, 
                        src:esrv_homc::numeric(38, 17) AS esrv_homc, 
                        src:esrv_refc::numeric(38, 17) AS esrv_refc, 
                        src:esrv_rpc1::numeric(38, 17) AS esrv_rpc1, 
                        src:esrv_rpc2::numeric(38, 17) AS esrv_rpc2, 
                        src:fspr::integer AS fspr, 
                        src:fsyr::integer AS fsyr, 
                        src:nody::integer AS nody, 
                        src:rcrv::numeric(38, 17) AS rcrv, 
                        src:rcrv_dwhc::numeric(38, 17) AS rcrv_dwhc, 
                        src:rcrv_homc::numeric(38, 17) AS rcrv_homc, 
                        src:rcrv_refc::numeric(38, 17) AS rcrv_refc, 
                        src:rcrv_rpc1::numeric(38, 17) AS rcrv_rpc1, 
                        src:rcrv_rpc2::numeric(38, 17) AS rcrv_rpc2, 
                        src:sequencenumber::integer AS sequencenumber, 
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
    
                        
                src:csco,
                src:cchn,
                src:fspr,
                src:fsyr,
                src:compnr,
                src:cfln,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:csco,
                src:cchn,
                src:fspr,
                src:fsyr,
                src:compnr,
                src:cfln  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TSCTM450 as strm))