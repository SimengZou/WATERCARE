CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TSCTM480 AS SELECT
                        src:acco_1::numeric(38, 17) AS acco_1, 
                        src:acco_2::numeric(38, 17) AS acco_2, 
                        src:acco_3::numeric(38, 17) AS acco_3, 
                        src:acco_cntc::numeric(38, 17) AS acco_cntc, 
                        src:acco_dwhc::numeric(38, 17) AS acco_dwhc, 
                        src:acco_refc::numeric(38, 17) AS acco_refc, 
                        src:cchn::integer AS cchn, 
                        src:cfln::integer AS cfln, 
                        src:codt::datetime AS codt, 
                        src:compnr::integer AS compnr, 
                        src:cotp::integer AS cotp, 
                        src:cotp_kw::varchar AS cotp_kw, 
                        src:csco::varchar AS csco, 
                        src:csco_cchn_ref_compnr::integer AS csco_cchn_ref_compnr, 
                        src:csco_ref_compnr::integer AS csco_ref_compnr, 
                        src:cvln::integer AS cvln, 
                        src:deleted::boolean AS deleted, 
                        src:ircl::integer AS ircl, 
                        src:ircl_kw::varchar AS ircl_kw, 
                        src:ivsq::integer AS ivsq, 
                        src:orno::varchar AS orno, 
                        src:ortp::integer AS ortp, 
                        src:ortp_kw::varchar AS ortp_kw, 
                        src:plpr::integer AS plpr, 
                        src:plyr::integer AS plyr, 
                        src:pono::integer AS pono, 
                        src:popr::integer AS popr, 
                        src:poyr::integer AS poyr, 
                        src:seqn::integer AS seqn, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:stat::integer AS stat, 
                        src:stat_kw::varchar AS stat_kw, 
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
    
                        
                src:cchn,
                src:csco,
                src:seqn,
                src:cfln,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:cchn,
                src:csco,
                src:seqn,
                src:cfln,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TSCTM480 as strm))