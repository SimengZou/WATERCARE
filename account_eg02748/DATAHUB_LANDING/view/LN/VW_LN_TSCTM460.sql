CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TSCTM460 AS SELECT
                        src:acco_1::numeric(38, 17) AS acco_1, 
                        src:acco_2::numeric(38, 17) AS acco_2, 
                        src:acco_3::numeric(38, 17) AS acco_3, 
                        src:camt_1::numeric(38, 17) AS camt_1, 
                        src:camt_2::numeric(38, 17) AS camt_2, 
                        src:camt_3::numeric(38, 17) AS camt_3, 
                        src:cchn::integer AS cchn, 
                        src:cfln::integer AS cfln, 
                        src:clrv::numeric(38, 17) AS clrv, 
                        src:cmdt::datetime AS cmdt, 
                        src:cmur::varchar AS cmur, 
                        src:compnr::integer AS compnr, 
                        src:corv::numeric(38, 17) AS corv, 
                        src:corv_dwhc::numeric(38, 17) AS corv_dwhc, 
                        src:corv_refc::numeric(38, 17) AS corv_refc, 
                        src:crdt::datetime AS crdt, 
                        src:csco::varchar AS csco, 
                        src:csco_cchn_ref_compnr::integer AS csco_cchn_ref_compnr, 
                        src:csco_ref_compnr::integer AS csco_ref_compnr, 
                        src:deleted::boolean AS deleted, 
                        src:erfa::numeric(38, 17) AS erfa, 
                        src:ndrc::integer AS ndrc, 
                        src:plpr::integer AS plpr, 
                        src:plyr::integer AS plyr, 
                        src:popr::integer AS popr, 
                        src:poyr::integer AS poyr, 
                        src:prov::numeric(38, 17) AS prov, 
                        src:rahc_1::numeric(38, 17) AS rahc_1, 
                        src:rahc_2::numeric(38, 17) AS rahc_2, 
                        src:rahc_3::numeric(38, 17) AS rahc_3, 
                        src:ratc_1::numeric(38, 17) AS ratc_1, 
                        src:ratc_2::numeric(38, 17) AS ratc_2, 
                        src:ratc_3::numeric(38, 17) AS ratc_3, 
                        src:ratd::datetime AS ratd, 
                        src:ratf_1::integer AS ratf_1, 
                        src:ratf_2::integer AS ratf_2, 
                        src:ratf_3::integer AS ratf_3, 
                        src:rcdt::datetime AS rcdt, 
                        src:rcur::varchar AS rcur, 
                        src:rtor::integer AS rtor, 
                        src:rtor_kw::varchar AS rtor_kw, 
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
    
                        
                src:csco,
                src:plpr,
                src:cchn,
                src:compnr,
                src:seqn,
                src:cfln,
                src:plyr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:csco,
                src:plpr,
                src:cchn,
                src:compnr,
                src:seqn,
                src:cfln,
                src:plyr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TSCTM460 as strm))