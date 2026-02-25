CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFGLD206 AS SELECT
                        src:bpid::varchar AS bpid, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cono::integer AS cono, 
                        src:deleted::boolean AS deleted, 
                        src:dim1::varchar AS dim1, 
                        src:dim2::varchar AS dim2, 
                        src:dim3::varchar AS dim3, 
                        src:dim4::varchar AS dim4, 
                        src:dim5::varchar AS dim5, 
                        src:dim6::varchar AS dim6, 
                        src:dim7::varchar AS dim7, 
                        src:dim8::varchar AS dim8, 
                        src:dim9::varchar AS dim9, 
                        src:dims::varchar AS dims, 
                        src:dimx_sgm1::varchar AS dimx_sgm1, 
                        src:dimx_sgm2::varchar AS dimx_sgm2, 
                        src:dm10::varchar AS dm10, 
                        src:dm11::varchar AS dm11, 
                        src:dm12::varchar AS dm12, 
                        src:duac::integer AS duac, 
                        src:duac_kw::varchar AS duac_kw, 
                        src:fobh_1::numeric(38, 17) AS fobh_1, 
                        src:fobh_2::numeric(38, 17) AS fobh_2, 
                        src:fobh_3::numeric(38, 17) AS fobh_3, 
                        src:fobh_dtwc::numeric(38, 17) AS fobh_dtwc, 
                        src:fobh_rfrc::numeric(38, 17) AS fobh_rfrc, 
                        src:ftob::numeric(38, 17) AS ftob, 
                        src:leac::varchar AS leac, 
                        src:nob1::numeric(38, 17) AS nob1, 
                        src:nob2::numeric(38, 17) AS nob2, 
                        src:nobh_1::numeric(38, 17) AS nobh_1, 
                        src:nobh_2::numeric(38, 17) AS nobh_2, 
                        src:nobh_3::numeric(38, 17) AS nobh_3, 
                        src:nobh_dtwc::numeric(38, 17) AS nobh_dtwc, 
                        src:nobh_rfrc::numeric(38, 17) AS nobh_rfrc, 
                        src:ntob::numeric(38, 17) AS ntob, 
                        src:olap::integer AS olap, 
                        src:qob1::numeric(38, 17) AS qob1, 
                        src:qob2::numeric(38, 17) AS qob2, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
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
    
                        
                src:dims,
                src:dim4,
                src:duac,
                src:dim1,
                src:dim3,
                src:ccur,
                src:bpid,
                src:dim5,
                src:dim2,
                src:cono,
                src:year,
                src:leac,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:dims,
                src:dim4,
                src:duac,
                src:dim1,
                src:dim3,
                src:ccur,
                src:bpid,
                src:dim5,
                src:dim2,
                src:cono,
                src:year,
                src:leac,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFGLD206 as strm))