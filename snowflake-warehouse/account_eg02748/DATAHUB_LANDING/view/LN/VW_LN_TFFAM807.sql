CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFFAM807 AS SELECT
                        src:acct::varchar AS acct, 
                        src:acqa_1::numeric(38, 17) AS acqa_1, 
                        src:acqa_2::numeric(38, 17) AS acqa_2, 
                        src:acqa_3::numeric(38, 17) AS acqa_3, 
                        src:adja_1::numeric(38, 17) AS adja_1, 
                        src:adja_2::numeric(38, 17) AS adja_2, 
                        src:adja_3::numeric(38, 17) AS adja_3, 
                        src:book::varchar AS book, 
                        src:book_ref_compnr::integer AS book_ref_compnr, 
                        src:comp::integer AS comp, 
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:depr_1::numeric(38, 17) AS depr_1, 
                        src:depr_2::numeric(38, 17) AS depr_2, 
                        src:depr_3::numeric(38, 17) AS depr_3, 
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
                        src:disa_1::numeric(38, 17) AS disa_1, 
                        src:disa_2::numeric(38, 17) AS disa_2, 
                        src:disa_3::numeric(38, 17) AS disa_3, 
                        src:dm10::varchar AS dm10, 
                        src:dm11::varchar AS dm11, 
                        src:dm12::varchar AS dm12, 
                        src:idtc::varchar AS idtc, 
                        src:lkey::integer AS lkey, 
                        src:proa_1::numeric(38, 17) AS proa_1, 
                        src:proa_2::numeric(38, 17) AS proa_2, 
                        src:proa_3::numeric(38, 17) AS proa_3, 
                        src:prod::integer AS prod, 
                        src:rcst_1::numeric(38, 17) AS rcst_1, 
                        src:rcst_2::numeric(38, 17) AS rcst_2, 
                        src:rcst_3::numeric(38, 17) AS rcst_3, 
                        src:rdpr_1::numeric(38, 17) AS rdpr_1, 
                        src:rdpr_2::numeric(38, 17) AS rdpr_2, 
                        src:rdpr_3::numeric(38, 17) AS rdpr_3, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:traa_1::numeric(38, 17) AS traa_1, 
                        src:traa_2::numeric(38, 17) AS traa_2, 
                        src:traa_3::numeric(38, 17) AS traa_3, 
                        src:trct_1::numeric(38, 17) AS trct_1, 
                        src:trct_2::numeric(38, 17) AS trct_2, 
                        src:trct_3::numeric(38, 17) AS trct_3, 
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
    
                        
                src:acct,
                src:dim4,
                src:dim1,
                src:dims,
                src:prod,
                src:comp,
                src:dim5,
                src:compnr,
                src:idtc,
                src:dim2,
                src:dim3,
                src:year,
                src:book,
                src:lkey,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:acct,
                src:dim4,
                src:dim1,
                src:dims,
                src:prod,
                src:comp,
                src:dim5,
                src:compnr,
                src:idtc,
                src:dim2,
                src:dim3,
                src:year,
                src:book,
                src:lkey  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFFAM807 as strm))