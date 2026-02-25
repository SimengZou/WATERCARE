CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM807 AS SELECT
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
                        src:year::integer AS year, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'LN' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'LN' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
            etl_load_datetime,
            etl_load_metadatafilename
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename
                from
                (
                    SELECT
                        
                src,
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
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM807 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acqa_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acqa_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acqa_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adja_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adja_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adja_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:book_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:comp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:depr_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:depr_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:depr_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disa_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disa_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disa_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lkey), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:proa_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:proa_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:proa_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prod), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcst_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcst_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rcst_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rdpr_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rdpr_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rdpr_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:traa_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:traa_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:traa_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trct_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trct_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:trct_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null