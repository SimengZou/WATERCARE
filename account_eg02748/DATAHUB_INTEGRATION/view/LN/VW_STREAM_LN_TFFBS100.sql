CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFFBS100 AS SELECT
                        src:amnt::numeric(38, 17) AS amnt, 
                        src:back::integer AS back, 
                        src:back_kw::varchar AS back_kw, 
                        src:budg::varchar AS budg, 
                        src:budg_ref_compnr::integer AS budg_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:dbcr::integer AS dbcr, 
                        src:dbcr_kw::varchar AS dbcr_kw, 
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
                        src:disb::varchar AS disb, 
                        src:disb_ref_compnr::integer AS disb_ref_compnr, 
                        src:dm10::varchar AS dm10, 
                        src:dm11::varchar AS dm11, 
                        src:dm12::varchar AS dm12, 
                        src:dt10::integer AS dt10, 
                        src:dt10_dm10_ref_compnr::integer AS dt10_dm10_ref_compnr, 
                        src:dt10_ref_compnr::integer AS dt10_ref_compnr, 
                        src:dt11::integer AS dt11, 
                        src:dt11_dm11_ref_compnr::integer AS dt11_dm11_ref_compnr, 
                        src:dt11_ref_compnr::integer AS dt11_ref_compnr, 
                        src:dt12::integer AS dt12, 
                        src:dt12_dm12_ref_compnr::integer AS dt12_dm12_ref_compnr, 
                        src:dt12_ref_compnr::integer AS dt12_ref_compnr, 
                        src:dty1::integer AS dty1, 
                        src:dty1_dim1_ref_compnr::integer AS dty1_dim1_ref_compnr, 
                        src:dty1_ref_compnr::integer AS dty1_ref_compnr, 
                        src:dty2::integer AS dty2, 
                        src:dty2_dim2_ref_compnr::integer AS dty2_dim2_ref_compnr, 
                        src:dty2_ref_compnr::integer AS dty2_ref_compnr, 
                        src:dty3::integer AS dty3, 
                        src:dty3_dim3_ref_compnr::integer AS dty3_dim3_ref_compnr, 
                        src:dty3_ref_compnr::integer AS dty3_ref_compnr, 
                        src:dty4::integer AS dty4, 
                        src:dty4_dim4_ref_compnr::integer AS dty4_dim4_ref_compnr, 
                        src:dty4_ref_compnr::integer AS dty4_ref_compnr, 
                        src:dty5::integer AS dty5, 
                        src:dty5_dim5_ref_compnr::integer AS dty5_dim5_ref_compnr, 
                        src:dty5_ref_compnr::integer AS dty5_ref_compnr, 
                        src:dty6::integer AS dty6, 
                        src:dty6_dim6_ref_compnr::integer AS dty6_dim6_ref_compnr, 
                        src:dty6_ref_compnr::integer AS dty6_ref_compnr, 
                        src:dty7::integer AS dty7, 
                        src:dty7_dim7_ref_compnr::integer AS dty7_dim7_ref_compnr, 
                        src:dty7_ref_compnr::integer AS dty7_ref_compnr, 
                        src:dty8::integer AS dty8, 
                        src:dty8_dim8_ref_compnr::integer AS dty8_dim8_ref_compnr, 
                        src:dty8_ref_compnr::integer AS dty8_ref_compnr, 
                        src:dty9::integer AS dty9, 
                        src:dty9_dim9_ref_compnr::integer AS dty9_dim9_ref_compnr, 
                        src:dty9_ref_compnr::integer AS dty9_ref_compnr, 
                        src:leac::varchar AS leac, 
                        src:leac_ref_compnr::integer AS leac_ref_compnr, 
                        src:olap::integer AS olap, 
                        src:pdmo::integer AS pdmo, 
                        src:pdmo_kw::varchar AS pdmo_kw, 
                        src:qan1::numeric(38, 17) AS qan1, 
                        src:qan2::numeric(38, 17) AS qan2, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:username::varchar AS username, 
                        src:year::integer AS year, 
                        src:year_budg_ref_compnr::integer AS year_budg_ref_compnr, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:dm11,
                src:dim9,
                src:dim6,
                src:dm10,
                src:compnr,
                src:leac,
                src:dim7,
                src:dm12,
                src:dim5,
                src:dim8,
                src:budg,
                src:dim1,
                src:dim2,
                src:dim3,
                src:dim4,
                src:year  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFBS100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:back), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:budg_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dbcr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:disb_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dt10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dt10_dm10_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dt10_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dt11), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dt11_dm11_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dt11_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dt12), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dt12_dm12_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dt12_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty1_dim1_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty1_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty2_dim2_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty2_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty3_dim3_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty3_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty4_dim4_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty4_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty5_dim5_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty5_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty6_dim6_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty6_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty7_dim7_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty7_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty8_dim8_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty8_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty9_dim9_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dty9_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:leac_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:olap), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pdmo), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qan1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:qan2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year_budg_ref_compnr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null