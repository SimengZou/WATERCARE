CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFBS101_ERROR AS SELECT src, 'LN_TFFBS101' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) THEN
                    'amnt contains a non-numeric value : \'' || AS_VARCHAR(src:amnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:back), '0'), 38, 10) is null and 
                    src:back is not null) THEN
                    'back contains a non-numeric value : \'' || AS_VARCHAR(src:back) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:budg_ref_compnr), '0'), 38, 10) is null and 
                    src:budg_ref_compnr is not null) THEN
                    'budg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:budg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coam), '0'), 38, 10) is null and 
                    src:coam is not null) THEN
                    'coam contains a non-numeric value : \'' || AS_VARCHAR(src:coam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) THEN
                    'dbcr contains a non-numeric value : \'' || AS_VARCHAR(src:dbcr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt10), '0'), 38, 10) is null and 
                    src:dt10 is not null) THEN
                    'dt10 contains a non-numeric value : \'' || AS_VARCHAR(src:dt10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt10_dm10_ref_compnr), '0'), 38, 10) is null and 
                    src:dt10_dm10_ref_compnr is not null) THEN
                    'dt10_dm10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dt10_dm10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt10_ref_compnr), '0'), 38, 10) is null and 
                    src:dt10_ref_compnr is not null) THEN
                    'dt10_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dt10_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt11), '0'), 38, 10) is null and 
                    src:dt11 is not null) THEN
                    'dt11 contains a non-numeric value : \'' || AS_VARCHAR(src:dt11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt11_dm11_ref_compnr), '0'), 38, 10) is null and 
                    src:dt11_dm11_ref_compnr is not null) THEN
                    'dt11_dm11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dt11_dm11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt11_ref_compnr), '0'), 38, 10) is null and 
                    src:dt11_ref_compnr is not null) THEN
                    'dt11_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dt11_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt12), '0'), 38, 10) is null and 
                    src:dt12 is not null) THEN
                    'dt12 contains a non-numeric value : \'' || AS_VARCHAR(src:dt12) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt12_dm12_ref_compnr), '0'), 38, 10) is null and 
                    src:dt12_dm12_ref_compnr is not null) THEN
                    'dt12_dm12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dt12_dm12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt12_ref_compnr), '0'), 38, 10) is null and 
                    src:dt12_ref_compnr is not null) THEN
                    'dt12_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dt12_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty1), '0'), 38, 10) is null and 
                    src:dty1 is not null) THEN
                    'dty1 contains a non-numeric value : \'' || AS_VARCHAR(src:dty1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty1_dim1_ref_compnr), '0'), 38, 10) is null and 
                    src:dty1_dim1_ref_compnr is not null) THEN
                    'dty1_dim1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty1_dim1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty1_ref_compnr), '0'), 38, 10) is null and 
                    src:dty1_ref_compnr is not null) THEN
                    'dty1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty2), '0'), 38, 10) is null and 
                    src:dty2 is not null) THEN
                    'dty2 contains a non-numeric value : \'' || AS_VARCHAR(src:dty2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty2_dim2_ref_compnr), '0'), 38, 10) is null and 
                    src:dty2_dim2_ref_compnr is not null) THEN
                    'dty2_dim2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty2_dim2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty2_ref_compnr), '0'), 38, 10) is null and 
                    src:dty2_ref_compnr is not null) THEN
                    'dty2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty3), '0'), 38, 10) is null and 
                    src:dty3 is not null) THEN
                    'dty3 contains a non-numeric value : \'' || AS_VARCHAR(src:dty3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty3_dim3_ref_compnr), '0'), 38, 10) is null and 
                    src:dty3_dim3_ref_compnr is not null) THEN
                    'dty3_dim3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty3_dim3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty3_ref_compnr), '0'), 38, 10) is null and 
                    src:dty3_ref_compnr is not null) THEN
                    'dty3_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty3_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty4), '0'), 38, 10) is null and 
                    src:dty4 is not null) THEN
                    'dty4 contains a non-numeric value : \'' || AS_VARCHAR(src:dty4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty4_dim4_ref_compnr), '0'), 38, 10) is null and 
                    src:dty4_dim4_ref_compnr is not null) THEN
                    'dty4_dim4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty4_dim4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty4_ref_compnr), '0'), 38, 10) is null and 
                    src:dty4_ref_compnr is not null) THEN
                    'dty4_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty4_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty5), '0'), 38, 10) is null and 
                    src:dty5 is not null) THEN
                    'dty5 contains a non-numeric value : \'' || AS_VARCHAR(src:dty5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty5_dim5_ref_compnr), '0'), 38, 10) is null and 
                    src:dty5_dim5_ref_compnr is not null) THEN
                    'dty5_dim5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty5_dim5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty5_ref_compnr), '0'), 38, 10) is null and 
                    src:dty5_ref_compnr is not null) THEN
                    'dty5_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty5_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty6), '0'), 38, 10) is null and 
                    src:dty6 is not null) THEN
                    'dty6 contains a non-numeric value : \'' || AS_VARCHAR(src:dty6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty6_dim6_ref_compnr), '0'), 38, 10) is null and 
                    src:dty6_dim6_ref_compnr is not null) THEN
                    'dty6_dim6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty6_dim6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty6_ref_compnr), '0'), 38, 10) is null and 
                    src:dty6_ref_compnr is not null) THEN
                    'dty6_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty6_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty7), '0'), 38, 10) is null and 
                    src:dty7 is not null) THEN
                    'dty7 contains a non-numeric value : \'' || AS_VARCHAR(src:dty7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty7_dim7_ref_compnr), '0'), 38, 10) is null and 
                    src:dty7_dim7_ref_compnr is not null) THEN
                    'dty7_dim7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty7_dim7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty7_ref_compnr), '0'), 38, 10) is null and 
                    src:dty7_ref_compnr is not null) THEN
                    'dty7_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty7_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty8), '0'), 38, 10) is null and 
                    src:dty8 is not null) THEN
                    'dty8 contains a non-numeric value : \'' || AS_VARCHAR(src:dty8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty8_dim8_ref_compnr), '0'), 38, 10) is null and 
                    src:dty8_dim8_ref_compnr is not null) THEN
                    'dty8_dim8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty8_dim8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty8_ref_compnr), '0'), 38, 10) is null and 
                    src:dty8_ref_compnr is not null) THEN
                    'dty8_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty8_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty9), '0'), 38, 10) is null and 
                    src:dty9 is not null) THEN
                    'dty9 contains a non-numeric value : \'' || AS_VARCHAR(src:dty9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty9_dim9_ref_compnr), '0'), 38, 10) is null and 
                    src:dty9_dim9_ref_compnr is not null) THEN
                    'dty9_dim9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty9_dim9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty9_ref_compnr), '0'), 38, 10) is null and 
                    src:dty9_ref_compnr is not null) THEN
                    'dty9_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dty9_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leac_ref_compnr), '0'), 38, 10) is null and 
                    src:leac_ref_compnr is not null) THEN
                    'leac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:leac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:olap), '0'), 38, 10) is null and 
                    src:olap is not null) THEN
                    'olap contains a non-numeric value : \'' || AS_VARCHAR(src:olap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) THEN
                    'peri contains a non-numeric value : \'' || AS_VARCHAR(src:peri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qan1), '0'), 38, 10) is null and 
                    src:qan1 is not null) THEN
                    'qan1 contains a non-numeric value : \'' || AS_VARCHAR(src:qan1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qan2), '0'), 38, 10) is null and 
                    src:qan2 is not null) THEN
                    'qan2 contains a non-numeric value : \'' || AS_VARCHAR(src:qan2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stam), '0'), 38, 10) is null and 
                    src:stam is not null) THEN
                    'stam contains a non-numeric value : \'' || AS_VARCHAR(src:stam) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) THEN
                    'year contains a non-numeric value : \'' || AS_VARCHAR(src:year) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year_budg_ref_compnr), '0'), 38, 10) is null and 
                    src:year_budg_ref_compnr is not null) THEN
                    'year_budg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:year_budg_ref_compnr) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null) THEN
                'sequencenumber contains a non-timestamp value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:dm10,
                src:compnr,
                src:budg,
                src:dim8,
                src:dim2,
                src:year,
                src:dim6,
                src:leac,
                src:dim5,
                src:dim9,
                src:dim3,
                src:dim1,
                src:dm11,
                src:dim7,
                src:dm12,
                src:dim4,
                src:peri  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFBS101 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:back), '0'), 38, 10) is null and 
                    src:back is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:budg_ref_compnr), '0'), 38, 10) is null and 
                    src:budg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coam), '0'), 38, 10) is null and 
                    src:coam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt10), '0'), 38, 10) is null and 
                    src:dt10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt10_dm10_ref_compnr), '0'), 38, 10) is null and 
                    src:dt10_dm10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt10_ref_compnr), '0'), 38, 10) is null and 
                    src:dt10_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt11), '0'), 38, 10) is null and 
                    src:dt11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt11_dm11_ref_compnr), '0'), 38, 10) is null and 
                    src:dt11_dm11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt11_ref_compnr), '0'), 38, 10) is null and 
                    src:dt11_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt12), '0'), 38, 10) is null and 
                    src:dt12 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt12_dm12_ref_compnr), '0'), 38, 10) is null and 
                    src:dt12_dm12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dt12_ref_compnr), '0'), 38, 10) is null and 
                    src:dt12_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty1), '0'), 38, 10) is null and 
                    src:dty1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty1_dim1_ref_compnr), '0'), 38, 10) is null and 
                    src:dty1_dim1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty1_ref_compnr), '0'), 38, 10) is null and 
                    src:dty1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty2), '0'), 38, 10) is null and 
                    src:dty2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty2_dim2_ref_compnr), '0'), 38, 10) is null and 
                    src:dty2_dim2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty2_ref_compnr), '0'), 38, 10) is null and 
                    src:dty2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty3), '0'), 38, 10) is null and 
                    src:dty3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty3_dim3_ref_compnr), '0'), 38, 10) is null and 
                    src:dty3_dim3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty3_ref_compnr), '0'), 38, 10) is null and 
                    src:dty3_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty4), '0'), 38, 10) is null and 
                    src:dty4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty4_dim4_ref_compnr), '0'), 38, 10) is null and 
                    src:dty4_dim4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty4_ref_compnr), '0'), 38, 10) is null and 
                    src:dty4_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty5), '0'), 38, 10) is null and 
                    src:dty5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty5_dim5_ref_compnr), '0'), 38, 10) is null and 
                    src:dty5_dim5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty5_ref_compnr), '0'), 38, 10) is null and 
                    src:dty5_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty6), '0'), 38, 10) is null and 
                    src:dty6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty6_dim6_ref_compnr), '0'), 38, 10) is null and 
                    src:dty6_dim6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty6_ref_compnr), '0'), 38, 10) is null and 
                    src:dty6_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty7), '0'), 38, 10) is null and 
                    src:dty7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty7_dim7_ref_compnr), '0'), 38, 10) is null and 
                    src:dty7_dim7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty7_ref_compnr), '0'), 38, 10) is null and 
                    src:dty7_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty8), '0'), 38, 10) is null and 
                    src:dty8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty8_dim8_ref_compnr), '0'), 38, 10) is null and 
                    src:dty8_dim8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty8_ref_compnr), '0'), 38, 10) is null and 
                    src:dty8_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty9), '0'), 38, 10) is null and 
                    src:dty9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty9_dim9_ref_compnr), '0'), 38, 10) is null and 
                    src:dty9_dim9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dty9_ref_compnr), '0'), 38, 10) is null and 
                    src:dty9_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leac_ref_compnr), '0'), 38, 10) is null and 
                    src:leac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:olap), '0'), 38, 10) is null and 
                    src:olap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peri), '0'), 38, 10) is null and 
                    src:peri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qan1), '0'), 38, 10) is null and 
                    src:qan1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qan2), '0'), 38, 10) is null and 
                    src:qan2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stam), '0'), 38, 10) is null and 
                    src:stam is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year_budg_ref_compnr), '0'), 38, 10) is null and 
                    src:year_budg_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)