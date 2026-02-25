CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDPUR303_ERROR AS SELECT src, 'LN_TDPUR303' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_pono_cofc_csqn_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_pono_cofc_csqn_ref_compnr is not null) THEN
                    'cono_pono_cofc_csqn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cono_pono_cofc_csqn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpwc), '0'), 38, 10) is null and 
                    src:cpwc is not null) THEN
                    'cpwc contains a non-numeric value : \'' || AS_VARCHAR(src:cpwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csqn), '0'), 38, 10) is null and 
                    src:csqn is not null) THEN
                    'csqn contains a non-numeric value : \'' || AS_VARCHAR(src:csqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupp_ref_compnr), '0'), 38, 10) is null and 
                    src:cupp_ref_compnr is not null) THEN
                    'cupp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cupp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpp), '0'), 38, 10) is null and 
                    src:cvpp is not null) THEN
                    'cvpp contains a non-numeric value : \'' || AS_VARCHAR(src:cvpp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_1), '0'), 38, 10) is null and 
                    src:disc_1 is not null) THEN
                    'disc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_10), '0'), 38, 10) is null and 
                    src:disc_10 is not null) THEN
                    'disc_10 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_11), '0'), 38, 10) is null and 
                    src:disc_11 is not null) THEN
                    'disc_11 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_2), '0'), 38, 10) is null and 
                    src:disc_2 is not null) THEN
                    'disc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_3), '0'), 38, 10) is null and 
                    src:disc_3 is not null) THEN
                    'disc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_4), '0'), 38, 10) is null and 
                    src:disc_4 is not null) THEN
                    'disc_4 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_5), '0'), 38, 10) is null and 
                    src:disc_5 is not null) THEN
                    'disc_5 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_6), '0'), 38, 10) is null and 
                    src:disc_6 is not null) THEN
                    'disc_6 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_7), '0'), 38, 10) is null and 
                    src:disc_7 is not null) THEN
                    'disc_7 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_8), '0'), 38, 10) is null and 
                    src:disc_8 is not null) THEN
                    'disc_8 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_9), '0'), 38, 10) is null and 
                    src:disc_9 is not null) THEN
                    'disc_9 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_1), '0'), 38, 10) is null and 
                    src:dmth_1 is not null) THEN
                    'dmth_1 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_10), '0'), 38, 10) is null and 
                    src:dmth_10 is not null) THEN
                    'dmth_10 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_11), '0'), 38, 10) is null and 
                    src:dmth_11 is not null) THEN
                    'dmth_11 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_2), '0'), 38, 10) is null and 
                    src:dmth_2 is not null) THEN
                    'dmth_2 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_3), '0'), 38, 10) is null and 
                    src:dmth_3 is not null) THEN
                    'dmth_3 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_4), '0'), 38, 10) is null and 
                    src:dmth_4 is not null) THEN
                    'dmth_4 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_5), '0'), 38, 10) is null and 
                    src:dmth_5 is not null) THEN
                    'dmth_5 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_6), '0'), 38, 10) is null and 
                    src:dmth_6 is not null) THEN
                    'dmth_6 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_7), '0'), 38, 10) is null and 
                    src:dmth_7 is not null) THEN
                    'dmth_7 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_8), '0'), 38, 10) is null and 
                    src:dmth_8 is not null) THEN
                    'dmth_8 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_9), '0'), 38, 10) is null and 
                    src:dmth_9 is not null) THEN
                    'dmth_9 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsor), '0'), 38, 10) is null and 
                    src:dsor is not null) THEN
                    'dsor contains a non-numeric value : \'' || AS_VARCHAR(src:dsor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtrm), '0'), 38, 10) is null and 
                    src:dtrm is not null) THEN
                    'dtrm contains a non-numeric value : \'' || AS_VARCHAR(src:dtrm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elgb), '0'), 38, 10) is null and 
                    src:elgb is not null) THEN
                    'elgb contains a non-numeric value : \'' || AS_VARCHAR(src:elgb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icap), '0'), 38, 10) is null and 
                    src:icap is not null) THEN
                    'icap contains a non-numeric value : \'' || AS_VARCHAR(src:icap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_1), '0'), 38, 10) is null and 
                    src:ldam_1 is not null) THEN
                    'ldam_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_10), '0'), 38, 10) is null and 
                    src:ldam_10 is not null) THEN
                    'ldam_10 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_11), '0'), 38, 10) is null and 
                    src:ldam_11 is not null) THEN
                    'ldam_11 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_2), '0'), 38, 10) is null and 
                    src:ldam_2 is not null) THEN
                    'ldam_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_3), '0'), 38, 10) is null and 
                    src:ldam_3 is not null) THEN
                    'ldam_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_4), '0'), 38, 10) is null and 
                    src:ldam_4 is not null) THEN
                    'ldam_4 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_5), '0'), 38, 10) is null and 
                    src:ldam_5 is not null) THEN
                    'ldam_5 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_6), '0'), 38, 10) is null and 
                    src:ldam_6 is not null) THEN
                    'ldam_6 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_7), '0'), 38, 10) is null and 
                    src:ldam_7 is not null) THEN
                    'ldam_7 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_8), '0'), 38, 10) is null and 
                    src:ldam_8 is not null) THEN
                    'ldam_8 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_9), '0'), 38, 10) is null and 
                    src:ldam_9 is not null) THEN
                    'ldam_9 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbor), '0'), 38, 10) is null and 
                    src:pbor is not null) THEN
                    'pbor contains a non-numeric value : \'' || AS_VARCHAR(src:pbor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prbk_ref_compnr), '0'), 38, 10) is null and 
                    src:prbk_ref_compnr is not null) THEN
                    'prbk_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prbk_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsg_ref_compnr), '0'), 38, 10) is null and 
                    src:prsg_ref_compnr is not null) THEN
                    'prsg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prsg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) THEN
                    'revn contains a non-numeric value : \'' || AS_VARCHAR(src:revn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsel), '0'), 38, 10) is null and 
                    src:rsel is not null) THEN
                    'rsel contains a non-numeric value : \'' || AS_VARCHAR(src:rsel) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) THEN
                    'sdat contains a non-timestamp value : \'' || AS_VARCHAR(src:sdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tapr), '0'), 38, 10) is null and 
                    src:tapr is not null) THEN
                    'tapr contains a non-numeric value : \'' || AS_VARCHAR(src:tapr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbk_ref_compnr), '0'), 38, 10) is null and 
                    src:tpbk_ref_compnr is not null) THEN
                    'tpbk_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tpbk_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbk_tpbl_ref_compnr), '0'), 38, 10) is null and 
                    src:tpbk_tpbl_ref_compnr is not null) THEN
                    'tpbk_tpbl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tpbk_tpbl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbl), '0'), 38, 10) is null and 
                    src:tpbl is not null) THEN
                    'tpbl contains a non-numeric value : \'' || AS_VARCHAR(src:tpbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtr), '0'), 38, 10) is null and 
                    src:txtr is not null) THEN
                    'txtr contains a non-numeric value : \'' || AS_VARCHAR(src:txtr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtr_ref_compnr), '0'), 38, 10) is null and 
                    src:txtr_ref_compnr is not null) THEN
                    'txtr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtr_ref_compnr) || '\'' WHEN 
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
                                    
                src:pono,
                src:csqn,
                src:compnr,
                src:cono,
                src:sdat,
                src:cofc  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR303 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono_pono_cofc_csqn_ref_compnr), '0'), 38, 10) is null and 
                    src:cono_pono_cofc_csqn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpwc), '0'), 38, 10) is null and 
                    src:cpwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csqn), '0'), 38, 10) is null and 
                    src:csqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupp_ref_compnr), '0'), 38, 10) is null and 
                    src:cupp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpp), '0'), 38, 10) is null and 
                    src:cvpp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_1), '0'), 38, 10) is null and 
                    src:disc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_10), '0'), 38, 10) is null and 
                    src:disc_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_11), '0'), 38, 10) is null and 
                    src:disc_11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_2), '0'), 38, 10) is null and 
                    src:disc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_3), '0'), 38, 10) is null and 
                    src:disc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_4), '0'), 38, 10) is null and 
                    src:disc_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_5), '0'), 38, 10) is null and 
                    src:disc_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_6), '0'), 38, 10) is null and 
                    src:disc_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_7), '0'), 38, 10) is null and 
                    src:disc_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_8), '0'), 38, 10) is null and 
                    src:disc_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_9), '0'), 38, 10) is null and 
                    src:disc_9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_1), '0'), 38, 10) is null and 
                    src:dmth_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_10), '0'), 38, 10) is null and 
                    src:dmth_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_11), '0'), 38, 10) is null and 
                    src:dmth_11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_2), '0'), 38, 10) is null and 
                    src:dmth_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_3), '0'), 38, 10) is null and 
                    src:dmth_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_4), '0'), 38, 10) is null and 
                    src:dmth_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_5), '0'), 38, 10) is null and 
                    src:dmth_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_6), '0'), 38, 10) is null and 
                    src:dmth_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_7), '0'), 38, 10) is null and 
                    src:dmth_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_8), '0'), 38, 10) is null and 
                    src:dmth_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_9), '0'), 38, 10) is null and 
                    src:dmth_9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsor), '0'), 38, 10) is null and 
                    src:dsor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtrm), '0'), 38, 10) is null and 
                    src:dtrm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elgb), '0'), 38, 10) is null and 
                    src:elgb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icap), '0'), 38, 10) is null and 
                    src:icap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_1), '0'), 38, 10) is null and 
                    src:ldam_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_10), '0'), 38, 10) is null and 
                    src:ldam_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_11), '0'), 38, 10) is null and 
                    src:ldam_11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_2), '0'), 38, 10) is null and 
                    src:ldam_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_3), '0'), 38, 10) is null and 
                    src:ldam_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_4), '0'), 38, 10) is null and 
                    src:ldam_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_5), '0'), 38, 10) is null and 
                    src:ldam_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_6), '0'), 38, 10) is null and 
                    src:ldam_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_7), '0'), 38, 10) is null and 
                    src:ldam_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_8), '0'), 38, 10) is null and 
                    src:ldam_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_9), '0'), 38, 10) is null and 
                    src:ldam_9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbor), '0'), 38, 10) is null and 
                    src:pbor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prbk_ref_compnr), '0'), 38, 10) is null and 
                    src:prbk_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsg_ref_compnr), '0'), 38, 10) is null and 
                    src:prsg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:revn), '0'), 38, 10) is null and 
                    src:revn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsel), '0'), 38, 10) is null and 
                    src:rsel is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tapr), '0'), 38, 10) is null and 
                    src:tapr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbk_ref_compnr), '0'), 38, 10) is null and 
                    src:tpbk_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbk_tpbl_ref_compnr), '0'), 38, 10) is null and 
                    src:tpbk_tpbl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbl), '0'), 38, 10) is null and 
                    src:tpbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtr), '0'), 38, 10) is null and 
                    src:txtr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtr_ref_compnr), '0'), 38, 10) is null and 
                    src:txtr_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)