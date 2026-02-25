CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSCTM450_ERROR AS SELECT src, 'LN_TSCTM450' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpp_1), '0'), 38, 10) is null and 
                    src:acpp_1 is not null) THEN
                    'acpp_1 contains a non-numeric value : \'' || AS_VARCHAR(src:acpp_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpp_2), '0'), 38, 10) is null and 
                    src:acpp_2 is not null) THEN
                    'acpp_2 contains a non-numeric value : \'' || AS_VARCHAR(src:acpp_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpp_3), '0'), 38, 10) is null and 
                    src:acpp_3 is not null) THEN
                    'acpp_3 contains a non-numeric value : \'' || AS_VARCHAR(src:acpp_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchn), '0'), 38, 10) is null and 
                    src:cchn is not null) THEN
                    'cchn contains a non-numeric value : \'' || AS_VARCHAR(src:cchn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) THEN
                    'cfln contains a non-numeric value : \'' || AS_VARCHAR(src:cfln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_cchn_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_cchn_ref_compnr is not null) THEN
                    'csco_cchn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csco_cchn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) THEN
                    'csco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvdy), '0'), 38, 10) is null and 
                    src:cvdy is not null) THEN
                    'cvdy contains a non-numeric value : \'' || AS_VARCHAR(src:cvdy) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esco_1), '0'), 38, 10) is null and 
                    src:esco_1 is not null) THEN
                    'esco_1 contains a non-numeric value : \'' || AS_VARCHAR(src:esco_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esco_2), '0'), 38, 10) is null and 
                    src:esco_2 is not null) THEN
                    'esco_2 contains a non-numeric value : \'' || AS_VARCHAR(src:esco_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esco_3), '0'), 38, 10) is null and 
                    src:esco_3 is not null) THEN
                    'esco_3 contains a non-numeric value : \'' || AS_VARCHAR(src:esco_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esrv), '0'), 38, 10) is null and 
                    src:esrv is not null) THEN
                    'esrv contains a non-numeric value : \'' || AS_VARCHAR(src:esrv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esrv_dwhc), '0'), 38, 10) is null and 
                    src:esrv_dwhc is not null) THEN
                    'esrv_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:esrv_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esrv_homc), '0'), 38, 10) is null and 
                    src:esrv_homc is not null) THEN
                    'esrv_homc contains a non-numeric value : \'' || AS_VARCHAR(src:esrv_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esrv_refc), '0'), 38, 10) is null and 
                    src:esrv_refc is not null) THEN
                    'esrv_refc contains a non-numeric value : \'' || AS_VARCHAR(src:esrv_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esrv_rpc1), '0'), 38, 10) is null and 
                    src:esrv_rpc1 is not null) THEN
                    'esrv_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:esrv_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esrv_rpc2), '0'), 38, 10) is null and 
                    src:esrv_rpc2 is not null) THEN
                    'esrv_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:esrv_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fspr), '0'), 38, 10) is null and 
                    src:fspr is not null) THEN
                    'fspr contains a non-numeric value : \'' || AS_VARCHAR(src:fspr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fsyr), '0'), 38, 10) is null and 
                    src:fsyr is not null) THEN
                    'fsyr contains a non-numeric value : \'' || AS_VARCHAR(src:fsyr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nody), '0'), 38, 10) is null and 
                    src:nody is not null) THEN
                    'nody contains a non-numeric value : \'' || AS_VARCHAR(src:nody) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrv), '0'), 38, 10) is null and 
                    src:rcrv is not null) THEN
                    'rcrv contains a non-numeric value : \'' || AS_VARCHAR(src:rcrv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrv_dwhc), '0'), 38, 10) is null and 
                    src:rcrv_dwhc is not null) THEN
                    'rcrv_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:rcrv_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrv_homc), '0'), 38, 10) is null and 
                    src:rcrv_homc is not null) THEN
                    'rcrv_homc contains a non-numeric value : \'' || AS_VARCHAR(src:rcrv_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrv_refc), '0'), 38, 10) is null and 
                    src:rcrv_refc is not null) THEN
                    'rcrv_refc contains a non-numeric value : \'' || AS_VARCHAR(src:rcrv_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrv_rpc1), '0'), 38, 10) is null and 
                    src:rcrv_rpc1 is not null) THEN
                    'rcrv_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:rcrv_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrv_rpc2), '0'), 38, 10) is null and 
                    src:rcrv_rpc2 is not null) THEN
                    'rcrv_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:rcrv_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
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
                                    
                src:csco,
                src:cchn,
                src:fspr,
                src:fsyr,
                src:compnr,
                src:cfln  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCTM450 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpp_1), '0'), 38, 10) is null and 
                    src:acpp_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpp_2), '0'), 38, 10) is null and 
                    src:acpp_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acpp_3), '0'), 38, 10) is null and 
                    src:acpp_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchn), '0'), 38, 10) is null and 
                    src:cchn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_cchn_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_cchn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvdy), '0'), 38, 10) is null and 
                    src:cvdy is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esco_1), '0'), 38, 10) is null and 
                    src:esco_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esco_2), '0'), 38, 10) is null and 
                    src:esco_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esco_3), '0'), 38, 10) is null and 
                    src:esco_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esrv), '0'), 38, 10) is null and 
                    src:esrv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esrv_dwhc), '0'), 38, 10) is null and 
                    src:esrv_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esrv_homc), '0'), 38, 10) is null and 
                    src:esrv_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esrv_refc), '0'), 38, 10) is null and 
                    src:esrv_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esrv_rpc1), '0'), 38, 10) is null and 
                    src:esrv_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:esrv_rpc2), '0'), 38, 10) is null and 
                    src:esrv_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fspr), '0'), 38, 10) is null and 
                    src:fspr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fsyr), '0'), 38, 10) is null and 
                    src:fsyr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nody), '0'), 38, 10) is null and 
                    src:nody is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrv), '0'), 38, 10) is null and 
                    src:rcrv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrv_dwhc), '0'), 38, 10) is null and 
                    src:rcrv_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrv_homc), '0'), 38, 10) is null and 
                    src:rcrv_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrv_refc), '0'), 38, 10) is null and 
                    src:rcrv_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrv_rpc1), '0'), 38, 10) is null and 
                    src:rcrv_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrv_rpc2), '0'), 38, 10) is null and 
                    src:rcrv_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)