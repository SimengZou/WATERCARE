CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSCLM350_ERROR AS SELECT src, 'LN_TSCLM350' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acln), '0'), 38, 10) is null and 
                    src:acln is not null) THEN
                    'acln contains a non-numeric value : \'' || AS_VARCHAR(src:acln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccll_ref_compnr), '0'), 38, 10) is null and 
                    src:ccll_ref_compnr is not null) THEN
                    'ccll_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccll_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cgrp_ref_compnr), '0'), 38, 10) is null and 
                    src:cgrp_ref_compnr is not null) THEN
                    'cgrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cgrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clst_ref_compnr), '0'), 38, 10) is null and 
                    src:clst_ref_compnr is not null) THEN
                    'clst_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clst_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crac_ref_compnr), '0'), 38, 10) is null and 
                    src:crac_ref_compnr is not null) THEN
                    'crac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csgr_ref_compnr), '0'), 38, 10) is null and 
                    src:csgr_ref_compnr is not null) THEN
                    'csgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csgr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:espr_ref_compnr), '0'), 38, 10) is null and 
                    src:espr_ref_compnr is not null) THEN
                    'espr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:espr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:expr_ref_compnr), '0'), 38, 10) is null and 
                    src:expr_ref_compnr is not null) THEN
                    'expr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:expr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exsl_ref_compnr), '0'), 38, 10) is null and 
                    src:exsl_ref_compnr is not null) THEN
                    'exsl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:exsl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:hidt), '1900-01-01')) is null and 
                    src:hidt is not null) THEN
                    'hidt contains a non-timestamp value : \'' || AS_VARCHAR(src:hidt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_sern_ref_compnr), '0'), 38, 10) is null and 
                    src:item_sern_ref_compnr is not null) THEN
                    'item_sern_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_sern_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orig), '0'), 38, 10) is null and 
                    src:orig is not null) THEN
                    'orig contains a non-numeric value : \'' || AS_VARCHAR(src:orig) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rprl_ref_compnr), '0'), 38, 10) is null and 
                    src:rprl_ref_compnr is not null) THEN
                    'rprl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rprl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scgr_ref_compnr), '0'), 38, 10) is null and 
                    src:scgr_ref_compnr is not null) THEN
                    'scgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:scgr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sigr_ref_compnr), '0'), 38, 10) is null and 
                    src:sigr_ref_compnr is not null) THEN
                    'sigr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sigr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sltn_ref_compnr), '0'), 38, 10) is null and 
                    src:sltn_ref_compnr is not null) THEN
                    'sltn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sltn_ref_compnr) || '\'' WHEN 
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
                                    
                src:orno,
                src:compnr,
                src:ccll,
                src:acln,
                src:orig,
                src:hidt  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCLM350 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acln), '0'), 38, 10) is null and 
                    src:acln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccll_ref_compnr), '0'), 38, 10) is null and 
                    src:ccll_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cgrp_ref_compnr), '0'), 38, 10) is null and 
                    src:cgrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clst_ref_compnr), '0'), 38, 10) is null and 
                    src:clst_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crac_ref_compnr), '0'), 38, 10) is null and 
                    src:crac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csgr_ref_compnr), '0'), 38, 10) is null and 
                    src:csgr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:espr_ref_compnr), '0'), 38, 10) is null and 
                    src:espr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:expr_ref_compnr), '0'), 38, 10) is null and 
                    src:expr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:exsl_ref_compnr), '0'), 38, 10) is null and 
                    src:exsl_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:hidt), '1900-01-01')) is null and 
                    src:hidt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_sern_ref_compnr), '0'), 38, 10) is null and 
                    src:item_sern_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orig), '0'), 38, 10) is null and 
                    src:orig is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rprl_ref_compnr), '0'), 38, 10) is null and 
                    src:rprl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scgr_ref_compnr), '0'), 38, 10) is null and 
                    src:scgr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sigr_ref_compnr), '0'), 38, 10) is null and 
                    src:sigr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sltn_ref_compnr), '0'), 38, 10) is null and 
                    src:sltn_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)