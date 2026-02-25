CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCEMM050_ERROR AS SELECT src, 'LN_TCEMM050' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:admo), '0'), 38, 10) is null and 
                    src:admo is not null) THEN
                    'admo contains a non-numeric value : \'' || AS_VARCHAR(src:admo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) THEN
                    'bpid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) THEN
                    'cadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) THEN
                    'ccal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clus_ref_compnr), '0'), 38, 10) is null and 
                    src:clus_ref_compnr is not null) THEN
                    'clus_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clus_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cust), '0'), 38, 10) is null and 
                    src:cust is not null) THEN
                    'cust contains a non-numeric value : \'' || AS_VARCHAR(src:cust) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eunt_ref_compnr), '0'), 38, 10) is null and 
                    src:eunt_ref_compnr is not null) THEN
                    'eunt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:eunt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:expl), '0'), 38, 10) is null and 
                    src:expl is not null) THEN
                    'expl contains a non-numeric value : \'' || AS_VARCHAR(src:expl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) THEN
                    'lcmp contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) THEN
                    'lcmp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psit_ref_compnr), '0'), 38, 10) is null and 
                    src:psit_ref_compnr is not null) THEN
                    'psit_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:psit_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ract_ref_compnr), '0'), 38, 10) is null and 
                    src:ract_ref_compnr is not null) THEN
                    'ract_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ract_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scat_ref_compnr), '0'), 38, 10) is null and 
                    src:scat_ref_compnr is not null) THEN
                    'scat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:scat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) THEN
                    'sfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) THEN
                    'stbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subs), '0'), 38, 10) is null and 
                    src:subs is not null) THEN
                    'subs contains a non-numeric value : \'' || AS_VARCHAR(src:subs) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xsit), '0'), 38, 10) is null and 
                    src:xsit is not null) THEN
                    'xsit contains a non-numeric value : \'' || AS_VARCHAR(src:xsit) || '\'' WHEN 
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
                                    
                src:compnr,
                src:site  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCEMM050 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:admo), '0'), 38, 10) is null and 
                    src:admo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clus_ref_compnr), '0'), 38, 10) is null and 
                    src:clus_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cust), '0'), 38, 10) is null and 
                    src:cust is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eunt_ref_compnr), '0'), 38, 10) is null and 
                    src:eunt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:expl), '0'), 38, 10) is null and 
                    src:expl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psit_ref_compnr), '0'), 38, 10) is null and 
                    src:psit_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ract_ref_compnr), '0'), 38, 10) is null and 
                    src:ract_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scat_ref_compnr), '0'), 38, 10) is null and 
                    src:scat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subs), '0'), 38, 10) is null and 
                    src:subs is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xsit), '0'), 38, 10) is null and 
                    src:xsit is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)