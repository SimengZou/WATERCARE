CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINA126_ERROR AS SELECT src, 'LN_WHINA126' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inwp), '0'), 38, 10) is null and 
                    src:inwp is not null) THEN
                    'inwp contains a non-numeric value : \'' || AS_VARCHAR(src:inwp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iror), '0'), 38, 10) is null and 
                    src:iror is not null) THEN
                    'iror contains a non-numeric value : \'' || AS_VARCHAR(src:iror) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_cwar_trdt_seqn_inwp_ref_compnr), '0'), 38, 10) is null and 
                    src:item_cwar_trdt_seqn_inwp_ref_compnr is not null) THEN
                    'item_cwar_trdt_seqn_inwp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_cwar_trdt_seqn_inwp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivsq), '0'), 38, 10) is null and 
                    src:ivsq is not null) THEN
                    'ivsq contains a non-numeric value : \'' || AS_VARCHAR(src:ivsq) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lgdt), '1900-01-01')) is null and 
                    src:lgdt is not null) THEN
                    'lgdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lgdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reva), '0'), 38, 10) is null and 
                    src:reva is not null) THEN
                    'reva contains a non-numeric value : \'' || AS_VARCHAR(src:reva) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rorg), '0'), 38, 10) is null and 
                    src:rorg is not null) THEN
                    'rorg contains a non-numeric value : \'' || AS_VARCHAR(src:rorg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) THEN
                    'rseq contains a non-numeric value : \'' || AS_VARCHAR(src:rseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) THEN
                    'trdt contains a non-timestamp value : \'' || AS_VARCHAR(src:trdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:vpdt), '1900-01-01')) is null and 
                    src:vpdt is not null) THEN
                    'vpdt contains a non-timestamp value : \'' || AS_VARCHAR(src:vpdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vseq), '0'), 38, 10) is null and 
                    src:vseq is not null) THEN
                    'vseq contains a non-numeric value : \'' || AS_VARCHAR(src:vseq) || '\'' WHEN 
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
                                    
                src:cwar,
                src:vseq,
                src:vpdt,
                src:seqn,
                src:compnr,
                src:item,
                src:trdt,
                src:inwp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINA126 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inwp), '0'), 38, 10) is null and 
                    src:inwp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iror), '0'), 38, 10) is null and 
                    src:iror is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_cwar_trdt_seqn_inwp_ref_compnr), '0'), 38, 10) is null and 
                    src:item_cwar_trdt_seqn_inwp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivsq), '0'), 38, 10) is null and 
                    src:ivsq is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lgdt), '1900-01-01')) is null and 
                    src:lgdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reva), '0'), 38, 10) is null and 
                    src:reva is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rorg), '0'), 38, 10) is null and 
                    src:rorg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:vpdt), '1900-01-01')) is null and 
                    src:vpdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vseq), '0'), 38, 10) is null and 
                    src:vseq is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)