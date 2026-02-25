CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINH600_ERROR AS SELECT src, 'LN_WHINH600' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:boml), '0'), 38, 10) is null and 
                    src:boml is not null) THEN
                    'boml contains a non-numeric value : \'' || AS_VARCHAR(src:boml) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdos), '0'), 38, 10) is null and 
                    src:cdos is not null) THEN
                    'cdos contains a non-numeric value : \'' || AS_VARCHAR(src:cdos) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdpd_ref_compnr), '0'), 38, 10) is null and 
                    src:cdpd_ref_compnr is not null) THEN
                    'cdpd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdpd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdty), '0'), 38, 10) is null and 
                    src:cdty is not null) THEN
                    'cdty contains a non-numeric value : \'' || AS_VARCHAR(src:cdty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdun_ref_compnr), '0'), 38, 10) is null and 
                    src:cdun_ref_compnr is not null) THEN
                    'cdun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_stlo_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_stlo_ref_compnr is not null) THEN
                    'cwar_stlo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_stlo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlin), '0'), 38, 10) is null and 
                    src:dlin is not null) THEN
                    'dlin contains a non-numeric value : \'' || AS_VARCHAR(src:dlin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lcdt), '1900-01-01')) is null and 
                    src:lcdt is not null) THEN
                    'lcdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lcdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oorg), '0'), 38, 10) is null and 
                    src:oorg is not null) THEN
                    'oorg contains a non-numeric value : \'' || AS_VARCHAR(src:oorg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oset), '0'), 38, 10) is null and 
                    src:oset is not null) THEN
                    'oset contains a non-numeric value : \'' || AS_VARCHAR(src:oset) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pddt), '1900-01-01')) is null and 
                    src:pddt is not null) THEN
                    'pddt contains a non-timestamp value : \'' || AS_VARCHAR(src:pddt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qact), '0'), 38, 10) is null and 
                    src:qact is not null) THEN
                    'qact contains a non-numeric value : \'' || AS_VARCHAR(src:qact) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadv), '0'), 38, 10) is null and 
                    src:qadv is not null) THEN
                    'qadv contains a non-numeric value : \'' || AS_VARCHAR(src:qadv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapp), '0'), 38, 10) is null and 
                    src:qapp is not null) THEN
                    'qapp contains a non-numeric value : \'' || AS_VARCHAR(src:qapp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpla), '0'), 38, 10) is null and 
                    src:qpla is not null) THEN
                    'qpla contains a non-numeric value : \'' || AS_VARCHAR(src:qpla) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qreq), '0'), 38, 10) is null and 
                    src:qreq is not null) THEN
                    'qreq contains a non-numeric value : \'' || AS_VARCHAR(src:qreq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrun), '0'), 38, 10) is null and 
                    src:qrun is not null) THEN
                    'qrun contains a non-numeric value : \'' || AS_VARCHAR(src:qrun) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uspr), '0'), 38, 10) is null and 
                    src:uspr is not null) THEN
                    'uspr contains a non-numeric value : \'' || AS_VARCHAR(src:uspr) || '\'' WHEN 
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
                                    
                src:cdor,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINH600 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:boml), '0'), 38, 10) is null and 
                    src:boml is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdos), '0'), 38, 10) is null and 
                    src:cdos is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdpd_ref_compnr), '0'), 38, 10) is null and 
                    src:cdpd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdty), '0'), 38, 10) is null and 
                    src:cdty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdun_ref_compnr), '0'), 38, 10) is null and 
                    src:cdun_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_stlo_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_stlo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlin), '0'), 38, 10) is null and 
                    src:dlin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lcdt), '1900-01-01')) is null and 
                    src:lcdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oorg), '0'), 38, 10) is null and 
                    src:oorg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oset), '0'), 38, 10) is null and 
                    src:oset is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pddt), '1900-01-01')) is null and 
                    src:pddt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qact), '0'), 38, 10) is null and 
                    src:qact is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadv), '0'), 38, 10) is null and 
                    src:qadv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapp), '0'), 38, 10) is null and 
                    src:qapp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpla), '0'), 38, 10) is null and 
                    src:qpla is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qreq), '0'), 38, 10) is null and 
                    src:qreq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrun), '0'), 38, 10) is null and 
                    src:qrun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uspr), '0'), 38, 10) is null and 
                    src:uspr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)