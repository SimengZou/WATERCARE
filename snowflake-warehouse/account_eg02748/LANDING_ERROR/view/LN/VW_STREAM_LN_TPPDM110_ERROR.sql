CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPDM110_ERROR AS SELECT src, 'LN_TPPDM110' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrt), '0'), 38, 10) is null and 
                    src:afrt is not null) THEN
                    'afrt contains a non-numeric value : \'' || AS_VARCHAR(src:afrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) THEN
                    'blbl contains a non-numeric value : \'' || AS_VARCHAR(src:blbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capt), '0'), 38, 10) is null and 
                    src:capt is not null) THEN
                    'capt contains a non-numeric value : \'' || AS_VARCHAR(src:capt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspt), '0'), 38, 10) is null and 
                    src:cspt is not null) THEN
                    'cspt contains a non-numeric value : \'' || AS_VARCHAR(src:cspt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) THEN
                    'cuti_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuti_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eoty), '0'), 38, 10) is null and 
                    src:eoty is not null) THEN
                    'eoty contains a non-numeric value : \'' || AS_VARCHAR(src:eoty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:freq), '0'), 38, 10) is null and 
                    src:freq is not null) THEN
                    'freq contains a non-numeric value : \'' || AS_VARCHAR(src:freq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lvps), '0'), 38, 10) is null and 
                    src:lvps is not null) THEN
                    'lvps contains a non-numeric value : \'' || AS_VARCHAR(src:lvps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mevl), '0'), 38, 10) is null and 
                    src:mevl is not null) THEN
                    'mevl contains a non-numeric value : \'' || AS_VARCHAR(src:mevl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plmc), '0'), 38, 10) is null and 
                    src:plmc is not null) THEN
                    'plmc contains a non-numeric value : \'' || AS_VARCHAR(src:plmc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plmc_ref_compnr), '0'), 38, 10) is null and 
                    src:plmc_ref_compnr is not null) THEN
                    'plmc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:plmc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prin), '0'), 38, 10) is null and 
                    src:prin is not null) THEN
                    'prin contains a non-numeric value : \'' || AS_VARCHAR(src:prin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prms), '0'), 38, 10) is null and 
                    src:prms is not null) THEN
                    'prms contains a non-numeric value : \'' || AS_VARCHAR(src:prms) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prnd), '0'), 38, 10) is null and 
                    src:prnd is not null) THEN
                    'prnd contains a non-numeric value : \'' || AS_VARCHAR(src:prnd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsp), '0'), 38, 10) is null and 
                    src:prsp is not null) THEN
                    'prsp contains a non-numeric value : \'' || AS_VARCHAR(src:prsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prst), '0'), 38, 10) is null and 
                    src:prst is not null) THEN
                    'prst contains a non-numeric value : \'' || AS_VARCHAR(src:prst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rent), '0'), 38, 10) is null and 
                    src:rent is not null) THEN
                    'rent contains a non-numeric value : \'' || AS_VARCHAR(src:rent) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:setl), '0'), 38, 10) is null and 
                    src:setl is not null) THEN
                    'setl contains a non-numeric value : \'' || AS_VARCHAR(src:setl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tact), '0'), 38, 10) is null and 
                    src:tact is not null) THEN
                    'tact contains a non-numeric value : \'' || AS_VARCHAR(src:tact) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
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
                                    
                src:cact,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM110 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrt), '0'), 38, 10) is null and 
                    src:afrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capt), '0'), 38, 10) is null and 
                    src:capt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspt), '0'), 38, 10) is null and 
                    src:cspt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eoty), '0'), 38, 10) is null and 
                    src:eoty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:freq), '0'), 38, 10) is null and 
                    src:freq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lvps), '0'), 38, 10) is null and 
                    src:lvps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mevl), '0'), 38, 10) is null and 
                    src:mevl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plmc), '0'), 38, 10) is null and 
                    src:plmc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plmc_ref_compnr), '0'), 38, 10) is null and 
                    src:plmc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prin), '0'), 38, 10) is null and 
                    src:prin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prms), '0'), 38, 10) is null and 
                    src:prms is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prnd), '0'), 38, 10) is null and 
                    src:prnd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsp), '0'), 38, 10) is null and 
                    src:prsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prst), '0'), 38, 10) is null and 
                    src:prst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rent), '0'), 38, 10) is null and 
                    src:rent is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:setl), '0'), 38, 10) is null and 
                    src:setl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tact), '0'), 38, 10) is null and 
                    src:tact is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)