CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSCTM320_ERROR AS SELECT src, 'LN_TSCTM320' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actn), '0'), 38, 10) is null and 
                    src:actn is not null) THEN
                    'actn contains a non-numeric value : \'' || AS_VARCHAR(src:actn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amdy), '0'), 38, 10) is null and 
                    src:amdy is not null) THEN
                    'amdy contains a non-numeric value : \'' || AS_VARCHAR(src:amdy) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_1), '0'), 38, 10) is null and 
                    src:camt_1 is not null) THEN
                    'camt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_2), '0'), 38, 10) is null and 
                    src:camt_2 is not null) THEN
                    'camt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_3), '0'), 38, 10) is null and 
                    src:camt_3 is not null) THEN
                    'camt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_cntc), '0'), 38, 10) is null and 
                    src:camt_cntc is not null) THEN
                    'camt_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_dwhc), '0'), 38, 10) is null and 
                    src:camt_dwhc is not null) THEN
                    'camt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_refc), '0'), 38, 10) is null and 
                    src:camt_refc is not null) THEN
                    'camt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchn), '0'), 38, 10) is null and 
                    src:cchn is not null) THEN
                    'cchn contains a non-numeric value : \'' || AS_VARCHAR(src:cchn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:chdt), '1900-01-01')) is null and 
                    src:chdt is not null) THEN
                    'chdt contains a non-timestamp value : \'' || AS_VARCHAR(src:chdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cind_ref_compnr), '0'), 38, 10) is null and 
                    src:cind_ref_compnr is not null) THEN
                    'cind_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cind_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cody_1), '0'), 38, 10) is null and 
                    src:cody_1 is not null) THEN
                    'cody_1 contains a non-numeric value : \'' || AS_VARCHAR(src:cody_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cody_2), '0'), 38, 10) is null and 
                    src:cody_2 is not null) THEN
                    'cody_2 contains a non-numeric value : \'' || AS_VARCHAR(src:cody_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cody_3), '0'), 38, 10) is null and 
                    src:cody_3 is not null) THEN
                    'cody_3 contains a non-numeric value : \'' || AS_VARCHAR(src:cody_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crtm), '1900-01-01')) is null and 
                    src:crtm is not null) THEN
                    'crtm contains a non-timestamp value : \'' || AS_VARCHAR(src:crtm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) THEN
                    'csco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csmt), '0'), 38, 10) is null and 
                    src:csmt is not null) THEN
                    'csmt contains a non-numeric value : \'' || AS_VARCHAR(src:csmt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) THEN
                    'efdt contains a non-timestamp value : \'' || AS_VARCHAR(src:efdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erfa), '0'), 38, 10) is null and 
                    src:erfa is not null) THEN
                    'erfa contains a non-numeric value : \'' || AS_VARCHAR(src:erfa) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) THEN
                    'exdt contains a non-timestamp value : \'' || AS_VARCHAR(src:exdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpn), '0'), 38, 10) is null and 
                    src:icpn is not null) THEN
                    'icpn contains a non-numeric value : \'' || AS_VARCHAR(src:icpn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpc), '0'), 38, 10) is null and 
                    src:inpc is not null) THEN
                    'inpc contains a non-numeric value : \'' || AS_VARCHAR(src:inpc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrpe), '0'), 38, 10) is null and 
                    src:nrpe is not null) THEN
                    'nrpe contains a non-numeric value : \'' || AS_VARCHAR(src:nrpe) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ochn), '0'), 38, 10) is null and 
                    src:ochn is not null) THEN
                    'ochn contains a non-numeric value : \'' || AS_VARCHAR(src:ochn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peru), '0'), 38, 10) is null and 
                    src:peru is not null) THEN
                    'peru contains a non-numeric value : \'' || AS_VARCHAR(src:peru) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt), '0'), 38, 10) is null and 
                    src:rsmt is not null) THEN
                    'rsmt contains a non-numeric value : \'' || AS_VARCHAR(src:rsmt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_dwhc), '0'), 38, 10) is null and 
                    src:rsmt_dwhc is not null) THEN
                    'rsmt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:rsmt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_homc), '0'), 38, 10) is null and 
                    src:rsmt_homc is not null) THEN
                    'rsmt_homc contains a non-numeric value : \'' || AS_VARCHAR(src:rsmt_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_refc), '0'), 38, 10) is null and 
                    src:rsmt_refc is not null) THEN
                    'rsmt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:rsmt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_rpc1), '0'), 38, 10) is null and 
                    src:rsmt_rpc1 is not null) THEN
                    'rsmt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:rsmt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_rpc2), '0'), 38, 10) is null and 
                    src:rsmt_rpc2 is not null) THEN
                    'rsmt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:rsmt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) THEN
                    'term contains a non-numeric value : \'' || AS_VARCHAR(src:term) || '\'' WHEN 
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
                                    
                src:csco,
                src:compnr,
                src:cchn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCTM320 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actn), '0'), 38, 10) is null and 
                    src:actn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amdy), '0'), 38, 10) is null and 
                    src:amdy is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_1), '0'), 38, 10) is null and 
                    src:camt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_2), '0'), 38, 10) is null and 
                    src:camt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_3), '0'), 38, 10) is null and 
                    src:camt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_cntc), '0'), 38, 10) is null and 
                    src:camt_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_dwhc), '0'), 38, 10) is null and 
                    src:camt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_refc), '0'), 38, 10) is null and 
                    src:camt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchn), '0'), 38, 10) is null and 
                    src:cchn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:chdt), '1900-01-01')) is null and 
                    src:chdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cind_ref_compnr), '0'), 38, 10) is null and 
                    src:cind_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cody_1), '0'), 38, 10) is null and 
                    src:cody_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cody_2), '0'), 38, 10) is null and 
                    src:cody_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cody_3), '0'), 38, 10) is null and 
                    src:cody_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crtm), '1900-01-01')) is null and 
                    src:crtm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csmt), '0'), 38, 10) is null and 
                    src:csmt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erfa), '0'), 38, 10) is null and 
                    src:erfa is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icpn), '0'), 38, 10) is null and 
                    src:icpn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpc), '0'), 38, 10) is null and 
                    src:inpc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrpe), '0'), 38, 10) is null and 
                    src:nrpe is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ochn), '0'), 38, 10) is null and 
                    src:ochn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:peru), '0'), 38, 10) is null and 
                    src:peru is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt), '0'), 38, 10) is null and 
                    src:rsmt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_dwhc), '0'), 38, 10) is null and 
                    src:rsmt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_homc), '0'), 38, 10) is null and 
                    src:rsmt_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_refc), '0'), 38, 10) is null and 
                    src:rsmt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_rpc1), '0'), 38, 10) is null and 
                    src:rsmt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmt_rpc2), '0'), 38, 10) is null and 
                    src:rsmt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)