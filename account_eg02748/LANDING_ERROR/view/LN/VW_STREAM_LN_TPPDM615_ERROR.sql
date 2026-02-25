CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPDM615_ERROR AS SELECT src, 'LN_TPPDM615' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) THEN
                    'blbl contains a non-numeric value : \'' || AS_VARCHAR(src:blbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) THEN
                    'ccco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cccr_ref_compnr), '0'), 38, 10) is null and 
                    src:cccr_ref_compnr is not null) THEN
                    'cccr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cccr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccfu), '0'), 38, 10) is null and 
                    src:ccfu is not null) THEN
                    'ccfu contains a non-numeric value : \'' || AS_VARCHAR(src:ccfu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccir_ref_compnr), '0'), 38, 10) is null and 
                    src:ccir_ref_compnr is not null) THEN
                    'ccir_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccir_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccsr_ref_compnr), '0'), 38, 10) is null and 
                    src:ccsr_ref_compnr is not null) THEN
                    'ccsr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccsr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctrg_ref_compnr), '0'), 38, 10) is null and 
                    src:ctrg_ref_compnr is not null) THEN
                    'ctrg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctrg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfrc), '0'), 38, 10) is null and 
                    src:dfrc is not null) THEN
                    'dfrc contains a non-numeric value : \'' || AS_VARCHAR(src:dfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfri), '0'), 38, 10) is null and 
                    src:dfri is not null) THEN
                    'dfri contains a non-numeric value : \'' || AS_VARCHAR(src:dfri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfrs), '0'), 38, 10) is null and 
                    src:dfrs is not null) THEN
                    'dfrs contains a non-numeric value : \'' || AS_VARCHAR(src:dfrs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfts_ref_compnr), '0'), 38, 10) is null and 
                    src:dfts_ref_compnr is not null) THEN
                    'dfts_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dfts_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eplf), '0'), 38, 10) is null and 
                    src:eplf is not null) THEN
                    'eplf contains a non-numeric value : \'' || AS_VARCHAR(src:eplf) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltrc), '1900-01-01')) is null and 
                    src:ltrc is not null) THEN
                    'ltrc contains a non-timestamp value : \'' || AS_VARCHAR(src:ltrc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltri), '1900-01-01')) is null and 
                    src:ltri is not null) THEN
                    'ltri contains a non-timestamp value : \'' || AS_VARCHAR(src:ltri) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltrs), '1900-01-01')) is null and 
                    src:ltrs is not null) THEN
                    'ltrs contains a non-timestamp value : \'' || AS_VARCHAR(src:ltrs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:norm), '0'), 38, 10) is null and 
                    src:norm is not null) THEN
                    'norm contains a non-numeric value : \'' || AS_VARCHAR(src:norm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prre), '0'), 38, 10) is null and 
                    src:prre is not null) THEN
                    'prre contains a non-numeric value : \'' || AS_VARCHAR(src:prre) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc), '0'), 38, 10) is null and 
                    src:ratc is not null) THEN
                    'ratc contains a non-numeric value : \'' || AS_VARCHAR(src:ratc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rati), '0'), 38, 10) is null and 
                    src:rati is not null) THEN
                    'rati contains a non-numeric value : \'' || AS_VARCHAR(src:rati) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats), '0'), 38, 10) is null and 
                    src:rats is not null) THEN
                    'rats contains a non-numeric value : \'' || AS_VARCHAR(src:rats) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_1), '0'), 38, 10) is null and 
                    src:rcmc_1 is not null) THEN
                    'rcmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rcmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_2), '0'), 38, 10) is null and 
                    src:rcmc_2 is not null) THEN
                    'rcmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rcmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_3), '0'), 38, 10) is null and 
                    src:rcmc_3 is not null) THEN
                    'rcmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rcmc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rimc_1), '0'), 38, 10) is null and 
                    src:rimc_1 is not null) THEN
                    'rimc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rimc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rimc_2), '0'), 38, 10) is null and 
                    src:rimc_2 is not null) THEN
                    'rimc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rimc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rimc_3), '0'), 38, 10) is null and 
                    src:rimc_3 is not null) THEN
                    'rimc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rimc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_1), '0'), 38, 10) is null and 
                    src:rsmc_1 is not null) THEN
                    'rsmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rsmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_2), '0'), 38, 10) is null and 
                    src:rsmc_2 is not null) THEN
                    'rsmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rsmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_3), '0'), 38, 10) is null and 
                    src:rsmc_3 is not null) THEN
                    'rsmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rsmc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unrt_ref_compnr), '0'), 38, 10) is null and 
                    src:unrt_ref_compnr is not null) THEN
                    'unrt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:unrt_ref_compnr) || '\'' WHEN 
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
                                    
                src:task,
                src:compnr,
                src:cprj  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM615 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cccr_ref_compnr), '0'), 38, 10) is null and 
                    src:cccr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccfu), '0'), 38, 10) is null and 
                    src:ccfu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccir_ref_compnr), '0'), 38, 10) is null and 
                    src:ccir_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccsr_ref_compnr), '0'), 38, 10) is null and 
                    src:ccsr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctrg_ref_compnr), '0'), 38, 10) is null and 
                    src:ctrg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfrc), '0'), 38, 10) is null and 
                    src:dfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfri), '0'), 38, 10) is null and 
                    src:dfri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfrs), '0'), 38, 10) is null and 
                    src:dfrs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfts_ref_compnr), '0'), 38, 10) is null and 
                    src:dfts_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eplf), '0'), 38, 10) is null and 
                    src:eplf is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltrc), '1900-01-01')) is null and 
                    src:ltrc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltri), '1900-01-01')) is null and 
                    src:ltri is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltrs), '1900-01-01')) is null and 
                    src:ltrs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:norm), '0'), 38, 10) is null and 
                    src:norm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prre), '0'), 38, 10) is null and 
                    src:prre is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc), '0'), 38, 10) is null and 
                    src:ratc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rati), '0'), 38, 10) is null and 
                    src:rati is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rats), '0'), 38, 10) is null and 
                    src:rats is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_1), '0'), 38, 10) is null and 
                    src:rcmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_2), '0'), 38, 10) is null and 
                    src:rcmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmc_3), '0'), 38, 10) is null and 
                    src:rcmc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rimc_1), '0'), 38, 10) is null and 
                    src:rimc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rimc_2), '0'), 38, 10) is null and 
                    src:rimc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rimc_3), '0'), 38, 10) is null and 
                    src:rimc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_1), '0'), 38, 10) is null and 
                    src:rsmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_2), '0'), 38, 10) is null and 
                    src:rsmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsmc_3), '0'), 38, 10) is null and 
                    src:rsmc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unrt_ref_compnr), '0'), 38, 10) is null and 
                    src:unrt_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)