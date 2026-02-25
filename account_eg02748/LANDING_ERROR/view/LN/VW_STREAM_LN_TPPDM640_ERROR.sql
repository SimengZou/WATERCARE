CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPDM640_ERROR AS SELECT src, 'LN_TPPDM640' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) THEN
                    'blbl contains a non-numeric value : \'' || AS_VARCHAR(src:blbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) THEN
                    'ccco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cccp_ref_compnr), '0'), 38, 10) is null and 
                    src:cccp_ref_compnr is not null) THEN
                    'cccp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cccp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccfu), '0'), 38, 10) is null and 
                    src:ccfu is not null) THEN
                    'ccfu contains a non-numeric value : \'' || AS_VARCHAR(src:ccfu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccip_ref_compnr), '0'), 38, 10) is null and 
                    src:ccip_ref_compnr is not null) THEN
                    'ccip_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccip_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccsp_ref_compnr), '0'), 38, 10) is null and 
                    src:ccsp_ref_compnr is not null) THEN
                    'ccsp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccsp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfic_ref_compnr), '0'), 38, 10) is null and 
                    src:dfic_ref_compnr is not null) THEN
                    'dfic_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dfic_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dipr), '0'), 38, 10) is null and 
                    src:dipr is not null) THEN
                    'dipr contains a non-numeric value : \'' || AS_VARCHAR(src:dipr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpri), '0'), 38, 10) is null and 
                    src:dpri is not null) THEN
                    'dpri contains a non-numeric value : \'' || AS_VARCHAR(src:dpri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dspr), '0'), 38, 10) is null and 
                    src:dspr is not null) THEN
                    'dspr contains a non-numeric value : \'' || AS_VARCHAR(src:dspr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltip), '1900-01-01')) is null and 
                    src:ltip is not null) THEN
                    'ltip contains a non-timestamp value : \'' || AS_VARCHAR(src:ltip) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltpr), '1900-01-01')) is null and 
                    src:ltpr is not null) THEN
                    'ltpr contains a non-timestamp value : \'' || AS_VARCHAR(src:ltpr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltsp), '1900-01-01')) is null and 
                    src:ltsp is not null) THEN
                    'ltsp contains a non-timestamp value : \'' || AS_VARCHAR(src:ltsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmc_1), '0'), 38, 10) is null and 
                    src:pcmc_1 is not null) THEN
                    'pcmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:pcmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmc_2), '0'), 38, 10) is null and 
                    src:pcmc_2 is not null) THEN
                    'pcmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:pcmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmc_3), '0'), 38, 10) is null and 
                    src:pcmc_3 is not null) THEN
                    'pcmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:pcmc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pimc_1), '0'), 38, 10) is null and 
                    src:pimc_1 is not null) THEN
                    'pimc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:pimc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pimc_2), '0'), 38, 10) is null and 
                    src:pimc_2 is not null) THEN
                    'pimc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:pimc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pimc_3), '0'), 38, 10) is null and 
                    src:pimc_3 is not null) THEN
                    'pimc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:pimc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prii), '0'), 38, 10) is null and 
                    src:prii is not null) THEN
                    'prii contains a non-numeric value : \'' || AS_VARCHAR(src:prii) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) THEN
                    'pris contains a non-numeric value : \'' || AS_VARCHAR(src:pris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prre), '0'), 38, 10) is null and 
                    src:prre is not null) THEN
                    'prre contains a non-numeric value : \'' || AS_VARCHAR(src:prre) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psmc_1), '0'), 38, 10) is null and 
                    src:psmc_1 is not null) THEN
                    'psmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:psmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psmc_2), '0'), 38, 10) is null and 
                    src:psmc_2 is not null) THEN
                    'psmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:psmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psmc_3), '0'), 38, 10) is null and 
                    src:psmc_3 is not null) THEN
                    'psmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:psmc_3) || '\'' WHEN 
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
                                    
                src:cprj,
                src:cico,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM640 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccco_ref_compnr), '0'), 38, 10) is null and 
                    src:ccco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cccp_ref_compnr), '0'), 38, 10) is null and 
                    src:cccp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccfu), '0'), 38, 10) is null and 
                    src:ccfu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccip_ref_compnr), '0'), 38, 10) is null and 
                    src:ccip_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccsp_ref_compnr), '0'), 38, 10) is null and 
                    src:ccsp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfic_ref_compnr), '0'), 38, 10) is null and 
                    src:dfic_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dipr), '0'), 38, 10) is null and 
                    src:dipr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dpri), '0'), 38, 10) is null and 
                    src:dpri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dspr), '0'), 38, 10) is null and 
                    src:dspr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltip), '1900-01-01')) is null and 
                    src:ltip is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltpr), '1900-01-01')) is null and 
                    src:ltpr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltsp), '1900-01-01')) is null and 
                    src:ltsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmc_1), '0'), 38, 10) is null and 
                    src:pcmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmc_2), '0'), 38, 10) is null and 
                    src:pcmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmc_3), '0'), 38, 10) is null and 
                    src:pcmc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pimc_1), '0'), 38, 10) is null and 
                    src:pimc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pimc_2), '0'), 38, 10) is null and 
                    src:pimc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pimc_3), '0'), 38, 10) is null and 
                    src:pimc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prii), '0'), 38, 10) is null and 
                    src:prii is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prre), '0'), 38, 10) is null and 
                    src:prre is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psmc_1), '0'), 38, 10) is null and 
                    src:psmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psmc_2), '0'), 38, 10) is null and 
                    src:psmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psmc_3), '0'), 38, 10) is null and 
                    src:psmc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)