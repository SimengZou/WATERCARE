CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPTC300_ERROR AS SELECT src, 'LN_TPPTC300' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actl), '0'), 38, 10) is null and 
                    src:actl is not null) THEN
                    'actl contains a non-numeric value : \'' || AS_VARCHAR(src:actl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bdtp), '0'), 38, 10) is null and 
                    src:bdtp is not null) THEN
                    'bdtp contains a non-numeric value : \'' || AS_VARCHAR(src:bdtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cexc_ref_compnr), '0'), 38, 10) is null and 
                    src:cexc_ref_compnr is not null) THEN
                    'cexc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cexc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) THEN
                    'cprj_cpla_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:defn), '0'), 38, 10) is null and 
                    src:defn is not null) THEN
                    'defn contains a non-numeric value : \'' || AS_VARCHAR(src:defn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) THEN
                    'exdt contains a non-timestamp value : \'' || AS_VARCHAR(src:exdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:free), '0'), 38, 10) is null and 
                    src:free is not null) THEN
                    'free contains a non-numeric value : \'' || AS_VARCHAR(src:free) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ibud), '0'), 38, 10) is null and 
                    src:ibud is not null) THEN
                    'ibud contains a non-numeric value : \'' || AS_VARCHAR(src:ibud) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icon), '0'), 38, 10) is null and 
                    src:icon is not null) THEN
                    'icon contains a non-numeric value : \'' || AS_VARCHAR(src:icon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iexc), '0'), 38, 10) is null and 
                    src:iexc is not null) THEN
                    'iexc contains a non-numeric value : \'' || AS_VARCHAR(src:iexc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lcdt), '1900-01-01')) is null and 
                    src:lcdt is not null) THEN
                    'lcdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lcdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upmd), '0'), 38, 10) is null and 
                    src:upmd is not null) THEN
                    'upmd contains a non-numeric value : \'' || AS_VARCHAR(src:upmd) || '\'' WHEN 
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
                src:ccal,
                src:cprj  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPTC300 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actl), '0'), 38, 10) is null and 
                    src:actl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bdtp), '0'), 38, 10) is null and 
                    src:bdtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cexc_ref_compnr), '0'), 38, 10) is null and 
                    src:cexc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:defn), '0'), 38, 10) is null and 
                    src:defn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:free), '0'), 38, 10) is null and 
                    src:free is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ibud), '0'), 38, 10) is null and 
                    src:ibud is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icon), '0'), 38, 10) is null and 
                    src:icon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iexc), '0'), 38, 10) is null and 
                    src:iexc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lcdt), '1900-01-01')) is null and 
                    src:lcdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upmd), '0'), 38, 10) is null and 
                    src:upmd is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)