CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TIPCS030_ERROR AS SELECT src, 'LN_TIPCS030' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:acdt), '1900-01-01')) is null and 
                    src:acdt is not null) THEN
                    'acdt contains a non-timestamp value : \'' || AS_VARCHAR(src:acdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aman), '0'), 38, 10) is null and 
                    src:aman is not null) THEN
                    'aman contains a non-numeric value : \'' || AS_VARCHAR(src:aman) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amch), '0'), 38, 10) is null and 
                    src:amch is not null) THEN
                    'amch contains a non-numeric value : \'' || AS_VARCHAR(src:amch) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascp), '0'), 38, 10) is null and 
                    src:ascp is not null) THEN
                    'ascp contains a non-numeric value : \'' || AS_VARCHAR(src:ascp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bkcs), '0'), 38, 10) is null and 
                    src:bkcs is not null) THEN
                    'bkcs contains a non-numeric value : \'' || AS_VARCHAR(src:bkcs) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:bkdt), '1900-01-01')) is null and 
                    src:bkdt is not null) THEN
                    'bkdt contains a non-timestamp value : \'' || AS_VARCHAR(src:bkdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbdg_ref_compnr), '0'), 38, 10) is null and 
                    src:cbdg_ref_compnr is not null) THEN
                    'cbdg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbdg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cldt), '1900-01-01')) is null and 
                    src:cldt is not null) THEN
                    'cldt contains a non-timestamp value : \'' || AS_VARCHAR(src:cldt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crfc), '0'), 38, 10) is null and 
                    src:crfc is not null) THEN
                    'crfc contains a non-numeric value : \'' || AS_VARCHAR(src:crfc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddat), '1900-01-01')) is null and 
                    src:ddat is not null) THEN
                    'ddat contains a non-timestamp value : \'' || AS_VARCHAR(src:ddat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:defc), '0'), 38, 10) is null and 
                    src:defc is not null) THEN
                    'defc contains a non-numeric value : \'' || AS_VARCHAR(src:defc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dstr), '0'), 38, 10) is null and 
                    src:dstr is not null) THEN
                    'dstr contains a non-numeric value : \'' || AS_VARCHAR(src:dstr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ecpa), '0'), 38, 10) is null and 
                    src:ecpa is not null) THEN
                    'ecpa contains a non-numeric value : \'' || AS_VARCHAR(src:ecpa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ecpr), '0'), 38, 10) is null and 
                    src:ecpr is not null) THEN
                    'ecpr contains a non-numeric value : \'' || AS_VARCHAR(src:ecpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erev), '0'), 38, 10) is null and 
                    src:erev is not null) THEN
                    'erev contains a non-numeric value : \'' || AS_VARCHAR(src:erev) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incp_ref_compnr), '0'), 38, 10) is null and 
                    src:incp_ref_compnr is not null) THEN
                    'incp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:incp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intc), '0'), 38, 10) is null and 
                    src:intc is not null) THEN
                    'intc contains a non-numeric value : \'' || AS_VARCHAR(src:intc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intp), '0'), 38, 10) is null and 
                    src:intp is not null) THEN
                    'intp contains a non-numeric value : \'' || AS_VARCHAR(src:intp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mib4), '0'), 38, 10) is null and 
                    src:mib4 is not null) THEN
                    'mib4 contains a non-numeric value : \'' || AS_VARCHAR(src:mib4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncac), '0'), 38, 10) is null and 
                    src:ncac is not null) THEN
                    'ncac contains a non-numeric value : \'' || AS_VARCHAR(src:ncac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otac), '0'), 38, 10) is null and 
                    src:otac is not null) THEN
                    'otac contains a non-numeric value : \'' || AS_VARCHAR(src:otac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plcd), '0'), 38, 10) is null and 
                    src:plcd is not null) THEN
                    'plcd contains a non-numeric value : \'' || AS_VARCHAR(src:plcd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pocm), '0'), 38, 10) is null and 
                    src:pocm is not null) THEN
                    'pocm contains a non-numeric value : \'' || AS_VARCHAR(src:pocm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psta_ref_compnr), '0'), 38, 10) is null and 
                    src:psta_ref_compnr is not null) THEN
                    'psta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:psta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psts), '0'), 38, 10) is null and 
                    src:psts is not null) THEN
                    'psts contains a non-numeric value : \'' || AS_VARCHAR(src:psts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtp), '0'), 38, 10) is null and 
                    src:rrtp is not null) THEN
                    'rrtp contains a non-numeric value : \'' || AS_VARCHAR(src:rrtp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) THEN
                    'sdat contains a non-timestamp value : \'' || AS_VARCHAR(src:sdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:soco), '0'), 38, 10) is null and 
                    src:soco is not null) THEN
                    'soco contains a non-numeric value : \'' || AS_VARCHAR(src:soco) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tinv_1), '0'), 38, 10) is null and 
                    src:tinv_1 is not null) THEN
                    'tinv_1 contains a non-numeric value : \'' || AS_VARCHAR(src:tinv_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tinv_2), '0'), 38, 10) is null and 
                    src:tinv_2 is not null) THEN
                    'tinv_2 contains a non-numeric value : \'' || AS_VARCHAR(src:tinv_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tinv_3), '0'), 38, 10) is null and 
                    src:tinv_3 is not null) THEN
                    'tinv_3 contains a non-numeric value : \'' || AS_VARCHAR(src:tinv_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpsr), '0'), 38, 10) is null and 
                    src:tpsr is not null) THEN
                    'tpsr contains a non-numeric value : \'' || AS_VARCHAR(src:tpsr) || '\'' WHEN 
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
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TIPCS030 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:acdt), '1900-01-01')) is null and 
                    src:acdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aman), '0'), 38, 10) is null and 
                    src:aman is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amch), '0'), 38, 10) is null and 
                    src:amch is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascp), '0'), 38, 10) is null and 
                    src:ascp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bkcs), '0'), 38, 10) is null and 
                    src:bkcs is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:bkdt), '1900-01-01')) is null and 
                    src:bkdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbdg_ref_compnr), '0'), 38, 10) is null and 
                    src:cbdg_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cldt), '1900-01-01')) is null and 
                    src:cldt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crfc), '0'), 38, 10) is null and 
                    src:crfc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddat), '1900-01-01')) is null and 
                    src:ddat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:defc), '0'), 38, 10) is null and 
                    src:defc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dstr), '0'), 38, 10) is null and 
                    src:dstr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ecpa), '0'), 38, 10) is null and 
                    src:ecpa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ecpr), '0'), 38, 10) is null and 
                    src:ecpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erev), '0'), 38, 10) is null and 
                    src:erev is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incp_ref_compnr), '0'), 38, 10) is null and 
                    src:incp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intc), '0'), 38, 10) is null and 
                    src:intc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intp), '0'), 38, 10) is null and 
                    src:intp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mib4), '0'), 38, 10) is null and 
                    src:mib4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncac), '0'), 38, 10) is null and 
                    src:ncac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otac), '0'), 38, 10) is null and 
                    src:otac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plcd), '0'), 38, 10) is null and 
                    src:plcd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pocm), '0'), 38, 10) is null and 
                    src:pocm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psta_ref_compnr), '0'), 38, 10) is null and 
                    src:psta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psts), '0'), 38, 10) is null and 
                    src:psts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrtp), '0'), 38, 10) is null and 
                    src:rrtp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:soco), '0'), 38, 10) is null and 
                    src:soco is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tinv_1), '0'), 38, 10) is null and 
                    src:tinv_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tinv_2), '0'), 38, 10) is null and 
                    src:tinv_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tinv_3), '0'), 38, 10) is null and 
                    src:tinv_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpsr), '0'), 38, 10) is null and 
                    src:tpsr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)