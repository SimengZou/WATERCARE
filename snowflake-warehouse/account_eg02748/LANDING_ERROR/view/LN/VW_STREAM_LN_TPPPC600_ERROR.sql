CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPPC600_ERROR AS SELECT src, 'LN_TPPPC600' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) THEN
                    'amnt contains a non-numeric value : \'' || AS_VARCHAR(src:amnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appl), '0'), 38, 10) is null and 
                    src:appl is not null) THEN
                    'appl contains a non-numeric value : \'' || AS_VARCHAR(src:appl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bamt), '0'), 38, 10) is null and 
                    src:bamt is not null) THEN
                    'bamt contains a non-numeric value : \'' || AS_VARCHAR(src:bamt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:base_vrsn_ref_compnr), '0'), 38, 10) is null and 
                    src:base_vrsn_ref_compnr is not null) THEN
                    'base_vrsn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:base_vrsn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bclc), '0'), 38, 10) is null and 
                    src:bclc is not null) THEN
                    'bclc contains a non-numeric value : \'' || AS_VARCHAR(src:bclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bprc), '0'), 38, 10) is null and 
                    src:bprc is not null) THEN
                    'bprc contains a non-numeric value : \'' || AS_VARCHAR(src:bprc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:brte), '0'), 38, 10) is null and 
                    src:brte is not null) THEN
                    'brte contains a non-numeric value : \'' || AS_VARCHAR(src:brte) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btba), '0'), 38, 10) is null and 
                    src:btba is not null) THEN
                    'btba contains a non-numeric value : \'' || AS_VARCHAR(src:btba) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btyp), '0'), 38, 10) is null and 
                    src:btyp is not null) THEN
                    'btyp contains a non-numeric value : \'' || AS_VARCHAR(src:btyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:calc), '0'), 38, 10) is null and 
                    src:calc is not null) THEN
                    'calc contains a non-numeric value : \'' || AS_VARCHAR(src:calc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cldt), '1900-01-01')) is null and 
                    src:cldt is not null) THEN
                    'cldt contains a non-timestamp value : \'' || AS_VARCHAR(src:cldt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp), '0'), 38, 10) is null and 
                    src:cotp is not null) THEN
                    'cotp contains a non-numeric value : \'' || AS_VARCHAR(src:cotp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ctdt), '1900-01-01')) is null and 
                    src:ctdt is not null) THEN
                    'ctdt contains a non-timestamp value : \'' || AS_VARCHAR(src:ctdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hour), '0'), 38, 10) is null and 
                    src:hour is not null) THEN
                    'hour contains a non-numeric value : \'' || AS_VARCHAR(src:hour) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:levl), '0'), 38, 10) is null and 
                    src:levl is not null) THEN
                    'levl contains a non-numeric value : \'' || AS_VARCHAR(src:levl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs), '0'), 38, 10) is null and 
                    src:ohcs is not null) THEN
                    'ohcs contains a non-numeric value : \'' || AS_VARCHAR(src:ohcs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohst), '0'), 38, 10) is null and 
                    src:ohst is not null) THEN
                    'ohst contains a non-numeric value : \'' || AS_VARCHAR(src:ohst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:perc), '0'), 38, 10) is null and 
                    src:perc is not null) THEN
                    'perc contains a non-numeric value : \'' || AS_VARCHAR(src:perc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate), '0'), 38, 10) is null and 
                    src:rate is not null) THEN
                    'rate contains a non-numeric value : \'' || AS_VARCHAR(src:rate) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) THEN
                    'sern contains a non-numeric value : \'' || AS_VARCHAR(src:sern) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tbap), '0'), 38, 10) is null and 
                    src:tbap is not null) THEN
                    'tbap contains a non-numeric value : \'' || AS_VARCHAR(src:tbap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcob_ref_compnr), '0'), 38, 10) is null and 
                    src:tcob_ref_compnr is not null) THEN
                    'tcob_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tcob_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) THEN
                    'trdt contains a non-timestamp value : \'' || AS_VARCHAR(src:trdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vrsn), '0'), 38, 10) is null and 
                    src:vrsn is not null) THEN
                    'vrsn contains a non-numeric value : \'' || AS_VARCHAR(src:vrsn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) THEN
                    'year contains a non-numeric value : \'' || AS_VARCHAR(src:year) || '\'' WHEN 
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
                                    
                src:cotp,
                src:compnr,
                src:user,
                src:coob,
                src:cprj,
                src:base,
                src:cspa,
                src:ohcs,
                src:cact,
                src:sern  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPPC600 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appl), '0'), 38, 10) is null and 
                    src:appl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bamt), '0'), 38, 10) is null and 
                    src:bamt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:base_vrsn_ref_compnr), '0'), 38, 10) is null and 
                    src:base_vrsn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bclc), '0'), 38, 10) is null and 
                    src:bclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bprc), '0'), 38, 10) is null and 
                    src:bprc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:brte), '0'), 38, 10) is null and 
                    src:brte is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btba), '0'), 38, 10) is null and 
                    src:btba is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btyp), '0'), 38, 10) is null and 
                    src:btyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:calc), '0'), 38, 10) is null and 
                    src:calc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cldt), '1900-01-01')) is null and 
                    src:cldt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp), '0'), 38, 10) is null and 
                    src:cotp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ctdt), '1900-01-01')) is null and 
                    src:ctdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hour), '0'), 38, 10) is null and 
                    src:hour is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:levl), '0'), 38, 10) is null and 
                    src:levl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs), '0'), 38, 10) is null and 
                    src:ohcs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohst), '0'), 38, 10) is null and 
                    src:ohst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:perc), '0'), 38, 10) is null and 
                    src:perc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate), '0'), 38, 10) is null and 
                    src:rate is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tbap), '0'), 38, 10) is null and 
                    src:tbap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcob_ref_compnr), '0'), 38, 10) is null and 
                    src:tcob_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vrsn), '0'), 38, 10) is null and 
                    src:vrsn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)