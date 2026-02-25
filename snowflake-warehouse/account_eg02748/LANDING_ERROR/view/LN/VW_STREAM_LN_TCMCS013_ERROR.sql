CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCMCS013_ERROR AS SELECT src, 'LN_TCMCS013' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atie), '0'), 38, 10) is null and 
                    src:atie is not null) THEN
                    'atie contains a non-numeric value : \'' || AS_VARCHAR(src:atie) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdde), '0'), 38, 10) is null and 
                    src:cdde is not null) THEN
                    'cdde contains a non-numeric value : \'' || AS_VARCHAR(src:cdde) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdis), '0'), 38, 10) is null and 
                    src:cdis is not null) THEN
                    'cdis contains a non-numeric value : \'' || AS_VARCHAR(src:cdis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:day1), '0'), 38, 10) is null and 
                    src:day1 is not null) THEN
                    'day1 contains a non-numeric value : \'' || AS_VARCHAR(src:day1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:day2), '0'), 38, 10) is null and 
                    src:day2 is not null) THEN
                    'day2 contains a non-numeric value : \'' || AS_VARCHAR(src:day2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:day3), '0'), 38, 10) is null and 
                    src:day3 is not null) THEN
                    'day3 contains a non-numeric value : \'' || AS_VARCHAR(src:day3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disa), '0'), 38, 10) is null and 
                    src:disa is not null) THEN
                    'disa contains a non-numeric value : \'' || AS_VARCHAR(src:disa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disb), '0'), 38, 10) is null and 
                    src:disb is not null) THEN
                    'disb contains a non-numeric value : \'' || AS_VARCHAR(src:disb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc), '0'), 38, 10) is null and 
                    src:disc is not null) THEN
                    'disc contains a non-numeric value : \'' || AS_VARCHAR(src:disc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtbs), '0'), 38, 10) is null and 
                    src:dtbs is not null) THEN
                    'dtbs contains a non-numeric value : \'' || AS_VARCHAR(src:dtbs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdis), '0'), 38, 10) is null and 
                    src:fdis is not null) THEN
                    'fdis contains a non-numeric value : \'' || AS_VARCHAR(src:fdis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdue), '0'), 38, 10) is null and 
                    src:fdue is not null) THEN
                    'fdue contains a non-numeric value : \'' || AS_VARCHAR(src:fdue) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pash_ref_compnr), '0'), 38, 10) is null and 
                    src:pash_ref_compnr is not null) THEN
                    'pash_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pash_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdin), '0'), 38, 10) is null and 
                    src:pdin is not null) THEN
                    'pdin contains a non-numeric value : \'' || AS_VARCHAR(src:pdin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdis), '0'), 38, 10) is null and 
                    src:pdis is not null) THEN
                    'pdis contains a non-numeric value : \'' || AS_VARCHAR(src:pdis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdyn), '0'), 38, 10) is null and 
                    src:pdyn is not null) THEN
                    'pdyn contains a non-numeric value : \'' || AS_VARCHAR(src:pdyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pper), '0'), 38, 10) is null and 
                    src:pper is not null) THEN
                    'pper contains a non-numeric value : \'' || AS_VARCHAR(src:pper) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prca), '0'), 38, 10) is null and 
                    src:prca is not null) THEN
                    'prca contains a non-numeric value : \'' || AS_VARCHAR(src:prca) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prcb), '0'), 38, 10) is null and 
                    src:prcb is not null) THEN
                    'prcb contains a non-numeric value : \'' || AS_VARCHAR(src:prcb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prcc), '0'), 38, 10) is null and 
                    src:prcc is not null) THEN
                    'prcc contains a non-numeric value : \'' || AS_VARCHAR(src:prcc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio), '0'), 38, 10) is null and 
                    src:prio is not null) THEN
                    'prio contains a non-numeric value : \'' || AS_VARCHAR(src:prio) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyp), '0'), 38, 10) is null and 
                    src:ptyp is not null) THEN
                    'ptyp contains a non-numeric value : \'' || AS_VARCHAR(src:ptyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tlsd), '0'), 38, 10) is null and 
                    src:tlsd is not null) THEN
                    'tlsd contains a non-numeric value : \'' || AS_VARCHAR(src:tlsd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tola), '0'), 38, 10) is null and 
                    src:tola is not null) THEN
                    'tola contains a non-numeric value : \'' || AS_VARCHAR(src:tola) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:told), '0'), 38, 10) is null and 
                    src:told is not null) THEN
                    'told contains a non-numeric value : \'' || AS_VARCHAR(src:told) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tolp), '0'), 38, 10) is null and 
                    src:tolp is not null) THEN
                    'tolp contains a non-numeric value : \'' || AS_VARCHAR(src:tolp) || '\'' WHEN 
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
                                    
                src:compnr,
                src:cpay  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCMCS013 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atie), '0'), 38, 10) is null and 
                    src:atie is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdde), '0'), 38, 10) is null and 
                    src:cdde is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdis), '0'), 38, 10) is null and 
                    src:cdis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:day1), '0'), 38, 10) is null and 
                    src:day1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:day2), '0'), 38, 10) is null and 
                    src:day2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:day3), '0'), 38, 10) is null and 
                    src:day3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disa), '0'), 38, 10) is null and 
                    src:disa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disb), '0'), 38, 10) is null and 
                    src:disb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc), '0'), 38, 10) is null and 
                    src:disc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtbs), '0'), 38, 10) is null and 
                    src:dtbs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdis), '0'), 38, 10) is null and 
                    src:fdis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdue), '0'), 38, 10) is null and 
                    src:fdue is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pash_ref_compnr), '0'), 38, 10) is null and 
                    src:pash_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdin), '0'), 38, 10) is null and 
                    src:pdin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdis), '0'), 38, 10) is null and 
                    src:pdis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdyn), '0'), 38, 10) is null and 
                    src:pdyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pper), '0'), 38, 10) is null and 
                    src:pper is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prca), '0'), 38, 10) is null and 
                    src:prca is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prcb), '0'), 38, 10) is null and 
                    src:prcb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prcc), '0'), 38, 10) is null and 
                    src:prcc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio), '0'), 38, 10) is null and 
                    src:prio is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyp), '0'), 38, 10) is null and 
                    src:ptyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tlsd), '0'), 38, 10) is null and 
                    src:tlsd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tola), '0'), 38, 10) is null and 
                    src:tola is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:told), '0'), 38, 10) is null and 
                    src:told is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tolp), '0'), 38, 10) is null and 
                    src:tolp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)