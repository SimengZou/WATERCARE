CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TIIPD001_ERROR AS SELECT src, 'LN_TIIPD001' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfcp), '0'), 38, 10) is null and 
                    src:bfcp is not null) THEN
                    'bfcp contains a non-numeric value : \'' || AS_VARCHAR(src:bfcp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfep), '0'), 38, 10) is null and 
                    src:bfep is not null) THEN
                    'bfep contains a non-numeric value : \'' || AS_VARCHAR(src:bfep) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfhr), '0'), 38, 10) is null and 
                    src:bfhr is not null) THEN
                    'bfhr contains a non-numeric value : \'' || AS_VARCHAR(src:bfhr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bomu), '0'), 38, 10) is null and 
                    src:bomu is not null) THEN
                    'bomu contains a non-numeric value : \'' || AS_VARCHAR(src:bomu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cick), '0'), 38, 10) is null and 
                    src:cick is not null) THEN
                    'cick contains a non-numeric value : \'' || AS_VARCHAR(src:cick) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coac), '0'), 38, 10) is null and 
                    src:coac is not null) THEN
                    'coac contains a non-numeric value : \'' || AS_VARCHAR(src:coac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpha), '0'), 38, 10) is null and 
                    src:cpha is not null) THEN
                    'cpha contains a non-numeric value : \'' || AS_VARCHAR(src:cpha) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:drin), '0'), 38, 10) is null and 
                    src:drin is not null) THEN
                    'drin contains a non-numeric value : \'' || AS_VARCHAR(src:drin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dris), '0'), 38, 10) is null and 
                    src:dris is not null) THEN
                    'dris contains a non-numeric value : \'' || AS_VARCHAR(src:dris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iima), '0'), 38, 10) is null and 
                    src:iima is not null) THEN
                    'iima contains a non-numeric value : \'' || AS_VARCHAR(src:iima) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iimf), '0'), 38, 10) is null and 
                    src:iimf is not null) THEN
                    'iimf contains a non-numeric value : \'' || AS_VARCHAR(src:iimf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iuma), '0'), 38, 10) is null and 
                    src:iuma is not null) THEN
                    'iuma contains a non-numeric value : \'' || AS_VARCHAR(src:iuma) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nsfc), '0'), 38, 10) is null and 
                    src:nsfc is not null) THEN
                    'nsfc contains a non-numeric value : \'' || AS_VARCHAR(src:nsfc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltm), '0'), 38, 10) is null and 
                    src:oltm is not null) THEN
                    'oltm contains a non-numeric value : \'' || AS_VARCHAR(src:oltm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltu), '0'), 38, 10) is null and 
                    src:oltu is not null) THEN
                    'oltu contains a non-numeric value : \'' || AS_VARCHAR(src:oltu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oqdr), '0'), 38, 10) is null and 
                    src:oqdr is not null) THEN
                    'oqdr contains a non-numeric value : \'' || AS_VARCHAR(src:oqdr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcrp), '0'), 38, 10) is null and 
                    src:pcrp is not null) THEN
                    'pcrp contains a non-numeric value : \'' || AS_VARCHAR(src:pcrp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phst), '0'), 38, 10) is null and 
                    src:phst is not null) THEN
                    'phst contains a non-numeric value : \'' || AS_VARCHAR(src:phst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:repi), '0'), 38, 10) is null and 
                    src:repi is not null) THEN
                    'repi contains a non-numeric value : \'' || AS_VARCHAR(src:repi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rgrp_ref_compnr), '0'), 38, 10) is null and 
                    src:rgrp_ref_compnr is not null) THEN
                    'rgrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rgrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:runi), '0'), 38, 10) is null and 
                    src:runi is not null) THEN
                    'runi contains a non-numeric value : \'' || AS_VARCHAR(src:runi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scdl_ref_compnr), '0'), 38, 10) is null and 
                    src:scdl_ref_compnr is not null) THEN
                    'scdl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:scdl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpf), '0'), 38, 10) is null and 
                    src:scpf is not null) THEN
                    'scpf contains a non-numeric value : \'' || AS_VARCHAR(src:scpf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq), '0'), 38, 10) is null and 
                    src:scpq is not null) THEN
                    'scpq contains a non-numeric value : \'' || AS_VARCHAR(src:scpq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfpl_ref_compnr), '0'), 38, 10) is null and 
                    src:sfpl_ref_compnr is not null) THEN
                    'sfpl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfpl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smfs), '0'), 38, 10) is null and 
                    src:smfs is not null) THEN
                    'smfs contains a non-numeric value : \'' || AS_VARCHAR(src:smfs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stoi), '0'), 38, 10) is null and 
                    src:stoi is not null) THEN
                    'stoi contains a non-numeric value : \'' || AS_VARCHAR(src:stoi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:swoc), '0'), 38, 10) is null and 
                    src:swoc is not null) THEN
                    'swoc contains a non-numeric value : \'' || AS_VARCHAR(src:swoc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unom), '0'), 38, 10) is null and 
                    src:unom is not null) THEN
                    'unom contains a non-numeric value : \'' || AS_VARCHAR(src:unom) || '\'' WHEN 
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
                src:item  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TIIPD001 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfcp), '0'), 38, 10) is null and 
                    src:bfcp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfep), '0'), 38, 10) is null and 
                    src:bfep is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfhr), '0'), 38, 10) is null and 
                    src:bfhr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bomu), '0'), 38, 10) is null and 
                    src:bomu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cick), '0'), 38, 10) is null and 
                    src:cick is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coac), '0'), 38, 10) is null and 
                    src:coac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpha), '0'), 38, 10) is null and 
                    src:cpha is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:drin), '0'), 38, 10) is null and 
                    src:drin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dris), '0'), 38, 10) is null and 
                    src:dris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iima), '0'), 38, 10) is null and 
                    src:iima is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iimf), '0'), 38, 10) is null and 
                    src:iimf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iuma), '0'), 38, 10) is null and 
                    src:iuma is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nsfc), '0'), 38, 10) is null and 
                    src:nsfc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltm), '0'), 38, 10) is null and 
                    src:oltm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltu), '0'), 38, 10) is null and 
                    src:oltu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oqdr), '0'), 38, 10) is null and 
                    src:oqdr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcrp), '0'), 38, 10) is null and 
                    src:pcrp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:phst), '0'), 38, 10) is null and 
                    src:phst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:repi), '0'), 38, 10) is null and 
                    src:repi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rgrp_ref_compnr), '0'), 38, 10) is null and 
                    src:rgrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:runi), '0'), 38, 10) is null and 
                    src:runi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scdl_ref_compnr), '0'), 38, 10) is null and 
                    src:scdl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpf), '0'), 38, 10) is null and 
                    src:scpf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq), '0'), 38, 10) is null and 
                    src:scpq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfpl_ref_compnr), '0'), 38, 10) is null and 
                    src:sfpl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smfs), '0'), 38, 10) is null and 
                    src:smfs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stoi), '0'), 38, 10) is null and 
                    src:stoi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:swoc), '0'), 38, 10) is null and 
                    src:swoc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unom), '0'), 38, 10) is null and 
                    src:unom is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)