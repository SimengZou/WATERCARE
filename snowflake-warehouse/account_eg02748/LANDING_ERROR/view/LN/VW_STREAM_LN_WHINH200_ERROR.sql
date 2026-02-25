CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINH200_ERROR AS SELECT src, 'LN_WHINH200' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:adat), '1900-01-01')) is null and 
                    src:adat is not null) THEN
                    'adat contains a non-timestamp value : \'' || AS_VARCHAR(src:adat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:akit_ref_compnr), '0'), 38, 10) is null and 
                    src:akit_ref_compnr is not null) THEN
                    'akit_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:akit_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asst), '0'), 38, 10) is null and 
                    src:asst is not null) THEN
                    'asst contains a non-numeric value : \'' || AS_VARCHAR(src:asst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bflh), '0'), 38, 10) is null and 
                    src:bflh is not null) THEN
                    'bflh contains a non-numeric value : \'' || AS_VARCHAR(src:bflh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blor), '0'), 38, 10) is null and 
                    src:blor is not null) THEN
                    'blor contains a non-numeric value : \'' || AS_VARCHAR(src:blor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:carr_ref_compnr), '0'), 38, 10) is null and 
                    src:carr_ref_compnr is not null) THEN
                    'carr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:carr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbin), '0'), 38, 10) is null and 
                    src:cbin is not null) THEN
                    'cbin contains a non-numeric value : \'' || AS_VARCHAR(src:cbin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) THEN
                    'clan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clgr_ref_compnr), '0'), 38, 10) is null and 
                    src:clgr_ref_compnr is not null) THEN
                    'clgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clgr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cons), '0'), 38, 10) is null and 
                    src:cons is not null) THEN
                    'cons contains a non-numeric value : \'' || AS_VARCHAR(src:cons) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) THEN
                    'crte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ctdt), '1900-01-01')) is null and 
                    src:ctdt is not null) THEN
                    'ctdt contains a non-timestamp value : \'' || AS_VARCHAR(src:ctdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delc_ref_compnr), '0'), 38, 10) is null and 
                    src:delc_ref_compnr is not null) THEN
                    'delc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:delc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depc), '0'), 38, 10) is null and 
                    src:depc is not null) THEN
                    'depc contains a non-numeric value : \'' || AS_VARCHAR(src:depc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depc_ref_compnr), '0'), 38, 10) is null and 
                    src:depc_ref_compnr is not null) THEN
                    'depc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:depc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmst), '0'), 38, 10) is null and 
                    src:dmst is not null) THEN
                    'dmst contains a non-numeric value : \'' || AS_VARCHAR(src:dmst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fqua), '0'), 38, 10) is null and 
                    src:fqua is not null) THEN
                    'fqua contains a non-numeric value : \'' || AS_VARCHAR(src:fqua) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hsta), '0'), 38, 10) is null and 
                    src:hsta is not null) THEN
                    'hsta contains a non-numeric value : \'' || AS_VARCHAR(src:hsta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invc), '0'), 38, 10) is null and 
                    src:invc is not null) THEN
                    'invc contains a non-numeric value : \'' || AS_VARCHAR(src:invc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iscn), '0'), 38, 10) is null and 
                    src:iscn is not null) THEN
                    'iscn contains a non-numeric value : \'' || AS_VARCHAR(src:iscn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isit), '0'), 38, 10) is null and 
                    src:isit is not null) THEN
                    'isit contains a non-numeric value : \'' || AS_VARCHAR(src:isit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ittp), '0'), 38, 10) is null and 
                    src:ittp is not null) THEN
                    'ittp contains a non-numeric value : \'' || AS_VARCHAR(src:ittp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ktor), '0'), 38, 10) is null and 
                    src:ktor is not null) THEN
                    'ktor contains a non-numeric value : \'' || AS_VARCHAR(src:ktor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lccl_ref_compnr), '0'), 38, 10) is null and 
                    src:lccl_ref_compnr is not null) THEN
                    'lccl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lccl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:list_ref_compnr), '0'), 38, 10) is null and 
                    src:list_ref_compnr is not null) THEN
                    'list_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:list_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maxd), '0'), 38, 10) is null and 
                    src:maxd is not null) THEN
                    'maxd contains a non-numeric value : \'' || AS_VARCHAR(src:maxd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maxt), '0'), 38, 10) is null and 
                    src:maxt is not null) THEN
                    'maxt contains a non-numeric value : \'' || AS_VARCHAR(src:maxt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mind), '0'), 38, 10) is null and 
                    src:mind is not null) THEN
                    'mind contains a non-numeric value : \'' || AS_VARCHAR(src:mind) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mint), '0'), 38, 10) is null and 
                    src:mint is not null) THEN
                    'mint contains a non-numeric value : \'' || AS_VARCHAR(src:mint) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:motv_ref_compnr), '0'), 38, 10) is null and 
                    src:motv_ref_compnr is not null) THEN
                    'motv_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:motv_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mrtd), '0'), 38, 10) is null and 
                    src:mrtd is not null) THEN
                    'mrtd contains a non-numeric value : \'' || AS_VARCHAR(src:mrtd) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:odat), '1900-01-01')) is null and 
                    src:odat is not null) THEN
                    'odat contains a non-timestamp value : \'' || AS_VARCHAR(src:odat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oorg), '0'), 38, 10) is null and 
                    src:oorg is not null) THEN
                    'oorg contains a non-numeric value : \'' || AS_VARCHAR(src:oorg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orun_ref_compnr), '0'), 38, 10) is null and 
                    src:orun_ref_compnr is not null) THEN
                    'orun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:orun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oset), '0'), 38, 10) is null and 
                    src:oset is not null) THEN
                    'oset contains a non-numeric value : \'' || AS_VARCHAR(src:oset) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otyp_ref_compnr), '0'), 38, 10) is null and 
                    src:otyp_ref_compnr is not null) THEN
                    'otyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pddt), '1900-01-01')) is null and 
                    src:pddt is not null) THEN
                    'pddt contains a non-timestamp value : \'' || AS_VARCHAR(src:pddt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plbs), '0'), 38, 10) is null and 
                    src:plbs is not null) THEN
                    'plbs contains a non-numeric value : \'' || AS_VARCHAR(src:plbs) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) THEN
                    'prdt contains a non-timestamp value : \'' || AS_VARCHAR(src:prdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qord), '0'), 38, 10) is null and 
                    src:qord is not null) THEN
                    'qord contains a non-numeric value : \'' || AS_VARCHAR(src:qord) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoro), '0'), 38, 10) is null and 
                    src:qoro is not null) THEN
                    'qoro contains a non-numeric value : \'' || AS_VARCHAR(src:qoro) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quar), '0'), 38, 10) is null and 
                    src:quar is not null) THEN
                    'quar contains a non-numeric value : \'' || AS_VARCHAR(src:quar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdgd), '0'), 38, 10) is null and 
                    src:rdgd is not null) THEN
                    'rdgd contains a non-numeric value : \'' || AS_VARCHAR(src:rdgd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrgd), '0'), 38, 10) is null and 
                    src:rrgd is not null) THEN
                    'rrgd contains a non-numeric value : \'' || AS_VARCHAR(src:rrgd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtrn), '0'), 38, 10) is null and 
                    src:rtrn is not null) THEN
                    'rtrn contains a non-numeric value : \'' || AS_VARCHAR(src:rtrn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rttr), '0'), 38, 10) is null and 
                    src:rttr is not null) THEN
                    'rttr contains a non-numeric value : \'' || AS_VARCHAR(src:rttr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) THEN
                    'scon contains a non-numeric value : \'' || AS_VARCHAR(src:scon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serv_ref_compnr), '0'), 38, 10) is null and 
                    src:serv_ref_compnr is not null) THEN
                    'serv_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:serv_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:setn), '0'), 38, 10) is null and 
                    src:setn is not null) THEN
                    'setn contains a non-numeric value : \'' || AS_VARCHAR(src:setn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfcp), '0'), 38, 10) is null and 
                    src:sfcp is not null) THEN
                    'sfcp contains a non-numeric value : \'' || AS_VARCHAR(src:sfcp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfit_ref_compnr), '0'), 38, 10) is null and 
                    src:sfit_ref_compnr is not null) THEN
                    'sfit_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfit_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfrc), '0'), 38, 10) is null and 
                    src:sfrc is not null) THEN
                    'sfrc contains a non-numeric value : \'' || AS_VARCHAR(src:sfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfrc_ref_compnr), '0'), 38, 10) is null and 
                    src:sfrc_ref_compnr is not null) THEN
                    'sfrc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfrc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfro_ref_compnr), '0'), 38, 10) is null and 
                    src:sfro_ref_compnr is not null) THEN
                    'sfro_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfro_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfsi_ref_compnr), '0'), 38, 10) is null and 
                    src:sfsi_ref_compnr is not null) THEN
                    'sfsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfty), '0'), 38, 10) is null and 
                    src:sfty is not null) THEN
                    'sfty contains a non-numeric value : \'' || AS_VARCHAR(src:sfty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcp), '0'), 38, 10) is null and 
                    src:stcp is not null) THEN
                    'stcp contains a non-numeric value : \'' || AS_VARCHAR(src:stcp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stit_ref_compnr), '0'), 38, 10) is null and 
                    src:stit_ref_compnr is not null) THEN
                    'stit_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stit_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:strc), '0'), 38, 10) is null and 
                    src:strc is not null) THEN
                    'strc contains a non-numeric value : \'' || AS_VARCHAR(src:strc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:strc_ref_compnr), '0'), 38, 10) is null and 
                    src:strc_ref_compnr is not null) THEN
                    'strc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:strc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stro_ref_compnr), '0'), 38, 10) is null and 
                    src:stro_ref_compnr is not null) THEN
                    'stro_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stro_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:stsi_ref_compnr is not null) THEN
                    'stsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stty), '0'), 38, 10) is null and 
                    src:stty is not null) THEN
                    'stty contains a non-numeric value : \'' || AS_VARCHAR(src:stty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) THEN
                    'subc contains a non-numeric value : \'' || AS_VARCHAR(src:subc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) THEN
                    'txtb contains a non-numeric value : \'' || AS_VARCHAR(src:txtb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) THEN
                    'txtb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtk), '0'), 38, 10) is null and 
                    src:txtk is not null) THEN
                    'txtk contains a non-numeric value : \'' || AS_VARCHAR(src:txtk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtk_ref_compnr), '0'), 38, 10) is null and 
                    src:txtk_ref_compnr is not null) THEN
                    'txtk_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtk_ref_compnr) || '\'' WHEN 
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
                                    
                src:oorg,
                src:oset,
                src:orno,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINH200 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:adat), '1900-01-01')) is null and 
                    src:adat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:akit_ref_compnr), '0'), 38, 10) is null and 
                    src:akit_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asst), '0'), 38, 10) is null and 
                    src:asst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bflh), '0'), 38, 10) is null and 
                    src:bflh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blor), '0'), 38, 10) is null and 
                    src:blor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:carr_ref_compnr), '0'), 38, 10) is null and 
                    src:carr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbin), '0'), 38, 10) is null and 
                    src:cbin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clgr_ref_compnr), '0'), 38, 10) is null and 
                    src:clgr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cons), '0'), 38, 10) is null and 
                    src:cons is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crte_ref_compnr), '0'), 38, 10) is null and 
                    src:crte_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ctdt), '1900-01-01')) is null and 
                    src:ctdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delc_ref_compnr), '0'), 38, 10) is null and 
                    src:delc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depc), '0'), 38, 10) is null and 
                    src:depc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depc_ref_compnr), '0'), 38, 10) is null and 
                    src:depc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmst), '0'), 38, 10) is null and 
                    src:dmst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fqua), '0'), 38, 10) is null and 
                    src:fqua is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hsta), '0'), 38, 10) is null and 
                    src:hsta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:invc), '0'), 38, 10) is null and 
                    src:invc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iscn), '0'), 38, 10) is null and 
                    src:iscn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:isit), '0'), 38, 10) is null and 
                    src:isit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ittp), '0'), 38, 10) is null and 
                    src:ittp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ktor), '0'), 38, 10) is null and 
                    src:ktor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lccl_ref_compnr), '0'), 38, 10) is null and 
                    src:lccl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:list_ref_compnr), '0'), 38, 10) is null and 
                    src:list_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maxd), '0'), 38, 10) is null and 
                    src:maxd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maxt), '0'), 38, 10) is null and 
                    src:maxt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mind), '0'), 38, 10) is null and 
                    src:mind is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mint), '0'), 38, 10) is null and 
                    src:mint is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:motv_ref_compnr), '0'), 38, 10) is null and 
                    src:motv_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mrtd), '0'), 38, 10) is null and 
                    src:mrtd is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:odat), '1900-01-01')) is null and 
                    src:odat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oorg), '0'), 38, 10) is null and 
                    src:oorg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orun_ref_compnr), '0'), 38, 10) is null and 
                    src:orun_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oset), '0'), 38, 10) is null and 
                    src:oset is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otyp_ref_compnr), '0'), 38, 10) is null and 
                    src:otyp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pddt), '1900-01-01')) is null and 
                    src:pddt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plbs), '0'), 38, 10) is null and 
                    src:plbs is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:prdt), '1900-01-01')) is null and 
                    src:prdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qord), '0'), 38, 10) is null and 
                    src:qord is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoro), '0'), 38, 10) is null and 
                    src:qoro is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:quar), '0'), 38, 10) is null and 
                    src:quar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdgd), '0'), 38, 10) is null and 
                    src:rdgd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrgd), '0'), 38, 10) is null and 
                    src:rrgd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtrn), '0'), 38, 10) is null and 
                    src:rtrn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rttr), '0'), 38, 10) is null and 
                    src:rttr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serv_ref_compnr), '0'), 38, 10) is null and 
                    src:serv_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:setn), '0'), 38, 10) is null and 
                    src:setn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfcp), '0'), 38, 10) is null and 
                    src:sfcp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfit_ref_compnr), '0'), 38, 10) is null and 
                    src:sfit_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfrc), '0'), 38, 10) is null and 
                    src:sfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfrc_ref_compnr), '0'), 38, 10) is null and 
                    src:sfrc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfro_ref_compnr), '0'), 38, 10) is null and 
                    src:sfro_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfsi_ref_compnr), '0'), 38, 10) is null and 
                    src:sfsi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfty), '0'), 38, 10) is null and 
                    src:sfty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcp), '0'), 38, 10) is null and 
                    src:stcp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stit_ref_compnr), '0'), 38, 10) is null and 
                    src:stit_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:strc), '0'), 38, 10) is null and 
                    src:strc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:strc_ref_compnr), '0'), 38, 10) is null and 
                    src:strc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stro_ref_compnr), '0'), 38, 10) is null and 
                    src:stro_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsi_ref_compnr), '0'), 38, 10) is null and 
                    src:stsi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stty), '0'), 38, 10) is null and 
                    src:stty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtk), '0'), 38, 10) is null and 
                    src:txtk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtk_ref_compnr), '0'), 38, 10) is null and 
                    src:txtk_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)