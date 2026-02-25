CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFAM100_ERROR AS SELECT src, 'LN_TFFAM100' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aacq_1), '0'), 38, 10) is null and 
                    src:aacq_1 is not null) THEN
                    'aacq_1 contains a non-numeric value : \'' || AS_VARCHAR(src:aacq_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aacq_2), '0'), 38, 10) is null and 
                    src:aacq_2 is not null) THEN
                    'aacq_2 contains a non-numeric value : \'' || AS_VARCHAR(src:aacq_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aacq_3), '0'), 38, 10) is null and 
                    src:aacq_3 is not null) THEN
                    'aacq_3 contains a non-numeric value : \'' || AS_VARCHAR(src:aacq_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_1), '0'), 38, 10) is null and 
                    src:acmc_1 is not null) THEN
                    'acmc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:acmc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_2), '0'), 38, 10) is null and 
                    src:acmc_2 is not null) THEN
                    'acmc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:acmc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_3), '0'), 38, 10) is null and 
                    src:acmc_3 is not null) THEN
                    'acmc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:acmc_3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:acmd), '1900-01-01')) is null and 
                    src:acmd is not null) THEN
                    'acmd contains a non-timestamp value : \'' || AS_VARCHAR(src:acmd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:agrp_ref_compnr), '0'), 38, 10) is null and 
                    src:agrp_ref_compnr is not null) THEN
                    'agrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:agrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascd), '0'), 38, 10) is null and 
                    src:ascd is not null) THEN
                    'ascd contains a non-numeric value : \'' || AS_VARCHAR(src:ascd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atbc_1), '0'), 38, 10) is null and 
                    src:atbc_1 is not null) THEN
                    'atbc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:atbc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atbc_2), '0'), 38, 10) is null and 
                    src:atbc_2 is not null) THEN
                    'atbc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:atbc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atbc_3), '0'), 38, 10) is null and 
                    src:atbc_3 is not null) THEN
                    'atbc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:atbc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:auto), '0'), 38, 10) is null and 
                    src:auto is not null) THEN
                    'auto contains a non-numeric value : \'' || AS_VARCHAR(src:auto) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpct), '0'), 38, 10) is null and 
                    src:bpct is not null) THEN
                    'bpct contains a non-numeric value : \'' || AS_VARCHAR(src:bpct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) THEN
                    'bpid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:c263), '0'), 38, 10) is null and 
                    src:c263 is not null) THEN
                    'c263 contains a non-numeric value : \'' || AS_VARCHAR(src:c263) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capi), '0'), 38, 10) is null and 
                    src:capi is not null) THEN
                    'capi contains a non-numeric value : \'' || AS_VARCHAR(src:capi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_atyp_ref_compnr), '0'), 38, 10) is null and 
                    src:cdf_atyp_ref_compnr is not null) THEN
                    'cdf_atyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_atyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_ipst), '0'), 38, 10) is null and 
                    src:cdf_ipst is not null) THEN
                    'cdf_ipst contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_ipst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_utyp_ref_compnr), '0'), 38, 10) is null and 
                    src:cdf_utyp_ref_compnr is not null) THEN
                    'cdf_utyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdf_utyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cola), '0'), 38, 10) is null and 
                    src:cola is not null) THEN
                    'cola contains a non-numeric value : \'' || AS_VARCHAR(src:cola) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp), '0'), 38, 10) is null and 
                    src:comp is not null) THEN
                    'comp contains a non-numeric value : \'' || AS_VARCHAR(src:comp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost), '0'), 38, 10) is null and 
                    src:cost is not null) THEN
                    'cost contains a non-numeric value : \'' || AS_VARCHAR(src:cost) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csth_1), '0'), 38, 10) is null and 
                    src:csth_1 is not null) THEN
                    'csth_1 contains a non-numeric value : \'' || AS_VARCHAR(src:csth_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csth_2), '0'), 38, 10) is null and 
                    src:csth_2 is not null) THEN
                    'csth_2 contains a non-numeric value : \'' || AS_VARCHAR(src:csth_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csth_3), '0'), 38, 10) is null and 
                    src:csth_3 is not null) THEN
                    'csth_3 contains a non-numeric value : \'' || AS_VARCHAR(src:csth_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctgy_ref_compnr), '0'), 38, 10) is null and 
                    src:ctgy_ref_compnr is not null) THEN
                    'ctgy_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctgy_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctgy_sctg_ref_compnr), '0'), 38, 10) is null and 
                    src:ctgy_sctg_ref_compnr is not null) THEN
                    'ctgy_sctg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctgy_sctg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:curr_ref_compnr), '0'), 38, 10) is null and 
                    src:curr_ref_compnr is not null) THEN
                    'curr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:curr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:date), '1900-01-01')) is null and 
                    src:date is not null) THEN
                    'date contains a non-timestamp value : \'' || AS_VARCHAR(src:date) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbfg), '0'), 38, 10) is null and 
                    src:dbfg is not null) THEN
                    'dbfg contains a non-numeric value : \'' || AS_VARCHAR(src:dbfg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itca), '0'), 38, 10) is null and 
                    src:itca is not null) THEN
                    'itca contains a non-numeric value : \'' || AS_VARCHAR(src:itca) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcd), '0'), 38, 10) is null and 
                    src:itcd is not null) THEN
                    'itcd contains a non-numeric value : \'' || AS_VARCHAR(src:itcd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:list), '0'), 38, 10) is null and 
                    src:list is not null) THEN
                    'list contains a non-numeric value : \'' || AS_VARCHAR(src:list) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdd), '0'), 38, 10) is null and 
                    src:ltdd is not null) THEN
                    'ltdd contains a non-numeric value : \'' || AS_VARCHAR(src:ltdd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:newu), '0'), 38, 10) is null and 
                    src:newu is not null) THEN
                    'newu contains a non-numeric value : \'' || AS_VARCHAR(src:newu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oqty), '0'), 38, 10) is null and 
                    src:oqty is not null) THEN
                    'oqty contains a non-numeric value : \'' || AS_VARCHAR(src:oqty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owco), '0'), 38, 10) is null and 
                    src:owco is not null) THEN
                    'owco contains a non-numeric value : \'' || AS_VARCHAR(src:owco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owco_ref_compnr), '0'), 38, 10) is null and 
                    src:owco_ref_compnr is not null) THEN
                    'owco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:owco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ownc), '0'), 38, 10) is null and 
                    src:ownc is not null) THEN
                    'ownc contains a non-numeric value : \'' || AS_VARCHAR(src:ownc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ownd_ref_compnr), '0'), 38, 10) is null and 
                    src:ownd_ref_compnr is not null) THEN
                    'ownd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ownd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prcm), '0'), 38, 10) is null and 
                    src:prcm is not null) THEN
                    'prcm contains a non-numeric value : \'' || AS_VARCHAR(src:prcm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prod), '0'), 38, 10) is null and 
                    src:prod is not null) THEN
                    'prod contains a non-numeric value : \'' || AS_VARCHAR(src:prod) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pudt), '1900-01-01')) is null and 
                    src:pudt is not null) THEN
                    'pudt contains a non-timestamp value : \'' || AS_VARCHAR(src:pudt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramt), '0'), 38, 10) is null and 
                    src:ramt is not null) THEN
                    'ramt contains a non-numeric value : \'' || AS_VARCHAR(src:ramt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) THEN
                    'ratd contains a non-timestamp value : \'' || AS_VARCHAR(src:ratd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_1), '0'), 38, 10) is null and 
                    src:rate_1 is not null) THEN
                    'rate_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rate_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_2), '0'), 38, 10) is null and 
                    src:rate_2 is not null) THEN
                    'rate_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rate_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_3), '0'), 38, 10) is null and 
                    src:rate_3 is not null) THEN
                    'rate_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rate_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) THEN
                    'ratf_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) THEN
                    'ratf_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) THEN
                    'ratf_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratu), '0'), 38, 10) is null and 
                    src:ratu is not null) THEN
                    'ratu contains a non-numeric value : \'' || AS_VARCHAR(src:ratu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmp), '0'), 38, 10) is null and 
                    src:rcmp is not null) THEN
                    'rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:rcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ryer), '0'), 38, 10) is null and 
                    src:ryer is not null) THEN
                    'ryer contains a non-numeric value : \'' || AS_VARCHAR(src:ryer) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179), '0'), 38, 10) is null and 
                    src:s179 is not null) THEN
                    's179 contains a non-numeric value : \'' || AS_VARCHAR(src:s179) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv), '0'), 38, 10) is null and 
                    src:salv is not null) THEN
                    'salv contains a non-numeric value : \'' || AS_VARCHAR(src:salv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shft), '0'), 38, 10) is null and 
                    src:shft is not null) THEN
                    'shft contains a non-numeric value : \'' || AS_VARCHAR(src:shft) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:simu), '0'), 38, 10) is null and 
                    src:simu is not null) THEN
                    'simu contains a non-numeric value : \'' || AS_VARCHAR(src:simu) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:srvc), '1900-01-01')) is null and 
                    src:srvc is not null) THEN
                    'srvc contains a non-timestamp value : \'' || AS_VARCHAR(src:srvc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tqty), '0'), 38, 10) is null and 
                    src:tqty is not null) THEN
                    'tqty contains a non-numeric value : \'' || AS_VARCHAR(src:tqty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trfg), '0'), 38, 10) is null and 
                    src:trfg is not null) THEN
                    'trfg contains a non-numeric value : \'' || AS_VARCHAR(src:trfg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tuam_1), '0'), 38, 10) is null and 
                    src:tuam_1 is not null) THEN
                    'tuam_1 contains a non-numeric value : \'' || AS_VARCHAR(src:tuam_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tuam_2), '0'), 38, 10) is null and 
                    src:tuam_2 is not null) THEN
                    'tuam_2 contains a non-numeric value : \'' || AS_VARCHAR(src:tuam_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tuam_3), '0'), 38, 10) is null and 
                    src:tuam_3 is not null) THEN
                    'tuam_3 contains a non-numeric value : \'' || AS_VARCHAR(src:tuam_3) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucsh), '0'), 38, 10) is null and 
                    src:ucsh is not null) THEN
                    'ucsh contains a non-numeric value : \'' || AS_VARCHAR(src:ucsh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vinb), '0'), 38, 10) is null and 
                    src:vinb is not null) THEN
                    'vinb contains a non-numeric value : \'' || AS_VARCHAR(src:vinb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vint_ref_compnr), '0'), 38, 10) is null and 
                    src:vint_ref_compnr is not null) THEN
                    'vint_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:vint_ref_compnr) || '\'' WHEN 
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
                                    
                src:anbr,
                src:compnr,
                src:aext  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aacq_1), '0'), 38, 10) is null and 
                    src:aacq_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aacq_2), '0'), 38, 10) is null and 
                    src:aacq_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aacq_3), '0'), 38, 10) is null and 
                    src:aacq_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_1), '0'), 38, 10) is null and 
                    src:acmc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_2), '0'), 38, 10) is null and 
                    src:acmc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmc_3), '0'), 38, 10) is null and 
                    src:acmc_3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:acmd), '1900-01-01')) is null and 
                    src:acmd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:agrp_ref_compnr), '0'), 38, 10) is null and 
                    src:agrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascd), '0'), 38, 10) is null and 
                    src:ascd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atbc_1), '0'), 38, 10) is null and 
                    src:atbc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atbc_2), '0'), 38, 10) is null and 
                    src:atbc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atbc_3), '0'), 38, 10) is null and 
                    src:atbc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:auto), '0'), 38, 10) is null and 
                    src:auto is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpct), '0'), 38, 10) is null and 
                    src:bpct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:c263), '0'), 38, 10) is null and 
                    src:c263 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capi), '0'), 38, 10) is null and 
                    src:capi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_atyp_ref_compnr), '0'), 38, 10) is null and 
                    src:cdf_atyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_ipst), '0'), 38, 10) is null and 
                    src:cdf_ipst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdf_utyp_ref_compnr), '0'), 38, 10) is null and 
                    src:cdf_utyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cola), '0'), 38, 10) is null and 
                    src:cola is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp), '0'), 38, 10) is null and 
                    src:comp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cost), '0'), 38, 10) is null and 
                    src:cost is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csth_1), '0'), 38, 10) is null and 
                    src:csth_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csth_2), '0'), 38, 10) is null and 
                    src:csth_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csth_3), '0'), 38, 10) is null and 
                    src:csth_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctgy_ref_compnr), '0'), 38, 10) is null and 
                    src:ctgy_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctgy_sctg_ref_compnr), '0'), 38, 10) is null and 
                    src:ctgy_sctg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:curr_ref_compnr), '0'), 38, 10) is null and 
                    src:curr_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:date), '1900-01-01')) is null and 
                    src:date is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbfg), '0'), 38, 10) is null and 
                    src:dbfg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itca), '0'), 38, 10) is null and 
                    src:itca is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itcd), '0'), 38, 10) is null and 
                    src:itcd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:list), '0'), 38, 10) is null and 
                    src:list is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltdd), '0'), 38, 10) is null and 
                    src:ltdd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:newu), '0'), 38, 10) is null and 
                    src:newu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oqty), '0'), 38, 10) is null and 
                    src:oqty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owco), '0'), 38, 10) is null and 
                    src:owco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owco_ref_compnr), '0'), 38, 10) is null and 
                    src:owco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ownc), '0'), 38, 10) is null and 
                    src:ownc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ownd_ref_compnr), '0'), 38, 10) is null and 
                    src:ownd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prcm), '0'), 38, 10) is null and 
                    src:prcm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prod), '0'), 38, 10) is null and 
                    src:prod is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pudt), '1900-01-01')) is null and 
                    src:pudt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ramt), '0'), 38, 10) is null and 
                    src:ramt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_1), '0'), 38, 10) is null and 
                    src:rate_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_2), '0'), 38, 10) is null and 
                    src:rate_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_3), '0'), 38, 10) is null and 
                    src:rate_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratu), '0'), 38, 10) is null and 
                    src:ratu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcmp), '0'), 38, 10) is null and 
                    src:rcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ryer), '0'), 38, 10) is null and 
                    src:ryer is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:s179), '0'), 38, 10) is null and 
                    src:s179 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:salv), '0'), 38, 10) is null and 
                    src:salv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shft), '0'), 38, 10) is null and 
                    src:shft is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:simu), '0'), 38, 10) is null and 
                    src:simu is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:srvc), '1900-01-01')) is null and 
                    src:srvc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tqty), '0'), 38, 10) is null and 
                    src:tqty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trfg), '0'), 38, 10) is null and 
                    src:trfg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tuam_1), '0'), 38, 10) is null and 
                    src:tuam_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tuam_2), '0'), 38, 10) is null and 
                    src:tuam_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tuam_3), '0'), 38, 10) is null and 
                    src:tuam_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucsh), '0'), 38, 10) is null and 
                    src:ucsh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vinb), '0'), 38, 10) is null and 
                    src:vinb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vint_ref_compnr), '0'), 38, 10) is null and 
                    src:vint_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)