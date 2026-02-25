CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCEMM170_ERROR AS SELECT src, 'LN_TCEMM170' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) THEN
                    'ccal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clcu_ref_compnr), '0'), 38, 10) is null and 
                    src:clcu_ref_compnr is not null) THEN
                    'clcu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clcu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp), '0'), 38, 10) is null and 
                    src:comp is not null) THEN
                    'comp contains a non-numeric value : \'' || AS_VARCHAR(src:comp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csys), '0'), 38, 10) is null and 
                    src:csys is not null) THEN
                    'csys contains a non-numeric value : \'' || AS_VARCHAR(src:csys) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctya), '0'), 38, 10) is null and 
                    src:ctya is not null) THEN
                    'ctya contains a non-numeric value : \'' || AS_VARCHAR(src:ctya) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyb), '0'), 38, 10) is null and 
                    src:ctyb is not null) THEN
                    'ctyb contains a non-numeric value : \'' || AS_VARCHAR(src:ctyb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyc), '0'), 38, 10) is null and 
                    src:ctyc is not null) THEN
                    'ctyc contains a non-numeric value : \'' || AS_VARCHAR(src:ctyc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp), '0'), 38, 10) is null and 
                    src:ctyp is not null) THEN
                    'ctyp contains a non-numeric value : \'' || AS_VARCHAR(src:ctyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erub_ref_compnr), '0'), 38, 10) is null and 
                    src:erub_ref_compnr is not null) THEN
                    'erub_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:erub_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eruc_ref_compnr), '0'), 38, 10) is null and 
                    src:eruc_ref_compnr is not null) THEN
                    'eruc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:eruc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:euro_ref_compnr), '0'), 38, 10) is null and 
                    src:euro_ref_compnr is not null) THEN
                    'euro_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:euro_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcua_ref_compnr), '0'), 38, 10) is null and 
                    src:fcua_ref_compnr is not null) THEN
                    'fcua_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fcua_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcub_ref_compnr), '0'), 38, 10) is null and 
                    src:fcub_ref_compnr is not null) THEN
                    'fcub_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fcub_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcuc_ref_compnr), '0'), 38, 10) is null and 
                    src:fcuc_ref_compnr is not null) THEN
                    'fcuc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fcuc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcur_ref_compnr), '0'), 38, 10) is null and 
                    src:lcur_ref_compnr is not null) THEN
                    'lcur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ract_ref_compnr), '0'), 38, 10) is null and 
                    src:ract_ref_compnr is not null) THEN
                    'ract_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ract_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdub), '0'), 38, 10) is null and 
                    src:rdub is not null) THEN
                    'rdub contains a non-numeric value : \'' || AS_VARCHAR(src:rdub) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rduc), '0'), 38, 10) is null and 
                    src:rduc is not null) THEN
                    'rduc contains a non-numeric value : \'' || AS_VARCHAR(src:rduc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:taxo_rcmp), '0'), 38, 10) is null and 
                    src:taxo_rcmp is not null) THEN
                    'taxo_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:taxo_rcmp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmub), '0'), 38, 10) is null and 
                    src:tmub is not null) THEN
                    'tmub contains a non-numeric value : \'' || AS_VARCHAR(src:tmub) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmuc), '0'), 38, 10) is null and 
                    src:tmuc is not null) THEN
                    'tmuc contains a non-numeric value : \'' || AS_VARCHAR(src:tmuc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tzid_ref_compnr), '0'), 38, 10) is null and 
                    src:tzid_ref_compnr is not null) THEN
                    'tzid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tzid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:umfc), '0'), 38, 10) is null and 
                    src:umfc is not null) THEN
                    'umfc contains a non-numeric value : \'' || AS_VARCHAR(src:umfc) || '\'' WHEN 
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
                src:comp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCEMM170 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clcu_ref_compnr), '0'), 38, 10) is null and 
                    src:clcu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:comp), '0'), 38, 10) is null and 
                    src:comp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csys), '0'), 38, 10) is null and 
                    src:csys is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctya), '0'), 38, 10) is null and 
                    src:ctya is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyb), '0'), 38, 10) is null and 
                    src:ctyb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyc), '0'), 38, 10) is null and 
                    src:ctyc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctyp), '0'), 38, 10) is null and 
                    src:ctyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erub_ref_compnr), '0'), 38, 10) is null and 
                    src:erub_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eruc_ref_compnr), '0'), 38, 10) is null and 
                    src:eruc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:euro_ref_compnr), '0'), 38, 10) is null and 
                    src:euro_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcua_ref_compnr), '0'), 38, 10) is null and 
                    src:fcua_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcub_ref_compnr), '0'), 38, 10) is null and 
                    src:fcub_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcuc_ref_compnr), '0'), 38, 10) is null and 
                    src:fcuc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcur_ref_compnr), '0'), 38, 10) is null and 
                    src:lcur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ract_ref_compnr), '0'), 38, 10) is null and 
                    src:ract_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdub), '0'), 38, 10) is null and 
                    src:rdub is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rduc), '0'), 38, 10) is null and 
                    src:rduc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:taxo_rcmp), '0'), 38, 10) is null and 
                    src:taxo_rcmp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmub), '0'), 38, 10) is null and 
                    src:tmub is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tmuc), '0'), 38, 10) is null and 
                    src:tmuc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tzid_ref_compnr), '0'), 38, 10) is null and 
                    src:tzid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:umfc), '0'), 38, 10) is null and 
                    src:umfc is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)