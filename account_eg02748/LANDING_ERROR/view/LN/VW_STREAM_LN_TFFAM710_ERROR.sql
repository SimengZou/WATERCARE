CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFAM710_ERROR AS SELECT src, 'LN_TFFAM710' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acnp), '0'), 38, 10) is null and 
                    src:acnp is not null) THEN
                    'acnp contains a non-numeric value : \'' || AS_VARCHAR(src:acnp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bdad), '0'), 38, 10) is null and 
                    src:bdad is not null) THEN
                    'bdad contains a non-numeric value : \'' || AS_VARCHAR(src:bdad) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bnbv), '0'), 38, 10) is null and 
                    src:bnbv is not null) THEN
                    'bnbv contains a non-numeric value : \'' || AS_VARCHAR(src:bnbv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:calb), '0'), 38, 10) is null and 
                    src:calb is not null) THEN
                    'calb contains a non-numeric value : \'' || AS_VARCHAR(src:calb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capp), '0'), 38, 10) is null and 
                    src:capp is not null) THEN
                    'capp contains a non-numeric value : \'' || AS_VARCHAR(src:capp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:conv), '0'), 38, 10) is null and 
                    src:conv is not null) THEN
                    'conv contains a non-numeric value : \'' || AS_VARCHAR(src:conv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cper), '0'), 38, 10) is null and 
                    src:cper is not null) THEN
                    'cper contains a non-numeric value : \'' || AS_VARCHAR(src:cper) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbpt), '0'), 38, 10) is null and 
                    src:dbpt is not null) THEN
                    'dbpt contains a non-numeric value : \'' || AS_VARCHAR(src:dbpt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depl), '0'), 38, 10) is null and 
                    src:depl is not null) THEN
                    'depl contains a non-numeric value : \'' || AS_VARCHAR(src:depl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:didt), '0'), 38, 10) is null and 
                    src:didt is not null) THEN
                    'didt contains a non-numeric value : \'' || AS_VARCHAR(src:didt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disp), '0'), 38, 10) is null and 
                    src:disp is not null) THEN
                    'disp contains a non-numeric value : \'' || AS_VARCHAR(src:disp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dprc), '0'), 38, 10) is null and 
                    src:dprc is not null) THEN
                    'dprc contains a non-numeric value : \'' || AS_VARCHAR(src:dprc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:face), '0'), 38, 10) is null and 
                    src:face is not null) THEN
                    'face contains a non-numeric value : \'' || AS_VARCHAR(src:face) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:famt), '0'), 38, 10) is null and 
                    src:famt is not null) THEN
                    'famt contains a non-numeric value : \'' || AS_VARCHAR(src:famt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcal), '0'), 38, 10) is null and 
                    src:fcal is not null) THEN
                    'fcal contains a non-numeric value : \'' || AS_VARCHAR(src:fcal) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcom), '0'), 38, 10) is null and 
                    src:fcom is not null) THEN
                    'fcom contains a non-numeric value : \'' || AS_VARCHAR(src:fcom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ffin), '0'), 38, 10) is null and 
                    src:ffin is not null) THEN
                    'ffin contains a non-numeric value : \'' || AS_VARCHAR(src:ffin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fixa_1), '0'), 38, 10) is null and 
                    src:fixa_1 is not null) THEN
                    'fixa_1 contains a non-numeric value : \'' || AS_VARCHAR(src:fixa_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fixa_2), '0'), 38, 10) is null and 
                    src:fixa_2 is not null) THEN
                    'fixa_2 contains a non-numeric value : \'' || AS_VARCHAR(src:fixa_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fixa_3), '0'), 38, 10) is null and 
                    src:fixa_3 is not null) THEN
                    'fixa_3 contains a non-numeric value : \'' || AS_VARCHAR(src:fixa_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:foth), '0'), 38, 10) is null and 
                    src:foth is not null) THEN
                    'foth contains a non-numeric value : \'' || AS_VARCHAR(src:foth) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fspe), '0'), 38, 10) is null and 
                    src:fspe is not null) THEN
                    'fspe contains a non-numeric value : \'' || AS_VARCHAR(src:fspe) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fsta), '0'), 38, 10) is null and 
                    src:fsta is not null) THEN
                    'fsta contains a non-numeric value : \'' || AS_VARCHAR(src:fsta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstd), '0'), 38, 10) is null and 
                    src:fstd is not null) THEN
                    'fstd contains a non-numeric value : \'' || AS_VARCHAR(src:fstd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gfac), '0'), 38, 10) is null and 
                    src:gfac is not null) THEN
                    'gfac contains a non-numeric value : \'' || AS_VARCHAR(src:gfac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intr), '0'), 38, 10) is null and 
                    src:intr is not null) THEN
                    'intr contains a non-numeric value : \'' || AS_VARCHAR(src:intr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:life), '0'), 38, 10) is null and 
                    src:life is not null) THEN
                    'life contains a non-numeric value : \'' || AS_VARCHAR(src:life) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nygf), '0'), 38, 10) is null and 
                    src:nygf is not null) THEN
                    'nygf contains a non-numeric value : \'' || AS_VARCHAR(src:nygf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:perc), '0'), 38, 10) is null and 
                    src:perc is not null) THEN
                    'perc contains a non-numeric value : \'' || AS_VARCHAR(src:perc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:post), '0'), 38, 10) is null and 
                    src:post is not null) THEN
                    'post contains a non-numeric value : \'' || AS_VARCHAR(src:post) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdgf), '0'), 38, 10) is null and 
                    src:rdgf is not null) THEN
                    'rdgf contains a non-numeric value : \'' || AS_VARCHAR(src:rdgf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rndy), '0'), 38, 10) is null and 
                    src:rndy is not null) THEN
                    'rndy contains a non-numeric value : \'' || AS_VARCHAR(src:rndy) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtty), '0'), 38, 10) is null and 
                    src:rtty is not null) THEN
                    'rtty contains a non-numeric value : \'' || AS_VARCHAR(src:rtty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbsv), '0'), 38, 10) is null and 
                    src:sbsv is not null) THEN
                    'sbsv contains a non-numeric value : \'' || AS_VARCHAR(src:sbsv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:silp), '0'), 38, 10) is null and 
                    src:silp is not null) THEN
                    'silp contains a non-numeric value : \'' || AS_VARCHAR(src:silp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcp), '0'), 38, 10) is null and 
                    src:stcp is not null) THEN
                    'stcp contains a non-numeric value : \'' || AS_VARCHAR(src:stcp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:swsl), '0'), 38, 10) is null and 
                    src:swsl is not null) THEN
                    'swsl contains a non-numeric value : \'' || AS_VARCHAR(src:swsl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:taxt), '0'), 38, 10) is null and 
                    src:taxt is not null) THEN
                    'taxt contains a non-numeric value : \'' || AS_VARCHAR(src:taxt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:type), '0'), 38, 10) is null and 
                    src:type is not null) THEN
                    'type contains a non-numeric value : \'' || AS_VARCHAR(src:type) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:updt), '0'), 38, 10) is null and 
                    src:updt is not null) THEN
                    'updt contains a non-numeric value : \'' || AS_VARCHAR(src:updt) || '\'' WHEN 
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
                src:meth  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFAM710 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acnp), '0'), 38, 10) is null and 
                    src:acnp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bdad), '0'), 38, 10) is null and 
                    src:bdad is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bnbv), '0'), 38, 10) is null and 
                    src:bnbv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:calb), '0'), 38, 10) is null and 
                    src:calb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capp), '0'), 38, 10) is null and 
                    src:capp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:conv), '0'), 38, 10) is null and 
                    src:conv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cper), '0'), 38, 10) is null and 
                    src:cper is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbpt), '0'), 38, 10) is null and 
                    src:dbpt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:depl), '0'), 38, 10) is null and 
                    src:depl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:didt), '0'), 38, 10) is null and 
                    src:didt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disp), '0'), 38, 10) is null and 
                    src:disp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dprc), '0'), 38, 10) is null and 
                    src:dprc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:face), '0'), 38, 10) is null and 
                    src:face is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:famt), '0'), 38, 10) is null and 
                    src:famt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcal), '0'), 38, 10) is null and 
                    src:fcal is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcom), '0'), 38, 10) is null and 
                    src:fcom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ffin), '0'), 38, 10) is null and 
                    src:ffin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fixa_1), '0'), 38, 10) is null and 
                    src:fixa_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fixa_2), '0'), 38, 10) is null and 
                    src:fixa_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fixa_3), '0'), 38, 10) is null and 
                    src:fixa_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:foth), '0'), 38, 10) is null and 
                    src:foth is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fspe), '0'), 38, 10) is null and 
                    src:fspe is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fsta), '0'), 38, 10) is null and 
                    src:fsta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstd), '0'), 38, 10) is null and 
                    src:fstd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gfac), '0'), 38, 10) is null and 
                    src:gfac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intr), '0'), 38, 10) is null and 
                    src:intr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:life), '0'), 38, 10) is null and 
                    src:life is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nygf), '0'), 38, 10) is null and 
                    src:nygf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:perc), '0'), 38, 10) is null and 
                    src:perc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:post), '0'), 38, 10) is null and 
                    src:post is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdgf), '0'), 38, 10) is null and 
                    src:rdgf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rndy), '0'), 38, 10) is null and 
                    src:rndy is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtty), '0'), 38, 10) is null and 
                    src:rtty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbsv), '0'), 38, 10) is null and 
                    src:sbsv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:silp), '0'), 38, 10) is null and 
                    src:silp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stcp), '0'), 38, 10) is null and 
                    src:stcp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:swsl), '0'), 38, 10) is null and 
                    src:swsl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:taxt), '0'), 38, 10) is null and 
                    src:taxt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:type), '0'), 38, 10) is null and 
                    src:type is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:updt), '0'), 38, 10) is null and 
                    src:updt is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)