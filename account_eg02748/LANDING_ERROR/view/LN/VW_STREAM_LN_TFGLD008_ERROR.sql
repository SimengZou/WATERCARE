CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFGLD008_ERROR AS SELECT src, 'LN_TFGLD008' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmp_ref_compnr), '0'), 38, 10) is null and 
                    src:acmp_ref_compnr is not null) THEN
                    'acmp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:acmp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alat), '0'), 38, 10) is null and 
                    src:alat is not null) THEN
                    'alat contains a non-numeric value : \'' || AS_VARCHAR(src:alat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atyp), '0'), 38, 10) is null and 
                    src:atyp is not null) THEN
                    'atyp contains a non-numeric value : \'' || AS_VARCHAR(src:atyp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:bfdt), '1900-01-01')) is null and 
                    src:bfdt is not null) THEN
                    'bfdt contains a non-timestamp value : \'' || AS_VARCHAR(src:bfdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbp), '0'), 38, 10) is null and 
                    src:blbp is not null) THEN
                    'blbp contains a non-numeric value : \'' || AS_VARCHAR(src:blbp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bloc), '0'), 38, 10) is null and 
                    src:bloc is not null) THEN
                    'bloc contains a non-numeric value : \'' || AS_VARCHAR(src:bloc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:budt), '1900-01-01')) is null and 
                    src:budt is not null) THEN
                    'budt contains a non-timestamp value : \'' || AS_VARCHAR(src:budt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdca_ref_compnr), '0'), 38, 10) is null and 
                    src:cdca_ref_compnr is not null) THEN
                    'cdca_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdca_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfic_ref_compnr), '0'), 38, 10) is null and 
                    src:cfic_ref_compnr is not null) THEN
                    'cfic_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfic_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrs_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrs_ref_compnr is not null) THEN
                    'cfrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctar), '0'), 38, 10) is null and 
                    src:ctar is not null) THEN
                    'ctar contains a non-numeric value : \'' || AS_VARCHAR(src:ctar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctlm), '0'), 38, 10) is null and 
                    src:ctlm is not null) THEN
                    'ctlm contains a non-numeric value : \'' || AS_VARCHAR(src:ctlm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) THEN
                    'dbcr contains a non-numeric value : \'' || AS_VARCHAR(src:dbcr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dblm), '0'), 38, 10) is null and 
                    src:dblm is not null) THEN
                    'dblm contains a non-numeric value : \'' || AS_VARCHAR(src:dblm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dga1_ref_compnr), '0'), 38, 10) is null and 
                    src:dga1_ref_compnr is not null) THEN
                    'dga1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dga1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dga2_ref_compnr), '0'), 38, 10) is null and 
                    src:dga2_ref_compnr is not null) THEN
                    'dga2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dga2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim1), '0'), 38, 10) is null and 
                    src:dim1 is not null) THEN
                    'dim1 contains a non-numeric value : \'' || AS_VARCHAR(src:dim1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim2), '0'), 38, 10) is null and 
                    src:dim2 is not null) THEN
                    'dim2 contains a non-numeric value : \'' || AS_VARCHAR(src:dim2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim3), '0'), 38, 10) is null and 
                    src:dim3 is not null) THEN
                    'dim3 contains a non-numeric value : \'' || AS_VARCHAR(src:dim3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim4), '0'), 38, 10) is null and 
                    src:dim4 is not null) THEN
                    'dim4 contains a non-numeric value : \'' || AS_VARCHAR(src:dim4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim5), '0'), 38, 10) is null and 
                    src:dim5 is not null) THEN
                    'dim5 contains a non-numeric value : \'' || AS_VARCHAR(src:dim5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim6), '0'), 38, 10) is null and 
                    src:dim6 is not null) THEN
                    'dim6 contains a non-numeric value : \'' || AS_VARCHAR(src:dim6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim7), '0'), 38, 10) is null and 
                    src:dim7 is not null) THEN
                    'dim7 contains a non-numeric value : \'' || AS_VARCHAR(src:dim7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim8), '0'), 38, 10) is null and 
                    src:dim8 is not null) THEN
                    'dim8 contains a non-numeric value : \'' || AS_VARCHAR(src:dim8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim9), '0'), 38, 10) is null and 
                    src:dim9 is not null) THEN
                    'dim9 contains a non-numeric value : \'' || AS_VARCHAR(src:dim9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dla1_ref_compnr), '0'), 38, 10) is null and 
                    src:dla1_ref_compnr is not null) THEN
                    'dla1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dla1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dla2_ref_compnr), '0'), 38, 10) is null and 
                    src:dla2_ref_compnr is not null) THEN
                    'dla2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dla2_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dm10), '0'), 38, 10) is null and 
                    src:dm10 is not null) THEN
                    'dm10 contains a non-numeric value : \'' || AS_VARCHAR(src:dm10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dm11), '0'), 38, 10) is null and 
                    src:dm11 is not null) THEN
                    'dm11 contains a non-numeric value : \'' || AS_VARCHAR(src:dm11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dm12), '0'), 38, 10) is null and 
                    src:dm12 is not null) THEN
                    'dm12 contains a non-numeric value : \'' || AS_VARCHAR(src:dm12) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmsq), '0'), 38, 10) is null and 
                    src:dmsq is not null) THEN
                    'dmsq contains a non-numeric value : \'' || AS_VARCHAR(src:dmsq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:duac), '0'), 38, 10) is null and 
                    src:duac is not null) THEN
                    'duac contains a non-numeric value : \'' || AS_VARCHAR(src:duac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etyp), '0'), 38, 10) is null and 
                    src:etyp is not null) THEN
                    'etyp contains a non-numeric value : \'' || AS_VARCHAR(src:etyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icom), '0'), 38, 10) is null and 
                    src:icom is not null) THEN
                    'icom contains a non-numeric value : \'' || AS_VARCHAR(src:icom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icur), '0'), 38, 10) is null and 
                    src:icur is not null) THEN
                    'icur contains a non-numeric value : \'' || AS_VARCHAR(src:icur) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifas), '0'), 38, 10) is null and 
                    src:ifas is not null) THEN
                    'ifas contains a non-numeric value : \'' || AS_VARCHAR(src:ifas) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:injb), '0'), 38, 10) is null and 
                    src:injb is not null) THEN
                    'injb contains a non-numeric value : \'' || AS_VARCHAR(src:injb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inta), '0'), 38, 10) is null and 
                    src:inta is not null) THEN
                    'inta contains a non-numeric value : \'' || AS_VARCHAR(src:inta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iprj), '0'), 38, 10) is null and 
                    src:iprj is not null) THEN
                    'iprj contains a non-numeric value : \'' || AS_VARCHAR(src:iprj) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loco), '0'), 38, 10) is null and 
                    src:loco is not null) THEN
                    'loco contains a non-numeric value : \'' || AS_VARCHAR(src:loco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mach), '0'), 38, 10) is null and 
                    src:mach is not null) THEN
                    'mach contains a non-numeric value : \'' || AS_VARCHAR(src:mach) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcac_ref_compnr), '0'), 38, 10) is null and 
                    src:pcac_ref_compnr is not null) THEN
                    'pcac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pcac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:perc), '0'), 38, 10) is null and 
                    src:perc is not null) THEN
                    'perc contains a non-numeric value : \'' || AS_VARCHAR(src:perc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plac_ref_compnr), '0'), 38, 10) is null and 
                    src:plac_ref_compnr is not null) THEN
                    'plac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:plac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) THEN
                    'pseq contains a non-numeric value : \'' || AS_VARCHAR(src:pseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sear_ref_compnr), '0'), 38, 10) is null and 
                    src:sear_ref_compnr is not null) THEN
                    'sear_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sear_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subl), '0'), 38, 10) is null and 
                    src:subl is not null) THEN
                    'subl contains a non-numeric value : \'' || AS_VARCHAR(src:subl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tagp_ref_compnr), '0'), 38, 10) is null and 
                    src:tagp_ref_compnr is not null) THEN
                    'tagp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tagp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uni1_ref_compnr), '0'), 38, 10) is null and 
                    src:uni1_ref_compnr is not null) THEN
                    'uni1_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:uni1_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uni2_ref_compnr), '0'), 38, 10) is null and 
                    src:uni2_ref_compnr is not null) THEN
                    'uni2_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:uni2_ref_compnr) || '\'' WHEN 
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
                src:leac  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFGLD008 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acmp_ref_compnr), '0'), 38, 10) is null and 
                    src:acmp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alat), '0'), 38, 10) is null and 
                    src:alat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atyp), '0'), 38, 10) is null and 
                    src:atyp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:bfdt), '1900-01-01')) is null and 
                    src:bfdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbp), '0'), 38, 10) is null and 
                    src:blbp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bloc), '0'), 38, 10) is null and 
                    src:bloc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:budt), '1900-01-01')) is null and 
                    src:budt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdca_ref_compnr), '0'), 38, 10) is null and 
                    src:cdca_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfic_ref_compnr), '0'), 38, 10) is null and 
                    src:cfic_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrs_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctar), '0'), 38, 10) is null and 
                    src:ctar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctlm), '0'), 38, 10) is null and 
                    src:ctlm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dblm), '0'), 38, 10) is null and 
                    src:dblm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dga1_ref_compnr), '0'), 38, 10) is null and 
                    src:dga1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dga2_ref_compnr), '0'), 38, 10) is null and 
                    src:dga2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim1), '0'), 38, 10) is null and 
                    src:dim1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim2), '0'), 38, 10) is null and 
                    src:dim2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim3), '0'), 38, 10) is null and 
                    src:dim3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim4), '0'), 38, 10) is null and 
                    src:dim4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim5), '0'), 38, 10) is null and 
                    src:dim5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim6), '0'), 38, 10) is null and 
                    src:dim6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim7), '0'), 38, 10) is null and 
                    src:dim7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim8), '0'), 38, 10) is null and 
                    src:dim8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim9), '0'), 38, 10) is null and 
                    src:dim9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dla1_ref_compnr), '0'), 38, 10) is null and 
                    src:dla1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dla2_ref_compnr), '0'), 38, 10) is null and 
                    src:dla2_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dm10), '0'), 38, 10) is null and 
                    src:dm10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dm11), '0'), 38, 10) is null and 
                    src:dm11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dm12), '0'), 38, 10) is null and 
                    src:dm12 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmsq), '0'), 38, 10) is null and 
                    src:dmsq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:duac), '0'), 38, 10) is null and 
                    src:duac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etyp), '0'), 38, 10) is null and 
                    src:etyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icom), '0'), 38, 10) is null and 
                    src:icom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icur), '0'), 38, 10) is null and 
                    src:icur is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifas), '0'), 38, 10) is null and 
                    src:ifas is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:injb), '0'), 38, 10) is null and 
                    src:injb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inta), '0'), 38, 10) is null and 
                    src:inta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iprj), '0'), 38, 10) is null and 
                    src:iprj is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loco), '0'), 38, 10) is null and 
                    src:loco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mach), '0'), 38, 10) is null and 
                    src:mach is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcac_ref_compnr), '0'), 38, 10) is null and 
                    src:pcac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:perc), '0'), 38, 10) is null and 
                    src:perc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plac_ref_compnr), '0'), 38, 10) is null and 
                    src:plac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sear_ref_compnr), '0'), 38, 10) is null and 
                    src:sear_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subl), '0'), 38, 10) is null and 
                    src:subl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tagp_ref_compnr), '0'), 38, 10) is null and 
                    src:tagp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uni1_ref_compnr), '0'), 38, 10) is null and 
                    src:uni1_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uni2_ref_compnr), '0'), 38, 10) is null and 
                    src:uni2_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)