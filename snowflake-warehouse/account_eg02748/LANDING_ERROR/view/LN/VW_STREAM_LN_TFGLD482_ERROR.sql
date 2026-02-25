CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFGLD482_ERROR AS SELECT src, 'LN_TFGLD482' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:admm), '0'), 38, 10) is null and 
                    src:admm is not null) THEN
                    'admm contains a non-numeric value : \'' || AS_VARCHAR(src:admm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alam), '0'), 38, 10) is null and 
                    src:alam is not null) THEN
                    'alam contains a non-numeric value : \'' || AS_VARCHAR(src:alam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) THEN
                    'amnt contains a non-numeric value : \'' || AS_VARCHAR(src:amnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_1), '0'), 38, 10) is null and 
                    src:amth_1 is not null) THEN
                    'amth_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amth_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_2), '0'), 38, 10) is null and 
                    src:amth_2 is not null) THEN
                    'amth_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amth_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_3), '0'), 38, 10) is null and 
                    src:amth_3 is not null) THEN
                    'amth_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amth_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btno), '0'), 38, 10) is null and 
                    src:btno is not null) THEN
                    'btno contains a non-numeric value : \'' || AS_VARCHAR(src:btno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) THEN
                    'dbcr contains a non-numeric value : \'' || AS_VARCHAR(src:dbcr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn), '0'), 38, 10) is null and 
                    src:docn is not null) THEN
                    'docn contains a non-numeric value : \'' || AS_VARCHAR(src:docn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eibc_1), '0'), 38, 10) is null and 
                    src:eibc_1 is not null) THEN
                    'eibc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:eibc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eibc_2), '0'), 38, 10) is null and 
                    src:eibc_2 is not null) THEN
                    'eibc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:eibc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eibc_3), '0'), 38, 10) is null and 
                    src:eibc_3 is not null) THEN
                    'eibc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:eibc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcom), '0'), 38, 10) is null and 
                    src:fcom is not null) THEN
                    'fcom contains a non-numeric value : \'' || AS_VARCHAR(src:fcom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fprd), '0'), 38, 10) is null and 
                    src:fprd is not null) THEN
                    'fprd contains a non-numeric value : \'' || AS_VARCHAR(src:fprd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fyer), '0'), 38, 10) is null and 
                    src:fyer is not null) THEN
                    'fyer contains a non-numeric value : \'' || AS_VARCHAR(src:fyer) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glcu), '0'), 38, 10) is null and 
                    src:glcu is not null) THEN
                    'glcu contains a non-numeric value : \'' || AS_VARCHAR(src:glcu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idtc_ref_compnr), '0'), 38, 10) is null and 
                    src:idtc_ref_compnr is not null) THEN
                    'idtc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:idtc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lino), '0'), 38, 10) is null and 
                    src:lino is not null) THEN
                    'lino contains a non-numeric value : \'' || AS_VARCHAR(src:lino) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:msvs), '0'), 38, 10) is null and 
                    src:msvs is not null) THEN
                    'msvs contains a non-numeric value : \'' || AS_VARCHAR(src:msvs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nuni), '0'), 38, 10) is null and 
                    src:nuni is not null) THEN
                    'nuni contains a non-numeric value : \'' || AS_VARCHAR(src:nuni) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) THEN
                    'ocmp contains a non-numeric value : \'' || AS_VARCHAR(src:ocmp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:podt), '1900-01-01')) is null and 
                    src:podt is not null) THEN
                    'podt contains a non-timestamp value : \'' || AS_VARCHAR(src:podt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prin), '0'), 38, 10) is null and 
                    src:prin is not null) THEN
                    'prin contains a non-numeric value : \'' || AS_VARCHAR(src:prin) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rbon_ref_compnr), '0'), 38, 10) is null and 
                    src:rbon_ref_compnr is not null) THEN
                    'rbon_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rbon_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpon), '0'), 38, 10) is null and 
                    src:rpon is not null) THEN
                    'rpon contains a non-numeric value : \'' || AS_VARCHAR(src:rpon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rprd), '0'), 38, 10) is null and 
                    src:rprd is not null) THEN
                    'rprd contains a non-numeric value : \'' || AS_VARCHAR(src:rprd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ryer), '0'), 38, 10) is null and 
                    src:ryer is not null) THEN
                    'ryer contains a non-numeric value : \'' || AS_VARCHAR(src:ryer) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sint), '0'), 38, 10) is null and 
                    src:sint is not null) THEN
                    'sint contains a non-numeric value : \'' || AS_VARCHAR(src:sint) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcmp), '0'), 38, 10) is null and 
                    src:tcmp is not null) THEN
                    'tcmp contains a non-numeric value : \'' || AS_VARCHAR(src:tcmp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprd), '0'), 38, 10) is null and 
                    src:tprd is not null) THEN
                    'tprd contains a non-numeric value : \'' || AS_VARCHAR(src:tprd) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) THEN
                    'trdt contains a non-timestamp value : \'' || AS_VARCHAR(src:trdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tyer), '0'), 38, 10) is null and 
                    src:tyer is not null) THEN
                    'tyer contains a non-numeric value : \'' || AS_VARCHAR(src:tyer) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usgr_ref_compnr), '0'), 38, 10) is null and 
                    src:usgr_ref_compnr is not null) THEN
                    'usgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:usgr_ref_compnr) || '\'' WHEN 
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
                                    
                src:dbcr,
                src:guid,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFGLD482 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:admm), '0'), 38, 10) is null and 
                    src:admm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alam), '0'), 38, 10) is null and 
                    src:alam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_1), '0'), 38, 10) is null and 
                    src:amth_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_2), '0'), 38, 10) is null and 
                    src:amth_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_3), '0'), 38, 10) is null and 
                    src:amth_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btno), '0'), 38, 10) is null and 
                    src:btno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn), '0'), 38, 10) is null and 
                    src:docn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eibc_1), '0'), 38, 10) is null and 
                    src:eibc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eibc_2), '0'), 38, 10) is null and 
                    src:eibc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eibc_3), '0'), 38, 10) is null and 
                    src:eibc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcom), '0'), 38, 10) is null and 
                    src:fcom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fprd), '0'), 38, 10) is null and 
                    src:fprd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fyer), '0'), 38, 10) is null and 
                    src:fyer is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glcu), '0'), 38, 10) is null and 
                    src:glcu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idtc_ref_compnr), '0'), 38, 10) is null and 
                    src:idtc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lino), '0'), 38, 10) is null and 
                    src:lino is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:msvs), '0'), 38, 10) is null and 
                    src:msvs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nuni), '0'), 38, 10) is null and 
                    src:nuni is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:podt), '1900-01-01')) is null and 
                    src:podt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prin), '0'), 38, 10) is null and 
                    src:prin is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rbon_ref_compnr), '0'), 38, 10) is null and 
                    src:rbon_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpon), '0'), 38, 10) is null and 
                    src:rpon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rprd), '0'), 38, 10) is null and 
                    src:rprd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ryer), '0'), 38, 10) is null and 
                    src:ryer is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sint), '0'), 38, 10) is null and 
                    src:sint is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcmp), '0'), 38, 10) is null and 
                    src:tcmp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprd), '0'), 38, 10) is null and 
                    src:tprd is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tyer), '0'), 38, 10) is null and 
                    src:tyer is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usgr_ref_compnr), '0'), 38, 10) is null and 
                    src:usgr_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)