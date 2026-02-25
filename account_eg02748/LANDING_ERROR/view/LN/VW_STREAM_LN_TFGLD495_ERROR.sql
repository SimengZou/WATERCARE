CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFGLD495_ERROR AS SELECT src, 'LN_TFGLD495' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acce), '0'), 38, 10) is null and 
                    src:acce is not null) THEN
                    'acce contains a non-numeric value : \'' || AS_VARCHAR(src:acce) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:accf), '0'), 38, 10) is null and 
                    src:accf is not null) THEN
                    'accf contains a non-numeric value : \'' || AS_VARCHAR(src:accf) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aobl), '0'), 38, 10) is null and 
                    src:aobl is not null) THEN
                    'aobl contains a non-numeric value : \'' || AS_VARCHAR(src:aobl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arat_1), '0'), 38, 10) is null and 
                    src:arat_1 is not null) THEN
                    'arat_1 contains a non-numeric value : \'' || AS_VARCHAR(src:arat_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arat_2), '0'), 38, 10) is null and 
                    src:arat_2 is not null) THEN
                    'arat_2 contains a non-numeric value : \'' || AS_VARCHAR(src:arat_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arat_3), '0'), 38, 10) is null and 
                    src:arat_3 is not null) THEN
                    'arat_3 contains a non-numeric value : \'' || AS_VARCHAR(src:arat_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btno), '0'), 38, 10) is null and 
                    src:btno is not null) THEN
                    'btno contains a non-numeric value : \'' || AS_VARCHAR(src:btno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) THEN
                    'dbcr contains a non-numeric value : \'' || AS_VARCHAR(src:dbcr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dcdt), '1900-01-01')) is null and 
                    src:dcdt is not null) THEN
                    'dcdt contains a non-timestamp value : \'' || AS_VARCHAR(src:dcdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn), '0'), 38, 10) is null and 
                    src:docn is not null) THEN
                    'docn contains a non-numeric value : \'' || AS_VARCHAR(src:docn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcom), '0'), 38, 10) is null and 
                    src:fcom is not null) THEN
                    'fcom contains a non-numeric value : \'' || AS_VARCHAR(src:fcom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fprd), '0'), 38, 10) is null and 
                    src:fprd is not null) THEN
                    'fprd contains a non-numeric value : \'' || AS_VARCHAR(src:fprd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fyer), '0'), 38, 10) is null and 
                    src:fyer is not null) THEN
                    'fyer contains a non-numeric value : \'' || AS_VARCHAR(src:fyer) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inad), '0'), 38, 10) is null and 
                    src:inad is not null) THEN
                    'inad contains a non-numeric value : \'' || AS_VARCHAR(src:inad) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inic), '0'), 38, 10) is null and 
                    src:inic is not null) THEN
                    'inic contains a non-numeric value : \'' || AS_VARCHAR(src:inic) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ktrn), '0'), 38, 10) is null and 
                    src:ktrn is not null) THEN
                    'ktrn contains a non-numeric value : \'' || AS_VARCHAR(src:ktrn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcit), '0'), 38, 10) is null and 
                    src:lcit is not null) THEN
                    'lcit contains a non-numeric value : \'' || AS_VARCHAR(src:lcit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lino), '0'), 38, 10) is null and 
                    src:lino is not null) THEN
                    'lino contains a non-numeric value : \'' || AS_VARCHAR(src:lino) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nuni), '0'), 38, 10) is null and 
                    src:nuni is not null) THEN
                    'nuni contains a non-numeric value : \'' || AS_VARCHAR(src:nuni) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) THEN
                    'ocmp contains a non-numeric value : \'' || AS_VARCHAR(src:ocmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odbc), '0'), 38, 10) is null and 
                    src:odbc is not null) THEN
                    'odbc contains a non-numeric value : \'' || AS_VARCHAR(src:odbc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:podt), '1900-01-01')) is null and 
                    src:podt is not null) THEN
                    'podt contains a non-timestamp value : \'' || AS_VARCHAR(src:podt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reco), '0'), 38, 10) is null and 
                    src:reco is not null) THEN
                    'reco contains a non-numeric value : \'' || AS_VARCHAR(src:reco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:recs), '0'), 38, 10) is null and 
                    src:recs is not null) THEN
                    'recs contains a non-numeric value : \'' || AS_VARCHAR(src:recs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpon), '0'), 38, 10) is null and 
                    src:rpon is not null) THEN
                    'rpon contains a non-numeric value : \'' || AS_VARCHAR(src:rpon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serl), '0'), 38, 10) is null and 
                    src:serl is not null) THEN
                    'serl contains a non-numeric value : \'' || AS_VARCHAR(src:serl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srno), '0'), 38, 10) is null and 
                    src:srno is not null) THEN
                    'srno contains a non-numeric value : \'' || AS_VARCHAR(src:srno) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:tdte), '1900-01-01')) is null and 
                    src:tdte is not null) THEN
                    'tdte contains a non-timestamp value : \'' || AS_VARCHAR(src:tdte) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) THEN
                    'trdt contains a non-timestamp value : \'' || AS_VARCHAR(src:trdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:urat_1), '0'), 38, 10) is null and 
                    src:urat_1 is not null) THEN
                    'urat_1 contains a non-numeric value : \'' || AS_VARCHAR(src:urat_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:urat_2), '0'), 38, 10) is null and 
                    src:urat_2 is not null) THEN
                    'urat_2 contains a non-numeric value : \'' || AS_VARCHAR(src:urat_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:urat_3), '0'), 38, 10) is null and 
                    src:urat_3 is not null) THEN
                    'urat_3 contains a non-numeric value : \'' || AS_VARCHAR(src:urat_3) || '\'' WHEN 
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
                                    
                src:guid,
                src:dbcr,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFGLD495 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acce), '0'), 38, 10) is null and 
                    src:acce is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:accf), '0'), 38, 10) is null and 
                    src:accf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_1), '0'), 38, 10) is null and 
                    src:amth_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_2), '0'), 38, 10) is null and 
                    src:amth_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_3), '0'), 38, 10) is null and 
                    src:amth_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aobl), '0'), 38, 10) is null and 
                    src:aobl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arat_1), '0'), 38, 10) is null and 
                    src:arat_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arat_2), '0'), 38, 10) is null and 
                    src:arat_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arat_3), '0'), 38, 10) is null and 
                    src:arat_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btno), '0'), 38, 10) is null and 
                    src:btno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dcdt), '1900-01-01')) is null and 
                    src:dcdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn), '0'), 38, 10) is null and 
                    src:docn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcom), '0'), 38, 10) is null and 
                    src:fcom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fprd), '0'), 38, 10) is null and 
                    src:fprd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fyer), '0'), 38, 10) is null and 
                    src:fyer is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inad), '0'), 38, 10) is null and 
                    src:inad is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inic), '0'), 38, 10) is null and 
                    src:inic is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ktrn), '0'), 38, 10) is null and 
                    src:ktrn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcit), '0'), 38, 10) is null and 
                    src:lcit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lino), '0'), 38, 10) is null and 
                    src:lino is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nuni), '0'), 38, 10) is null and 
                    src:nuni is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odbc), '0'), 38, 10) is null and 
                    src:odbc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:podt), '1900-01-01')) is null and 
                    src:podt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reco), '0'), 38, 10) is null and 
                    src:reco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:recs), '0'), 38, 10) is null and 
                    src:recs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpon), '0'), 38, 10) is null and 
                    src:rpon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:serl), '0'), 38, 10) is null and 
                    src:serl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srno), '0'), 38, 10) is null and 
                    src:srno is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:tdte), '1900-01-01')) is null and 
                    src:tdte is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:urat_1), '0'), 38, 10) is null and 
                    src:urat_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:urat_2), '0'), 38, 10) is null and 
                    src:urat_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:urat_3), '0'), 38, 10) is null and 
                    src:urat_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)