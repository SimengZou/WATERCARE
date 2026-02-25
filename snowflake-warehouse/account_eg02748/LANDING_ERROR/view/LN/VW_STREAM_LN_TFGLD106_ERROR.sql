CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFGLD106_ERROR AS SELECT src, 'LN_TFGLD106' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adty), '0'), 38, 10) is null and 
                    src:adty is not null) THEN
                    'adty contains a non-numeric value : \'' || AS_VARCHAR(src:adty) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:buex), '0'), 38, 10) is null and 
                    src:buex is not null) THEN
                    'buex contains a non-numeric value : \'' || AS_VARCHAR(src:buex) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:catg), '0'), 38, 10) is null and 
                    src:catg is not null) THEN
                    'catg contains a non-numeric value : \'' || AS_VARCHAR(src:catg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdln), '0'), 38, 10) is null and 
                    src:cdln is not null) THEN
                    'cdln contains a non-numeric value : \'' || AS_VARCHAR(src:cdln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinv), '0'), 38, 10) is null and 
                    src:cinv is not null) THEN
                    'cinv contains a non-numeric value : \'' || AS_VARCHAR(src:cinv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clin), '0'), 38, 10) is null and 
                    src:clin is not null) THEN
                    'clin contains a non-numeric value : \'' || AS_VARCHAR(src:clin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cont), '0'), 38, 10) is null and 
                    src:cont is not null) THEN
                    'cont contains a non-numeric value : \'' || AS_VARCHAR(src:cont) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corn), '0'), 38, 10) is null and 
                    src:corn is not null) THEN
                    'corn contains a non-numeric value : \'' || AS_VARCHAR(src:corn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csrn), '0'), 38, 10) is null and 
                    src:csrn is not null) THEN
                    'csrn contains a non-numeric value : \'' || AS_VARCHAR(src:csrn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:date), '1900-01-01')) is null and 
                    src:date is not null) THEN
                    'date contains a non-timestamp value : \'' || AS_VARCHAR(src:date) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) THEN
                    'dbcr contains a non-numeric value : \'' || AS_VARCHAR(src:dbcr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dcdt), '1900-01-01')) is null and 
                    src:dcdt is not null) THEN
                    'dcdt contains a non-timestamp value : \'' || AS_VARCHAR(src:dcdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dele), '0'), 38, 10) is null and 
                    src:dele is not null) THEN
                    'dele contains a non-numeric value : \'' || AS_VARCHAR(src:dele) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:expd), '0'), 38, 10) is null and 
                    src:expd is not null) THEN
                    'expd contains a non-numeric value : \'' || AS_VARCHAR(src:expd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fact_1), '0'), 38, 10) is null and 
                    src:fact_1 is not null) THEN
                    'fact_1 contains a non-numeric value : \'' || AS_VARCHAR(src:fact_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fact_2), '0'), 38, 10) is null and 
                    src:fact_2 is not null) THEN
                    'fact_2 contains a non-numeric value : \'' || AS_VARCHAR(src:fact_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fact_3), '0'), 38, 10) is null and 
                    src:fact_3 is not null) THEN
                    'fact_3 contains a non-numeric value : \'' || AS_VARCHAR(src:fact_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdoc), '0'), 38, 10) is null and 
                    src:fdoc is not null) THEN
                    'fdoc contains a non-numeric value : \'' || AS_VARCHAR(src:fdoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fprd), '0'), 38, 10) is null and 
                    src:fprd is not null) THEN
                    'fprd contains a non-numeric value : \'' || AS_VARCHAR(src:fprd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fyer), '0'), 38, 10) is null and 
                    src:fyer is not null) THEN
                    'fyer contains a non-numeric value : \'' || AS_VARCHAR(src:fyer) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intt), '0'), 38, 10) is null and 
                    src:intt is not null) THEN
                    'intt contains a non-numeric value : \'' || AS_VARCHAR(src:intt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obat), '0'), 38, 10) is null and 
                    src:obat is not null) THEN
                    'obat contains a non-numeric value : \'' || AS_VARCHAR(src:obat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) THEN
                    'ocmp contains a non-numeric value : \'' || AS_VARCHAR(src:ocmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odoc), '0'), 38, 10) is null and 
                    src:odoc is not null) THEN
                    'odoc contains a non-numeric value : \'' || AS_VARCHAR(src:odoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:olin), '0'), 38, 10) is null and 
                    src:olin is not null) THEN
                    'olin contains a non-numeric value : \'' || AS_VARCHAR(src:olin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrl), '0'), 38, 10) is null and 
                    src:osrl is not null) THEN
                    'osrl contains a non-numeric value : \'' || AS_VARCHAR(src:osrl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrn), '0'), 38, 10) is null and 
                    src:osrn is not null) THEN
                    'osrn contains a non-numeric value : \'' || AS_VARCHAR(src:osrn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oyer), '0'), 38, 10) is null and 
                    src:oyer is not null) THEN
                    'oyer contains a non-numeric value : \'' || AS_VARCHAR(src:oyer) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qty1), '0'), 38, 10) is null and 
                    src:qty1 is not null) THEN
                    'qty1 contains a non-numeric value : \'' || AS_VARCHAR(src:qty1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qty2), '0'), 38, 10) is null and 
                    src:qty2 is not null) THEN
                    'qty2 contains a non-numeric value : \'' || AS_VARCHAR(src:qty2) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:recl), '0'), 38, 10) is null and 
                    src:recl is not null) THEN
                    'recl contains a non-numeric value : \'' || AS_VARCHAR(src:recl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reco), '0'), 38, 10) is null and 
                    src:reco is not null) THEN
                    'reco contains a non-numeric value : \'' || AS_VARCHAR(src:reco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:recs), '0'), 38, 10) is null and 
                    src:recs is not null) THEN
                    'recs contains a non-numeric value : \'' || AS_VARCHAR(src:recs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:regc_ref_compnr), '0'), 38, 10) is null and 
                    src:regc_ref_compnr is not null) THEN
                    'regc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:regc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rprd), '0'), 38, 10) is null and 
                    src:rprd is not null) THEN
                    'rprd contains a non-numeric value : \'' || AS_VARCHAR(src:rprd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ryer), '0'), 38, 10) is null and 
                    src:ryer is not null) THEN
                    'ryer contains a non-numeric value : \'' || AS_VARCHAR(src:ryer) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcsh), '0'), 38, 10) is null and 
                    src:tcsh is not null) THEN
                    'tcsh contains a non-numeric value : \'' || AS_VARCHAR(src:tcsh) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:tedt), '1900-01-01')) is null and 
                    src:tedt is not null) THEN
                    'tedt contains a non-timestamp value : \'' || AS_VARCHAR(src:tedt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:time), '0'), 38, 10) is null and 
                    src:time is not null) THEN
                    'time contains a non-numeric value : \'' || AS_VARCHAR(src:time) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tran), '0'), 38, 10) is null and 
                    src:tran is not null) THEN
                    'tran contains a non-numeric value : \'' || AS_VARCHAR(src:tran) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trun), '0'), 38, 10) is null and 
                    src:trun is not null) THEN
                    'trun contains a non-numeric value : \'' || AS_VARCHAR(src:trun) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1), '0'), 38, 10) is null and 
                    src:typ1 is not null) THEN
                    'typ1 contains a non-numeric value : \'' || AS_VARCHAR(src:typ1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2), '0'), 38, 10) is null and 
                    src:typ2 is not null) THEN
                    'typ2 contains a non-numeric value : \'' || AS_VARCHAR(src:typ2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3), '0'), 38, 10) is null and 
                    src:typ3 is not null) THEN
                    'typ3 contains a non-numeric value : \'' || AS_VARCHAR(src:typ3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4), '0'), 38, 10) is null and 
                    src:typ4 is not null) THEN
                    'typ4 contains a non-numeric value : \'' || AS_VARCHAR(src:typ4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5), '0'), 38, 10) is null and 
                    src:typ5 is not null) THEN
                    'typ5 contains a non-numeric value : \'' || AS_VARCHAR(src:typ5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vamh_1), '0'), 38, 10) is null and 
                    src:vamh_1 is not null) THEN
                    'vamh_1 contains a non-numeric value : \'' || AS_VARCHAR(src:vamh_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vamh_2), '0'), 38, 10) is null and 
                    src:vamh_2 is not null) THEN
                    'vamh_2 contains a non-numeric value : \'' || AS_VARCHAR(src:vamh_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vamh_3), '0'), 38, 10) is null and 
                    src:vamh_3 is not null) THEN
                    'vamh_3 contains a non-numeric value : \'' || AS_VARCHAR(src:vamh_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vamt), '0'), 38, 10) is null and 
                    src:vamt is not null) THEN
                    'vamt contains a non-numeric value : \'' || AS_VARCHAR(src:vamt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vprd), '0'), 38, 10) is null and 
                    src:vprd is not null) THEN
                    'vprd contains a non-numeric value : \'' || AS_VARCHAR(src:vprd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vtyp), '0'), 38, 10) is null and 
                    src:vtyp is not null) THEN
                    'vtyp contains a non-numeric value : \'' || AS_VARCHAR(src:vtyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vyer), '0'), 38, 10) is null and 
                    src:vyer is not null) THEN
                    'vyer contains a non-numeric value : \'' || AS_VARCHAR(src:vyer) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtsc), '0'), 38, 10) is null and 
                    src:wtsc is not null) THEN
                    'wtsc contains a non-numeric value : \'' || AS_VARCHAR(src:wtsc) || '\'' WHEN 
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
                                    
                src:osrl,
                src:odoc,
                src:osrn,
                src:olin,
                src:compnr,
                src:otyp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFGLD106 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adty), '0'), 38, 10) is null and 
                    src:adty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_1), '0'), 38, 10) is null and 
                    src:amth_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_2), '0'), 38, 10) is null and 
                    src:amth_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_3), '0'), 38, 10) is null and 
                    src:amth_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:buex), '0'), 38, 10) is null and 
                    src:buex is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:catg), '0'), 38, 10) is null and 
                    src:catg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdln), '0'), 38, 10) is null and 
                    src:cdln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinv), '0'), 38, 10) is null and 
                    src:cinv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clin), '0'), 38, 10) is null and 
                    src:clin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cont), '0'), 38, 10) is null and 
                    src:cont is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corn), '0'), 38, 10) is null and 
                    src:corn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csrn), '0'), 38, 10) is null and 
                    src:csrn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:date), '1900-01-01')) is null and 
                    src:date is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dcdt), '1900-01-01')) is null and 
                    src:dcdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dele), '0'), 38, 10) is null and 
                    src:dele is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:expd), '0'), 38, 10) is null and 
                    src:expd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fact_1), '0'), 38, 10) is null and 
                    src:fact_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fact_2), '0'), 38, 10) is null and 
                    src:fact_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fact_3), '0'), 38, 10) is null and 
                    src:fact_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdoc), '0'), 38, 10) is null and 
                    src:fdoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fprd), '0'), 38, 10) is null and 
                    src:fprd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fyer), '0'), 38, 10) is null and 
                    src:fyer is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intt), '0'), 38, 10) is null and 
                    src:intt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obat), '0'), 38, 10) is null and 
                    src:obat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odoc), '0'), 38, 10) is null and 
                    src:odoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:olin), '0'), 38, 10) is null and 
                    src:olin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrl), '0'), 38, 10) is null and 
                    src:osrl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrn), '0'), 38, 10) is null and 
                    src:osrn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oyer), '0'), 38, 10) is null and 
                    src:oyer is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qty1), '0'), 38, 10) is null and 
                    src:qty1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qty2), '0'), 38, 10) is null and 
                    src:qty2 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_1), '0'), 38, 10) is null and 
                    src:rate_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_2), '0'), 38, 10) is null and 
                    src:rate_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_3), '0'), 38, 10) is null and 
                    src:rate_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:recl), '0'), 38, 10) is null and 
                    src:recl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reco), '0'), 38, 10) is null and 
                    src:reco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:recs), '0'), 38, 10) is null and 
                    src:recs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:regc_ref_compnr), '0'), 38, 10) is null and 
                    src:regc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rprd), '0'), 38, 10) is null and 
                    src:rprd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ryer), '0'), 38, 10) is null and 
                    src:ryer is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcsh), '0'), 38, 10) is null and 
                    src:tcsh is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:tedt), '1900-01-01')) is null and 
                    src:tedt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:time), '0'), 38, 10) is null and 
                    src:time is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tran), '0'), 38, 10) is null and 
                    src:tran is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trun), '0'), 38, 10) is null and 
                    src:trun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ1), '0'), 38, 10) is null and 
                    src:typ1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ2), '0'), 38, 10) is null and 
                    src:typ2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ3), '0'), 38, 10) is null and 
                    src:typ3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ4), '0'), 38, 10) is null and 
                    src:typ4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typ5), '0'), 38, 10) is null and 
                    src:typ5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vamh_1), '0'), 38, 10) is null and 
                    src:vamh_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vamh_2), '0'), 38, 10) is null and 
                    src:vamh_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vamh_3), '0'), 38, 10) is null and 
                    src:vamh_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vamt), '0'), 38, 10) is null and 
                    src:vamt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vprd), '0'), 38, 10) is null and 
                    src:vprd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vtyp), '0'), 38, 10) is null and 
                    src:vtyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vyer), '0'), 38, 10) is null and 
                    src:vyer is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtsc), '0'), 38, 10) is null and 
                    src:wtsc is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)