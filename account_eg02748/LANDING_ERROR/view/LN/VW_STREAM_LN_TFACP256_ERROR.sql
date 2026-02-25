CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFACP256_ERROR AS SELECT src, 'LN_TFACP256' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aprp), '0'), 38, 10) is null and 
                    src:aprp is not null) THEN
                    'aprp contains a non-numeric value : \'' || AS_VARCHAR(src:aprp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apry), '0'), 38, 10) is null and 
                    src:apry is not null) THEN
                    'apry contains a non-numeric value : \'' || AS_VARCHAR(src:apry) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:boor), '0'), 38, 10) is null and 
                    src:boor is not null) THEN
                    'boor contains a non-numeric value : \'' || AS_VARCHAR(src:boor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:boty), '0'), 38, 10) is null and 
                    src:boty is not null) THEN
                    'boty contains a non-numeric value : \'' || AS_VARCHAR(src:boty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cisq), '0'), 38, 10) is null and 
                    src:cisq is not null) THEN
                    'cisq contains a non-numeric value : \'' || AS_VARCHAR(src:cisq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clin), '0'), 38, 10) is null and 
                    src:clin is not null) THEN
                    'clin contains a non-numeric value : \'' || AS_VARCHAR(src:clin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clln), '0'), 38, 10) is null and 
                    src:clln is not null) THEN
                    'clln contains a non-numeric value : \'' || AS_VARCHAR(src:clln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cseq), '0'), 38, 10) is null and 
                    src:cseq is not null) THEN
                    'cseq contains a non-numeric value : \'' || AS_VARCHAR(src:cseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvli), '0'), 38, 10) is null and 
                    src:cvli is not null) THEN
                    'cvli contains a non-numeric value : \'' || AS_VARCHAR(src:cvli) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:data), '1900-01-01')) is null and 
                    src:data is not null) THEN
                    'data contains a non-timestamp value : \'' || AS_VARCHAR(src:data) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) THEN
                    'dbcr contains a non-numeric value : \'' || AS_VARCHAR(src:dbcr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iamt), '0'), 38, 10) is null and 
                    src:iamt is not null) THEN
                    'iamt contains a non-numeric value : \'' || AS_VARCHAR(src:iamt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icom), '0'), 38, 10) is null and 
                    src:icom is not null) THEN
                    'icom contains a non-numeric value : \'' || AS_VARCHAR(src:icom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idcn), '0'), 38, 10) is null and 
                    src:idcn is not null) THEN
                    'idcn contains a non-numeric value : \'' || AS_VARCHAR(src:idcn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idln), '0'), 38, 10) is null and 
                    src:idln is not null) THEN
                    'idln contains a non-numeric value : \'' || AS_VARCHAR(src:idln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) THEN
                    'idoc contains a non-numeric value : \'' || AS_VARCHAR(src:idoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ilin), '0'), 38, 10) is null and 
                    src:ilin is not null) THEN
                    'ilin contains a non-numeric value : \'' || AS_VARCHAR(src:ilin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ilno), '0'), 38, 10) is null and 
                    src:ilno is not null) THEN
                    'ilno contains a non-numeric value : \'' || AS_VARCHAR(src:ilno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iqan), '0'), 38, 10) is null and 
                    src:iqan is not null) THEN
                    'iqan contains a non-numeric value : \'' || AS_VARCHAR(src:iqan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iseq), '0'), 38, 10) is null and 
                    src:iseq is not null) THEN
                    'iseq contains a non-numeric value : \'' || AS_VARCHAR(src:iseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcln), '0'), 38, 10) is null and 
                    src:lcln is not null) THEN
                    'lcln contains a non-numeric value : \'' || AS_VARCHAR(src:lcln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmat), '0'), 38, 10) is null and 
                    src:lmat is not null) THEN
                    'lmat contains a non-numeric value : \'' || AS_VARCHAR(src:lmat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loco), '0'), 38, 10) is null and 
                    src:loco is not null) THEN
                    'loco contains a non-numeric value : \'' || AS_VARCHAR(src:loco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maam), '0'), 38, 10) is null and 
                    src:maam is not null) THEN
                    'maam contains a non-numeric value : \'' || AS_VARCHAR(src:maam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtrc), '0'), 38, 10) is null and 
                    src:mtrc is not null) THEN
                    'mtrc contains a non-numeric value : \'' || AS_VARCHAR(src:mtrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtyp), '0'), 38, 10) is null and 
                    src:mtyp is not null) THEN
                    'mtyp contains a non-numeric value : \'' || AS_VARCHAR(src:mtyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt), '0'), 38, 10) is null and 
                    src:oamt is not null) THEN
                    'oamt contains a non-numeric value : \'' || AS_VARCHAR(src:oamt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocvt_ref_compnr), '0'), 38, 10) is null and 
                    src:ocvt_ref_compnr is not null) THEN
                    'ocvt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ocvt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oqan), '0'), 38, 10) is null and 
                    src:oqan is not null) THEN
                    'oqan contains a non-numeric value : \'' || AS_VARCHAR(src:oqan) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otyp), '0'), 38, 10) is null and 
                    src:otyp is not null) THEN
                    'otyp contains a non-numeric value : \'' || AS_VARCHAR(src:otyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovtc_ocvt_ref_compnr), '0'), 38, 10) is null and 
                    src:ovtc_ocvt_ref_compnr is not null) THEN
                    'ovtc_ocvt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ovtc_ocvt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovtc_ref_compnr), '0'), 38, 10) is null and 
                    src:ovtc_ref_compnr is not null) THEN
                    'ovtc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ovtc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdam), '0'), 38, 10) is null and 
                    src:rdam is not null) THEN
                    'rdam contains a non-numeric value : \'' || AS_VARCHAR(src:rdam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rlin), '0'), 38, 10) is null and 
                    src:rlin is not null) THEN
                    'rlin contains a non-numeric value : \'' || AS_VARCHAR(src:rlin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) THEN
                    'rseq contains a non-numeric value : \'' || AS_VARCHAR(src:rseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rttp), '0'), 38, 10) is null and 
                    src:rttp is not null) THEN
                    'rttp contains a non-numeric value : \'' || AS_VARCHAR(src:rttp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) THEN
                    'sern contains a non-numeric value : \'' || AS_VARCHAR(src:sern) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shln), '0'), 38, 10) is null and 
                    src:shln is not null) THEN
                    'shln contains a non-numeric value : \'' || AS_VARCHAR(src:shln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sort), '0'), 38, 10) is null and 
                    src:sort is not null) THEN
                    'sort contains a non-numeric value : \'' || AS_VARCHAR(src:sort) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spln), '0'), 38, 10) is null and 
                    src:spln is not null) THEN
                    'spln contains a non-numeric value : \'' || AS_VARCHAR(src:spln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) THEN
                    'sqnb contains a non-numeric value : \'' || AS_VARCHAR(src:sqnb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tbai), '0'), 38, 10) is null and 
                    src:tbai is not null) THEN
                    'tbai contains a non-numeric value : \'' || AS_VARCHAR(src:tbai) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcna), '0'), 38, 10) is null and 
                    src:tcna is not null) THEN
                    'tcna contains a non-numeric value : \'' || AS_VARCHAR(src:tcna) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcrd), '0'), 38, 10) is null and 
                    src:tcrd is not null) THEN
                    'tcrd contains a non-numeric value : \'' || AS_VARCHAR(src:tcrd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tctb), '0'), 38, 10) is null and 
                    src:tctb is not null) THEN
                    'tctb contains a non-numeric value : \'' || AS_VARCHAR(src:tctb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tctc), '0'), 38, 10) is null and 
                    src:tctc is not null) THEN
                    'tctc contains a non-numeric value : \'' || AS_VARCHAR(src:tctc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vatc_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:vatc_cvat_ref_compnr is not null) THEN
                    'vatc_cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:vatc_cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vatc_ref_compnr), '0'), 38, 10) is null and 
                    src:vatc_ref_compnr is not null) THEN
                    'vatc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:vatc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vrln), '0'), 38, 10) is null and 
                    src:vrln is not null) THEN
                    'vrln contains a non-numeric value : \'' || AS_VARCHAR(src:vrln) || '\'' WHEN 
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
                                    
                src:icom,
                src:compnr,
                src:ilin,
                src:iseq,
                src:idoc,
                src:idln,
                src:ityp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFACP256 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aprp), '0'), 38, 10) is null and 
                    src:aprp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apry), '0'), 38, 10) is null and 
                    src:apry is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:boor), '0'), 38, 10) is null and 
                    src:boor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:boty), '0'), 38, 10) is null and 
                    src:boty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cisq), '0'), 38, 10) is null and 
                    src:cisq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clin), '0'), 38, 10) is null and 
                    src:clin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clln), '0'), 38, 10) is null and 
                    src:clln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cseq), '0'), 38, 10) is null and 
                    src:cseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvli), '0'), 38, 10) is null and 
                    src:cvli is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:data), '1900-01-01')) is null and 
                    src:data is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iamt), '0'), 38, 10) is null and 
                    src:iamt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:icom), '0'), 38, 10) is null and 
                    src:icom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idcn), '0'), 38, 10) is null and 
                    src:idcn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idln), '0'), 38, 10) is null and 
                    src:idln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:idoc), '0'), 38, 10) is null and 
                    src:idoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ilin), '0'), 38, 10) is null and 
                    src:ilin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ilno), '0'), 38, 10) is null and 
                    src:ilno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iqan), '0'), 38, 10) is null and 
                    src:iqan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iseq), '0'), 38, 10) is null and 
                    src:iseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcln), '0'), 38, 10) is null and 
                    src:lcln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmat), '0'), 38, 10) is null and 
                    src:lmat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loco), '0'), 38, 10) is null and 
                    src:loco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maam), '0'), 38, 10) is null and 
                    src:maam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtrc), '0'), 38, 10) is null and 
                    src:mtrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mtyp), '0'), 38, 10) is null and 
                    src:mtyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oamt), '0'), 38, 10) is null and 
                    src:oamt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocvt_ref_compnr), '0'), 38, 10) is null and 
                    src:ocvt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oqan), '0'), 38, 10) is null and 
                    src:oqan is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otyp), '0'), 38, 10) is null and 
                    src:otyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovtc_ocvt_ref_compnr), '0'), 38, 10) is null and 
                    src:ovtc_ocvt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ovtc_ref_compnr), '0'), 38, 10) is null and 
                    src:ovtc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdam), '0'), 38, 10) is null and 
                    src:rdam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rlin), '0'), 38, 10) is null and 
                    src:rlin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rttp), '0'), 38, 10) is null and 
                    src:rttp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sern), '0'), 38, 10) is null and 
                    src:sern is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shln), '0'), 38, 10) is null and 
                    src:shln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sort), '0'), 38, 10) is null and 
                    src:sort is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spln), '0'), 38, 10) is null and 
                    src:spln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tbai), '0'), 38, 10) is null and 
                    src:tbai is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcna), '0'), 38, 10) is null and 
                    src:tcna is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcrd), '0'), 38, 10) is null and 
                    src:tcrd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tctb), '0'), 38, 10) is null and 
                    src:tctb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tctc), '0'), 38, 10) is null and 
                    src:tctc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vatc_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:vatc_cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vatc_ref_compnr), '0'), 38, 10) is null and 
                    src:vatc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vrln), '0'), 38, 10) is null and 
                    src:vrln is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)