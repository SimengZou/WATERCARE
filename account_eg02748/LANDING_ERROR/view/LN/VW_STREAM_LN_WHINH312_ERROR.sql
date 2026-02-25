CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHINH312_ERROR AS SELECT src, 'LN_WHINH312' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ardt), '1900-01-01')) is null and 
                    src:ardt is not null) THEN
                    'ardt contains a non-timestamp value : \'' || AS_VARCHAR(src:ardt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arej), '0'), 38, 10) is null and 
                    src:arej is not null) THEN
                    'arej contains a non-numeric value : \'' || AS_VARCHAR(src:arej) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) THEN
                    'atse_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:atse_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgxc), '0'), 38, 10) is null and 
                    src:bgxc is not null) THEN
                    'bgxc contains a non-numeric value : \'' || AS_VARCHAR(src:bgxc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blck), '0'), 38, 10) is null and 
                    src:blck is not null) THEN
                    'blck contains a non-numeric value : \'' || AS_VARCHAR(src:blck) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casi_ref_compnr), '0'), 38, 10) is null and 
                    src:casi_ref_compnr is not null) THEN
                    'casi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:casi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdck), '0'), 38, 10) is null and 
                    src:cdck is not null) THEN
                    'cdck contains a non-numeric value : \'' || AS_VARCHAR(src:cdck) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdcr), '0'), 38, 10) is null and 
                    src:cdcr is not null) THEN
                    'cdcr contains a non-numeric value : \'' || AS_VARCHAR(src:cdcr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) THEN
                    'cmnf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmnf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmpl), '0'), 38, 10) is null and 
                    src:cmpl is not null) THEN
                    'cmpl contains a non-numeric value : \'' || AS_VARCHAR(src:cmpl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:conf), '0'), 38, 10) is null and 
                    src:conf is not null) THEN
                    'conf contains a non-numeric value : \'' || AS_VARCHAR(src:conf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csvc_ref_compnr), '0'), 38, 10) is null and 
                    src:csvc_ref_compnr is not null) THEN
                    'csvc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csvc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csvl), '0'), 38, 10) is null and 
                    src:csvl is not null) THEN
                    'csvl contains a non-numeric value : \'' || AS_VARCHAR(src:csvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_dslo_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_dslo_ref_compnr is not null) THEN
                    'cwar_dslo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_dslo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_rclo_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_rclo_ref_compnr is not null) THEN
                    'cwar_rclo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_rclo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disp), '0'), 38, 10) is null and 
                    src:disp is not null) THEN
                    'disp contains a non-numeric value : \'' || AS_VARCHAR(src:disp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse), '0'), 38, 10) is null and 
                    src:dmse is not null) THEN
                    'dmse contains a non-numeric value : \'' || AS_VARCHAR(src:dmse) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) THEN
                    'effn contains a non-numeric value : \'' || AS_VARCHAR(src:effn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) THEN
                    'effn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:effn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fire), '0'), 38, 10) is null and 
                    src:fire is not null) THEN
                    'fire contains a non-numeric value : \'' || AS_VARCHAR(src:fire) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:huid_ref_compnr), '0'), 38, 10) is null and 
                    src:huid_ref_compnr is not null) THEN
                    'huid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:huid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hupr), '0'), 38, 10) is null and 
                    src:hupr is not null) THEN
                    'hupr contains a non-numeric value : \'' || AS_VARCHAR(src:hupr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:idat), '1900-01-01')) is null and 
                    src:idat is not null) THEN
                    'idat contains a non-timestamp value : \'' || AS_VARCHAR(src:idat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insp), '0'), 38, 10) is null and 
                    src:insp is not null) THEN
                    'insp contains a non-numeric value : \'' || AS_VARCHAR(src:insp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:irdt), '1900-01-01')) is null and 
                    src:irdt is not null) THEN
                    'irdt contains a non-timestamp value : \'' || AS_VARCHAR(src:irdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_pkdf_ref_compnr), '0'), 38, 10) is null and 
                    src:item_pkdf_ref_compnr is not null) THEN
                    'item_pkdf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_pkdf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itxt), '0'), 38, 10) is null and 
                    src:itxt is not null) THEN
                    'itxt contains a non-numeric value : \'' || AS_VARCHAR(src:itxt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itxt_ref_compnr), '0'), 38, 10) is null and 
                    src:itxt_ref_compnr is not null) THEN
                    'itxt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itxt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsta), '0'), 38, 10) is null and 
                    src:lsta is not null) THEN
                    'lsta contains a non-numeric value : \'' || AS_VARCHAR(src:lsta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oorg), '0'), 38, 10) is null and 
                    src:oorg is not null) THEN
                    'oorg contains a non-numeric value : \'' || AS_VARCHAR(src:oorg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oorg_orno_pono_seqn_ref_compnr), '0'), 38, 10) is null and 
                    src:oorg_orno_pono_seqn_ref_compnr is not null) THEN
                    'oorg_orno_pono_seqn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:oorg_orno_pono_seqn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oset), '0'), 38, 10) is null and 
                    src:oset is not null) THEN
                    'oset contains a non-numeric value : \'' || AS_VARCHAR(src:oset) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pkdf_ref_compnr), '0'), 38, 10) is null and 
                    src:pkdf_ref_compnr is not null) THEN
                    'pkdf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pkdf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psqp), '0'), 38, 10) is null and 
                    src:psqp is not null) THEN
                    'psqp contains a non-numeric value : \'' || AS_VARCHAR(src:psqp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psqr), '0'), 38, 10) is null and 
                    src:psqr is not null) THEN
                    'psqr contains a non-numeric value : \'' || AS_VARCHAR(src:psqr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psqu), '0'), 38, 10) is null and 
                    src:psqu is not null) THEN
                    'psqu contains a non-numeric value : \'' || AS_VARCHAR(src:psqu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psun_ref_compnr), '0'), 38, 10) is null and 
                    src:psun_ref_compnr is not null) THEN
                    'psun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:psun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadi), '0'), 38, 10) is null and 
                    src:qadi is not null) THEN
                    'qadi contains a non-numeric value : \'' || AS_VARCHAR(src:qadi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadp), '0'), 38, 10) is null and 
                    src:qadp is not null) THEN
                    'qadp contains a non-numeric value : \'' || AS_VARCHAR(src:qadp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadv), '0'), 38, 10) is null and 
                    src:qadv is not null) THEN
                    'qadv contains a non-numeric value : \'' || AS_VARCHAR(src:qadv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapp), '0'), 38, 10) is null and 
                    src:qapp is not null) THEN
                    'qapp contains a non-numeric value : \'' || AS_VARCHAR(src:qapp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapr), '0'), 38, 10) is null and 
                    src:qapr is not null) THEN
                    'qapr contains a non-numeric value : \'' || AS_VARCHAR(src:qapr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapu), '0'), 38, 10) is null and 
                    src:qapu is not null) THEN
                    'qapu contains a non-numeric value : \'' || AS_VARCHAR(src:qapu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdep), '0'), 38, 10) is null and 
                    src:qdep is not null) THEN
                    'qdep contains a non-numeric value : \'' || AS_VARCHAR(src:qdep) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdes), '0'), 38, 10) is null and 
                    src:qdes is not null) THEN
                    'qdes contains a non-numeric value : \'' || AS_VARCHAR(src:qdes) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdms), '0'), 38, 10) is null and 
                    src:qdms is not null) THEN
                    'qdms contains a non-numeric value : \'' || AS_VARCHAR(src:qdms) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qppu), '0'), 38, 10) is null and 
                    src:qppu is not null) THEN
                    'qppu contains a non-numeric value : \'' || AS_VARCHAR(src:qppu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpui), '0'), 38, 10) is null and 
                    src:qpui is not null) THEN
                    'qpui contains a non-numeric value : \'' || AS_VARCHAR(src:qpui) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpup), '0'), 38, 10) is null and 
                    src:qpup is not null) THEN
                    'qpup contains a non-numeric value : \'' || AS_VARCHAR(src:qpup) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qput), '0'), 38, 10) is null and 
                    src:qput is not null) THEN
                    'qput contains a non-numeric value : \'' || AS_VARCHAR(src:qput) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrcp), '0'), 38, 10) is null and 
                    src:qrcp is not null) THEN
                    'qrcp contains a non-numeric value : \'' || AS_VARCHAR(src:qrcp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrcr), '0'), 38, 10) is null and 
                    src:qrcr is not null) THEN
                    'qrcr contains a non-numeric value : \'' || AS_VARCHAR(src:qrcr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrec), '0'), 38, 10) is null and 
                    src:qrec is not null) THEN
                    'qrec contains a non-numeric value : \'' || AS_VARCHAR(src:qrec) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrej), '0'), 38, 10) is null and 
                    src:qrej is not null) THEN
                    'qrej contains a non-numeric value : \'' || AS_VARCHAR(src:qrej) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrpu), '0'), 38, 10) is null and 
                    src:qrpu is not null) THEN
                    'qrpu contains a non-numeric value : \'' || AS_VARCHAR(src:qrpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrsc), '0'), 38, 10) is null and 
                    src:qrsc is not null) THEN
                    'qrsc contains a non-numeric value : \'' || AS_VARCHAR(src:qrsc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrsp), '0'), 38, 10) is null and 
                    src:qrsp is not null) THEN
                    'qrsp contains a non-numeric value : \'' || AS_VARCHAR(src:qrsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcln), '0'), 38, 10) is null and 
                    src:rcln is not null) THEN
                    'rcln contains a non-numeric value : \'' || AS_VARCHAR(src:rcln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcun_ref_compnr), '0'), 38, 10) is null and 
                    src:rcun_ref_compnr is not null) THEN
                    'rcun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:resn_ref_compnr), '0'), 38, 10) is null and 
                    src:resn_ref_compnr is not null) THEN
                    'resn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:resn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc), '0'), 38, 10) is null and 
                    src:rowc is not null) THEN
                    'rowc contains a non-numeric value : \'' || AS_VARCHAR(src:rowc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is null and 
                    src:rowc_ref_compnr is not null) THEN
                    'rowc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rowc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) THEN
                    'rown_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rown_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) THEN
                    'sfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:shdt), '1900-01-01')) is null and 
                    src:shdt is not null) THEN
                    'shdt contains a non-timestamp value : \'' || AS_VARCHAR(src:shdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shsq), '0'), 38, 10) is null and 
                    src:shsq is not null) THEN
                    'shsq contains a non-numeric value : \'' || AS_VARCHAR(src:shsq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcst), '0'), 38, 10) is null and 
                    src:tcst is not null) THEN
                    'tcst contains a non-numeric value : \'' || AS_VARCHAR(src:tcst) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) THEN
                    'trdt contains a non-timestamp value : \'' || AS_VARCHAR(src:trdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn), '0'), 38, 10) is null and 
                    src:txtn is not null) THEN
                    'txtn contains a non-numeric value : \'' || AS_VARCHAR(src:txtn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn_ref_compnr), '0'), 38, 10) is null and 
                    src:txtn_ref_compnr is not null) THEN
                    'txtn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) THEN
                    'wght contains a non-numeric value : \'' || AS_VARCHAR(src:wght) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtun_ref_compnr), '0'), 38, 10) is null and 
                    src:wtun_ref_compnr is not null) THEN
                    'wtun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:wtun_ref_compnr) || '\'' WHEN 
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
                                    
                src:rcln,
                src:compnr,
                src:rcno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHINH312 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ardt), '1900-01-01')) is null and 
                    src:ardt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arej), '0'), 38, 10) is null and 
                    src:arej is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgxc), '0'), 38, 10) is null and 
                    src:bgxc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blck), '0'), 38, 10) is null and 
                    src:blck is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:casi_ref_compnr), '0'), 38, 10) is null and 
                    src:casi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdck), '0'), 38, 10) is null and 
                    src:cdck is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdcr), '0'), 38, 10) is null and 
                    src:cdcr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmpl), '0'), 38, 10) is null and 
                    src:cmpl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:conf), '0'), 38, 10) is null and 
                    src:conf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csvc_ref_compnr), '0'), 38, 10) is null and 
                    src:csvc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csvl), '0'), 38, 10) is null and 
                    src:csvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_dslo_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_dslo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_rclo_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_rclo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disp), '0'), 38, 10) is null and 
                    src:disp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse), '0'), 38, 10) is null and 
                    src:dmse is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn), '0'), 38, 10) is null and 
                    src:effn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:effn_ref_compnr), '0'), 38, 10) is null and 
                    src:effn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fire), '0'), 38, 10) is null and 
                    src:fire is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:huid_ref_compnr), '0'), 38, 10) is null and 
                    src:huid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hupr), '0'), 38, 10) is null and 
                    src:hupr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:idat), '1900-01-01')) is null and 
                    src:idat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:insp), '0'), 38, 10) is null and 
                    src:insp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:irdt), '1900-01-01')) is null and 
                    src:irdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_pkdf_ref_compnr), '0'), 38, 10) is null and 
                    src:item_pkdf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itxt), '0'), 38, 10) is null and 
                    src:itxt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itxt_ref_compnr), '0'), 38, 10) is null and 
                    src:itxt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsta), '0'), 38, 10) is null and 
                    src:lsta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oorg), '0'), 38, 10) is null and 
                    src:oorg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oorg_orno_pono_seqn_ref_compnr), '0'), 38, 10) is null and 
                    src:oorg_orno_pono_seqn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oset), '0'), 38, 10) is null and 
                    src:oset is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pkdf_ref_compnr), '0'), 38, 10) is null and 
                    src:pkdf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psqp), '0'), 38, 10) is null and 
                    src:psqp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psqr), '0'), 38, 10) is null and 
                    src:psqr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psqu), '0'), 38, 10) is null and 
                    src:psqu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psun_ref_compnr), '0'), 38, 10) is null and 
                    src:psun_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadi), '0'), 38, 10) is null and 
                    src:qadi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadp), '0'), 38, 10) is null and 
                    src:qadp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qadv), '0'), 38, 10) is null and 
                    src:qadv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapp), '0'), 38, 10) is null and 
                    src:qapp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapr), '0'), 38, 10) is null and 
                    src:qapr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qapu), '0'), 38, 10) is null and 
                    src:qapu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdep), '0'), 38, 10) is null and 
                    src:qdep is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdes), '0'), 38, 10) is null and 
                    src:qdes is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qdms), '0'), 38, 10) is null and 
                    src:qdms is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qppu), '0'), 38, 10) is null and 
                    src:qppu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpui), '0'), 38, 10) is null and 
                    src:qpui is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpup), '0'), 38, 10) is null and 
                    src:qpup is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qput), '0'), 38, 10) is null and 
                    src:qput is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrcp), '0'), 38, 10) is null and 
                    src:qrcp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrcr), '0'), 38, 10) is null and 
                    src:qrcr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrec), '0'), 38, 10) is null and 
                    src:qrec is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrej), '0'), 38, 10) is null and 
                    src:qrej is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrpu), '0'), 38, 10) is null and 
                    src:qrpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrsc), '0'), 38, 10) is null and 
                    src:qrsc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrsp), '0'), 38, 10) is null and 
                    src:qrsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcln), '0'), 38, 10) is null and 
                    src:rcln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcun_ref_compnr), '0'), 38, 10) is null and 
                    src:rcun_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:resn_ref_compnr), '0'), 38, 10) is null and 
                    src:resn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc), '0'), 38, 10) is null and 
                    src:rowc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is null and 
                    src:rowc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:shdt), '1900-01-01')) is null and 
                    src:shdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shsq), '0'), 38, 10) is null and 
                    src:shsq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tcst), '0'), 38, 10) is null and 
                    src:tcst is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:trdt), '1900-01-01')) is null and 
                    src:trdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn), '0'), 38, 10) is null and 
                    src:txtn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtn_ref_compnr), '0'), 38, 10) is null and 
                    src:txtn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtun_ref_compnr), '0'), 38, 10) is null and 
                    src:wtun_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)