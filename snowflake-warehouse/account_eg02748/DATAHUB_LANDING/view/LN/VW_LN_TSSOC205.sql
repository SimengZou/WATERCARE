CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TSSOC205 AS SELECT
                        src:acdu::numeric(38, 17) AS acdu, 
                        src:acln::integer AS acln, 
                        src:actm::datetime AS actm, 
                        src:actp::integer AS actp, 
                        src:actp_kw::varchar AS actp_kw, 
                        src:aftm::datetime AS aftm, 
                        src:astm::datetime AS astm, 
                        src:atft::datetime AS atft, 
                        src:atst::datetime AS atst, 
                        src:cmbb_orno_acln_ref_compnr::integer AS cmbb_orno_acln_ref_compnr, 
                        src:cmbc_orno_acln_ref_compnr::integer AS cmbc_orno_acln_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dltm::datetime AS dltm, 
                        src:emno::varchar AS emno, 
                        src:emno_ref_compnr::integer AS emno_ref_compnr, 
                        src:esdt::datetime AS esdt, 
                        src:jctm::datetime AS jctm, 
                        src:line::integer AS line, 
                        src:ltct::datetime AS ltct, 
                        src:orgn::integer AS orgn, 
                        src:orgn_kw::varchar AS orgn_kw, 
                        src:orno::varchar AS orno, 
                        src:pftm::datetime AS pftm, 
                        src:pgcn::integer AS pgcn, 
                        src:pgdt::datetime AS pgdt, 
                        src:pged::integer AS pged, 
                        src:pged_kw::varchar AS pged_kw, 
                        src:pgit::integer AS pgit, 
                        src:pgrd::integer AS pgrd, 
                        src:pgrd_kw::varchar AS pgrd_kw, 
                        src:pstm::datetime AS pstm, 
                        src:ptft::datetime AS ptft, 
                        src:ptst::datetime AS ptst, 
                        src:rejr::varchar AS rejr, 
                        src:rejr_ref_compnr::integer AS rejr_ref_compnr, 
                        src:rjtm::datetime AS rjtm, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:stat::integer AS stat, 
                        src:stat_kw::varchar AS stat_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:trdi::numeric(38, 17) AS trdi, 
                        src:trdi_buln::numeric(38, 17) AS trdi_buln, 
                        src:trdu::numeric(38, 17) AS trdu, 
                        src:trdu_butm::numeric(38, 17) AS trdu_butm, 
                        src:txta::integer AS txta, 
                        src:txta_ref_compnr::integer AS txta_ref_compnr, 
                        src:uecp::integer AS uecp, 
                        src:uecp_kw::varchar AS uecp_kw, 
                        src:username::varchar AS username, 
            CASE
                WHEN 'LN' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'LN' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:orgn,
                src:compnr,
                src:line,
                src:orno,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:orgn,
                src:compnr,
                src:line,
                src:orno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TSSOC205 as strm))