CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CASHIERING_CASHIERINGSETUP_ERROR AS SELECT src, 'IPS_CASHIERING_CASHIERINGSETUP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL1), '0'), 38, 10) is null and 
                    src:BILL1 is not null) THEN
                    'BILL1 contains a non-numeric value : \'' || AS_VARCHAR(src:BILL1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL2), '0'), 38, 10) is null and 
                    src:BILL2 is not null) THEN
                    'BILL2 contains a non-numeric value : \'' || AS_VARCHAR(src:BILL2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL3), '0'), 38, 10) is null and 
                    src:BILL3 is not null) THEN
                    'BILL3 contains a non-numeric value : \'' || AS_VARCHAR(src:BILL3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL4), '0'), 38, 10) is null and 
                    src:BILL4 is not null) THEN
                    'BILL4 contains a non-numeric value : \'' || AS_VARCHAR(src:BILL4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL5), '0'), 38, 10) is null and 
                    src:BILL5 is not null) THEN
                    'BILL5 contains a non-numeric value : \'' || AS_VARCHAR(src:BILL5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL6), '0'), 38, 10) is null and 
                    src:BILL6 is not null) THEN
                    'BILL6 contains a non-numeric value : \'' || AS_VARCHAR(src:BILL6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL7), '0'), 38, 10) is null and 
                    src:BILL7 is not null) THEN
                    'BILL7 contains a non-numeric value : \'' || AS_VARCHAR(src:BILL7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL8), '0'), 38, 10) is null and 
                    src:BILL8 is not null) THEN
                    'BILL8 contains a non-numeric value : \'' || AS_VARCHAR(src:BILL8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASHBGTNO), '0'), 38, 10) is null and 
                    src:CASHBGTNO is not null) THEN
                    'CASHBGTNO contains a non-numeric value : \'' || AS_VARCHAR(src:CASHBGTNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASHSUKEY), '0'), 38, 10) is null and 
                    src:CASHSUKEY is not null) THEN
                    'CASHSUKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CASHSUKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN1), '0'), 38, 10) is null and 
                    src:COIN1 is not null) THEN
                    'COIN1 contains a non-numeric value : \'' || AS_VARCHAR(src:COIN1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN2), '0'), 38, 10) is null and 
                    src:COIN2 is not null) THEN
                    'COIN2 contains a non-numeric value : \'' || AS_VARCHAR(src:COIN2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN3), '0'), 38, 10) is null and 
                    src:COIN3 is not null) THEN
                    'COIN3 contains a non-numeric value : \'' || AS_VARCHAR(src:COIN3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN4), '0'), 38, 10) is null and 
                    src:COIN4 is not null) THEN
                    'COIN4 contains a non-numeric value : \'' || AS_VARCHAR(src:COIN4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN5), '0'), 38, 10) is null and 
                    src:COIN5 is not null) THEN
                    'COIN5 contains a non-numeric value : \'' || AS_VARCHAR(src:COIN5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN6), '0'), 38, 10) is null and 
                    src:COIN6 is not null) THEN
                    'COIN6 contains a non-numeric value : \'' || AS_VARCHAR(src:COIN6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN7), '0'), 38, 10) is null and 
                    src:COIN7 is not null) THEN
                    'COIN7 contains a non-numeric value : \'' || AS_VARCHAR(src:COIN7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN8), '0'), 38, 10) is null and 
                    src:COIN8 is not null) THEN
                    'COIN8 contains a non-numeric value : \'' || AS_VARCHAR(src:COIN8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL1), '0'), 38, 10) is null and 
                    src:COINROLL1 is not null) THEN
                    'COINROLL1 contains a non-numeric value : \'' || AS_VARCHAR(src:COINROLL1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL2), '0'), 38, 10) is null and 
                    src:COINROLL2 is not null) THEN
                    'COINROLL2 contains a non-numeric value : \'' || AS_VARCHAR(src:COINROLL2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL3), '0'), 38, 10) is null and 
                    src:COINROLL3 is not null) THEN
                    'COINROLL3 contains a non-numeric value : \'' || AS_VARCHAR(src:COINROLL3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL4), '0'), 38, 10) is null and 
                    src:COINROLL4 is not null) THEN
                    'COINROLL4 contains a non-numeric value : \'' || AS_VARCHAR(src:COINROLL4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL5), '0'), 38, 10) is null and 
                    src:COINROLL5 is not null) THEN
                    'COINROLL5 contains a non-numeric value : \'' || AS_VARCHAR(src:COINROLL5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL6), '0'), 38, 10) is null and 
                    src:COINROLL6 is not null) THEN
                    'COINROLL6 contains a non-numeric value : \'' || AS_VARCHAR(src:COINROLL6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL7), '0'), 38, 10) is null and 
                    src:COINROLL7 is not null) THEN
                    'COINROLL7 contains a non-numeric value : \'' || AS_VARCHAR(src:COINROLL7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL8), '0'), 38, 10) is null and 
                    src:COINROLL8 is not null) THEN
                    'COINROLL8 contains a non-numeric value : \'' || AS_VARCHAR(src:COINROLL8) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RECEIPTFRMLAKEY), '0'), 38, 10) is null and 
                    src:RECEIPTFRMLAKEY is not null) THEN
                    'RECEIPTFRMLAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RECEIPTFRMLAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TAXRATE), '0'), 38, 10) is null and 
                    src:TAXRATE is not null) THEN
                    'TAXRATE contains a non-numeric value : \'' || AS_VARCHAR(src:TAXRATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null) THEN
                'VARIATION_ID contains a non-timestamp value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:CASHSUKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CASHIERING_CASHIERINGSETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL1), '0'), 38, 10) is null and 
                    src:BILL1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL2), '0'), 38, 10) is null and 
                    src:BILL2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL3), '0'), 38, 10) is null and 
                    src:BILL3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL4), '0'), 38, 10) is null and 
                    src:BILL4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL5), '0'), 38, 10) is null and 
                    src:BILL5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL6), '0'), 38, 10) is null and 
                    src:BILL6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL7), '0'), 38, 10) is null and 
                    src:BILL7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILL8), '0'), 38, 10) is null and 
                    src:BILL8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASHBGTNO), '0'), 38, 10) is null and 
                    src:CASHBGTNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASHSUKEY), '0'), 38, 10) is null and 
                    src:CASHSUKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN1), '0'), 38, 10) is null and 
                    src:COIN1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN2), '0'), 38, 10) is null and 
                    src:COIN2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN3), '0'), 38, 10) is null and 
                    src:COIN3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN4), '0'), 38, 10) is null and 
                    src:COIN4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN5), '0'), 38, 10) is null and 
                    src:COIN5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN6), '0'), 38, 10) is null and 
                    src:COIN6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN7), '0'), 38, 10) is null and 
                    src:COIN7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COIN8), '0'), 38, 10) is null and 
                    src:COIN8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL1), '0'), 38, 10) is null and 
                    src:COINROLL1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL2), '0'), 38, 10) is null and 
                    src:COINROLL2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL3), '0'), 38, 10) is null and 
                    src:COINROLL3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL4), '0'), 38, 10) is null and 
                    src:COINROLL4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL5), '0'), 38, 10) is null and 
                    src:COINROLL5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL6), '0'), 38, 10) is null and 
                    src:COINROLL6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL7), '0'), 38, 10) is null and 
                    src:COINROLL7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COINROLL8), '0'), 38, 10) is null and 
                    src:COINROLL8 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RECEIPTFRMLAKEY), '0'), 38, 10) is null and 
                    src:RECEIPTFRMLAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TAXRATE), '0'), 38, 10) is null and 
                    src:TAXRATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)