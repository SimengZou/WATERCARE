CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_DEPM_CUBE_LEDGER_ERROR AS SELECT src, 'DEPM_CUBE_LEDGER' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:Timestamp), '1900-01-01')) is null and 
                    src:Timestamp is not null) THEN
                    'Timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:Timestamp) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:Timestamp), '1900-01-01')) is null and 
                src:Timestamp is not null) THEN
                'Timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:Timestamp) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,etl_load_datetime, etl_load_metadatafilename FROM (SELECT * FROM ( SELECT
            src,
            etl_load_datetime,
            etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
                                    
                    src:Analysis09,
                    src:Analysis02,
                    src:Currency,
                    src:Analysis04,
                    src:Period,
                    src:Analysis07,
                    src:Analysis01,
                    src:Analysis10,
                    src:Analysis05,
                    src:Version,
                    src:Account,
                    src:LedgerMeas,
                    src:Analysis08,
                    src:Analysis03,
                    src:LdgTrxType,
                    src:Analysis06,
                    src:Year  ORDER BY src:Timestamp desc, src:Value desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_DEPM_CUBE_LEDGER as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:Timestamp), '1900-01-01')) is null and 
                    src:Timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:Timestamp), '1900-01-01')) is null and 
                src:Timestamp is not null)