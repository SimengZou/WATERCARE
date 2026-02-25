CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_ACCTTRAN AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADJUSTMENTKEY::integer AS ADJUSTMENTKEY, 
                        src:APPFEEKEY::integer AS APPFEEKEY, 
                        src:COMMENTSKEY::integer AS COMMENTSKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DEPOSITTRANSACTIONKEY::integer AS DEPOSITTRANSACTIONKEY, 
                        src:INTERNALFLAG::varchar AS INTERNALFLAG, 
                        src:LINEITEMKEY::integer AS LINEITEMKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEEDSJOURNAL::varchar AS NEEDSJOURNAL, 
                        src:ONEOFFCHARGEKEY::integer AS ONEOFFCHARGEKEY, 
                        src:PAYKEY::integer AS PAYKEY, 
                        src:PENALTYKEY::integer AS PENALTYKEY, 
                        src:REFTRANNO::integer AS REFTRANNO, 
                        src:REFUNDKEY::integer AS REFUNDKEY, 
                        src:SOURCEKEY::integer AS SOURCEKEY, 
                        src:SOURCETYPE::integer AS SOURCETYPE, 
                        src:TRANAMT::numeric(38, 10) AS TRANAMT, 
                        src:TRANBY::varchar AS TRANBY, 
                        src:TRANDATE::datetime AS TRANDATE, 
                        src:TRANNO::integer AS TRANNO, 
                        src:TRANSACTIONDESIGNATOR::integer AS TRANSACTIONDESIGNATOR, 
                        src:TRANSACTIONTYPE::integer AS TRANSACTIONTYPE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
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
    
                        
                src:TRANNO,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:TRANNO  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_ACCTTRAN as strm))