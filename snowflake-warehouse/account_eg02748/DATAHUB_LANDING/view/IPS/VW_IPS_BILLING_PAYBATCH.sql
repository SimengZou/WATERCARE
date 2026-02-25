CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_PAYBATCH AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AUTOPOST::varchar AS AUTOPOST, 
                        src:BACKFLAG::varchar AS BACKFLAG, 
                        src:BATCFRDTTM::datetime AS BATCFRDTTM, 
                        src:BATCHAMT::numeric(38, 10) AS BATCHAMT, 
                        src:BATCHCT::integer AS BATCHCT, 
                        src:BATCHDESC::varchar AS BATCHDESC, 
                        src:BATCHKEY::integer AS BATCHKEY, 
                        src:BATCHNO::varchar AS BATCHNO, 
                        src:BATCHSTAT::varchar AS BATCHSTAT, 
                        src:BATCTODTTM::datetime AS BATCTODTTM, 
                        src:COMMENTSKEY::integer AS COMMENTSKEY, 
                        src:CRPENALTYFLAG::varchar AS CRPENALTYFLAG, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DIRECTDEBITRUNKEY::integer AS DIRECTDEBITRUNKEY, 
                        src:DPFLAG::varchar AS DPFLAG, 
                        src:DRAWERBATCHGROUP::varchar AS DRAWERBATCHGROUP, 
                        src:FORTRANSFERPAYMENTFLAG::varchar AS FORTRANSFERPAYMENTFLAG, 
                        src:LASTINVOCATIONSTATUS::integer AS LASTINVOCATIONSTATUS, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAYMENTIMPORTRUNFLAG::varchar AS PAYMENTIMPORTRUNFLAG, 
                        src:POSTBY::varchar AS POSTBY, 
                        src:POSTDTTM::datetime AS POSTDTTM, 
                        src:POSTFLAG::varchar AS POSTFLAG, 
                        src:RDYPSTFLAG::varchar AS RDYPSTFLAG, 
                        src:REVERSALREASONCODE::varchar AS REVERSALREASONCODE, 
                        src:SCHEDULEKEY::integer AS SCHEDULEKEY, 
                        src:SRC::varchar AS SRC, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VOIDEDBY::varchar AS VOIDEDBY, 
                        src:VOIDEDDATE::datetime AS VOIDEDDATE, 
                        src:WAIVEFLAG::varchar AS WAIVEFLAG, 
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
    
                        
                src:BATCHKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:BATCHKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_PAYBATCH as strm))