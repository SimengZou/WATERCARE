CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_PAYMENT AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCOUNTNUMBER::varchar AS ACCOUNTNUMBER, 
                        src:ACCOUNTTYPE::integer AS ACCOUNTTYPE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADJFLAG::varchar AS ADJFLAG, 
                        src:AMOUNT::numeric(38, 10) AS AMOUNT, 
                        src:AMTCONF::numeric(38, 10) AS AMTCONF, 
                        src:APPLYCONC::varchar AS APPLYCONC, 
                        src:BATCHKEY::integer AS BATCHKEY, 
                        src:BILLTYPE::integer AS BILLTYPE, 
                        src:CARDAUTH::varchar AS CARDAUTH, 
                        src:CARDBANK::varchar AS CARDBANK, 
                        src:CARDEXPIREDATE::datetime AS CARDEXPIREDATE, 
                        src:CARDNAME::varchar AS CARDNAME, 
                        src:CARDNO::varchar AS CARDNO, 
                        src:CARDTYPE::varchar AS CARDTYPE, 
                        src:CHECKACCOUNTNUMBER::varchar AS CHECKACCOUNTNUMBER, 
                        src:CHECKACCOUNTTYPE::varchar AS CHECKACCOUNTTYPE, 
                        src:CHECKBANK::varchar AS CHECKBANK, 
                        src:CHECKID::varchar AS CHECKID, 
                        src:CHECKIDTYPE::varchar AS CHECKIDTYPE, 
                        src:CHECKNAME::varchar AS CHECKNAME, 
                        src:CHECKNO::varchar AS CHECKNO, 
                        src:CHECKROUTINGNUMBER::varchar AS CHECKROUTINGNUMBER, 
                        src:CNTCTKEY::integer AS CNTCTKEY, 
                        src:COMMENTS::varchar AS COMMENTS, 
                        src:CONFDTTM::datetime AS CONFDTTM, 
                        src:DELETED::boolean AS DELETED, 
                        src:DISPUTEDFLAG::varchar AS DISPUTEDFLAG, 
                        src:DONATEAMT::numeric(38, 10) AS DONATEAMT, 
                        src:DONATEFLAG::varchar AS DONATEFLAG, 
                        src:DRAWERTRANNO::integer AS DRAWERTRANNO, 
                        src:ENTNO::varchar AS ENTNO, 
                        src:ESCROWACCOUNTKEY::integer AS ESCROWACCOUNTKEY, 
                        src:ESCROWTRNNO::integer AS ESCROWTRNNO, 
                        src:METHOD::varchar AS METHOD, 
                        src:MISCISSUEDBY::varchar AS MISCISSUEDBY, 
                        src:MISCREFNO::varchar AS MISCREFNO, 
                        src:MISCTYPE::varchar AS MISCTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAYKEY::integer AS PAYKEY, 
                        src:PAYMENTREVERSALCODE::varchar AS PAYMENTREVERSALCODE, 
                        src:PAYSPECIFICBILLSFLAG::varchar AS PAYSPECIFICBILLSFLAG, 
                        src:PENDTYPE::varchar AS PENDTYPE, 
                        src:RECDBY::varchar AS RECDBY, 
                        src:RECDDTTM::datetime AS RECDDTTM, 
                        src:REGTRANNO::integer AS REGTRANNO, 
                        src:RESBY::varchar AS RESBY, 
                        src:RESCODE::varchar AS RESCODE, 
                        src:RESDTTM::datetime AS RESDTTM, 
                        src:SOURCE::varchar AS SOURCE, 
                        src:STATUS::varchar AS STATUS, 
                        src:TRACENO::integer AS TRACENO, 
                        src:TRANSFERREDFROMPAYMENT::integer AS TRANSFERREDFROMPAYMENT, 
                        src:UNMATCHEDPROCESS::varchar AS UNMATCHEDPROCESS, 
                        src:UNMATCHEDPROCESSBY::varchar AS UNMATCHEDPROCESSBY, 
                        src:UNMATCHEDPROCESSDATETIME::datetime AS UNMATCHEDPROCESSDATETIME, 
                        src:UNMATCHEDPROCESSEXCEPTION::varchar AS UNMATCHEDPROCESSEXCEPTION, 
                        src:UNMATCRES::varchar AS UNMATCRES, 
                        src:VARIATION_ID::integer AS VARIATION_ID, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
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
                                        
                src:PAYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_PAYMENT as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AMTCONF), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BATCHKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CARDEXPIREDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CONFDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DONATEAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DRAWERTRANNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESCROWACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESCROWTRNNO), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PAYKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RECDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REGTRANNO), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RESDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRACENO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRANSFERREDFROMPAYMENT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UNMATCHEDPROCESSDATETIME), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null