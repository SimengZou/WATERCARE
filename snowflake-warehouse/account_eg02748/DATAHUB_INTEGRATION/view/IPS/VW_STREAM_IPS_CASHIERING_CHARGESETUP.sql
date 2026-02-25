CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CASHIERING_CHARGESETUP AS SELECT
                        src:ACCESSID::integer AS ACCESSID, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BGTNO::integer AS BGTNO, 
                        src:CHARGEGROUP::varchar AS CHARGEGROUP, 
                        src:CHARGETYPE::varchar AS CHARGETYPE, 
                        src:CHGAMT::numeric(38, 10) AS CHGAMT, 
                        src:CHGDESC::varchar AS CHGDESC, 
                        src:CONVERSIONFORMULAKEY::integer AS CONVERSIONFORMULAKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DISPLAYEXTERNAL::varchar AS DISPLAYEXTERNAL, 
                        src:DISPLAYPAGE::integer AS DISPLAYPAGE, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:ESCROWDEP::varchar AS ESCROWDEP, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:IDTITLE::varchar AS IDTITLE, 
                        src:ISCONTACTREQUIRED::varchar AS ISCONTACTREQUIRED, 
                        src:MENUIMAGE::varchar AS MENUIMAGE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OVERALLOWED::varchar AS OVERALLOWED, 
                        src:PAYMENTFORMULA::integer AS PAYMENTFORMULA, 
                        src:QTYFLAG::varchar AS QTYFLAG, 
                        src:REVERSALFORMULA::integer AS REVERSALFORMULA, 
                        src:SEARCHPAGE::integer AS SEARCHPAGE, 
                        src:TAXFLAG::varchar AS TAXFLAG, 
                        src:TAXRATE::numeric(38, 10) AS TAXRATE, 
                        src:TENDERALLOWED::integer AS TENDERALLOWED, 
                        src:UNDERALLOWED::varchar AS UNDERALLOWED, 
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
                                        
                src:CHARGETYPE  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CASHIERING_CHARGESETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCESSID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BGTNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CHGAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONVERSIONFORMULAKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISPLAYPAGE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PAYMENTFORMULA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVERSALFORMULA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SEARCHPAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TAXRATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TENDERALLOWED), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null