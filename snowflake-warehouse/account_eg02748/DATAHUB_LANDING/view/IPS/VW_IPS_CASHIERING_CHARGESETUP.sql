CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CASHIERING_CHARGESETUP AS SELECT
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
    
                        
                src:CHARGETYPE,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CHARGETYPE  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CASHIERING_CHARGESETUP as strm))