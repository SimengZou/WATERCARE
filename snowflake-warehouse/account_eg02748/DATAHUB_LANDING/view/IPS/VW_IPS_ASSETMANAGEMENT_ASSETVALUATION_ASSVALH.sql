CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALH AS SELECT
                        src:ACCDEPREC::numeric(38, 10) AS ACCDEPREC, 
                        src:ACCPERIOD::integer AS ACCPERIOD, 
                        src:ACQDATE::datetime AS ACQDATE, 
                        src:ACQTYPE::varchar AS ACQTYPE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSVALHISTKEY::integer AS ASSVALHISTKEY, 
                        src:ASSVALKEY::integer AS ASSVALKEY, 
                        src:ASSVALRUNKEY::integer AS ASSVALRUNKEY, 
                        src:BACKDATEADJFLAG::varchar AS BACKDATEADJFLAG, 
                        src:BGTKEY::integer AS BGTKEY, 
                        src:BGTKEY2::integer AS BGTKEY2, 
                        src:BGTKEY3::integer AS BGTKEY3, 
                        src:BGTNO::varchar AS BGTNO, 
                        src:BOOKID::varchar AS BOOKID, 
                        src:CAPTLDATE::datetime AS CAPTLDATE, 
                        src:CAPTLEXP::numeric(38, 10) AS CAPTLEXP, 
                        src:CATEGORY::varchar AS CATEGORY, 
                        src:CLASS::varchar AS CLASS, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:COMPTYPE::integer AS COMPTYPE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DEPRECRATE::numeric(38, 10) AS DEPRECRATE, 
                        src:DEPRECTYPE::integer AS DEPRECTYPE, 
                        src:DEPRECYTD::numeric(38, 10) AS DEPRECYTD, 
                        src:DEPRESIDLF::numeric(38, 10) AS DEPRESIDLF, 
                        src:DISJOURNALDATE::datetime AS DISJOURNALDATE, 
                        src:DISPBGTKEY::integer AS DISPBGTKEY, 
                        src:DISPBGTNO::varchar AS DISPBGTNO, 
                        src:DISPDATE::datetime AS DISPDATE, 
                        src:DISPDESC::varchar AS DISPDESC, 
                        src:DISPVAL::numeric(38, 10) AS DISPVAL, 
                        src:EXPLIFE::numeric(38, 10) AS EXPLIFE, 
                        src:INITCOST::numeric(38, 10) AS INITCOST, 
                        src:INSUREVAL::numeric(38, 10) AS INSUREVAL, 
                        src:LINEARFROMFT::numeric(38, 10) AS LINEARFROMFT, 
                        src:LINEARFROMM::numeric(38, 10) AS LINEARFROMM, 
                        src:LINEARTOFT::numeric(38, 10) AS LINEARTOFT, 
                        src:LINEARTOM::numeric(38, 10) AS LINEARTOM, 
                        src:MAINTEXP::numeric(38, 10) AS MAINTEXP, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PROFITLOSS::numeric(38, 10) AS PROFITLOSS, 
                        src:RECLNO::integer AS RECLNO, 
                        src:RESIDLIFE::numeric(38, 10) AS RESIDLIFE, 
                        src:RESIDVAL::numeric(38, 10) AS RESIDVAL, 
                        src:REVALAMT::numeric(38, 10) AS REVALAMT, 
                        src:ROLLEDBACKFLAG::varchar AS ROLLEDBACKFLAG, 
                        src:RWATTRKEY::integer AS RWATTRKEY, 
                        src:RWATTRTYPE::varchar AS RWATTRTYPE, 
                        src:TAXVAL::numeric(38, 10) AS TAXVAL, 
                        src:TOBESOLDDATE::datetime AS TOBESOLDDATE, 
                        src:TOBESOLDDESC::varchar AS TOBESOLDDESC, 
                        src:VALDATE::datetime AS VALDATE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WRITEDNVAL::numeric(38, 10) AS WRITEDNVAL, 
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
    
                        
                src:ASSVALHISTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ASSVALHISTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALH as strm))