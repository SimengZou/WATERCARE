CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALUE AS SELECT
                        src:ACCDEPREC::numeric(38, 10) AS ACCDEPREC, 
                        src:ACQDATE::datetime AS ACQDATE, 
                        src:ACQTYPE::varchar AS ACQTYPE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSVALKEY::integer AS ASSVALKEY, 
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
                        src:ISDISPOSED::varchar AS ISDISPOSED, 
                        src:LINEARFROMFT::numeric(38, 10) AS LINEARFROMFT, 
                        src:LINEARFROMM::numeric(38, 10) AS LINEARFROMM, 
                        src:LINEARTOFT::numeric(38, 10) AS LINEARTOFT, 
                        src:LINEARTOM::numeric(38, 10) AS LINEARTOM, 
                        src:MAINTEXP::numeric(38, 10) AS MAINTEXP, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PROFITLOSS::numeric(38, 10) AS PROFITLOSS, 
                        src:RESIDLIFE::numeric(38, 10) AS RESIDLIFE, 
                        src:RESIDVAL::numeric(38, 10) AS RESIDVAL, 
                        src:REVALAMT::numeric(38, 10) AS REVALAMT, 
                        src:RWATTRKEY::integer AS RWATTRKEY, 
                        src:RWATTRTYPE::varchar AS RWATTRTYPE, 
                        src:TAXVAL::numeric(38, 10) AS TAXVAL, 
                        src:TOBESOLDDATE::datetime AS TOBESOLDDATE, 
                        src:TOBESOLDDESC::varchar AS TOBESOLDDESC, 
                        src:VALDATE::datetime AS VALDATE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WRITEDNVAL::numeric(38, 10) AS WRITEDNVAL, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:ASSVALKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALUE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCDEPREC), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACQDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSVALKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BGTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BGTKEY2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BGTKEY3), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CAPTLDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CAPTLEXP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEPRECRATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEPRECTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEPRECYTD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEPRESIDLF), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DISJOURNALDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISPBGTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DISPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISPVAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EXPLIFE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INITCOST), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INSUREVAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARFROMFT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARFROMM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARTOFT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARTOM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAINTEXP), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROFITLOSS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RESIDLIFE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RESIDVAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVALAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RWATTRKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TAXVAL), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TOBESOLDDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VALDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WRITEDNVAL), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null