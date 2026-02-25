CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSALGD AS SELECT
                        src:ACCUMDEPRECAFTERDISPOSAL::numeric(38, 10) AS ACCUMDEPRECAFTERDISPOSAL, 
                        src:ACCUMDEPRECAMOUNT::numeric(38, 10) AS ACCUMDEPRECAMOUNT, 
                        src:ACCUMDEPRECBEFOREDISPOSAL::numeric(38, 10) AS ACCUMDEPRECBEFOREDISPOSAL, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSVALPARTIALDISPOSALGDKEY::integer AS ASSVALPARTIALDISPOSALGDKEY, 
                        src:ASSVALPARTIALDISPOSALKEY::integer AS ASSVALPARTIALDISPOSALKEY, 
                        src:BOOKID::varchar AS BOOKID, 
                        src:CURRENTCOSTAFTERDISPOSAL::numeric(38, 10) AS CURRENTCOSTAFTERDISPOSAL, 
                        src:CURRENTCOSTB4DISPOSAL::numeric(38, 10) AS CURRENTCOSTB4DISPOSAL, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DISPOSALDATE::datetime AS DISPOSALDATE, 
                        src:DISPOSALNUMBER::integer AS DISPOSALNUMBER, 
                        src:DISPOSALVALUE::numeric(38, 10) AS DISPOSALVALUE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OPENINGQUANTITY::numeric(38, 10) AS OPENINGQUANTITY, 
                        src:PERCENTAGEAFTERDISPOSAL::numeric(38, 10) AS PERCENTAGEAFTERDISPOSAL, 
                        src:PERCENTAGEB4DISPOSAL::numeric(38, 10) AS PERCENTAGEB4DISPOSAL, 
                        src:PERCENTAGEDISPOSED::numeric(38, 10) AS PERCENTAGEDISPOSED, 
                        src:PROFITLOSS::numeric(38, 10) AS PROFITLOSS, 
                        src:QUANTITYAFTERDISPOSAL::numeric(38, 10) AS QUANTITYAFTERDISPOSAL, 
                        src:QUANTITYB4DISPOSAL::numeric(38, 10) AS QUANTITYB4DISPOSAL, 
                        src:QUANTITYDISPOSED::numeric(38, 10) AS QUANTITYDISPOSED, 
                        src:UNITTYPE::varchar AS UNITTYPE, 
                        src:VALUATIONKEY::integer AS VALUATIONKEY, 
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
                                        
                src:ASSVALPARTIALDISPOSALGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSALGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCUMDEPRECAFTERDISPOSAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCUMDEPRECAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCUMDEPRECBEFOREDISPOSAL), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSVALPARTIALDISPOSALGDKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSVALPARTIALDISPOSALKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTCOSTAFTERDISPOSAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTCOSTB4DISPOSAL), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DISPOSALDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISPOSALNUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISPOSALVALUE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OPENINGQUANTITY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PERCENTAGEAFTERDISPOSAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PERCENTAGEB4DISPOSAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PERCENTAGEDISPOSED), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROFITLOSS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:QUANTITYAFTERDISPOSAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:QUANTITYB4DISPOSAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:QUANTITYDISPOSED), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VALUATIONKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null