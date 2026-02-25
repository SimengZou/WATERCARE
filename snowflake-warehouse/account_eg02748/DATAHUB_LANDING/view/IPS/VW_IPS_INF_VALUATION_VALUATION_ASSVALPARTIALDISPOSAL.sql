CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSAL AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSVALKEY::integer AS ASSVALKEY, 
                        src:ASSVALPARTIALDISPOSALKEY::integer AS ASSVALPARTIALDISPOSALKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DISPOSALDATE::datetime AS DISPOSALDATE, 
                        src:DISPOSALPERCENTAGE::numeric(38, 10) AS DISPOSALPERCENTAGE, 
                        src:DISPOSALQUANTITY::numeric(38, 10) AS DISPOSALQUANTITY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OPENINGQUANTITY::numeric(38, 10) AS OPENINGQUANTITY, 
                        src:PROCEEDSONDISPOSAL::numeric(38, 10) AS PROCEEDSONDISPOSAL, 
                        src:UNITTYPE::varchar AS UNITTYPE, 
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
    
                        
                src:ASSVALPARTIALDISPOSALKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ASSVALPARTIALDISPOSALKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSAL as strm))