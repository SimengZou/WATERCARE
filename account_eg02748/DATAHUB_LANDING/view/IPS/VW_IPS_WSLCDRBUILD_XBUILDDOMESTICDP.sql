CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRBUILD_XBUILDDOMESTICDP AS SELECT
                        src:ACCOUNTNO::varchar AS ACCOUNTNO, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDITIONALDETAILS::varchar AS ADDITIONALDETAILS, 
                        src:APBLDGAPPLDTLKEY::integer AS APBLDGAPPLDTLKEY, 
                        src:BILLADDRESS::varchar AS BILLADDRESS, 
                        src:BILLMETHOD::varchar AS BILLMETHOD, 
                        src:BILLOWNERORAPPLICANT::varchar AS BILLOWNERORAPPLICANT, 
                        src:BUILDCONSENTNO::varchar AS BUILDCONSENTNO, 
                        src:CT::varchar AS CT, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DEVCONTPAID::varchar AS DEVCONTPAID, 
                        src:DISCONNECTIONREASON::varchar AS DISCONNECTIONREASON, 
                        src:DP::varchar AS DP, 
                        src:FLOORAREALT65::varchar AS FLOORAREALT65, 
                        src:INSTALLIN10DAYS::varchar AS INSTALLIN10DAYS, 
                        src:LOT::varchar AS LOT, 
                        src:METER::integer AS METER, 
                        src:METERNO::varchar AS METERNO, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEIGHBOURHOOD::varchar AS NEIGHBOURHOOD, 
                        src:RELATIONTOPROPERTY::varchar AS RELATIONTOPROPERTY, 
                        src:RELOCATIONTYPE::varchar AS RELOCATIONTYPE, 
                        src:SHAREDTO1REASON::varchar AS SHAREDTO1REASON, 
                        src:SITECONNECTEDTOWASTEWATER::varchar AS SITECONNECTEDTOWASTEWATER, 
                        src:SITECONNECTEDTOWATER::varchar AS SITECONNECTEDTOWATER, 
                        src:STAGE::varchar AS STAGE, 
                        src:SUPERLOT::varchar AS SUPERLOT, 
                        src:TENURE::varchar AS TENURE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XBUILDDOEMSTICDETAILSKEY::integer AS XBUILDDOEMSTICDETAILSKEY, 
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
    
                        
                src:XBUILDDOEMSTICDETAILSKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XBUILDDOEMSTICDETAILSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRBUILD_XBUILDDOMESTICDP as strm))