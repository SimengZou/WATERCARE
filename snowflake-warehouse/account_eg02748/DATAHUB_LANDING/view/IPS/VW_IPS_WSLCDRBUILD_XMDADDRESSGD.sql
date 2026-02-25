CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRBUILD_XMDADDRESSGD AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CT::varchar AS CT, 
                        src:CUSTOMERREFERENCE::varchar AS CUSTOMERREFERENCE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DEVCONTPAID::varchar AS DEVCONTPAID, 
                        src:DP::varchar AS DP, 
                        src:FLOORAREALT65M2::varchar AS FLOORAREALT65M2, 
                        src:HOUSENO::varchar AS HOUSENO, 
                        src:LOT::varchar AS LOT, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:POSTCODE::varchar AS POSTCODE, 
                        src:STREETNAME::varchar AS STREETNAME, 
                        src:STTYPE::varchar AS STTYPE, 
                        src:SUBURB::varchar AS SUBURB, 
                        src:UNIT::varchar AS UNIT, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XBUILDMULCONNECTIONSKEY::integer AS XBUILDMULCONNECTIONSKEY, 
                        src:XMDADDRESSGDKEY::integer AS XMDADDRESSGDKEY, 
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
    
                        
                src:XMDADDRESSGDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XMDADDRESSGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRBUILD_XMDADDRESSGD as strm))