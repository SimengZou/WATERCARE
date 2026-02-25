CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRBUILD_XBUILDWOINSP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APBLDGINSPDTLKEY::integer AS APBLDGINSPDTLKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:HVCLEARANCE::varchar AS HVCLEARANCE, 
                        src:MAINSLOCATEDMARKED::varchar AS MAINSLOCATEDMARKED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OBERVEOHSCOMPLIANCE::varchar AS OBERVEOHSCOMPLIANCE, 
                        src:PILESFOUNDATIONEXCAVATED::varchar AS PILESFOUNDATIONEXCAVATED, 
                        src:PILESFOUNDATIONMARKED::varchar AS PILESFOUNDATIONMARKED, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WORKSCOMPLETED::varchar AS WORKSCOMPLETED, 
                        src:XBUILDWOINSPKEY::integer AS XBUILDWOINSPKEY, 
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
    
                        
                src:XBUILDWOINSPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XBUILDWOINSPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRBUILD_XBUILDWOINSP as strm))