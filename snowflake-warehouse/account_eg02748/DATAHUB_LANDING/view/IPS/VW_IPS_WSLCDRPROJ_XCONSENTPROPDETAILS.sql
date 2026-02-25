CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRPROJ_XCONSENTPROPDETAILS AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AKLCOUNCILPROPOSALNO::varchar AS AKLCOUNCILPROPOSALNO, 
                        src:APPROJAPPLDTLKEY::integer AS APPROJAPPLDTLKEY, 
                        src:CT::varchar AS CT, 
                        src:DELETED::boolean AS DELETED, 
                        src:DP::varchar AS DP, 
                        src:LEGALPROPSITEADDRESS::varchar AS LEGALPROPSITEADDRESS, 
                        src:LOT::varchar AS LOT, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WARD::varchar AS WARD, 
                        src:XCONSENTPROPDETAILSKEY::integer AS XCONSENTPROPDETAILSKEY, 
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
    
                        
                src:XCONSENTPROPDETAILSKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XCONSENTPROPDETAILSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRPROJ_XCONSENTPROPDETAILS as strm))