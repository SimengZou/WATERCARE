CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5RCODES AS SELECT
                        src:RCO_DESC::varchar AS RCO_DESC, 
                        src:RCO_LASTSAVED::datetime AS RCO_LASTSAVED, 
                        src:RCO_RCODE::varchar AS RCO_RCODE, 
                        src:RCO_RENTITY::varchar AS RCO_RENTITY, 
                        src:RCO_TEXTCODE::varchar AS RCO_TEXTCODE, 
                        src:RCO_UPDATECOUNT::numeric(38, 10) AS RCO_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
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
    
                        
                src:RCO_RCODE,
                src:RCO_RENTITY,
            src:RCO_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:RCO_RCODE,
                src:RCO_RENTITY  ORDER BY 
            src:RCO_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5RCODES as strm))