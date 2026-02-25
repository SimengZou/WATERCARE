CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5SCHEDGROUPS AS SELECT
                        src:SCG_CLASS::varchar AS SCG_CLASS, 
                        src:SCG_CLASS_ORG::varchar AS SCG_CLASS_ORG, 
                        src:SCG_CODE::varchar AS SCG_CODE, 
                        src:SCG_DESC::varchar AS SCG_DESC, 
                        src:SCG_LASTSAVED::datetime AS SCG_LASTSAVED, 
                        src:SCG_NOTUSED::varchar AS SCG_NOTUSED, 
                        src:SCG_ORG::varchar AS SCG_ORG, 
                        src:SCG_PROFILEPICTURE::varchar AS SCG_PROFILEPICTURE, 
                        src:SCG_UPDATECOUNT::numeric(38, 10) AS SCG_UPDATECOUNT, 
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
    
                        
                src:SCG_CODE,
            src:SCG_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:SCG_CODE  ORDER BY 
            src:SCG_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5SCHEDGROUPS as strm))