CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5PMFORECASTEQUIPLIST AS SELECT
                        src:PFL_LASTSAVED::datetime AS PFL_LASTSAVED, 
                        src:PFL_OBJECT::varchar AS PFL_OBJECT, 
                        src:PFL_OBJECT_ORG::varchar AS PFL_OBJECT_ORG, 
                        src:PFL_PARENT::varchar AS PFL_PARENT, 
                        src:PFL_PARENT_ORG::varchar AS PFL_PARENT_ORG, 
                        src:PFL_SELECT::varchar AS PFL_SELECT, 
                        src:PFL_SESSIONID::numeric(38, 10) AS PFL_SESSIONID, 
                        src:PFL_UPDATECOUNT::numeric(38, 10) AS PFL_UPDATECOUNT, 
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
    
                        
                src:PFL_OBJECT,
                src:PFL_OBJECT_ORG,
                src:PFL_SESSIONID,
            src:PFL_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PFL_OBJECT,
                src:PFL_OBJECT_ORG,
                src:PFL_SESSIONID  ORDER BY 
            src:PFL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5PMFORECASTEQUIPLIST as strm))