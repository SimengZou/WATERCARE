CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5PPMPLAN AS SELECT
                        src:PMP_CLASS::varchar AS PMP_CLASS, 
                        src:PMP_CLASS_ORG::varchar AS PMP_CLASS_ORG, 
                        src:PMP_CODE::varchar AS PMP_CODE, 
                        src:PMP_DESC::varchar AS PMP_DESC, 
                        src:PMP_LASTSAVED::datetime AS PMP_LASTSAVED, 
                        src:PMP_OBJECTCLASS::varchar AS PMP_OBJECTCLASS, 
                        src:PMP_OBJECTCLASS_ORG::varchar AS PMP_OBJECTCLASS_ORG, 
                        src:PMP_ORG::varchar AS PMP_ORG, 
                        src:PMP_UPDATECOUNT::numeric(38, 10) AS PMP_UPDATECOUNT, 
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
    
                        
                src:PMP_CODE,
            src:PMP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PMP_CODE  ORDER BY 
            src:PMP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5PPMPLAN as strm))