CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5HAZARDPRECAUTIONS AS SELECT
                        src:HZP_HAZARD::varchar AS HZP_HAZARD, 
                        src:HZP_HAZARD_ORG::varchar AS HZP_HAZARD_ORG, 
                        src:HZP_LASTSAVED::datetime AS HZP_LASTSAVED, 
                        src:HZP_PRECAUTION::varchar AS HZP_PRECAUTION, 
                        src:HZP_PRECAUTION_ORG::varchar AS HZP_PRECAUTION_ORG, 
                        src:HZP_REVISION::numeric(38, 10) AS HZP_REVISION, 
                        src:HZP_SEQUENCE::numeric(38, 10) AS HZP_SEQUENCE, 
                        src:HZP_UPDATECOUNT::numeric(38, 10) AS HZP_UPDATECOUNT, 
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
    
                        
                src:HZP_HAZARD,
                src:HZP_HAZARD_ORG,
                src:HZP_PRECAUTION,
                src:HZP_PRECAUTION_ORG,
                src:HZP_REVISION,
            src:HZP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:HZP_HAZARD,
                src:HZP_HAZARD_ORG,
                src:HZP_PRECAUTION,
                src:HZP_PRECAUTION_ORG,
                src:HZP_REVISION  ORDER BY 
            src:HZP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5HAZARDPRECAUTIONS as strm))