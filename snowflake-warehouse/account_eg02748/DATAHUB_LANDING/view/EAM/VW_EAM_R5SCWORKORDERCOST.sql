CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5SCWORKORDERCOST AS SELECT
                        src:CWS_COST::numeric(38, 10) AS CWS_COST, 
                        src:CWS_COSTDEFCURR::numeric(38, 10) AS CWS_COSTDEFCURR, 
                        src:CWS_DATE::datetime AS CWS_DATE, 
                        src:CWS_JOBTYPE::varchar AS CWS_JOBTYPE, 
                        src:CWS_LASTSAVED::datetime AS CWS_LASTSAVED, 
                        src:CWS_ORG::varchar AS CWS_ORG, 
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
    
                        
                src:CWS_DATE,
                src:CWS_JOBTYPE,
                src:CWS_ORG,
            src:CWS_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CWS_DATE,
                src:CWS_JOBTYPE,
                src:CWS_ORG  ORDER BY 
            src:CWS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5SCWORKORDERCOST as strm))