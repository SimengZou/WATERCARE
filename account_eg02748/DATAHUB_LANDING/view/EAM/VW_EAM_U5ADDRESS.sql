CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_U5ADDRESS AS SELECT
                        src:ADR_FLAT::varchar AS ADR_FLAT, 
                        src:ADR_HOUSENO::varchar AS ADR_HOUSENO, 
                        src:ADR_KEY::varchar AS ADR_KEY, 
                        src:ADR_MISC::varchar AS ADR_MISC, 
                        src:ADR_ORG::varchar AS ADR_ORG, 
                        src:ADR_POSTALCODE::varchar AS ADR_POSTALCODE, 
                        src:ADR_SERVICEAREA::varchar AS ADR_SERVICEAREA, 
                        src:ADR_STATE::varchar AS ADR_STATE, 
                        src:ADR_STREET::varchar AS ADR_STREET, 
                        src:ADR_STREETTYPE::varchar AS ADR_STREETTYPE, 
                        src:ADR_SUBDIVISION::varchar AS ADR_SUBDIVISION, 
                        src:ADR_SUBURB::varchar AS ADR_SUBURB, 
                        src:CREATED::datetime AS CREATED, 
                        src:CREATEDBY::varchar AS CREATEDBY, 
                        src:LASTSAVED::datetime AS LASTSAVED, 
                        src:UPDATECOUNT::numeric(38, 10) AS UPDATECOUNT, 
                        src:UPDATED::datetime AS UPDATED, 
                        src:UPDATEDBY::varchar AS UPDATEDBY, 
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
    
                        
                src:ADR_KEY,
            src:LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ADR_KEY  ORDER BY 
            src:LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_U5ADDRESS as strm))