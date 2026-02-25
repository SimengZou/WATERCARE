CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5STRUCTURES AS SELECT
                        src:STC_CHILD::varchar AS STC_CHILD, 
                        src:STC_CHILDRTYPE::varchar AS STC_CHILDRTYPE, 
                        src:STC_CHILDTYPE::varchar AS STC_CHILDTYPE, 
                        src:STC_CHILD_ORG::varchar AS STC_CHILD_ORG, 
                        src:STC_DOWNTIME::varchar AS STC_DOWNTIME, 
                        src:STC_EQUIVALENT::varchar AS STC_EQUIVALENT, 
                        src:STC_LASTSAVED::datetime AS STC_LASTSAVED, 
                        src:STC_MNBDEFINITION::varchar AS STC_MNBDEFINITION, 
                        src:STC_PARENT::varchar AS STC_PARENT, 
                        src:STC_PARENTRTYPE::varchar AS STC_PARENTRTYPE, 
                        src:STC_PARENTTYPE::varchar AS STC_PARENTTYPE, 
                        src:STC_PARENT_ORG::varchar AS STC_PARENT_ORG, 
                        src:STC_ROLLDOWN::varchar AS STC_ROLLDOWN, 
                        src:STC_ROLLUP::varchar AS STC_ROLLUP, 
                        src:STC_SEQUENCE::numeric(38, 10) AS STC_SEQUENCE, 
                        src:STC_UPDATECOUNT::numeric(38, 10) AS STC_UPDATECOUNT, 
                        src:STC_UPDATED::datetime AS STC_UPDATED, 
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
    
                        
                src:STC_CHILD,
                src:STC_CHILD_ORG,
                src:STC_PARENT,
                src:STC_PARENT_ORG,
            src:STC_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:STC_CHILD,
                src:STC_CHILD_ORG,
                src:STC_PARENT,
                src:STC_PARENT_ORG  ORDER BY 
            src:STC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5STRUCTURES as strm))