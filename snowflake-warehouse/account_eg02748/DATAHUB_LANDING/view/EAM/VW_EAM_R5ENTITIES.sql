CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ENTITIES AS SELECT
                        src:ENT_ADDATTRIBS::varchar AS ENT_ADDATTRIBS, 
                        src:ENT_ADDRESSES::varchar AS ENT_ADDRESSES, 
                        src:ENT_ASSPARTS::varchar AS ENT_ASSPARTS, 
                        src:ENT_ASSPERMITS::varchar AS ENT_ASSPERMITS, 
                        src:ENT_AUDITS::varchar AS ENT_AUDITS, 
                        src:ENT_CAAUDIT::varchar AS ENT_CAAUDIT, 
                        src:ENT_CLASSIF::varchar AS ENT_CLASSIF, 
                        src:ENT_DOCUMENTS::varchar AS ENT_DOCUMENTS, 
                        src:ENT_ENTITY::varchar AS ENT_ENTITY, 
                        src:ENT_ERECORD::varchar AS ENT_ERECORD, 
                        src:ENT_FREETEXT::varchar AS ENT_FREETEXT, 
                        src:ENT_FTAUDIT::varchar AS ENT_FTAUDIT, 
                        src:ENT_LASTSAVED::datetime AS ENT_LASTSAVED, 
                        src:ENT_MULTILANG::varchar AS ENT_MULTILANG, 
                        src:ENT_RENTITY::varchar AS ENT_RENTITY, 
                        src:ENT_STATENT::varchar AS ENT_STATENT, 
                        src:ENT_TABLE::varchar AS ENT_TABLE, 
                        src:ENT_TYPENT::varchar AS ENT_TYPENT, 
                        src:ENT_UPDATECOUNT::numeric(38, 10) AS ENT_UPDATECOUNT, 
                        src:ENT_UPDATED::datetime AS ENT_UPDATED, 
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
    
                        
                src:ENT_RENTITY,
            src:ENT_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ENT_RENTITY  ORDER BY 
            src:ENT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ENTITIES as strm))