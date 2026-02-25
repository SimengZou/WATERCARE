CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5USERORGANIZATION AS SELECT
                        src:UOG_COMMON::varchar AS UOG_COMMON, 
                        src:UOG_CREATED::datetime AS UOG_CREATED, 
                        src:UOG_DEFAULT::varchar AS UOG_DEFAULT, 
                        src:UOG_GROUP::varchar AS UOG_GROUP, 
                        src:UOG_INVAPPVLIMIT::numeric(38, 10) AS UOG_INVAPPVLIMIT, 
                        src:UOG_INVAPPVLIMITNONPO::numeric(38, 10) AS UOG_INVAPPVLIMITNONPO, 
                        src:UOG_LASTSAVED::datetime AS UOG_LASTSAVED, 
                        src:UOG_ORG::varchar AS UOG_ORG, 
                        src:UOG_PICAPPVLIMIT::numeric(38, 10) AS UOG_PICAPPVLIMIT, 
                        src:UOG_PORDAPPVLIMIT::numeric(38, 10) AS UOG_PORDAPPVLIMIT, 
                        src:UOG_PORDAUTHAPPVLIMIT::numeric(38, 10) AS UOG_PORDAUTHAPPVLIMIT, 
                        src:UOG_REQAPPVLIMIT::numeric(38, 10) AS UOG_REQAPPVLIMIT, 
                        src:UOG_REQAUTHAPPVLIMIT::numeric(38, 10) AS UOG_REQAUTHAPPVLIMIT, 
                        src:UOG_ROLE::varchar AS UOG_ROLE, 
                        src:UOG_UPDATECOUNT::numeric(38, 10) AS UOG_UPDATECOUNT, 
                        src:UOG_UPDATED::datetime AS UOG_UPDATED, 
                        src:UOG_USER::varchar AS UOG_USER, 
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
    
                        
                src:UOG_ORG,
                src:UOG_ROLE,
                src:UOG_USER,
            src:UOG_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:UOG_ORG,
                src:UOG_ROLE,
                src:UOG_USER  ORDER BY 
            src:UOG_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5USERORGANIZATION as strm))