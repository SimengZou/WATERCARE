CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5HOMEGROUPS AS SELECT
                        src:HMG_AUTOFRESH::varchar AS HMG_AUTOFRESH, 
                        src:HMG_GROUP::varchar AS HMG_GROUP, 
                        src:HMG_HOMCODE::varchar AS HMG_HOMCODE, 
                        src:HMG_HOMTYPE::varchar AS HMG_HOMTYPE, 
                        src:HMG_LASTSAVED::datetime AS HMG_LASTSAVED, 
                        src:HMG_SEQ::numeric(38, 10) AS HMG_SEQ, 
                        src:HMG_TAB::varchar AS HMG_TAB, 
                        src:HMG_UPDATECOUNT::numeric(38, 10) AS HMG_UPDATECOUNT, 
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
    
                        
                src:HMG_GROUP,
                src:HMG_HOMCODE,
                src:HMG_HOMTYPE,
            src:HMG_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:HMG_GROUP,
                src:HMG_HOMCODE,
                src:HMG_HOMTYPE  ORDER BY 
            src:HMG_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5HOMEGROUPS as strm))