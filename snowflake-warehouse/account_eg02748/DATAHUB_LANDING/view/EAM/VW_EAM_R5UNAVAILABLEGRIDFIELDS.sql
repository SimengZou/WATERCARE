CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5UNAVAILABLEGRIDFIELDS AS SELECT
                        src:UGF_FIELDID::varchar AS UGF_FIELDID, 
                        src:UGF_GRIDNAME::varchar AS UGF_GRIDNAME, 
                        src:UGF_LASTSAVED::datetime AS UGF_LASTSAVED, 
                        src:UGF_MEKEY::varchar AS UGF_MEKEY, 
                        src:UGF_USERGROUP::varchar AS UGF_USERGROUP, 
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
    
                        
                src:UGF_FIELDID,
                src:UGF_GRIDNAME,
                src:UGF_USERGROUP,
            src:UGF_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:UGF_FIELDID,
                src:UGF_GRIDNAME,
                src:UGF_USERGROUP  ORDER BY 
            src:UGF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5UNAVAILABLEGRIDFIELDS as strm))