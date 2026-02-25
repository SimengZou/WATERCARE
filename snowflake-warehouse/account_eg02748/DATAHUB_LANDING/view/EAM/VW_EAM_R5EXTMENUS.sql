CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5EXTMENUS AS SELECT
                        src:EMN_CODE::varchar AS EMN_CODE, 
                        src:EMN_DUXMOBILE::varchar AS EMN_DUXMOBILE, 
                        src:EMN_FUNCTION::varchar AS EMN_FUNCTION, 
                        src:EMN_GROUP::varchar AS EMN_GROUP, 
                        src:EMN_HIDE::varchar AS EMN_HIDE, 
                        src:EMN_ICON::varchar AS EMN_ICON, 
                        src:EMN_LASTSAVED::datetime AS EMN_LASTSAVED, 
                        src:EMN_MEFLAG::varchar AS EMN_MEFLAG, 
                        src:EMN_MOBILE::varchar AS EMN_MOBILE, 
                        src:EMN_PARENT::varchar AS EMN_PARENT, 
                        src:EMN_SEQUENCE::numeric(38, 10) AS EMN_SEQUENCE, 
                        src:EMN_TRANSIT::varchar AS EMN_TRANSIT, 
                        src:EMN_TYPE::varchar AS EMN_TYPE, 
                        src:EMN_UPDATECOUNT::numeric(38, 10) AS EMN_UPDATECOUNT, 
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
    
                        
                src:EMN_CODE,
            src:EMN_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:EMN_CODE  ORDER BY 
            src:EMN_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5EXTMENUS as strm))