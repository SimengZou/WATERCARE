CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ALERTWOFIELDMAPPING AS SELECT
                        src:AWF_ALERT::varchar AS AWF_ALERT, 
                        src:AWF_BOILERTEXTNUMBER::numeric(38, 10) AS AWF_BOILERTEXTNUMBER, 
                        src:AWF_GRIDFIELD::numeric(38, 10) AS AWF_GRIDFIELD, 
                        src:AWF_GRIDFIELDDATATYPE::varchar AS AWF_GRIDFIELDDATATYPE, 
                        src:AWF_LASTSAVED::datetime AS AWF_LASTSAVED, 
                        src:AWF_UPDATECOUNT::numeric(38, 10) AS AWF_UPDATECOUNT, 
                        src:AWF_WOFIELD::varchar AS AWF_WOFIELD, 
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
    
                        
                src:AWF_ALERT,
                src:AWF_WOFIELD,
            src:AWF_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:AWF_ALERT,
                src:AWF_WOFIELD  ORDER BY 
            src:AWF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ALERTWOFIELDMAPPING as strm))