CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5BINS AS SELECT
                        src:BIN_CODE::varchar AS BIN_CODE, 
                        src:BIN_CREATED::datetime AS BIN_CREATED, 
                        src:BIN_CREATEDBY::varchar AS BIN_CREATEDBY, 
                        src:BIN_DESC::varchar AS BIN_DESC, 
                        src:BIN_FORHELDITEMS::varchar AS BIN_FORHELDITEMS, 
                        src:BIN_FORSTOCK::varchar AS BIN_FORSTOCK, 
                        src:BIN_LASTSAVED::datetime AS BIN_LASTSAVED, 
                        src:BIN_NOTUSED::varchar AS BIN_NOTUSED, 
                        src:BIN_STORE::varchar AS BIN_STORE, 
                        src:BIN_UPDATECOUNT::numeric(38, 10) AS BIN_UPDATECOUNT, 
                        src:BIN_UPDATED::datetime AS BIN_UPDATED, 
                        src:BIN_UPDATEDBY::varchar AS BIN_UPDATEDBY, 
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
    
                        
                src:BIN_CODE,
                src:BIN_STORE,
            src:BIN_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:BIN_CODE,
                src:BIN_STORE  ORDER BY 
            src:BIN_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5BINS as strm))