CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5IPPERMISSIONS AS SELECT
                        src:IPP_CODE::numeric(38, 10) AS IPP_CODE, 
                        src:IPP_FUNCTION::numeric(38, 10) AS IPP_FUNCTION, 
                        src:IPP_LASTSAVED::datetime AS IPP_LASTSAVED, 
                        src:IPP_MNEMONIC::varchar AS IPP_MNEMONIC, 
                        src:IPP_UPDATECOUNT::numeric(38, 10) AS IPP_UPDATECOUNT, 
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
    
                        
                src:IPP_CODE,
            src:IPP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:IPP_CODE  ORDER BY 
            src:IPP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5IPPERMISSIONS as strm))