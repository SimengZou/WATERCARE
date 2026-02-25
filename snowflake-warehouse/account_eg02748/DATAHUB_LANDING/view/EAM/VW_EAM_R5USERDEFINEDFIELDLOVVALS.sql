CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5USERDEFINEDFIELDLOVVALS AS SELECT
                        src:UDL_CODE::varchar AS UDL_CODE, 
                        src:UDL_DESC::varchar AS UDL_DESC, 
                        src:UDL_FIELD::varchar AS UDL_FIELD, 
                        src:UDL_LASTSAVED::datetime AS UDL_LASTSAVED, 
                        src:UDL_NOTUSED::varchar AS UDL_NOTUSED, 
                        src:UDL_RENTITY::varchar AS UDL_RENTITY, 
                        src:UDL_UPDATECOUNT::numeric(38, 10) AS UDL_UPDATECOUNT, 
                        src:UDL_UPDATED::datetime AS UDL_UPDATED, 
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
    
                        
                src:UDL_CODE,
                src:UDL_FIELD,
                src:UDL_RENTITY,
            src:UDL_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:UDL_CODE,
                src:UDL_FIELD,
                src:UDL_RENTITY  ORDER BY 
            src:UDL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5USERDEFINEDFIELDLOVVALS as strm))