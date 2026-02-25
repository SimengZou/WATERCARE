CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5UNAVAILABLEGRIDFIELDS AS SELECT
                        src:UGF_FIELDID::varchar AS UGF_FIELDID, 
                        src:UGF_GRIDNAME::varchar AS UGF_GRIDNAME, 
                        src:UGF_LASTSAVED::datetime AS UGF_LASTSAVED, 
                        src:UGF_MEKEY::varchar AS UGF_MEKEY, 
                        src:UGF_USERGROUP::varchar AS UGF_USERGROUP, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:UGF_LASTSAVED::datetime as ETL_LASTSAVED,
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
            etl_load_datetime,
            etl_load_metadatafilename
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename
                from
                (
                    SELECT
                        
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:UGF_FIELDID,
                src:UGF_GRIDNAME,
                src:UGF_USERGROUP  ORDER BY 
            src:UGF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5UNAVAILABLEGRIDFIELDS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UGF_LASTSAVED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UGF_LASTSAVED), '1900-01-01')) is not null