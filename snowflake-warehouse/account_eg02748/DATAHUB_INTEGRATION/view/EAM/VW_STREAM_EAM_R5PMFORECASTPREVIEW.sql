CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PMFORECASTPREVIEW AS SELECT
                        src:PFV_LASTSAVED::datetime AS PFV_LASTSAVED, 
                        src:PFV_OBJECT::varchar AS PFV_OBJECT, 
                        src:PFV_OBJECT_ORG::varchar AS PFV_OBJECT_ORG, 
                        src:PFV_PARENT::varchar AS PFV_PARENT, 
                        src:PFV_PARENT_ORG::varchar AS PFV_PARENT_ORG, 
                        src:PFV_SELECT::varchar AS PFV_SELECT, 
                        src:PFV_SESSIONID::numeric(38, 10) AS PFV_SESSIONID, 
                        src:PFV_UPDATECOUNT::numeric(38, 10) AS PFV_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PFV_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:PFV_OBJECT,
                src:PFV_OBJECT_ORG,
                src:PFV_SESSIONID  ORDER BY 
            src:PFV_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PMFORECASTPREVIEW as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PFV_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PFV_SESSIONID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PFV_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PFV_LASTSAVED), '1900-01-01')) is not null