CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PMFORECASTEQUIPLIST AS SELECT
                        src:PFL_LASTSAVED::datetime AS PFL_LASTSAVED, 
                        src:PFL_OBJECT::varchar AS PFL_OBJECT, 
                        src:PFL_OBJECT_ORG::varchar AS PFL_OBJECT_ORG, 
                        src:PFL_PARENT::varchar AS PFL_PARENT, 
                        src:PFL_PARENT_ORG::varchar AS PFL_PARENT_ORG, 
                        src:PFL_SELECT::varchar AS PFL_SELECT, 
                        src:PFL_SESSIONID::numeric(38, 10) AS PFL_SESSIONID, 
                        src:PFL_UPDATECOUNT::numeric(38, 10) AS PFL_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PFL_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:PFL_OBJECT,
                src:PFL_OBJECT_ORG,
                src:PFL_SESSIONID  ORDER BY 
            src:PFL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PMFORECASTEQUIPLIST as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PFL_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PFL_SESSIONID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PFL_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PFL_LASTSAVED), '1900-01-01')) is not null