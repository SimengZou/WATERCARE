CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5TABPERMISSIONS AS SELECT
                        src:TRP_ALTERSAVE::varchar AS TRP_ALTERSAVE, 
                        src:TRP_DELETE::varchar AS TRP_DELETE, 
                        src:TRP_FUNCTION::varchar AS TRP_FUNCTION, 
                        src:TRP_GROUP::varchar AS TRP_GROUP, 
                        src:TRP_INSERT::varchar AS TRP_INSERT, 
                        src:TRP_LASTSAVED::datetime AS TRP_LASTSAVED, 
                        src:TRP_SECURITYDDSPYID::numeric(38, 10) AS TRP_SECURITYDDSPYID, 
                        src:TRP_SELECT::varchar AS TRP_SELECT, 
                        src:TRP_SEQUENCE::numeric(38, 10) AS TRP_SEQUENCE, 
                        src:TRP_SYSREQUIRED::varchar AS TRP_SYSREQUIRED, 
                        src:TRP_TAB::varchar AS TRP_TAB, 
                        src:TRP_UPDATE::varchar AS TRP_UPDATE, 
                        src:TRP_UPDATECOUNT::numeric(38, 10) AS TRP_UPDATECOUNT, 
                        src:TRP_UPDATED::datetime AS TRP_UPDATED, 
                        src:TRP_VISIBLE::varchar AS TRP_VISIBLE, 
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
    
                        
                src:TRP_FUNCTION,
                src:TRP_GROUP,
                src:TRP_TAB,
            src:TRP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:TRP_FUNCTION,
                src:TRP_GROUP,
                src:TRP_TAB  ORDER BY 
            src:TRP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5TABPERMISSIONS as strm))