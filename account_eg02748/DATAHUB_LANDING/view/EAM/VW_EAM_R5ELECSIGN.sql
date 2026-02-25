CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ELECSIGN AS SELECT
                        src:ELS_CERTIFYNUM::varchar AS ELS_CERTIFYNUM, 
                        src:ELS_CERTIFYTYPE::varchar AS ELS_CERTIFYTYPE, 
                        src:ELS_CODE::varchar AS ELS_CODE, 
                        src:ELS_DATE::datetime AS ELS_DATE, 
                        src:ELS_ENTCODE::varchar AS ELS_ENTCODE, 
                        src:ELS_ENTCODE2::varchar AS ELS_ENTCODE2, 
                        src:ELS_ENTITY::varchar AS ELS_ENTITY, 
                        src:ELS_EXTERNALDATE::datetime AS ELS_EXTERNALDATE, 
                        src:ELS_LASTCHANGED::varchar AS ELS_LASTCHANGED, 
                        src:ELS_LASTSAVED::datetime AS ELS_LASTSAVED, 
                        src:ELS_ORG::varchar AS ELS_ORG, 
                        src:ELS_PARENT::varchar AS ELS_PARENT, 
                        src:ELS_SIGNTYPE::varchar AS ELS_SIGNTYPE, 
                        src:ELS_SIGNTYPEDESC::varchar AS ELS_SIGNTYPEDESC, 
                        src:ELS_STATUS::varchar AS ELS_STATUS, 
                        src:ELS_USER::varchar AS ELS_USER, 
                        src:ELS_USERDESC::varchar AS ELS_USERDESC, 
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
    
                        
                src:ELS_CODE,
            src:ELS_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ELS_CODE  ORDER BY 
            src:ELS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ELECSIGN as strm))