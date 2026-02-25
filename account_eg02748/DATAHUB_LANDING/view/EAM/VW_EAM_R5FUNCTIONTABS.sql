CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5FUNCTIONTABS AS SELECT
                        src:FTB_ALTERSAVE::varchar AS FTB_ALTERSAVE, 
                        src:FTB_DELETE::varchar AS FTB_DELETE, 
                        src:FTB_DESIGNPOPUP::varchar AS FTB_DESIGNPOPUP, 
                        src:FTB_DISPLAYFT::varchar AS FTB_DISPLAYFT, 
                        src:FTB_EWSBTN::varchar AS FTB_EWSBTN, 
                        src:FTB_FUNCTION::varchar AS FTB_FUNCTION, 
                        src:FTB_INSERT::varchar AS FTB_INSERT, 
                        src:FTB_INTERFACECODE::varchar AS FTB_INTERFACECODE, 
                        src:FTB_LASTSAVED::datetime AS FTB_LASTSAVED, 
                        src:FTB_MEKEY::varchar AS FTB_MEKEY, 
                        src:FTB_NODESIGN::varchar AS FTB_NODESIGN, 
                        src:FTB_PRODUCT::varchar AS FTB_PRODUCT, 
                        src:FTB_RENTITY::varchar AS FTB_RENTITY, 
                        src:FTB_SELECT::varchar AS FTB_SELECT, 
                        src:FTB_SEQ::numeric(38, 10) AS FTB_SEQ, 
                        src:FTB_SQLEXIST::varchar AS FTB_SQLEXIST, 
                        src:FTB_SYSREQUIRED::varchar AS FTB_SYSREQUIRED, 
                        src:FTB_TAB::varchar AS FTB_TAB, 
                        src:FTB_UPDATE::varchar AS FTB_UPDATE, 
                        src:FTB_UPDATECOUNT::numeric(38, 10) AS FTB_UPDATECOUNT, 
                        src:FTB_VISIBLE::varchar AS FTB_VISIBLE, 
                        src:FTB_XTYPE::varchar AS FTB_XTYPE, 
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
    
                        
                src:FTB_FUNCTION,
                src:FTB_TAB,
            src:FTB_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:FTB_FUNCTION,
                src:FTB_TAB  ORDER BY 
            src:FTB_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5FUNCTIONTABS as strm))