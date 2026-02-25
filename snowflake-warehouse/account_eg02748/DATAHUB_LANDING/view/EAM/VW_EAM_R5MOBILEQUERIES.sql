CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5MOBILEQUERIES AS SELECT
                        src:MQR_COLUMNMAP::varchar AS MQR_COLUMNMAP, 
                        src:MQR_CREATETABLE::varchar AS MQR_CREATETABLE, 
                        src:MQR_GRIDNAME::varchar AS MQR_GRIDNAME, 
                        src:MQR_LASTSAVED::datetime AS MQR_LASTSAVED, 
                        src:MQR_PREPAREGRID::varchar AS MQR_PREPAREGRID, 
                        src:MQR_PREPARETABLEUSED::varchar AS MQR_PREPARETABLEUSED, 
                        src:MQR_SINGLETHREADPERCONN::varchar AS MQR_SINGLETHREADPERCONN, 
                        src:MQR_SQLTEXT::varchar AS MQR_SQLTEXT, 
                        src:MQR_TABLENAME::varchar AS MQR_TABLENAME, 
                        src:MQR_UPDATECOUNT::numeric(38, 10) AS MQR_UPDATECOUNT, 
                        src:MQR_UPDATED::datetime AS MQR_UPDATED, 
                        src:MQR_VERSION::varchar AS MQR_VERSION, 
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
    
                        
                src:MQR_GRIDNAME,
                src:MQR_TABLENAME,
                src:MQR_VERSION,
            src:MQR_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:MQR_GRIDNAME,
                src:MQR_TABLENAME,
                src:MQR_VERSION  ORDER BY 
            src:MQR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5MOBILEQUERIES as strm))