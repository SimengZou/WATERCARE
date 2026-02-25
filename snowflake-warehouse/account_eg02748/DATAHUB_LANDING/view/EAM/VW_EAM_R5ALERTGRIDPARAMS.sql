CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ALERTGRIDPARAMS AS SELECT
                        src:AGP_ALERT::varchar AS AGP_ALERT, 
                        src:AGP_DVALUE::datetime AS AGP_DVALUE, 
                        src:AGP_LASTSAVED::datetime AS AGP_LASTSAVED, 
                        src:AGP_NVALUE::numeric(38, 10) AS AGP_NVALUE, 
                        src:AGP_PARAM::varchar AS AGP_PARAM, 
                        src:AGP_UPDATECOUNT::numeric(38, 10) AS AGP_UPDATECOUNT, 
                        src:AGP_VALUE::varchar AS AGP_VALUE, 
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
    
                        
                src:AGP_ALERT,
                src:AGP_PARAM,
            src:AGP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:AGP_ALERT,
                src:AGP_PARAM  ORDER BY 
            src:AGP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ALERTGRIDPARAMS as strm))