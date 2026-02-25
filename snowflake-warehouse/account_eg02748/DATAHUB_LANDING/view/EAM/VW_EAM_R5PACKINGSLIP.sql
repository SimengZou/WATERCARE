CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5PACKINGSLIP AS SELECT
                        src:PSL_BIN::varchar AS PSL_BIN, 
                        src:PSL_COMMENT::varchar AS PSL_COMMENT, 
                        src:PSL_DELQTY::numeric(38, 10) AS PSL_DELQTY, 
                        src:PSL_DELUOM::varchar AS PSL_DELUOM, 
                        src:PSL_DOCK::varchar AS PSL_DOCK, 
                        src:PSL_LASTSAVED::datetime AS PSL_LASTSAVED, 
                        src:PSL_LINE::numeric(38, 10) AS PSL_LINE, 
                        src:PSL_MANLOT::varchar AS PSL_MANLOT, 
                        src:PSL_MANLOTEXP::datetime AS PSL_MANLOTEXP, 
                        src:PSL_MULTIPLY::numeric(38, 10) AS PSL_MULTIPLY, 
                        src:PSL_ORDER::varchar AS PSL_ORDER, 
                        src:PSL_ORDER_ORG::varchar AS PSL_ORDER_ORG, 
                        src:PSL_ORDLINE::numeric(38, 10) AS PSL_ORDLINE, 
                        src:PSL_PART::varchar AS PSL_PART, 
                        src:PSL_PART_ORG::varchar AS PSL_PART_ORG, 
                        src:PSL_RECVQTY::numeric(38, 10) AS PSL_RECVQTY, 
                        src:PSL_SUPPLIERREF::varchar AS PSL_SUPPLIERREF, 
                        src:PSL_UPDATECOUNT::numeric(38, 10) AS PSL_UPDATECOUNT, 
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
    
                        
                src:PSL_DOCK,
                src:PSL_LINE,
            src:PSL_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PSL_DOCK,
                src:PSL_LINE  ORDER BY 
            src:PSL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5PACKINGSLIP as strm))