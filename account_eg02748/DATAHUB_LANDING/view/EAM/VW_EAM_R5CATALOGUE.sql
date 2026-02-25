CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5CATALOGUE AS SELECT
                        src:CAT_COSTCODE::varchar AS CAT_COSTCODE, 
                        src:CAT_CURR::varchar AS CAT_CURR, 
                        src:CAT_DATE::datetime AS CAT_DATE, 
                        src:CAT_DESC::varchar AS CAT_DESC, 
                        src:CAT_DOCUMOTO_BOOKID::varchar AS CAT_DOCUMOTO_BOOKID, 
                        src:CAT_DOCUMOTO_PART::varchar AS CAT_DOCUMOTO_PART, 
                        src:CAT_EXCH::numeric(38, 10) AS CAT_EXCH, 
                        src:CAT_EXCHFROMDUAL::numeric(38, 10) AS CAT_EXCHFROMDUAL, 
                        src:CAT_EXCHTODUAL::numeric(38, 10) AS CAT_EXCHTODUAL, 
                        src:CAT_EXPPURPR::datetime AS CAT_EXPPURPR, 
                        src:CAT_EXPQUOT::datetime AS CAT_EXPQUOT, 
                        src:CAT_GROSS::numeric(38, 10) AS CAT_GROSS, 
                        src:CAT_INSERTOLDREFERENCE::varchar AS CAT_INSERTOLDREFERENCE, 
                        src:CAT_IPERROR::numeric(38, 10) AS CAT_IPERROR, 
                        src:CAT_IPERRORMESSAGE::varchar AS CAT_IPERRORMESSAGE, 
                        src:CAT_IPLASTUPDATE::datetime AS CAT_IPLASTUPDATE, 
                        src:CAT_LASTSAVED::datetime AS CAT_LASTSAVED, 
                        src:CAT_LEADTIME::numeric(38, 10) AS CAT_LEADTIME, 
                        src:CAT_MINORDQTY::numeric(38, 10) AS CAT_MINORDQTY, 
                        src:CAT_MULTIPLY::numeric(38, 10) AS CAT_MULTIPLY, 
                        src:CAT_PART::varchar AS CAT_PART, 
                        src:CAT_PART_ORG::varchar AS CAT_PART_ORG, 
                        src:CAT_PURPRICE::numeric(38, 10) AS CAT_PURPRICE, 
                        src:CAT_PURUOM::varchar AS CAT_PURUOM, 
                        src:CAT_QUOTATION::varchar AS CAT_QUOTATION, 
                        src:CAT_QUOTPRICE::numeric(38, 10) AS CAT_QUOTPRICE, 
                        src:CAT_QUOTUOM::varchar AS CAT_QUOTUOM, 
                        src:CAT_REF::varchar AS CAT_REF, 
                        src:CAT_REPPRICE::numeric(38, 10) AS CAT_REPPRICE, 
                        src:CAT_REPREF::varchar AS CAT_REPREF, 
                        src:CAT_SOURCECODE::varchar AS CAT_SOURCECODE, 
                        src:CAT_SOURCESYSTEM::varchar AS CAT_SOURCESYSTEM, 
                        src:CAT_SUPPLIER::varchar AS CAT_SUPPLIER, 
                        src:CAT_SUPPLIER_ORG::varchar AS CAT_SUPPLIER_ORG, 
                        src:CAT_TAX::varchar AS CAT_TAX, 
                        src:CAT_UPDATECOUNT::numeric(38, 10) AS CAT_UPDATECOUNT, 
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
    
                        
                src:CAT_PART,
                src:CAT_PART_ORG,
                src:CAT_SUPPLIER,
                src:CAT_SUPPLIER_ORG,
            src:CAT_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CAT_PART,
                src:CAT_PART_ORG,
                src:CAT_SUPPLIER,
                src:CAT_SUPPLIER_ORG  ORDER BY 
            src:CAT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5CATALOGUE as strm))