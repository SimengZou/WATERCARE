CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5DEFAULTPAGELAYOUT AS SELECT
                        src:PLD_ATTRIBUTE::varchar AS PLD_ATTRIBUTE, 
                        src:PLD_CASE::varchar AS PLD_CASE, 
                        src:PLD_DDFIELDNAME::varchar AS PLD_DDFIELDNAME, 
                        src:PLD_DDTABLENAME::varchar AS PLD_DDTABLENAME, 
                        src:PLD_DESTXPATH::varchar AS PLD_DESTXPATH, 
                        src:PLD_ELEMENTID::varchar AS PLD_ELEMENTID, 
                        src:PLD_ELEMENTTYPE::varchar AS PLD_ELEMENTTYPE, 
                        src:PLD_FIELDCONTAINER::varchar AS PLD_FIELDCONTAINER, 
                        src:PLD_FIELDCONTTYPE::numeric(38, 10) AS PLD_FIELDCONTTYPE, 
                        src:PLD_FIELDGROUP::numeric(38, 10) AS PLD_FIELDGROUP, 
                        src:PLD_FIELDTYPE::varchar AS PLD_FIELDTYPE, 
                        src:PLD_JSPFILE::varchar AS PLD_JSPFILE, 
                        src:PLD_LASTSAVED::datetime AS PLD_LASTSAVED, 
                        src:PLD_MAXLENGTH::numeric(38, 10) AS PLD_MAXLENGTH, 
                        src:PLD_NUMBERTYPE::varchar AS PLD_NUMBERTYPE, 
                        src:PLD_ONCHAINEDLOOKUP::varchar AS PLD_ONCHAINEDLOOKUP, 
                        src:PLD_ONCLASSCHANGE::varchar AS PLD_ONCLASSCHANGE, 
                        src:PLD_ONLOOKUP::varchar AS PLD_ONLOOKUP, 
                        src:PLD_ONVALIDATE::varchar AS PLD_ONVALIDATE, 
                        src:PLD_OTHERATTRIBS::varchar AS PLD_OTHERATTRIBS, 
                        src:PLD_OTHERTAGS::varchar AS PLD_OTHERTAGS, 
                        src:PLD_PAGENAME::varchar AS PLD_PAGENAME, 
                        src:PLD_POSITIONINGROUP::numeric(38, 10) AS PLD_POSITIONINGROUP, 
                        src:PLD_PRESENTINJSP::varchar AS PLD_PRESENTINJSP, 
                        src:PLD_RESPONSEXPATH::varchar AS PLD_RESPONSEXPATH, 
                        src:PLD_SIZE::numeric(38, 10) AS PLD_SIZE, 
                        src:PLD_SYSTEMATTRIBUTE::varchar AS PLD_SYSTEMATTRIBUTE, 
                        src:PLD_TABINDEX::numeric(38, 10) AS PLD_TABINDEX, 
                        src:PLD_UPDATECOUNT::numeric(38, 10) AS PLD_UPDATECOUNT, 
                        src:PLD_XPATH::varchar AS PLD_XPATH, 
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
    
                        
                src:PLD_ELEMENTID,
                src:PLD_PAGENAME,
            src:PLD_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PLD_ELEMENTID,
                src:PLD_PAGENAME  ORDER BY 
            src:PLD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5DEFAULTPAGELAYOUT as strm))