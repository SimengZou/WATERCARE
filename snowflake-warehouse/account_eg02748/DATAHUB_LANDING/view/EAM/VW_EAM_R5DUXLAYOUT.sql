CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5DUXLAYOUT AS SELECT
                        src:DXL_ATTRIBUTE::varchar AS DXL_ATTRIBUTE, 
                        src:DXL_DEFAULTVALUE::varchar AS DXL_DEFAULTVALUE, 
                        src:DXL_ELEMENTID::varchar AS DXL_ELEMENTID, 
                        src:DXL_ELEMENTTYPE::varchar AS DXL_ELEMENTTYPE, 
                        src:DXL_FIELDINFO::varchar AS DXL_FIELDINFO, 
                        src:DXL_FIELDSIZE::numeric(38, 10) AS DXL_FIELDSIZE, 
                        src:DXL_LASTSAVED::datetime AS DXL_LASTSAVED, 
                        src:DXL_NEWCARD::varchar AS DXL_NEWCARD, 
                        src:DXL_PAGENAME::varchar AS DXL_PAGENAME, 
                        src:DXL_POSITIONINGROUP::numeric(38, 10) AS DXL_POSITIONINGROUP, 
                        src:DXL_PRESENTINJSP::varchar AS DXL_PRESENTINJSP, 
                        src:DXL_RADIOOPTIONS::varchar AS DXL_RADIOOPTIONS, 
                        src:DXL_SECTION::varchar AS DXL_SECTION, 
                        src:DXL_SECTIONLABEL::varchar AS DXL_SECTIONLABEL, 
                        src:DXL_SECTIONPOSITION::numeric(38, 10) AS DXL_SECTIONPOSITION, 
                        src:DXL_SYSTEMATTRIBUTE::varchar AS DXL_SYSTEMATTRIBUTE, 
                        src:DXL_UPDATECOUNT::numeric(38, 10) AS DXL_UPDATECOUNT, 
                        src:DXL_USERGROUP::varchar AS DXL_USERGROUP, 
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
    
                        
                src:DXL_ELEMENTID,
                src:DXL_PAGENAME,
                src:DXL_USERGROUP,
            src:DXL_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:DXL_ELEMENTID,
                src:DXL_PAGENAME,
                src:DXL_USERGROUP  ORDER BY 
            src:DXL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5DUXLAYOUT as strm))