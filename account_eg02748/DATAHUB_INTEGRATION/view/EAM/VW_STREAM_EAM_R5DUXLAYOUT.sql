CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DUXLAYOUT AS SELECT
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
                        src:DXL_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:DXL_ELEMENTID,
                src:DXL_PAGENAME,
                src:DXL_USERGROUP  ORDER BY 
            src:DXL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DUXLAYOUT as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DXL_FIELDSIZE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DXL_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DXL_POSITIONINGROUP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DXL_SECTIONPOSITION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DXL_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DXL_LASTSAVED), '1900-01-01')) is not null