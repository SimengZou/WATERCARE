CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5GRID AS SELECT
                        src:GRD_ACTIVE::varchar AS GRD_ACTIVE, 
                        src:GRD_BASEQUERY::varchar AS GRD_BASEQUERY, 
                        src:GRD_BASEQUERY_MULTIORG::varchar AS GRD_BASEQUERY_MULTIORG, 
                        src:GRD_BOTFUNCTION::varchar AS GRD_BOTFUNCTION, 
                        src:GRD_COMMITFLAG::varchar AS GRD_COMMITFLAG, 
                        src:GRD_COMPLEX::varchar AS GRD_COMPLEX, 
                        src:GRD_CUSTOMFIELDCODE::varchar AS GRD_CUSTOMFIELDCODE, 
                        src:GRD_DESC::varchar AS GRD_DESC, 
                        src:GRD_DISPLAYABLELIST::varchar AS GRD_DISPLAYABLELIST, 
                        src:GRD_DISPLAYABLE_MULTIORG::varchar AS GRD_DISPLAYABLE_MULTIORG, 
                        src:GRD_DISTINCT::varchar AS GRD_DISTINCT, 
                        src:GRD_FILTERABLELIST::varchar AS GRD_FILTERABLELIST, 
                        src:GRD_FILTERABLE_MULTIORG::varchar AS GRD_FILTERABLE_MULTIORG, 
                        src:GRD_GISDATAFILTER::varchar AS GRD_GISDATAFILTER, 
                        src:GRD_GISWOFIELDMAP::varchar AS GRD_GISWOFIELDMAP, 
                        src:GRD_GRIDID::numeric(38, 10) AS GRD_GRIDID, 
                        src:GRD_GRIDNAME::varchar AS GRD_GRIDNAME, 
                        src:GRD_GRIDTYPE::numeric(38, 10) AS GRD_GRIDTYPE, 
                        src:GRD_HINTS::varchar AS GRD_HINTS, 
                        src:GRD_KEYFIELDS::varchar AS GRD_KEYFIELDS, 
                        src:GRD_KEYFIELDS_MULTIORG::varchar AS GRD_KEYFIELDS_MULTIORG, 
                        src:GRD_LASTSAVED::datetime AS GRD_LASTSAVED, 
                        src:GRD_MAXGRIDCOST::numeric(38, 10) AS GRD_MAXGRIDCOST, 
                        src:GRD_MOBILE::varchar AS GRD_MOBILE, 
                        src:GRD_NOSCREENDESIGNER::varchar AS GRD_NOSCREENDESIGNER, 
                        src:GRD_OPTIMIZERON::varchar AS GRD_OPTIMIZERON, 
                        src:GRD_ORGCOLNAME::varchar AS GRD_ORGCOLNAME, 
                        src:GRD_PORTLETFLAG::varchar AS GRD_PORTLETFLAG, 
                        src:GRD_SECENTITY::varchar AS GRD_SECENTITY, 
                        src:GRD_SORTABLELIST::varchar AS GRD_SORTABLELIST, 
                        src:GRD_SORTABLE_MULTIORG::varchar AS GRD_SORTABLE_MULTIORG, 
                        src:GRD_TAB::varchar AS GRD_TAB, 
                        src:GRD_UNITS::varchar AS GRD_UNITS, 
                        src:GRD_UPDATECOUNT::numeric(38, 10) AS GRD_UPDATECOUNT, 
                        src:GRD_UPDATED::datetime AS GRD_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:GRD_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:GRD_GRIDID  ORDER BY 
            src:GRD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5GRID as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GRD_GRIDID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GRD_GRIDTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:GRD_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GRD_MAXGRIDCOST), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GRD_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:GRD_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:GRD_LASTSAVED), '1900-01-01')) is not null