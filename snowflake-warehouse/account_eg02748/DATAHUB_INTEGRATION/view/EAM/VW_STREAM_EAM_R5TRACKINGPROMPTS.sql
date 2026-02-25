CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5TRACKINGPROMPTS AS SELECT
                        src:TKP_ARCCOLUMN::varchar AS TKP_ARCCOLUMN, 
                        src:TKP_ARCHIVECOLUMN::varchar AS TKP_ARCHIVECOLUMN, 
                        src:TKP_ARCRCOLUMN::varchar AS TKP_ARCRCOLUMN, 
                        src:TKP_BRANCHCONDITION::varchar AS TKP_BRANCHCONDITION, 
                        src:TKP_BRANCHGOTO::numeric(38, 10) AS TKP_BRANCHGOTO, 
                        src:TKP_BRANCHPATTERN::varchar AS TKP_BRANCHPATTERN, 
                        src:TKP_COLUMN::varchar AS TKP_COLUMN, 
                        src:TKP_COMPUTEDATA::varchar AS TKP_COMPUTEDATA, 
                        src:TKP_DATARTYPE::varchar AS TKP_DATARTYPE, 
                        src:TKP_DATATYPE::varchar AS TKP_DATATYPE, 
                        src:TKP_DATEFORMAT::varchar AS TKP_DATEFORMAT, 
                        src:TKP_DEFAULTPREVDATA::varchar AS TKP_DEFAULTPREVDATA, 
                        src:TKP_EWSQUERYCODE::varchar AS TKP_EWSQUERYCODE, 
                        src:TKP_FIXEDDATA::varchar AS TKP_FIXEDDATA, 
                        src:TKP_INTERFACERTYPE::varchar AS TKP_INTERFACERTYPE, 
                        src:TKP_INTERFACETYPE::varchar AS TKP_INTERFACETYPE, 
                        src:TKP_LASTSAVED::datetime AS TKP_LASTSAVED, 
                        src:TKP_LOVID::varchar AS TKP_LOVID, 
                        src:TKP_LOVOVERRIDEFLAG::varchar AS TKP_LOVOVERRIDEFLAG, 
                        src:TKP_MATCHPATTERN::varchar AS TKP_MATCHPATTERN, 
                        src:TKP_MAXSIZE::numeric(38, 10) AS TKP_MAXSIZE, 
                        src:TKP_MINSIZE::numeric(38, 10) AS TKP_MINSIZE, 
                        src:TKP_NOTONFILEFLAG::varchar AS TKP_NOTONFILEFLAG, 
                        src:TKP_OBTRANSRTYPE::varchar AS TKP_OBTRANSRTYPE, 
                        src:TKP_OBTRANSTYPE::varchar AS TKP_OBTRANSTYPE, 
                        src:TKP_PROMPT::varchar AS TKP_PROMPT, 
                        src:TKP_PROMPTSEQ::numeric(38, 10) AS TKP_PROMPTSEQ, 
                        src:TKP_RCOLUMN::varchar AS TKP_RCOLUMN, 
                        src:TKP_RETURNTOPROMPT::numeric(38, 10) AS TKP_RETURNTOPROMPT, 
                        src:TKP_SQLCODE::varchar AS TKP_SQLCODE, 
                        src:TKP_TRANS::varchar AS TKP_TRANS, 
                        src:TKP_TRANSGROUP::numeric(38, 10) AS TKP_TRANSGROUP, 
                        src:TKP_TRANSSEQ::numeric(38, 10) AS TKP_TRANSSEQ, 
                        src:TKP_UPDATECOUNT::numeric(38, 10) AS TKP_UPDATECOUNT, 
                        src:TKP_UPDATED::datetime AS TKP_UPDATED, 
                        src:TKP_UPLOADCOLUMN::varchar AS TKP_UPLOADCOLUMN, 
                        src:TKP_VALIDATEFILE::varchar AS TKP_VALIDATEFILE, 
                        src:TKP_VALIDATELOV::varchar AS TKP_VALIDATELOV, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:TKP_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:TKP_TRANS,
                src:TKP_TRANSSEQ  ORDER BY 
            src:TKP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5TRACKINGPROMPTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TKP_BRANCHGOTO), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TKP_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TKP_MAXSIZE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TKP_MINSIZE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TKP_PROMPTSEQ), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TKP_RETURNTOPROMPT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TKP_TRANSGROUP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TKP_TRANSSEQ), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TKP_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TKP_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TKP_LASTSAVED), '1900-01-01')) is not null