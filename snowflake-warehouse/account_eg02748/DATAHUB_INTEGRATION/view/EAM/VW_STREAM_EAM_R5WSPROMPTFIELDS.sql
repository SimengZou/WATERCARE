CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WSPROMPTFIELDS AS SELECT
                        src:WSF_BRANCHCOND::varchar AS WSF_BRANCHCOND, 
                        src:WSF_BRANCHGOTO::numeric(38, 10) AS WSF_BRANCHGOTO, 
                        src:WSF_BRANCHPATTERN::varchar AS WSF_BRANCHPATTERN, 
                        src:WSF_CLEARPREVVALUES::varchar AS WSF_CLEARPREVVALUES, 
                        src:WSF_COMPUTEDDATA::varchar AS WSF_COMPUTEDDATA, 
                        src:WSF_CUSTOMFIELDID::varchar AS WSF_CUSTOMFIELDID, 
                        src:WSF_DESTWEBSERVICE::varchar AS WSF_DESTWEBSERVICE, 
                        src:WSF_DESTXPATH::varchar AS WSF_DESTXPATH, 
                        src:WSF_DISPLAYTYPE::varchar AS WSF_DISPLAYTYPE, 
                        src:WSF_DUPPREVVALUE::varchar AS WSF_DUPPREVVALUE, 
                        src:WSF_FIELD::varchar AS WSF_FIELD, 
                        src:WSF_FIELDLABEL::varchar AS WSF_FIELDLABEL, 
                        src:WSF_FIELDXPATH::varchar AS WSF_FIELDXPATH, 
                        src:WSF_FIXEDDATA::varchar AS WSF_FIXEDDATA, 
                        src:WSF_ISCATEGORY::varchar AS WSF_ISCATEGORY, 
                        src:WSF_ISCLASS::varchar AS WSF_ISCLASS, 
                        src:WSF_ISCLASSORG::varchar AS WSF_ISCLASSORG, 
                        src:WSF_ISCONTROLORG::varchar AS WSF_ISCONTROLORG, 
                        src:WSF_LASTSAVED::datetime AS WSF_LASTSAVED, 
                        src:WSF_LINE::numeric(38, 10) AS WSF_LINE, 
                        src:WSF_MAXLENGTH::numeric(38, 10) AS WSF_MAXLENGTH, 
                        src:WSF_MINLENGTH::numeric(38, 10) AS WSF_MINLENGTH, 
                        src:WSF_MOBILEQUERYCODE::varchar AS WSF_MOBILEQUERYCODE, 
                        src:WSF_NEXTLINE::numeric(38, 10) AS WSF_NEXTLINE, 
                        src:WSF_PATTERN::varchar AS WSF_PATTERN, 
                        src:WSF_PRECISION::numeric(38, 10) AS WSF_PRECISION, 
                        src:WSF_PRIMARYKEY::varchar AS WSF_PRIMARYKEY, 
                        src:WSF_PROCESSGROUP::numeric(38, 10) AS WSF_PROCESSGROUP, 
                        src:WSF_RESPONSEXPATH::varchar AS WSF_RESPONSEXPATH, 
                        src:WSF_RETRIEVEFIELD::varchar AS WSF_RETRIEVEFIELD, 
                        src:WSF_RETRIEVEFROMGROUP::numeric(38, 10) AS WSF_RETRIEVEFROMGROUP, 
                        src:WSF_RETRIEVEXPATH::varchar AS WSF_RETRIEVEXPATH, 
                        src:WSF_RTYPE::varchar AS WSF_RTYPE, 
                        src:WSF_SCALE::numeric(38, 10) AS WSF_SCALE, 
                        src:WSF_SQLQUERYCODE::varchar AS WSF_SQLQUERYCODE, 
                        src:WSF_SYSTEMFIELDTYPE::varchar AS WSF_SYSTEMFIELDTYPE, 
                        src:WSF_TAGNAME::varchar AS WSF_TAGNAME, 
                        src:WSF_TYPE::varchar AS WSF_TYPE, 
                        src:WSF_UNMAPPED::varchar AS WSF_UNMAPPED, 
                        src:WSF_UPDATECOUNT::numeric(38, 10) AS WSF_UPDATECOUNT, 
                        src:WSF_UPDATED::datetime AS WSF_UPDATED, 
                        src:WSF_UPPERCASE::varchar AS WSF_UPPERCASE, 
                        src:WSF_WSPMTCODE::varchar AS WSF_WSPMTCODE, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:WSF_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:WSF_LINE,
                src:WSF_WSPMTCODE  ORDER BY 
            src:WSF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5WSPROMPTFIELDS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WSF_BRANCHGOTO), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WSF_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WSF_LINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WSF_MAXLENGTH), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WSF_MINLENGTH), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WSF_NEXTLINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WSF_PRECISION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WSF_PROCESSGROUP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WSF_RETRIEVEFROMGROUP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WSF_SCALE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WSF_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WSF_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WSF_LASTSAVED), '1900-01-01')) is not null