CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5PPMS_ERROR AS SELECT src, 'EAM_R5PPMS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_DURATION), '0'), 38, 10) is null and 
                    src:PPM_DURATION is not null) THEN
                    'PPM_DURATION contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_DURATION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_ESTWORKLOAD), '0'), 38, 10) is null and 
                    src:PPM_ESTWORKLOAD is not null) THEN
                    'PPM_ESTWORKLOAD contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_ESTWORKLOAD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_FREQ), '0'), 38, 10) is null and 
                    src:PPM_FREQ is not null) THEN
                    'PPM_FREQ contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_FREQ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_GENWINDOW), '0'), 38, 10) is null and 
                    src:PPM_GENWINDOW is not null) THEN
                    'PPM_GENWINDOW contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_GENWINDOW) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_LASTSAVED), '1900-01-01')) is null and 
                    src:PPM_LASTSAVED is not null) THEN
                    'PPM_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_METER), '0'), 38, 10) is null and 
                    src:PPM_METER is not null) THEN
                    'PPM_METER contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_METER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_METER2), '0'), 38, 10) is null and 
                    src:PPM_METER2 is not null) THEN
                    'PPM_METER2 contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_METER2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_NEARWINDOW), '0'), 38, 10) is null and 
                    src:PPM_NEARWINDOW is not null) THEN
                    'PPM_NEARWINDOW contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_NEARWINDOW) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_NESTEDTOLMAX), '0'), 38, 10) is null and 
                    src:PPM_NESTEDTOLMAX is not null) THEN
                    'PPM_NESTEDTOLMAX contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_NESTEDTOLMAX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_NESTEDTOLMET2MAX), '0'), 38, 10) is null and 
                    src:PPM_NESTEDTOLMET2MAX is not null) THEN
                    'PPM_NESTEDTOLMET2MAX contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_NESTEDTOLMET2MAX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_NESTEDTOLMET2MIN), '0'), 38, 10) is null and 
                    src:PPM_NESTEDTOLMET2MIN is not null) THEN
                    'PPM_NESTEDTOLMET2MIN contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_NESTEDTOLMET2MIN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_NESTEDTOLMETMAX), '0'), 38, 10) is null and 
                    src:PPM_NESTEDTOLMETMAX is not null) THEN
                    'PPM_NESTEDTOLMETMAX contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_NESTEDTOLMETMAX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_NESTEDTOLMETMIN), '0'), 38, 10) is null and 
                    src:PPM_NESTEDTOLMETMIN is not null) THEN
                    'PPM_NESTEDTOLMETMIN contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_NESTEDTOLMETMIN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_NESTEDTOLMIN), '0'), 38, 10) is null and 
                    src:PPM_NESTEDTOLMIN is not null) THEN
                    'PPM_NESTEDTOLMIN contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_NESTEDTOLMIN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_OKWINDOW), '0'), 38, 10) is null and 
                    src:PPM_OKWINDOW is not null) THEN
                    'PPM_OKWINDOW contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_OKWINDOW) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_PERFORMONDAY), '0'), 38, 10) is null and 
                    src:PPM_PERFORMONDAY is not null) THEN
                    'PPM_PERFORMONDAY contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_PERFORMONDAY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_PERMITREVIEWED), '1900-01-01')) is null and 
                    src:PPM_PERMITREVIEWED is not null) THEN
                    'PPM_PERMITREVIEWED contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_PERMITREVIEWED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_PERMITREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:PPM_PERMITREVIEWREQUIRED is not null) THEN
                    'PPM_PERMITREVIEWREQUIRED contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_PERMITREVIEWREQUIRED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_REQUESTEDENDDATEBUFFER), '0'), 38, 10) is null and 
                    src:PPM_REQUESTEDENDDATEBUFFER is not null) THEN
                    'PPM_REQUESTEDENDDATEBUFFER contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_REQUESTEDENDDATEBUFFER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_REQUESTEDSTARTDATEBUFFER), '0'), 38, 10) is null and 
                    src:PPM_REQUESTEDSTARTDATEBUFFER is not null) THEN
                    'PPM_REQUESTEDSTARTDATEBUFFER contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_REQUESTEDSTARTDATEBUFFER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_REVISION), '0'), 38, 10) is null and 
                    src:PPM_REVISION is not null) THEN
                    'PPM_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_REVISION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_SAFETYREVIEWED), '1900-01-01')) is null and 
                    src:PPM_SAFETYREVIEWED is not null) THEN
                    'PPM_SAFETYREVIEWED contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_SAFETYREVIEWED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_SAFETYREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:PPM_SAFETYREVIEWREQUIRED is not null) THEN
                    'PPM_SAFETYREVIEWREQUIRED contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_SAFETYREVIEWREQUIRED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE01), '1900-01-01')) is null and 
                    src:PPM_UDFDATE01 is not null) THEN
                    'PPM_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE02), '1900-01-01')) is null and 
                    src:PPM_UDFDATE02 is not null) THEN
                    'PPM_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE03), '1900-01-01')) is null and 
                    src:PPM_UDFDATE03 is not null) THEN
                    'PPM_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE04), '1900-01-01')) is null and 
                    src:PPM_UDFDATE04 is not null) THEN
                    'PPM_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE05), '1900-01-01')) is null and 
                    src:PPM_UDFDATE05 is not null) THEN
                    'PPM_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE06), '1900-01-01')) is null and 
                    src:PPM_UDFDATE06 is not null) THEN
                    'PPM_UDFDATE06 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_UDFDATE06) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE07), '1900-01-01')) is null and 
                    src:PPM_UDFDATE07 is not null) THEN
                    'PPM_UDFDATE07 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_UDFDATE07) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE08), '1900-01-01')) is null and 
                    src:PPM_UDFDATE08 is not null) THEN
                    'PPM_UDFDATE08 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_UDFDATE08) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE09), '1900-01-01')) is null and 
                    src:PPM_UDFDATE09 is not null) THEN
                    'PPM_UDFDATE09 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_UDFDATE09) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE10), '1900-01-01')) is null and 
                    src:PPM_UDFDATE10 is not null) THEN
                    'PPM_UDFDATE10 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_UDFDATE10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM01), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM01 is not null) THEN
                    'PPM_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM02), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM02 is not null) THEN
                    'PPM_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM03), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM03 is not null) THEN
                    'PPM_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM04), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM04 is not null) THEN
                    'PPM_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM05), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM05 is not null) THEN
                    'PPM_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM06), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM06 is not null) THEN
                    'PPM_UDFNUM06 contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_UDFNUM06) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM07), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM07 is not null) THEN
                    'PPM_UDFNUM07 contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_UDFNUM07) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM08), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM08 is not null) THEN
                    'PPM_UDFNUM08 contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_UDFNUM08) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM09), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM09 is not null) THEN
                    'PPM_UDFNUM09 contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_UDFNUM09) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM10), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM10 is not null) THEN
                    'PPM_UDFNUM10 contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_UDFNUM10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PPM_UPDATECOUNT is not null) THEN
                    'PPM_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PPM_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_LASTSAVED), '1900-01-01')) is null and 
                src:PPM_LASTSAVED is not null) THEN
                'PPM_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PPM_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PPM_CODE,
                src:PPM_REVISION  ORDER BY 
            src:PPM_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PPMS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_DURATION), '0'), 38, 10) is null and 
                    src:PPM_DURATION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_ESTWORKLOAD), '0'), 38, 10) is null and 
                    src:PPM_ESTWORKLOAD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_FREQ), '0'), 38, 10) is null and 
                    src:PPM_FREQ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_GENWINDOW), '0'), 38, 10) is null and 
                    src:PPM_GENWINDOW is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_LASTSAVED), '1900-01-01')) is null and 
                    src:PPM_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_METER), '0'), 38, 10) is null and 
                    src:PPM_METER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_METER2), '0'), 38, 10) is null and 
                    src:PPM_METER2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_NEARWINDOW), '0'), 38, 10) is null and 
                    src:PPM_NEARWINDOW is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_NESTEDTOLMAX), '0'), 38, 10) is null and 
                    src:PPM_NESTEDTOLMAX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_NESTEDTOLMET2MAX), '0'), 38, 10) is null and 
                    src:PPM_NESTEDTOLMET2MAX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_NESTEDTOLMET2MIN), '0'), 38, 10) is null and 
                    src:PPM_NESTEDTOLMET2MIN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_NESTEDTOLMETMAX), '0'), 38, 10) is null and 
                    src:PPM_NESTEDTOLMETMAX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_NESTEDTOLMETMIN), '0'), 38, 10) is null and 
                    src:PPM_NESTEDTOLMETMIN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_NESTEDTOLMIN), '0'), 38, 10) is null and 
                    src:PPM_NESTEDTOLMIN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_OKWINDOW), '0'), 38, 10) is null and 
                    src:PPM_OKWINDOW is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_PERFORMONDAY), '0'), 38, 10) is null and 
                    src:PPM_PERFORMONDAY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_PERMITREVIEWED), '1900-01-01')) is null and 
                    src:PPM_PERMITREVIEWED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_PERMITREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:PPM_PERMITREVIEWREQUIRED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_REQUESTEDENDDATEBUFFER), '0'), 38, 10) is null and 
                    src:PPM_REQUESTEDENDDATEBUFFER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_REQUESTEDSTARTDATEBUFFER), '0'), 38, 10) is null and 
                    src:PPM_REQUESTEDSTARTDATEBUFFER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_REVISION), '0'), 38, 10) is null and 
                    src:PPM_REVISION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_SAFETYREVIEWED), '1900-01-01')) is null and 
                    src:PPM_SAFETYREVIEWED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_SAFETYREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:PPM_SAFETYREVIEWREQUIRED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE01), '1900-01-01')) is null and 
                    src:PPM_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE02), '1900-01-01')) is null and 
                    src:PPM_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE03), '1900-01-01')) is null and 
                    src:PPM_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE04), '1900-01-01')) is null and 
                    src:PPM_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE05), '1900-01-01')) is null and 
                    src:PPM_UDFDATE05 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE06), '1900-01-01')) is null and 
                    src:PPM_UDFDATE06 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE07), '1900-01-01')) is null and 
                    src:PPM_UDFDATE07 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE08), '1900-01-01')) is null and 
                    src:PPM_UDFDATE08 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE09), '1900-01-01')) is null and 
                    src:PPM_UDFDATE09 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_UDFDATE10), '1900-01-01')) is null and 
                    src:PPM_UDFDATE10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM01), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM02), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM03), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM04), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM05), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM06), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM06 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM07), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM07 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM08), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM08 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM09), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM09 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UDFNUM10), '0'), 38, 10) is null and 
                    src:PPM_UDFNUM10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPM_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PPM_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPM_LASTSAVED), '1900-01-01')) is null and 
                src:PPM_LASTSAVED is not null)