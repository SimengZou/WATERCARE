CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5EVENTPOINTS_ERROR AS SELECT src, 'EAM_R5EVENTPOINTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_ASSESSEDSLOPE), '0'), 38, 10) is null and 
                    src:EPO_ASSESSEDSLOPE is not null) THEN
                    'EPO_ASSESSEDSLOPE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_ASSESSEDSLOPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_AVGSLOPE), '0'), 38, 10) is null and 
                    src:EPO_AVGSLOPE is not null) THEN
                    'EPO_AVGSLOPE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_AVGSLOPE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_CREATED), '1900-01-01')) is null and 
                    src:EPO_CREATED is not null) THEN
                    'EPO_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_DATE), '1900-01-01')) is null and 
                    src:EPO_DATE is not null) THEN
                    'EPO_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_DATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROMPOINT), '0'), 38, 10) is null and 
                    src:EPO_FROMPOINT is not null) THEN
                    'EPO_FROMPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_FROMPOINT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:EPO_FROM_HORIZONTAL_OFFSET is not null) THEN
                    'EPO_FROM_HORIZONTAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_FROM_HORIZONTAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_LATITUDE), '0'), 38, 10) is null and 
                    src:EPO_FROM_LATITUDE is not null) THEN
                    'EPO_FROM_LATITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_FROM_LATITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_LONGITUDE), '0'), 38, 10) is null and 
                    src:EPO_FROM_LONGITUDE is not null) THEN
                    'EPO_FROM_LONGITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_FROM_LONGITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_OFFSET), '0'), 38, 10) is null and 
                    src:EPO_FROM_OFFSET is not null) THEN
                    'EPO_FROM_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_FROM_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:EPO_FROM_OFFSET_PERCENTAGE is not null) THEN
                    'EPO_FROM_OFFSET_PERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_FROM_OFFSET_PERCENTAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_REFERENCE), '0'), 38, 10) is null and 
                    src:EPO_FROM_REFERENCE is not null) THEN
                    'EPO_FROM_REFERENCE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_FROM_REFERENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:EPO_FROM_VERTICAL_OFFSET is not null) THEN
                    'EPO_FROM_VERTICAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_FROM_VERTICAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_XCOORDINATE), '0'), 38, 10) is null and 
                    src:EPO_FROM_XCOORDINATE is not null) THEN
                    'EPO_FROM_XCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_FROM_XCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_YCOORDINATE), '0'), 38, 10) is null and 
                    src:EPO_FROM_YCOORDINATE is not null) THEN
                    'EPO_FROM_YCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_FROM_YCOORDINATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_LASTSAVED), '1900-01-01')) is null and 
                    src:EPO_LASTSAVED is not null) THEN
                    'EPO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_LASTSLOPE), '0'), 38, 10) is null and 
                    src:EPO_LASTSLOPE is not null) THEN
                    'EPO_LASTSLOPE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_LASTSLOPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_LINE), '0'), 38, 10) is null and 
                    src:EPO_LINE is not null) THEN
                    'EPO_LINE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_LINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_MAX), '0'), 38, 10) is null and 
                    src:EPO_MAX is not null) THEN
                    'EPO_MAX contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_MAX) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_MAXDATE), '1900-01-01')) is null and 
                    src:EPO_MAXDATE is not null) THEN
                    'EPO_MAXDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_MAXDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_MAXTOL), '0'), 38, 10) is null and 
                    src:EPO_MAXTOL is not null) THEN
                    'EPO_MAXTOL contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_MAXTOL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_MAXTOLDATE), '1900-01-01')) is null and 
                    src:EPO_MAXTOLDATE is not null) THEN
                    'EPO_MAXTOLDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_MAXTOLDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_MIN), '0'), 38, 10) is null and 
                    src:EPO_MIN is not null) THEN
                    'EPO_MIN contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_MIN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_MINDATE), '1900-01-01')) is null and 
                    src:EPO_MINDATE is not null) THEN
                    'EPO_MINDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_MINDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_MINTOL), '0'), 38, 10) is null and 
                    src:EPO_MINTOL is not null) THEN
                    'EPO_MINTOL contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_MINTOL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_MINTOLDATE), '1900-01-01')) is null and 
                    src:EPO_MINTOLDATE is not null) THEN
                    'EPO_MINTOLDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_MINTOLDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_NIDATEMAXCRIT), '1900-01-01')) is null and 
                    src:EPO_NIDATEMAXCRIT is not null) THEN
                    'EPO_NIDATEMAXCRIT contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_NIDATEMAXCRIT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_NIDATEMAXTOL), '1900-01-01')) is null and 
                    src:EPO_NIDATEMAXTOL is not null) THEN
                    'EPO_NIDATEMAXTOL contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_NIDATEMAXTOL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_NIDATEMINCRIT), '1900-01-01')) is null and 
                    src:EPO_NIDATEMINCRIT is not null) THEN
                    'EPO_NIDATEMINCRIT contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_NIDATEMINCRIT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_NIDATEMINTOL), '1900-01-01')) is null and 
                    src:EPO_NIDATEMINTOL is not null) THEN
                    'EPO_NIDATEMINTOL contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_NIDATEMINTOL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TOPOINT), '0'), 38, 10) is null and 
                    src:EPO_TOPOINT is not null) THEN
                    'EPO_TOPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_TOPOINT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:EPO_TO_HORIZONTAL_OFFSET is not null) THEN
                    'EPO_TO_HORIZONTAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_TO_HORIZONTAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_LATITUDE), '0'), 38, 10) is null and 
                    src:EPO_TO_LATITUDE is not null) THEN
                    'EPO_TO_LATITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_TO_LATITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_LONGITUDE), '0'), 38, 10) is null and 
                    src:EPO_TO_LONGITUDE is not null) THEN
                    'EPO_TO_LONGITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_TO_LONGITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_OFFSET), '0'), 38, 10) is null and 
                    src:EPO_TO_OFFSET is not null) THEN
                    'EPO_TO_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_TO_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:EPO_TO_OFFSET_PERCENTAGE is not null) THEN
                    'EPO_TO_OFFSET_PERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_TO_OFFSET_PERCENTAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_REFERENCE), '0'), 38, 10) is null and 
                    src:EPO_TO_REFERENCE is not null) THEN
                    'EPO_TO_REFERENCE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_TO_REFERENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:EPO_TO_VERTICAL_OFFSET is not null) THEN
                    'EPO_TO_VERTICAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_TO_VERTICAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_XCOORDINATE), '0'), 38, 10) is null and 
                    src:EPO_TO_XCOORDINATE is not null) THEN
                    'EPO_TO_XCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_TO_XCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_YCOORDINATE), '0'), 38, 10) is null and 
                    src:EPO_TO_YCOORDINATE is not null) THEN
                    'EPO_TO_YCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_TO_YCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:EPO_UPDATECOUNT is not null) THEN
                    'EPO_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_UPDATED), '1900-01-01')) is null and 
                    src:EPO_UPDATED is not null) THEN
                    'EPO_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_UPDATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VALUE), '0'), 38, 10) is null and 
                    src:EPO_VALUE is not null) THEN
                    'EPO_VALUE contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VALUE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE1), '1900-01-01')) is null and 
                    src:EPO_VARDATE1 is not null) THEN
                    'EPO_VARDATE1 contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_VARDATE1) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE10), '1900-01-01')) is null and 
                    src:EPO_VARDATE10 is not null) THEN
                    'EPO_VARDATE10 contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_VARDATE10) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE11), '1900-01-01')) is null and 
                    src:EPO_VARDATE11 is not null) THEN
                    'EPO_VARDATE11 contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_VARDATE11) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE12), '1900-01-01')) is null and 
                    src:EPO_VARDATE12 is not null) THEN
                    'EPO_VARDATE12 contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_VARDATE12) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE13), '1900-01-01')) is null and 
                    src:EPO_VARDATE13 is not null) THEN
                    'EPO_VARDATE13 contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_VARDATE13) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE14), '1900-01-01')) is null and 
                    src:EPO_VARDATE14 is not null) THEN
                    'EPO_VARDATE14 contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_VARDATE14) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE15), '1900-01-01')) is null and 
                    src:EPO_VARDATE15 is not null) THEN
                    'EPO_VARDATE15 contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_VARDATE15) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE2), '1900-01-01')) is null and 
                    src:EPO_VARDATE2 is not null) THEN
                    'EPO_VARDATE2 contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_VARDATE2) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE3), '1900-01-01')) is null and 
                    src:EPO_VARDATE3 is not null) THEN
                    'EPO_VARDATE3 contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_VARDATE3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE4), '1900-01-01')) is null and 
                    src:EPO_VARDATE4 is not null) THEN
                    'EPO_VARDATE4 contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_VARDATE4) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE5), '1900-01-01')) is null and 
                    src:EPO_VARDATE5 is not null) THEN
                    'EPO_VARDATE5 contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_VARDATE5) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE6), '1900-01-01')) is null and 
                    src:EPO_VARDATE6 is not null) THEN
                    'EPO_VARDATE6 contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_VARDATE6) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE7), '1900-01-01')) is null and 
                    src:EPO_VARDATE7 is not null) THEN
                    'EPO_VARDATE7 contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_VARDATE7) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE8), '1900-01-01')) is null and 
                    src:EPO_VARDATE8 is not null) THEN
                    'EPO_VARDATE8 contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_VARDATE8) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE9), '1900-01-01')) is null and 
                    src:EPO_VARDATE9 is not null) THEN
                    'EPO_VARDATE9 contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_VARDATE9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM1), '0'), 38, 10) is null and 
                    src:EPO_VARNUM1 is not null) THEN
                    'EPO_VARNUM1 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM10), '0'), 38, 10) is null and 
                    src:EPO_VARNUM10 is not null) THEN
                    'EPO_VARNUM10 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM11), '0'), 38, 10) is null and 
                    src:EPO_VARNUM11 is not null) THEN
                    'EPO_VARNUM11 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM12), '0'), 38, 10) is null and 
                    src:EPO_VARNUM12 is not null) THEN
                    'EPO_VARNUM12 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM12) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM13), '0'), 38, 10) is null and 
                    src:EPO_VARNUM13 is not null) THEN
                    'EPO_VARNUM13 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM13) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM14), '0'), 38, 10) is null and 
                    src:EPO_VARNUM14 is not null) THEN
                    'EPO_VARNUM14 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM14) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM15), '0'), 38, 10) is null and 
                    src:EPO_VARNUM15 is not null) THEN
                    'EPO_VARNUM15 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM15) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM16), '0'), 38, 10) is null and 
                    src:EPO_VARNUM16 is not null) THEN
                    'EPO_VARNUM16 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM16) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM17), '0'), 38, 10) is null and 
                    src:EPO_VARNUM17 is not null) THEN
                    'EPO_VARNUM17 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM17) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM18), '0'), 38, 10) is null and 
                    src:EPO_VARNUM18 is not null) THEN
                    'EPO_VARNUM18 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM18) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM19), '0'), 38, 10) is null and 
                    src:EPO_VARNUM19 is not null) THEN
                    'EPO_VARNUM19 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM19) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM2), '0'), 38, 10) is null and 
                    src:EPO_VARNUM2 is not null) THEN
                    'EPO_VARNUM2 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM20), '0'), 38, 10) is null and 
                    src:EPO_VARNUM20 is not null) THEN
                    'EPO_VARNUM20 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM20) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM21), '0'), 38, 10) is null and 
                    src:EPO_VARNUM21 is not null) THEN
                    'EPO_VARNUM21 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM21) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM22), '0'), 38, 10) is null and 
                    src:EPO_VARNUM22 is not null) THEN
                    'EPO_VARNUM22 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM22) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM23), '0'), 38, 10) is null and 
                    src:EPO_VARNUM23 is not null) THEN
                    'EPO_VARNUM23 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM23) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM24), '0'), 38, 10) is null and 
                    src:EPO_VARNUM24 is not null) THEN
                    'EPO_VARNUM24 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM24) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM25), '0'), 38, 10) is null and 
                    src:EPO_VARNUM25 is not null) THEN
                    'EPO_VARNUM25 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM25) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM26), '0'), 38, 10) is null and 
                    src:EPO_VARNUM26 is not null) THEN
                    'EPO_VARNUM26 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM26) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM27), '0'), 38, 10) is null and 
                    src:EPO_VARNUM27 is not null) THEN
                    'EPO_VARNUM27 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM27) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM28), '0'), 38, 10) is null and 
                    src:EPO_VARNUM28 is not null) THEN
                    'EPO_VARNUM28 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM28) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM29), '0'), 38, 10) is null and 
                    src:EPO_VARNUM29 is not null) THEN
                    'EPO_VARNUM29 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM29) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM3), '0'), 38, 10) is null and 
                    src:EPO_VARNUM3 is not null) THEN
                    'EPO_VARNUM3 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM30), '0'), 38, 10) is null and 
                    src:EPO_VARNUM30 is not null) THEN
                    'EPO_VARNUM30 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM30) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM31), '0'), 38, 10) is null and 
                    src:EPO_VARNUM31 is not null) THEN
                    'EPO_VARNUM31 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM31) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM32), '0'), 38, 10) is null and 
                    src:EPO_VARNUM32 is not null) THEN
                    'EPO_VARNUM32 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM32) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM33), '0'), 38, 10) is null and 
                    src:EPO_VARNUM33 is not null) THEN
                    'EPO_VARNUM33 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM33) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM34), '0'), 38, 10) is null and 
                    src:EPO_VARNUM34 is not null) THEN
                    'EPO_VARNUM34 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM34) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM35), '0'), 38, 10) is null and 
                    src:EPO_VARNUM35 is not null) THEN
                    'EPO_VARNUM35 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM35) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM4), '0'), 38, 10) is null and 
                    src:EPO_VARNUM4 is not null) THEN
                    'EPO_VARNUM4 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM5), '0'), 38, 10) is null and 
                    src:EPO_VARNUM5 is not null) THEN
                    'EPO_VARNUM5 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM6), '0'), 38, 10) is null and 
                    src:EPO_VARNUM6 is not null) THEN
                    'EPO_VARNUM6 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM7), '0'), 38, 10) is null and 
                    src:EPO_VARNUM7 is not null) THEN
                    'EPO_VARNUM7 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM8), '0'), 38, 10) is null and 
                    src:EPO_VARNUM8 is not null) THEN
                    'EPO_VARNUM8 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM9), '0'), 38, 10) is null and 
                    src:EPO_VARNUM9 is not null) THEN
                    'EPO_VARNUM9 contains a non-numeric value : \'' || AS_VARCHAR(src:EPO_VARNUM9) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_LASTSAVED), '1900-01-01')) is null and 
                src:EPO_LASTSAVED is not null) THEN
                'EPO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:EPO_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:EPO_CODE  ORDER BY 
            src:EPO_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5EVENTPOINTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_ASSESSEDSLOPE), '0'), 38, 10) is null and 
                    src:EPO_ASSESSEDSLOPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_AVGSLOPE), '0'), 38, 10) is null and 
                    src:EPO_AVGSLOPE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_CREATED), '1900-01-01')) is null and 
                    src:EPO_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_DATE), '1900-01-01')) is null and 
                    src:EPO_DATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROMPOINT), '0'), 38, 10) is null and 
                    src:EPO_FROMPOINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:EPO_FROM_HORIZONTAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_LATITUDE), '0'), 38, 10) is null and 
                    src:EPO_FROM_LATITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_LONGITUDE), '0'), 38, 10) is null and 
                    src:EPO_FROM_LONGITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_OFFSET), '0'), 38, 10) is null and 
                    src:EPO_FROM_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:EPO_FROM_OFFSET_PERCENTAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_REFERENCE), '0'), 38, 10) is null and 
                    src:EPO_FROM_REFERENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:EPO_FROM_VERTICAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_XCOORDINATE), '0'), 38, 10) is null and 
                    src:EPO_FROM_XCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_FROM_YCOORDINATE), '0'), 38, 10) is null and 
                    src:EPO_FROM_YCOORDINATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_LASTSAVED), '1900-01-01')) is null and 
                    src:EPO_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_LASTSLOPE), '0'), 38, 10) is null and 
                    src:EPO_LASTSLOPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_LINE), '0'), 38, 10) is null and 
                    src:EPO_LINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_MAX), '0'), 38, 10) is null and 
                    src:EPO_MAX is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_MAXDATE), '1900-01-01')) is null and 
                    src:EPO_MAXDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_MAXTOL), '0'), 38, 10) is null and 
                    src:EPO_MAXTOL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_MAXTOLDATE), '1900-01-01')) is null and 
                    src:EPO_MAXTOLDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_MIN), '0'), 38, 10) is null and 
                    src:EPO_MIN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_MINDATE), '1900-01-01')) is null and 
                    src:EPO_MINDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_MINTOL), '0'), 38, 10) is null and 
                    src:EPO_MINTOL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_MINTOLDATE), '1900-01-01')) is null and 
                    src:EPO_MINTOLDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_NIDATEMAXCRIT), '1900-01-01')) is null and 
                    src:EPO_NIDATEMAXCRIT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_NIDATEMAXTOL), '1900-01-01')) is null and 
                    src:EPO_NIDATEMAXTOL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_NIDATEMINCRIT), '1900-01-01')) is null and 
                    src:EPO_NIDATEMINCRIT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_NIDATEMINTOL), '1900-01-01')) is null and 
                    src:EPO_NIDATEMINTOL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TOPOINT), '0'), 38, 10) is null and 
                    src:EPO_TOPOINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:EPO_TO_HORIZONTAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_LATITUDE), '0'), 38, 10) is null and 
                    src:EPO_TO_LATITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_LONGITUDE), '0'), 38, 10) is null and 
                    src:EPO_TO_LONGITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_OFFSET), '0'), 38, 10) is null and 
                    src:EPO_TO_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:EPO_TO_OFFSET_PERCENTAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_REFERENCE), '0'), 38, 10) is null and 
                    src:EPO_TO_REFERENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:EPO_TO_VERTICAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_XCOORDINATE), '0'), 38, 10) is null and 
                    src:EPO_TO_XCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_TO_YCOORDINATE), '0'), 38, 10) is null and 
                    src:EPO_TO_YCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:EPO_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_UPDATED), '1900-01-01')) is null and 
                    src:EPO_UPDATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VALUE), '0'), 38, 10) is null and 
                    src:EPO_VALUE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE1), '1900-01-01')) is null and 
                    src:EPO_VARDATE1 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE10), '1900-01-01')) is null and 
                    src:EPO_VARDATE10 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE11), '1900-01-01')) is null and 
                    src:EPO_VARDATE11 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE12), '1900-01-01')) is null and 
                    src:EPO_VARDATE12 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE13), '1900-01-01')) is null and 
                    src:EPO_VARDATE13 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE14), '1900-01-01')) is null and 
                    src:EPO_VARDATE14 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE15), '1900-01-01')) is null and 
                    src:EPO_VARDATE15 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE2), '1900-01-01')) is null and 
                    src:EPO_VARDATE2 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE3), '1900-01-01')) is null and 
                    src:EPO_VARDATE3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE4), '1900-01-01')) is null and 
                    src:EPO_VARDATE4 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE5), '1900-01-01')) is null and 
                    src:EPO_VARDATE5 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE6), '1900-01-01')) is null and 
                    src:EPO_VARDATE6 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE7), '1900-01-01')) is null and 
                    src:EPO_VARDATE7 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE8), '1900-01-01')) is null and 
                    src:EPO_VARDATE8 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_VARDATE9), '1900-01-01')) is null and 
                    src:EPO_VARDATE9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM1), '0'), 38, 10) is null and 
                    src:EPO_VARNUM1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM10), '0'), 38, 10) is null and 
                    src:EPO_VARNUM10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM11), '0'), 38, 10) is null and 
                    src:EPO_VARNUM11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM12), '0'), 38, 10) is null and 
                    src:EPO_VARNUM12 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM13), '0'), 38, 10) is null and 
                    src:EPO_VARNUM13 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM14), '0'), 38, 10) is null and 
                    src:EPO_VARNUM14 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM15), '0'), 38, 10) is null and 
                    src:EPO_VARNUM15 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM16), '0'), 38, 10) is null and 
                    src:EPO_VARNUM16 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM17), '0'), 38, 10) is null and 
                    src:EPO_VARNUM17 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM18), '0'), 38, 10) is null and 
                    src:EPO_VARNUM18 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM19), '0'), 38, 10) is null and 
                    src:EPO_VARNUM19 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM2), '0'), 38, 10) is null and 
                    src:EPO_VARNUM2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM20), '0'), 38, 10) is null and 
                    src:EPO_VARNUM20 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM21), '0'), 38, 10) is null and 
                    src:EPO_VARNUM21 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM22), '0'), 38, 10) is null and 
                    src:EPO_VARNUM22 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM23), '0'), 38, 10) is null and 
                    src:EPO_VARNUM23 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM24), '0'), 38, 10) is null and 
                    src:EPO_VARNUM24 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM25), '0'), 38, 10) is null and 
                    src:EPO_VARNUM25 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM26), '0'), 38, 10) is null and 
                    src:EPO_VARNUM26 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM27), '0'), 38, 10) is null and 
                    src:EPO_VARNUM27 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM28), '0'), 38, 10) is null and 
                    src:EPO_VARNUM28 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM29), '0'), 38, 10) is null and 
                    src:EPO_VARNUM29 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM3), '0'), 38, 10) is null and 
                    src:EPO_VARNUM3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM30), '0'), 38, 10) is null and 
                    src:EPO_VARNUM30 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM31), '0'), 38, 10) is null and 
                    src:EPO_VARNUM31 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM32), '0'), 38, 10) is null and 
                    src:EPO_VARNUM32 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM33), '0'), 38, 10) is null and 
                    src:EPO_VARNUM33 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM34), '0'), 38, 10) is null and 
                    src:EPO_VARNUM34 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM35), '0'), 38, 10) is null and 
                    src:EPO_VARNUM35 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM4), '0'), 38, 10) is null and 
                    src:EPO_VARNUM4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM5), '0'), 38, 10) is null and 
                    src:EPO_VARNUM5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM6), '0'), 38, 10) is null and 
                    src:EPO_VARNUM6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM7), '0'), 38, 10) is null and 
                    src:EPO_VARNUM7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM8), '0'), 38, 10) is null and 
                    src:EPO_VARNUM8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EPO_VARNUM9), '0'), 38, 10) is null and 
                    src:EPO_VARNUM9 is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EPO_LASTSAVED), '1900-01-01')) is null and 
                src:EPO_LASTSAVED is not null)