CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5EVENTS_ERROR AS SELECT src, 'EAM_R5EVENTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_ACD), '0'), 38, 10) is null and 
                    src:EVT_ACD is not null) THEN
                    'EVT_ACD contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_ACD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_CALCULATEDPRIORITY), '0'), 38, 10) is null and 
                    src:EVT_CALCULATEDPRIORITY is not null) THEN
                    'EVT_CALCULATEDPRIORITY contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_CALCULATEDPRIORITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_CAMPAIGN_LINE), '0'), 38, 10) is null and 
                    src:EVT_CAMPAIGN_LINE is not null) THEN
                    'EVT_CAMPAIGN_LINE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_CAMPAIGN_LINE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_COMPLETED), '1900-01-01')) is null and 
                    src:EVT_COMPLETED is not null) THEN
                    'EVT_COMPLETED contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_COMPLETED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_COMPLETED_TRUNC), '1900-01-01')) is null and 
                    src:EVT_COMPLETED_TRUNC is not null) THEN
                    'EVT_COMPLETED_TRUNC contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_COMPLETED_TRUNC) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_CREATED), '1900-01-01')) is null and 
                    src:EVT_CREATED is not null) THEN
                    'EVT_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_DATE), '1900-01-01')) is null and 
                    src:EVT_DATE is not null) THEN
                    'EVT_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_DATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_DOWNTIME), '0'), 38, 10) is null and 
                    src:EVT_DOWNTIME is not null) THEN
                    'EVT_DOWNTIME contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_DOWNTIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_DOWNTIMEHRS), '0'), 38, 10) is null and 
                    src:EVT_DOWNTIMEHRS is not null) THEN
                    'EVT_DOWNTIMEHRS contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_DOWNTIMEHRS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_DUE), '1900-01-01')) is null and 
                    src:EVT_DUE is not null) THEN
                    'EVT_DUE contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_DUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_DURATION), '0'), 38, 10) is null and 
                    src:EVT_DURATION is not null) THEN
                    'EVT_DURATION contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_DURATION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_EARLYSTART), '1900-01-01')) is null and 
                    src:EVT_EARLYSTART is not null) THEN
                    'EVT_EARLYSTART contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_EARLYSTART) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FAILUREUSAGE), '0'), 38, 10) is null and 
                    src:EVT_FAILUREUSAGE is not null) THEN
                    'EVT_FAILUREUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_FAILUREUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FREQ), '0'), 38, 10) is null and 
                    src:EVT_FREQ is not null) THEN
                    'EVT_FREQ contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_FREQ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROMPOINT), '0'), 38, 10) is null and 
                    src:EVT_FROMPOINT is not null) THEN
                    'EVT_FROMPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_FROMPOINT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:EVT_FROM_HORIZONTAL_OFFSET is not null) THEN
                    'EVT_FROM_HORIZONTAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_FROM_HORIZONTAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_LATITUDE), '0'), 38, 10) is null and 
                    src:EVT_FROM_LATITUDE is not null) THEN
                    'EVT_FROM_LATITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_FROM_LATITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_LONGITUDE), '0'), 38, 10) is null and 
                    src:EVT_FROM_LONGITUDE is not null) THEN
                    'EVT_FROM_LONGITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_FROM_LONGITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_OFFSET), '0'), 38, 10) is null and 
                    src:EVT_FROM_OFFSET is not null) THEN
                    'EVT_FROM_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_FROM_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:EVT_FROM_OFFSET_PERCENTAGE is not null) THEN
                    'EVT_FROM_OFFSET_PERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_FROM_OFFSET_PERCENTAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_REFERENCE), '0'), 38, 10) is null and 
                    src:EVT_FROM_REFERENCE is not null) THEN
                    'EVT_FROM_REFERENCE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_FROM_REFERENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:EVT_FROM_VERTICAL_OFFSET is not null) THEN
                    'EVT_FROM_VERTICAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_FROM_VERTICAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_XCOORDINATE), '0'), 38, 10) is null and 
                    src:EVT_FROM_XCOORDINATE is not null) THEN
                    'EVT_FROM_XCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_FROM_XCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_YCOORDINATE), '0'), 38, 10) is null and 
                    src:EVT_FROM_YCOORDINATE is not null) THEN
                    'EVT_FROM_YCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_FROM_YCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_GENWINBEGVAL), '0'), 38, 10) is null and 
                    src:EVT_GENWINBEGVAL is not null) THEN
                    'EVT_GENWINBEGVAL contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_GENWINBEGVAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_GENWINBEGVAL2), '0'), 38, 10) is null and 
                    src:EVT_GENWINBEGVAL2 is not null) THEN
                    'EVT_GENWINBEGVAL2 contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_GENWINBEGVAL2) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_GENWINSTART), '1900-01-01')) is null and 
                    src:EVT_GENWINSTART is not null) THEN
                    'EVT_GENWINSTART contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_GENWINSTART) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_GLTRANSFER), '1900-01-01')) is null and 
                    src:EVT_GLTRANSFER is not null) THEN
                    'EVT_GLTRANSFER contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_GLTRANSFER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_GUESTPROFILEID), '0'), 38, 10) is null and 
                    src:EVT_GUESTPROFILEID is not null) THEN
                    'EVT_GUESTPROFILEID contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_GUESTPROFILEID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_INCREMENT), '0'), 38, 10) is null and 
                    src:EVT_INCREMENT is not null) THEN
                    'EVT_INCREMENT contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_INCREMENT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_INTERFACE), '1900-01-01')) is null and 
                    src:EVT_INTERFACE is not null) THEN
                    'EVT_INTERFACE contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_INTERFACE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_INVOICELINE), '0'), 38, 10) is null and 
                    src:EVT_INVOICELINE is not null) THEN
                    'EVT_INVOICELINE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_INVOICELINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_INVOICE_REVISION), '0'), 38, 10) is null and 
                    src:EVT_INVOICE_REVISION is not null) THEN
                    'EVT_INVOICE_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_INVOICE_REVISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_LABTOTAL), '0'), 38, 10) is null and 
                    src:EVT_LABTOTAL is not null) THEN
                    'EVT_LABTOTAL contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_LABTOTAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LASTCAL), '1900-01-01')) is null and 
                    src:EVT_LASTCAL is not null) THEN
                    'EVT_LASTCAL contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_LASTCAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LASTPLAN), '1900-01-01')) is null and 
                    src:EVT_LASTPLAN is not null) THEN
                    'EVT_LASTPLAN contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_LASTPLAN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LASTSAVED), '1900-01-01')) is null and 
                    src:EVT_LASTSAVED is not null) THEN
                    'EVT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:EVT_LASTSTATUSUPDATE is not null) THEN
                    'EVT_LASTSTATUSUPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_LASTSTATUSUPDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LASTTIMEPB), '1900-01-01')) is null and 
                    src:EVT_LASTTIMEPB is not null) THEN
                    'EVT_LASTTIMEPB contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_LASTTIMEPB) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LASTTIMERB), '1900-01-01')) is null and 
                    src:EVT_LASTTIMERB is not null) THEN
                    'EVT_LASTTIMERB contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_LASTTIMERB) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LATEEND), '1900-01-01')) is null and 
                    src:EVT_LATEEND is not null) THEN
                    'EVT_LATEEND contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_LATEEND) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_LATITUDE), '0'), 38, 10) is null and 
                    src:EVT_LATITUDE is not null) THEN
                    'EVT_LATITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_LATITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_LONGITUDE), '0'), 38, 10) is null and 
                    src:EVT_LONGITUDE is not null) THEN
                    'EVT_LONGITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_LONGITUDE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_MATAVAIL), '1900-01-01')) is null and 
                    src:EVT_MATAVAIL is not null) THEN
                    'EVT_MATAVAIL contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_MATAVAIL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_MATTOTAL), '0'), 38, 10) is null and 
                    src:EVT_MATTOTAL is not null) THEN
                    'EVT_MATTOTAL contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_MATTOTAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_MAXCOST), '0'), 38, 10) is null and 
                    src:EVT_MAXCOST is not null) THEN
                    'EVT_MAXCOST contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_MAXCOST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_METERDUE), '0'), 38, 10) is null and 
                    src:EVT_METERDUE is not null) THEN
                    'EVT_METERDUE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_METERDUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_METERDUE2), '0'), 38, 10) is null and 
                    src:EVT_METERDUE2 is not null) THEN
                    'EVT_METERDUE2 contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_METERDUE2) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_METERDUEDATE), '1900-01-01')) is null and 
                    src:EVT_METERDUEDATE is not null) THEN
                    'EVT_METERDUEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_METERDUEDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_METERDUEDATE2), '1900-01-01')) is null and 
                    src:EVT_METERDUEDATE2 is not null) THEN
                    'EVT_METERDUEDATE2 contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_METERDUEDATE2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_METERINTERVAL), '0'), 38, 10) is null and 
                    src:EVT_METERINTERVAL is not null) THEN
                    'EVT_METERINTERVAL contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_METERINTERVAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_METERINTERVAL2), '0'), 38, 10) is null and 
                    src:EVT_METERINTERVAL2 is not null) THEN
                    'EVT_METERINTERVAL2 contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_METERINTERVAL2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_METERREADING), '0'), 38, 10) is null and 
                    src:EVT_METERREADING is not null) THEN
                    'EVT_METERREADING contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_METERREADING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_MP_REV), '0'), 38, 10) is null and 
                    src:EVT_MP_REV is not null) THEN
                    'EVT_MP_REV contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_MP_REV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_MP_SEQ), '0'), 38, 10) is null and 
                    src:EVT_MP_SEQ is not null) THEN
                    'EVT_MP_SEQ contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_MP_SEQ) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_MUSTEND), '1900-01-01')) is null and 
                    src:EVT_MUSTEND is not null) THEN
                    'EVT_MUSTEND contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_MUSTEND) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_NEARWINBEGVAL), '0'), 38, 10) is null and 
                    src:EVT_NEARWINBEGVAL is not null) THEN
                    'EVT_NEARWINBEGVAL contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_NEARWINBEGVAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_NEARWINBEGVAL2), '0'), 38, 10) is null and 
                    src:EVT_NEARWINBEGVAL2 is not null) THEN
                    'EVT_NEARWINBEGVAL2 contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_NEARWINBEGVAL2) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_NEARWINSTART), '1900-01-01')) is null and 
                    src:EVT_NEARWINSTART is not null) THEN
                    'EVT_NEARWINSTART contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_NEARWINSTART) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_NEWDUR), '0'), 38, 10) is null and 
                    src:EVT_NEWDUR is not null) THEN
                    'EVT_NEWDUR contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_NEWDUR) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_NEWTARGET), '1900-01-01')) is null and 
                    src:EVT_NEWTARGET is not null) THEN
                    'EVT_NEWTARGET contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_NEWTARGET) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_OKWINEND), '1900-01-01')) is null and 
                    src:EVT_OKWINEND is not null) THEN
                    'EVT_OKWINEND contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_OKWINEND) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_OKWINENDVAL), '0'), 38, 10) is null and 
                    src:EVT_OKWINENDVAL is not null) THEN
                    'EVT_OKWINENDVAL contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_OKWINENDVAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_OKWINENDVAL2), '0'), 38, 10) is null and 
                    src:EVT_OKWINENDVAL2 is not null) THEN
                    'EVT_OKWINENDVAL2 contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_OKWINENDVAL2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_ORIGACT), '0'), 38, 10) is null and 
                    src:EVT_ORIGACT is not null) THEN
                    'EVT_ORIGACT contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_ORIGACT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_PERFORMONDAY), '0'), 38, 10) is null and 
                    src:EVT_PERFORMONDAY is not null) THEN
                    'EVT_PERFORMONDAY contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_PERFORMONDAY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_PERMITREVIEWED), '1900-01-01')) is null and 
                    src:EVT_PERMITREVIEWED is not null) THEN
                    'EVT_PERMITREVIEWED contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_PERMITREVIEWED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_PFPROMISEDATE), '1900-01-01')) is null and 
                    src:EVT_PFPROMISEDATE is not null) THEN
                    'EVT_PFPROMISEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_PFPROMISEDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_PPMREV), '0'), 38, 10) is null and 
                    src:EVT_PPMREV is not null) THEN
                    'EVT_PPMREV contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_PPMREV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_PPOPK), '0'), 38, 10) is null and 
                    src:EVT_PPOPK is not null) THEN
                    'EVT_PPOPK contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_PPOPK) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_PRECISION), '0'), 38, 10) is null and 
                    src:EVT_PRECISION is not null) THEN
                    'EVT_PRECISION contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_PRECISION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_PRODUCTIONEND), '1900-01-01')) is null and 
                    src:EVT_PRODUCTIONEND is not null) THEN
                    'EVT_PRODUCTIONEND contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_PRODUCTIONEND) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_PRODUCTIONREQUESTREV), '0'), 38, 10) is null and 
                    src:EVT_PRODUCTIONREQUESTREV is not null) THEN
                    'EVT_PRODUCTIONREQUESTREV contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_PRODUCTIONREQUESTREV) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_PRODUCTIONSTART), '1900-01-01')) is null and 
                    src:EVT_PRODUCTIONSTART is not null) THEN
                    'EVT_PRODUCTIONSTART contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_PRODUCTIONSTART) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_PSQPK), '0'), 38, 10) is null and 
                    src:EVT_PSQPK is not null) THEN
                    'EVT_PSQPK contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_PSQPK) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_REPORTED), '1900-01-01')) is null and 
                    src:EVT_REPORTED is not null) THEN
                    'EVT_REPORTED contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_REPORTED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_REQUESTEND), '1900-01-01')) is null and 
                    src:EVT_REQUESTEND is not null) THEN
                    'EVT_REQUESTEND contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_REQUESTEND) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_REQUESTSTART), '1900-01-01')) is null and 
                    src:EVT_REQUESTSTART is not null) THEN
                    'EVT_REQUESTSTART contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_REQUESTSTART) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_ROUTEREV), '0'), 38, 10) is null and 
                    src:EVT_ROUTEREV is not null) THEN
                    'EVT_ROUTEREV contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_ROUTEREV) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_SAFETYREVIEWED), '1900-01-01')) is null and 
                    src:EVT_SAFETYREVIEWED is not null) THEN
                    'EVT_SAFETYREVIEWED contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_SAFETYREVIEWED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_SCHEDEND), '1900-01-01')) is null and 
                    src:EVT_SCHEDEND is not null) THEN
                    'EVT_SCHEDEND contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_SCHEDEND) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_SCHEDNO), '0'), 38, 10) is null and 
                    src:EVT_SCHEDNO is not null) THEN
                    'EVT_SCHEDNO contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_SCHEDNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_SCHEDULEDENDTIME), '0'), 38, 10) is null and 
                    src:EVT_SCHEDULEDENDTIME is not null) THEN
                    'EVT_SCHEDULEDENDTIME contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_SCHEDULEDENDTIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_SCHEDULEDSTARTTIME), '0'), 38, 10) is null and 
                    src:EVT_SCHEDULEDSTARTTIME is not null) THEN
                    'EVT_SCHEDULEDSTARTTIME contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_SCHEDULEDSTARTTIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_SEQ), '0'), 38, 10) is null and 
                    src:EVT_SEQ is not null) THEN
                    'EVT_SEQ contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_SEQ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_SERVICEPERC), '0'), 38, 10) is null and 
                    src:EVT_SERVICEPERC is not null) THEN
                    'EVT_SERVICEPERC contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_SERVICEPERC) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_SLACK), '0'), 38, 10) is null and 
                    src:EVT_SLACK is not null) THEN
                    'EVT_SLACK contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_SLACK) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_SOPEFFECTIVE), '1900-01-01')) is null and 
                    src:EVT_SOPEFFECTIVE is not null) THEN
                    'EVT_SOPEFFECTIVE contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_SOPEFFECTIVE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_START), '1900-01-01')) is null and 
                    src:EVT_START is not null) THEN
                    'EVT_START contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_START) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_STEP), '0'), 38, 10) is null and 
                    src:EVT_STEP is not null) THEN
                    'EVT_STEP contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_STEP) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_TARGET), '1900-01-01')) is null and 
                    src:EVT_TARGET is not null) THEN
                    'EVT_TARGET contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_TARGET) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_TFDATECOMPLETED), '1900-01-01')) is null and 
                    src:EVT_TFDATECOMPLETED is not null) THEN
                    'EVT_TFDATECOMPLETED contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_TFDATECOMPLETED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_TFPROMISEDATE), '1900-01-01')) is null and 
                    src:EVT_TFPROMISEDATE is not null) THEN
                    'EVT_TFPROMISEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_TFPROMISEDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TOPOINT), '0'), 38, 10) is null and 
                    src:EVT_TOPOINT is not null) THEN
                    'EVT_TOPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_TOPOINT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:EVT_TO_HORIZONTAL_OFFSET is not null) THEN
                    'EVT_TO_HORIZONTAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_TO_HORIZONTAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_LATITUDE), '0'), 38, 10) is null and 
                    src:EVT_TO_LATITUDE is not null) THEN
                    'EVT_TO_LATITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_TO_LATITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_LONGITUDE), '0'), 38, 10) is null and 
                    src:EVT_TO_LONGITUDE is not null) THEN
                    'EVT_TO_LONGITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_TO_LONGITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_OFFSET), '0'), 38, 10) is null and 
                    src:EVT_TO_OFFSET is not null) THEN
                    'EVT_TO_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_TO_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:EVT_TO_OFFSET_PERCENTAGE is not null) THEN
                    'EVT_TO_OFFSET_PERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_TO_OFFSET_PERCENTAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_REFERENCE), '0'), 38, 10) is null and 
                    src:EVT_TO_REFERENCE is not null) THEN
                    'EVT_TO_REFERENCE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_TO_REFERENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:EVT_TO_VERTICAL_OFFSET is not null) THEN
                    'EVT_TO_VERTICAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_TO_VERTICAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_XCOORDINATE), '0'), 38, 10) is null and 
                    src:EVT_TO_XCOORDINATE is not null) THEN
                    'EVT_TO_XCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_TO_XCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_YCOORDINATE), '0'), 38, 10) is null and 
                    src:EVT_TO_YCOORDINATE is not null) THEN
                    'EVT_TO_YCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_TO_YCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TRANSGROUP), '0'), 38, 10) is null and 
                    src:EVT_TRANSGROUP is not null) THEN
                    'EVT_TRANSGROUP contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_TRANSGROUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TRANSORGID), '0'), 38, 10) is null and 
                    src:EVT_TRANSORGID is not null) THEN
                    'EVT_TRANSORGID contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_TRANSORGID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE01), '1900-01-01')) is null and 
                    src:EVT_UDFDATE01 is not null) THEN
                    'EVT_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE02), '1900-01-01')) is null and 
                    src:EVT_UDFDATE02 is not null) THEN
                    'EVT_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE03), '1900-01-01')) is null and 
                    src:EVT_UDFDATE03 is not null) THEN
                    'EVT_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE04), '1900-01-01')) is null and 
                    src:EVT_UDFDATE04 is not null) THEN
                    'EVT_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE05), '1900-01-01')) is null and 
                    src:EVT_UDFDATE05 is not null) THEN
                    'EVT_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE06), '1900-01-01')) is null and 
                    src:EVT_UDFDATE06 is not null) THEN
                    'EVT_UDFDATE06 contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_UDFDATE06) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE07), '1900-01-01')) is null and 
                    src:EVT_UDFDATE07 is not null) THEN
                    'EVT_UDFDATE07 contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_UDFDATE07) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE08), '1900-01-01')) is null and 
                    src:EVT_UDFDATE08 is not null) THEN
                    'EVT_UDFDATE08 contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_UDFDATE08) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE09), '1900-01-01')) is null and 
                    src:EVT_UDFDATE09 is not null) THEN
                    'EVT_UDFDATE09 contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_UDFDATE09) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE10), '1900-01-01')) is null and 
                    src:EVT_UDFDATE10 is not null) THEN
                    'EVT_UDFDATE10 contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_UDFDATE10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM01), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM01 is not null) THEN
                    'EVT_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM02), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM02 is not null) THEN
                    'EVT_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM03), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM03 is not null) THEN
                    'EVT_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM04), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM04 is not null) THEN
                    'EVT_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM05), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM05 is not null) THEN
                    'EVT_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM06), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM06 is not null) THEN
                    'EVT_UDFNUM06 contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_UDFNUM06) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM07), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM07 is not null) THEN
                    'EVT_UDFNUM07 contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_UDFNUM07) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM08), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM08 is not null) THEN
                    'EVT_UDFNUM08 contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_UDFNUM08) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM09), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM09 is not null) THEN
                    'EVT_UDFNUM09 contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_UDFNUM09) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM10), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM10 is not null) THEN
                    'EVT_UDFNUM10 contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_UDFNUM10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:EVT_UPDATECOUNT is not null) THEN
                    'EVT_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:EVT_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UPDATED), '1900-01-01')) is null and 
                    src:EVT_UPDATED is not null) THEN
                    'EVT_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LASTSAVED), '1900-01-01')) is null and 
                src:EVT_LASTSAVED is not null) THEN
                'EVT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:EVT_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:EVT_CODE  ORDER BY 
            src:EVT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5EVENTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_ACD), '0'), 38, 10) is null and 
                    src:EVT_ACD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_CALCULATEDPRIORITY), '0'), 38, 10) is null and 
                    src:EVT_CALCULATEDPRIORITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_CAMPAIGN_LINE), '0'), 38, 10) is null and 
                    src:EVT_CAMPAIGN_LINE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_COMPLETED), '1900-01-01')) is null and 
                    src:EVT_COMPLETED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_COMPLETED_TRUNC), '1900-01-01')) is null and 
                    src:EVT_COMPLETED_TRUNC is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_CREATED), '1900-01-01')) is null and 
                    src:EVT_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_DATE), '1900-01-01')) is null and 
                    src:EVT_DATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_DOWNTIME), '0'), 38, 10) is null and 
                    src:EVT_DOWNTIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_DOWNTIMEHRS), '0'), 38, 10) is null and 
                    src:EVT_DOWNTIMEHRS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_DUE), '1900-01-01')) is null and 
                    src:EVT_DUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_DURATION), '0'), 38, 10) is null and 
                    src:EVT_DURATION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_EARLYSTART), '1900-01-01')) is null and 
                    src:EVT_EARLYSTART is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FAILUREUSAGE), '0'), 38, 10) is null and 
                    src:EVT_FAILUREUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FREQ), '0'), 38, 10) is null and 
                    src:EVT_FREQ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROMPOINT), '0'), 38, 10) is null and 
                    src:EVT_FROMPOINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:EVT_FROM_HORIZONTAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_LATITUDE), '0'), 38, 10) is null and 
                    src:EVT_FROM_LATITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_LONGITUDE), '0'), 38, 10) is null and 
                    src:EVT_FROM_LONGITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_OFFSET), '0'), 38, 10) is null and 
                    src:EVT_FROM_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:EVT_FROM_OFFSET_PERCENTAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_REFERENCE), '0'), 38, 10) is null and 
                    src:EVT_FROM_REFERENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:EVT_FROM_VERTICAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_XCOORDINATE), '0'), 38, 10) is null and 
                    src:EVT_FROM_XCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_FROM_YCOORDINATE), '0'), 38, 10) is null and 
                    src:EVT_FROM_YCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_GENWINBEGVAL), '0'), 38, 10) is null and 
                    src:EVT_GENWINBEGVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_GENWINBEGVAL2), '0'), 38, 10) is null and 
                    src:EVT_GENWINBEGVAL2 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_GENWINSTART), '1900-01-01')) is null and 
                    src:EVT_GENWINSTART is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_GLTRANSFER), '1900-01-01')) is null and 
                    src:EVT_GLTRANSFER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_GUESTPROFILEID), '0'), 38, 10) is null and 
                    src:EVT_GUESTPROFILEID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_INCREMENT), '0'), 38, 10) is null and 
                    src:EVT_INCREMENT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_INTERFACE), '1900-01-01')) is null and 
                    src:EVT_INTERFACE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_INVOICELINE), '0'), 38, 10) is null and 
                    src:EVT_INVOICELINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_INVOICE_REVISION), '0'), 38, 10) is null and 
                    src:EVT_INVOICE_REVISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_LABTOTAL), '0'), 38, 10) is null and 
                    src:EVT_LABTOTAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LASTCAL), '1900-01-01')) is null and 
                    src:EVT_LASTCAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LASTPLAN), '1900-01-01')) is null and 
                    src:EVT_LASTPLAN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LASTSAVED), '1900-01-01')) is null and 
                    src:EVT_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:EVT_LASTSTATUSUPDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LASTTIMEPB), '1900-01-01')) is null and 
                    src:EVT_LASTTIMEPB is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LASTTIMERB), '1900-01-01')) is null and 
                    src:EVT_LASTTIMERB is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LATEEND), '1900-01-01')) is null and 
                    src:EVT_LATEEND is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_LATITUDE), '0'), 38, 10) is null and 
                    src:EVT_LATITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_LONGITUDE), '0'), 38, 10) is null and 
                    src:EVT_LONGITUDE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_MATAVAIL), '1900-01-01')) is null and 
                    src:EVT_MATAVAIL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_MATTOTAL), '0'), 38, 10) is null and 
                    src:EVT_MATTOTAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_MAXCOST), '0'), 38, 10) is null and 
                    src:EVT_MAXCOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_METERDUE), '0'), 38, 10) is null and 
                    src:EVT_METERDUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_METERDUE2), '0'), 38, 10) is null and 
                    src:EVT_METERDUE2 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_METERDUEDATE), '1900-01-01')) is null and 
                    src:EVT_METERDUEDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_METERDUEDATE2), '1900-01-01')) is null and 
                    src:EVT_METERDUEDATE2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_METERINTERVAL), '0'), 38, 10) is null and 
                    src:EVT_METERINTERVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_METERINTERVAL2), '0'), 38, 10) is null and 
                    src:EVT_METERINTERVAL2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_METERREADING), '0'), 38, 10) is null and 
                    src:EVT_METERREADING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_MP_REV), '0'), 38, 10) is null and 
                    src:EVT_MP_REV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_MP_SEQ), '0'), 38, 10) is null and 
                    src:EVT_MP_SEQ is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_MUSTEND), '1900-01-01')) is null and 
                    src:EVT_MUSTEND is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_NEARWINBEGVAL), '0'), 38, 10) is null and 
                    src:EVT_NEARWINBEGVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_NEARWINBEGVAL2), '0'), 38, 10) is null and 
                    src:EVT_NEARWINBEGVAL2 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_NEARWINSTART), '1900-01-01')) is null and 
                    src:EVT_NEARWINSTART is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_NEWDUR), '0'), 38, 10) is null and 
                    src:EVT_NEWDUR is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_NEWTARGET), '1900-01-01')) is null and 
                    src:EVT_NEWTARGET is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_OKWINEND), '1900-01-01')) is null and 
                    src:EVT_OKWINEND is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_OKWINENDVAL), '0'), 38, 10) is null and 
                    src:EVT_OKWINENDVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_OKWINENDVAL2), '0'), 38, 10) is null and 
                    src:EVT_OKWINENDVAL2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_ORIGACT), '0'), 38, 10) is null and 
                    src:EVT_ORIGACT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_PERFORMONDAY), '0'), 38, 10) is null and 
                    src:EVT_PERFORMONDAY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_PERMITREVIEWED), '1900-01-01')) is null and 
                    src:EVT_PERMITREVIEWED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_PFPROMISEDATE), '1900-01-01')) is null and 
                    src:EVT_PFPROMISEDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_PPMREV), '0'), 38, 10) is null and 
                    src:EVT_PPMREV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_PPOPK), '0'), 38, 10) is null and 
                    src:EVT_PPOPK is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_PRECISION), '0'), 38, 10) is null and 
                    src:EVT_PRECISION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_PRODUCTIONEND), '1900-01-01')) is null and 
                    src:EVT_PRODUCTIONEND is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_PRODUCTIONREQUESTREV), '0'), 38, 10) is null and 
                    src:EVT_PRODUCTIONREQUESTREV is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_PRODUCTIONSTART), '1900-01-01')) is null and 
                    src:EVT_PRODUCTIONSTART is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_PSQPK), '0'), 38, 10) is null and 
                    src:EVT_PSQPK is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_REPORTED), '1900-01-01')) is null and 
                    src:EVT_REPORTED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_REQUESTEND), '1900-01-01')) is null and 
                    src:EVT_REQUESTEND is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_REQUESTSTART), '1900-01-01')) is null and 
                    src:EVT_REQUESTSTART is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_ROUTEREV), '0'), 38, 10) is null and 
                    src:EVT_ROUTEREV is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_SAFETYREVIEWED), '1900-01-01')) is null and 
                    src:EVT_SAFETYREVIEWED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_SCHEDEND), '1900-01-01')) is null and 
                    src:EVT_SCHEDEND is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_SCHEDNO), '0'), 38, 10) is null and 
                    src:EVT_SCHEDNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_SCHEDULEDENDTIME), '0'), 38, 10) is null and 
                    src:EVT_SCHEDULEDENDTIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_SCHEDULEDSTARTTIME), '0'), 38, 10) is null and 
                    src:EVT_SCHEDULEDSTARTTIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_SEQ), '0'), 38, 10) is null and 
                    src:EVT_SEQ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_SERVICEPERC), '0'), 38, 10) is null and 
                    src:EVT_SERVICEPERC is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_SLACK), '0'), 38, 10) is null and 
                    src:EVT_SLACK is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_SOPEFFECTIVE), '1900-01-01')) is null and 
                    src:EVT_SOPEFFECTIVE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_START), '1900-01-01')) is null and 
                    src:EVT_START is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_STEP), '0'), 38, 10) is null and 
                    src:EVT_STEP is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_TARGET), '1900-01-01')) is null and 
                    src:EVT_TARGET is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_TFDATECOMPLETED), '1900-01-01')) is null and 
                    src:EVT_TFDATECOMPLETED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_TFPROMISEDATE), '1900-01-01')) is null and 
                    src:EVT_TFPROMISEDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TOPOINT), '0'), 38, 10) is null and 
                    src:EVT_TOPOINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:EVT_TO_HORIZONTAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_LATITUDE), '0'), 38, 10) is null and 
                    src:EVT_TO_LATITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_LONGITUDE), '0'), 38, 10) is null and 
                    src:EVT_TO_LONGITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_OFFSET), '0'), 38, 10) is null and 
                    src:EVT_TO_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:EVT_TO_OFFSET_PERCENTAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_REFERENCE), '0'), 38, 10) is null and 
                    src:EVT_TO_REFERENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:EVT_TO_VERTICAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_XCOORDINATE), '0'), 38, 10) is null and 
                    src:EVT_TO_XCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TO_YCOORDINATE), '0'), 38, 10) is null and 
                    src:EVT_TO_YCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TRANSGROUP), '0'), 38, 10) is null and 
                    src:EVT_TRANSGROUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_TRANSORGID), '0'), 38, 10) is null and 
                    src:EVT_TRANSORGID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE01), '1900-01-01')) is null and 
                    src:EVT_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE02), '1900-01-01')) is null and 
                    src:EVT_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE03), '1900-01-01')) is null and 
                    src:EVT_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE04), '1900-01-01')) is null and 
                    src:EVT_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE05), '1900-01-01')) is null and 
                    src:EVT_UDFDATE05 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE06), '1900-01-01')) is null and 
                    src:EVT_UDFDATE06 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE07), '1900-01-01')) is null and 
                    src:EVT_UDFDATE07 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE08), '1900-01-01')) is null and 
                    src:EVT_UDFDATE08 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE09), '1900-01-01')) is null and 
                    src:EVT_UDFDATE09 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UDFDATE10), '1900-01-01')) is null and 
                    src:EVT_UDFDATE10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM01), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM02), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM03), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM04), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM05), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM06), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM06 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM07), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM07 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM08), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM08 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM09), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM09 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UDFNUM10), '0'), 38, 10) is null and 
                    src:EVT_UDFNUM10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:EVT_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_UPDATED), '1900-01-01')) is null and 
                    src:EVT_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVT_LASTSAVED), '1900-01-01')) is null and 
                src:EVT_LASTSAVED is not null)