CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5OBJECTS_ERROR AS SELECT src, 'EAM_R5OBJECTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_ACD), '0'), 38, 10) is null and 
                    src:OBJ_ACD is not null) THEN
                    'OBJ_ACD contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_ACD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_AVAILABLECAPACITY), '0'), 38, 10) is null and 
                    src:OBJ_AVAILABLECAPACITY is not null) THEN
                    'OBJ_AVAILABLECAPACITY contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_AVAILABLECAPACITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_BATHCOUNT), '0'), 38, 10) is null and 
                    src:OBJ_BATHCOUNT is not null) THEN
                    'OBJ_BATHCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_BATHCOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_BEDCOUNT), '0'), 38, 10) is null and 
                    src:OBJ_BEDCOUNT is not null) THEN
                    'OBJ_BEDCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_BEDCOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_BILLEVERYPERIOD), '0'), 38, 10) is null and 
                    src:OBJ_BILLEVERYPERIOD is not null) THEN
                    'OBJ_BILLEVERYPERIOD contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_BILLEVERYPERIOD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_BLACKSWANCOST), '0'), 38, 10) is null and 
                    src:OBJ_BLACKSWANCOST is not null) THEN
                    'OBJ_BLACKSWANCOST contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_BLACKSWANCOST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CAPACITYRATING), '0'), 38, 10) is null and 
                    src:OBJ_CAPACITYRATING is not null) THEN
                    'OBJ_CAPACITYRATING contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_CAPACITYRATING) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_COMMISS), '1900-01-01')) is null and 
                    src:OBJ_COMMISS is not null) THEN
                    'OBJ_COMMISS contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_COMMISS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CONDITIONRATING), '0'), 38, 10) is null and 
                    src:OBJ_CONDITIONRATING is not null) THEN
                    'OBJ_CONDITIONRATING contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_CONDITIONRATING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CONDITIONSCORE), '0'), 38, 10) is null and 
                    src:OBJ_CONDITIONSCORE is not null) THEN
                    'OBJ_CONDITIONSCORE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_CONDITIONSCORE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_CONFIGAPPROVED), '1900-01-01')) is null and 
                    src:OBJ_CONFIGAPPROVED is not null) THEN
                    'OBJ_CONFIGAPPROVED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_CONFIGAPPROVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_CONFIGREQUESTED), '1900-01-01')) is null and 
                    src:OBJ_CONFIGREQUESTED is not null) THEN
                    'OBJ_CONFIGREQUESTED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_CONFIGREQUESTED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CONFIGREVISION), '0'), 38, 10) is null and 
                    src:OBJ_CONFIGREVISION is not null) THEN
                    'OBJ_CONFIGREVISION contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_CONFIGREVISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CONFIGSEQLENGTH), '0'), 38, 10) is null and 
                    src:OBJ_CONFIGSEQLENGTH is not null) THEN
                    'OBJ_CONFIGSEQLENGTH contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_CONFIGSEQLENGTH) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_CORRCONDITIONDATE), '1900-01-01')) is null and 
                    src:OBJ_CORRCONDITIONDATE is not null) THEN
                    'OBJ_CORRCONDITIONDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_CORRCONDITIONDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CORRCONDITIONSCORE), '0'), 38, 10) is null and 
                    src:OBJ_CORRCONDITIONSCORE is not null) THEN
                    'OBJ_CORRCONDITIONSCORE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_CORRCONDITIONSCORE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CORRCONDITIONUSAGE), '0'), 38, 10) is null and 
                    src:OBJ_CORRCONDITIONUSAGE is not null) THEN
                    'OBJ_CORRCONDITIONUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_CORRCONDITIONUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_COSTOFNEEDEDREPAIRS), '0'), 38, 10) is null and 
                    src:OBJ_COSTOFNEEDEDREPAIRS is not null) THEN
                    'OBJ_COSTOFNEEDEDREPAIRS contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_COSTOFNEEDEDREPAIRS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_COUNTRY), '0'), 38, 10) is null and 
                    src:OBJ_COUNTRY is not null) THEN
                    'OBJ_COUNTRY contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_COUNTRY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_CREATED), '1900-01-01')) is null and 
                    src:OBJ_CREATED is not null) THEN
                    'OBJ_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CRITICALITYSCORE), '0'), 38, 10) is null and 
                    src:OBJ_CRITICALITYSCORE is not null) THEN
                    'OBJ_CRITICALITYSCORE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_CRITICALITYSCORE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_DESIREDCAPACITY), '0'), 38, 10) is null and 
                    src:OBJ_DESIREDCAPACITY is not null) THEN
                    'OBJ_DESIREDCAPACITY contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_DESIREDCAPACITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_DEVICERANGEFROM), '0'), 38, 10) is null and 
                    src:OBJ_DEVICERANGEFROM is not null) THEN
                    'OBJ_DEVICERANGEFROM contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_DEVICERANGEFROM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_DEVICERANGETO), '0'), 38, 10) is null and 
                    src:OBJ_DEVICERANGETO is not null) THEN
                    'OBJ_DEVICERANGETO contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_DEVICERANGETO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_DEVICETOLFROM), '0'), 38, 10) is null and 
                    src:OBJ_DEVICETOLFROM is not null) THEN
                    'OBJ_DEVICETOLFROM contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_DEVICETOLFROM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_DEVICETOLTO), '0'), 38, 10) is null and 
                    src:OBJ_DEVICETOLTO is not null) THEN
                    'OBJ_DEVICETOLTO contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_DEVICETOLTO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_DORMEND), '1900-01-01')) is null and 
                    src:OBJ_DORMEND is not null) THEN
                    'OBJ_DORMEND contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_DORMEND) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_DORMSTART), '1900-01-01')) is null and 
                    src:OBJ_DORMSTART is not null) THEN
                    'OBJ_DORMSTART contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_DORMSTART) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_EFFLOSSPHASEIMBALANCE1), '0'), 38, 10) is null and 
                    src:OBJ_EFFLOSSPHASEIMBALANCE1 is not null) THEN
                    'OBJ_EFFLOSSPHASEIMBALANCE1 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_EFFLOSSPHASEIMBALANCE1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_EFFLOSSPHASEIMBALANCE2), '0'), 38, 10) is null and 
                    src:OBJ_EFFLOSSPHASEIMBALANCE2 is not null) THEN
                    'OBJ_EFFLOSSPHASEIMBALANCE2 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_EFFLOSSPHASEIMBALANCE2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_EFFLOSSPHASEIMBALANCE3), '0'), 38, 10) is null and 
                    src:OBJ_EFFLOSSPHASEIMBALANCE3 is not null) THEN
                    'OBJ_EFFLOSSPHASEIMBALANCE3 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_EFFLOSSPHASEIMBALANCE3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_EFFLOSSPHASEIMBALANCE4), '0'), 38, 10) is null and 
                    src:OBJ_EFFLOSSPHASEIMBALANCE4 is not null) THEN
                    'OBJ_EFFLOSSPHASEIMBALANCE4 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_EFFLOSSPHASEIMBALANCE4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_EFFLOSSPHASEIMBALANCE5), '0'), 38, 10) is null and 
                    src:OBJ_EFFLOSSPHASEIMBALANCE5 is not null) THEN
                    'OBJ_EFFLOSSPHASEIMBALANCE5 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_EFFLOSSPHASEIMBALANCE5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_ELECSUBMETERINTERVAL), '0'), 38, 10) is null and 
                    src:OBJ_ELECSUBMETERINTERVAL is not null) THEN
                    'OBJ_ELECSUBMETERINTERVAL contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_ELECSUBMETERINTERVAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_ELECUSAGETHRESHOLD), '0'), 38, 10) is null and 
                    src:OBJ_ELECUSAGETHRESHOLD is not null) THEN
                    'OBJ_ELECUSAGETHRESHOLD contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_ELECUSAGETHRESHOLD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_ENDUSEFULLIFE), '1900-01-01')) is null and 
                    src:OBJ_ENDUSEFULLIFE is not null) THEN
                    'OBJ_ENDUSEFULLIFE contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_ENDUSEFULLIFE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_ESTREVENUE), '0'), 38, 10) is null and 
                    src:OBJ_ESTREVENUE is not null) THEN
                    'OBJ_ESTREVENUE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_ESTREVENUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_FACILITYCONDITIONINDEX), '0'), 38, 10) is null and 
                    src:OBJ_FACILITYCONDITIONINDEX is not null) THEN
                    'OBJ_FACILITYCONDITIONINDEX contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_FACILITYCONDITIONINDEX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_FAILUREPROBPERCENTAGE), '0'), 38, 10) is null and 
                    src:OBJ_FAILUREPROBPERCENTAGE is not null) THEN
                    'OBJ_FAILUREPROBPERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_FAILUREPROBPERCENTAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_FLOORAREA), '0'), 38, 10) is null and 
                    src:OBJ_FLOORAREA is not null) THEN
                    'OBJ_FLOORAREA contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_FLOORAREA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_FROMPOINT), '0'), 38, 10) is null and 
                    src:OBJ_FROMPOINT is not null) THEN
                    'OBJ_FROMPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_FROMPOINT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_GISOBJID), '0'), 38, 10) is null and 
                    src:OBJ_GISOBJID is not null) THEN
                    'OBJ_GISOBJID contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_GISOBJID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_GIS_SYNCCOUNT), '0'), 38, 10) is null and 
                    src:OBJ_GIS_SYNCCOUNT is not null) THEN
                    'OBJ_GIS_SYNCCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_GIS_SYNCCOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_GIS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:OBJ_GIS_UPDATECOUNT is not null) THEN
                    'OBJ_GIS_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_GIS_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_GLTRANSFER), '1900-01-01')) is null and 
                    src:OBJ_GLTRANSFER is not null) THEN
                    'OBJ_GLTRANSFER contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_GLTRANSFER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_GUESTPROFILEID), '0'), 38, 10) is null and 
                    src:OBJ_GUESTPROFILEID is not null) THEN
                    'OBJ_GUESTPROFILEID contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_GUESTPROFILEID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_INCREMENT), '0'), 38, 10) is null and 
                    src:OBJ_INCREMENT is not null) THEN
                    'OBJ_INCREMENT contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_INCREMENT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_INSTALLDATE), '1900-01-01')) is null and 
                    src:OBJ_INSTALLDATE is not null) THEN
                    'OBJ_INSTALLDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_INSTALLDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_INTERFACE), '1900-01-01')) is null and 
                    src:OBJ_INTERFACE is not null) THEN
                    'OBJ_INTERFACE contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_INTERFACE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_INVENTORYVERIFICATIONDATE), '1900-01-01')) is null and 
                    src:OBJ_INVENTORYVERIFICATIONDATE is not null) THEN
                    'OBJ_INVENTORYVERIFICATIONDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_INVENTORYVERIFICATIONDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_LASTSAVED), '1900-01-01')) is null and 
                    src:OBJ_LASTSAVED is not null) THEN
                    'OBJ_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:OBJ_LASTSTATUSUPDATE is not null) THEN
                    'OBJ_LASTSTATUSUPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_LASTSTATUSUPDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_LATESTINSTALLDATE), '1900-01-01')) is null and 
                    src:OBJ_LATESTINSTALLDATE is not null) THEN
                    'OBJ_LATESTINSTALLDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_LATESTINSTALLDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_LATESTRECEIPTDATE), '1900-01-01')) is null and 
                    src:OBJ_LATESTRECEIPTDATE is not null) THEN
                    'OBJ_LATESTRECEIPTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_LATESTRECEIPTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_LENGTH), '0'), 38, 10) is null and 
                    src:OBJ_LENGTH is not null) THEN
                    'OBJ_LENGTH contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_LENGTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_LINEARCOSTWEIGHT), '0'), 38, 10) is null and 
                    src:OBJ_LINEARCOSTWEIGHT is not null) THEN
                    'OBJ_LINEARCOSTWEIGHT contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_LINEARCOSTWEIGHT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_LINEARREFPRECISION), '0'), 38, 10) is null and 
                    src:OBJ_LINEARREFPRECISION is not null) THEN
                    'OBJ_LINEARREFPRECISION contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_LINEARREFPRECISION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_LOTOREVIEWED), '1900-01-01')) is null and 
                    src:OBJ_LOTOREVIEWED is not null) THEN
                    'OBJ_LOTOREVIEWED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_LOTOREVIEWED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_LOTOREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:OBJ_LOTOREVIEWREQUIRED is not null) THEN
                    'OBJ_LOTOREVIEWREQUIRED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_LOTOREVIEWREQUIRED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_MINPENALTY), '0'), 38, 10) is null and 
                    src:OBJ_MINPENALTY is not null) THEN
                    'OBJ_MINPENALTY contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_MINPENALTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_MTBFDAYS), '0'), 38, 10) is null and 
                    src:OBJ_MTBFDAYS is not null) THEN
                    'OBJ_MTBFDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_MTBFDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_MTBFRATING), '0'), 38, 10) is null and 
                    src:OBJ_MTBFRATING is not null) THEN
                    'OBJ_MTBFRATING contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_MTBFRATING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_MTTRHRS), '0'), 38, 10) is null and 
                    src:OBJ_MTTRHRS is not null) THEN
                    'OBJ_MTTRHRS contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_MTTRHRS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_MTTRRATING), '0'), 38, 10) is null and 
                    src:OBJ_MTTRRATING is not null) THEN
                    'OBJ_MTTRRATING contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_MTTRRATING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_MUBF), '0'), 38, 10) is null and 
                    src:OBJ_MUBF is not null) THEN
                    'OBJ_MUBF contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_MUBF) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_MUBFRATING), '0'), 38, 10) is null and 
                    src:OBJ_MUBFRATING is not null) THEN
                    'OBJ_MUBFRATING contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_MUBFRATING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_NUMBEROFFAILURES), '0'), 38, 10) is null and 
                    src:OBJ_NUMBEROFFAILURES is not null) THEN
                    'OBJ_NUMBEROFFAILURES contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_NUMBEROFFAILURES) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_ORDERLINE), '0'), 38, 10) is null and 
                    src:OBJ_ORDERLINE is not null) THEN
                    'OBJ_ORDERLINE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_ORDERLINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_ORIGCONFIGREV), '0'), 38, 10) is null and 
                    src:OBJ_ORIGCONFIGREV is not null) THEN
                    'OBJ_ORIGCONFIGREV contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_ORIGCONFIGREV) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_ORIGINALINSTALLDATE), '1900-01-01')) is null and 
                    src:OBJ_ORIGINALINSTALLDATE is not null) THEN
                    'OBJ_ORIGINALINSTALLDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_ORIGINALINSTALLDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_ORIGINALRECEIPTDATE), '1900-01-01')) is null and 
                    src:OBJ_ORIGINALRECEIPTDATE is not null) THEN
                    'OBJ_ORIGINALRECEIPTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_ORIGINALRECEIPTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_OUTPUTRANGEFROM), '0'), 38, 10) is null and 
                    src:OBJ_OUTPUTRANGEFROM is not null) THEN
                    'OBJ_OUTPUTRANGEFROM contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_OUTPUTRANGEFROM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_OUTPUTRANGETO), '0'), 38, 10) is null and 
                    src:OBJ_OUTPUTRANGETO is not null) THEN
                    'OBJ_OUTPUTRANGETO contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_OUTPUTRANGETO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PENALTYFACTOR), '0'), 38, 10) is null and 
                    src:OBJ_PENALTYFACTOR is not null) THEN
                    'OBJ_PENALTYFACTOR contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_PENALTYFACTOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PERFORMANCE), '0'), 38, 10) is null and 
                    src:OBJ_PERFORMANCE is not null) THEN
                    'OBJ_PERFORMANCE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_PERFORMANCE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_PERFORMANCELASTUPDATED), '1900-01-01')) is null and 
                    src:OBJ_PERFORMANCELASTUPDATED is not null) THEN
                    'OBJ_PERFORMANCELASTUPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_PERFORMANCELASTUPDATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_PERMITREVIEWED), '1900-01-01')) is null and 
                    src:OBJ_PERMITREVIEWED is not null) THEN
                    'OBJ_PERMITREVIEWED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_PERMITREVIEWED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_PERMITREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:OBJ_PERMITREVIEWREQUIRED is not null) THEN
                    'OBJ_PERMITREVIEWREQUIRED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_PERMITREVIEWREQUIRED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PRECISION), '0'), 38, 10) is null and 
                    src:OBJ_PRECISION is not null) THEN
                    'OBJ_PRECISION contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_PRECISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PROCESSRANGEFROM), '0'), 38, 10) is null and 
                    src:OBJ_PROCESSRANGEFROM is not null) THEN
                    'OBJ_PROCESSRANGEFROM contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_PROCESSRANGEFROM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PROCESSRANGETO), '0'), 38, 10) is null and 
                    src:OBJ_PROCESSRANGETO is not null) THEN
                    'OBJ_PROCESSRANGETO contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_PROCESSRANGETO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PROCESSTOLFROM), '0'), 38, 10) is null and 
                    src:OBJ_PROCESSTOLFROM is not null) THEN
                    'OBJ_PROCESSTOLFROM contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_PROCESSTOLFROM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PROCESSTOLTO), '0'), 38, 10) is null and 
                    src:OBJ_PROCESSTOLTO is not null) THEN
                    'OBJ_PROCESSTOLTO contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_PROCESSTOLTO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PURCHASECOST), '0'), 38, 10) is null and 
                    src:OBJ_PURCHASECOST is not null) THEN
                    'OBJ_PURCHASECOST contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_PURCHASECOST) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_PURCHASEDATE), '1900-01-01')) is null and 
                    src:OBJ_PURCHASEDATE is not null) THEN
                    'OBJ_PURCHASEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_PURCHASEDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_RECORD), '1900-01-01')) is null and 
                    src:OBJ_RECORD is not null) THEN
                    'OBJ_RECORD contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_RECORD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_RELIABILITYRANKINGREV), '0'), 38, 10) is null and 
                    src:OBJ_RELIABILITYRANKINGREV is not null) THEN
                    'OBJ_RELIABILITYRANKINGREV contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_RELIABILITYRANKINGREV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_RELIABILITYRANKINGSCORE), '0'), 38, 10) is null and 
                    src:OBJ_RELIABILITYRANKINGSCORE is not null) THEN
                    'OBJ_RELIABILITYRANKINGSCORE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_RELIABILITYRANKINGSCORE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_REPLACEMENTVALUE), '0'), 38, 10) is null and 
                    src:OBJ_REPLACEMENTVALUE is not null) THEN
                    'OBJ_REPLACEMENTVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_REPLACEMENTVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_RISKCONSEQUENCECOST), '0'), 38, 10) is null and 
                    src:OBJ_RISKCONSEQUENCECOST is not null) THEN
                    'OBJ_RISKCONSEQUENCECOST contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_RISKCONSEQUENCECOST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_RPN), '0'), 38, 10) is null and 
                    src:OBJ_RPN is not null) THEN
                    'OBJ_RPN contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_RPN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_RRSURVEYLASTUPDATED), '1900-01-01')) is null and 
                    src:OBJ_RRSURVEYLASTUPDATED is not null) THEN
                    'OBJ_RRSURVEYLASTUPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_RRSURVEYLASTUPDATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_RRVALUESLASTCALCULATED), '1900-01-01')) is null and 
                    src:OBJ_RRVALUESLASTCALCULATED is not null) THEN
                    'OBJ_RRVALUESLASTCALCULATED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_RRVALUESLASTCALCULATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_SAFETYREVIEWED), '1900-01-01')) is null and 
                    src:OBJ_SAFETYREVIEWED is not null) THEN
                    'OBJ_SAFETYREVIEWED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_SAFETYREVIEWED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_SAFETYREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:OBJ_SAFETYREVIEWREQUIRED is not null) THEN
                    'OBJ_SAFETYREVIEWREQUIRED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_SAFETYREVIEWREQUIRED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_SERVICELIFE), '0'), 38, 10) is null and 
                    src:OBJ_SERVICELIFE is not null) THEN
                    'OBJ_SERVICELIFE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_SERVICELIFE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_SERVICELIFEUSAGE), '0'), 38, 10) is null and 
                    src:OBJ_SERVICELIFEUSAGE is not null) THEN
                    'OBJ_SERVICELIFEUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_SERVICELIFEUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_SERVICEPERC), '0'), 38, 10) is null and 
                    src:OBJ_SERVICEPERC is not null) THEN
                    'OBJ_SERVICEPERC contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_SERVICEPERC) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_SOLDDATE), '1900-01-01')) is null and 
                    src:OBJ_SOLDDATE is not null) THEN
                    'OBJ_SOLDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_SOLDDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_SQFOOTAGE), '0'), 38, 10) is null and 
                    src:OBJ_SQFOOTAGE is not null) THEN
                    'OBJ_SQFOOTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_SQFOOTAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_STARTBILLINGPERIOD), '1900-01-01')) is null and 
                    src:OBJ_STARTBILLINGPERIOD is not null) THEN
                    'OBJ_STARTBILLINGPERIOD contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_STARTBILLINGPERIOD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_TARGETPEAKDEMAND), '0'), 38, 10) is null and 
                    src:OBJ_TARGETPEAKDEMAND is not null) THEN
                    'OBJ_TARGETPEAKDEMAND contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_TARGETPEAKDEMAND) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_TARGETPOWERFACTOR), '0'), 38, 10) is null and 
                    src:OBJ_TARGETPOWERFACTOR is not null) THEN
                    'OBJ_TARGETPOWERFACTOR contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_TARGETPOWERFACTOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_TOLERANCE_SIZE), '0'), 38, 10) is null and 
                    src:OBJ_TOLERANCE_SIZE is not null) THEN
                    'OBJ_TOLERANCE_SIZE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_TOLERANCE_SIZE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_TOPOINT), '0'), 38, 10) is null and 
                    src:OBJ_TOPOINT is not null) THEN
                    'OBJ_TOPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_TOPOINT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_TRANSFERDATE), '1900-01-01')) is null and 
                    src:OBJ_TRANSFERDATE is not null) THEN
                    'OBJ_TRANSFERDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_TRANSFERDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE01), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE01 is not null) THEN
                    'OBJ_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE02), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE02 is not null) THEN
                    'OBJ_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE03), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE03 is not null) THEN
                    'OBJ_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE04), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE04 is not null) THEN
                    'OBJ_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE05), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE05 is not null) THEN
                    'OBJ_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE06), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE06 is not null) THEN
                    'OBJ_UDFDATE06 contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_UDFDATE06) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE07), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE07 is not null) THEN
                    'OBJ_UDFDATE07 contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_UDFDATE07) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE08), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE08 is not null) THEN
                    'OBJ_UDFDATE08 contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_UDFDATE08) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE09), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE09 is not null) THEN
                    'OBJ_UDFDATE09 contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_UDFDATE09) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE10), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE10 is not null) THEN
                    'OBJ_UDFDATE10 contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_UDFDATE10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM01), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM01 is not null) THEN
                    'OBJ_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM02), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM02 is not null) THEN
                    'OBJ_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM03), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM03 is not null) THEN
                    'OBJ_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM04), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM04 is not null) THEN
                    'OBJ_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM05), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM05 is not null) THEN
                    'OBJ_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM06), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM06 is not null) THEN
                    'OBJ_UDFNUM06 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_UDFNUM06) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM07), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM07 is not null) THEN
                    'OBJ_UDFNUM07 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_UDFNUM07) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM08), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM08 is not null) THEN
                    'OBJ_UDFNUM08 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_UDFNUM08) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM09), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM09 is not null) THEN
                    'OBJ_UDFNUM09 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_UDFNUM09) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM10), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM10 is not null) THEN
                    'OBJ_UDFNUM10 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_UDFNUM10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:OBJ_UPDATECOUNT is not null) THEN
                    'OBJ_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UPDATED), '1900-01-01')) is null and 
                    src:OBJ_UPDATED is not null) THEN
                    'OBJ_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_UPDATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VALUE), '0'), 38, 10) is null and 
                    src:OBJ_VALUE is not null) THEN
                    'OBJ_VALUE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_VALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERATING1), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERATING1 is not null) THEN
                    'OBJ_VARIABLERATING1 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_VARIABLERATING1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERATING2), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERATING2 is not null) THEN
                    'OBJ_VARIABLERATING2 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_VARIABLERATING2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERATING3), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERATING3 is not null) THEN
                    'OBJ_VARIABLERATING3 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_VARIABLERATING3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERATING4), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERATING4 is not null) THEN
                    'OBJ_VARIABLERATING4 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_VARIABLERATING4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERATING5), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERATING5 is not null) THEN
                    'OBJ_VARIABLERATING5 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_VARIABLERATING5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERATING6), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERATING6 is not null) THEN
                    'OBJ_VARIABLERATING6 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_VARIABLERATING6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERESULT1), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERESULT1 is not null) THEN
                    'OBJ_VARIABLERESULT1 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_VARIABLERESULT1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERESULT2), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERESULT2 is not null) THEN
                    'OBJ_VARIABLERESULT2 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_VARIABLERESULT2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERESULT3), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERESULT3 is not null) THEN
                    'OBJ_VARIABLERESULT3 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_VARIABLERESULT3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERESULT4), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERESULT4 is not null) THEN
                    'OBJ_VARIABLERESULT4 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_VARIABLERESULT4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERESULT5), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERESULT5 is not null) THEN
                    'OBJ_VARIABLERESULT5 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_VARIABLERESULT5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERESULT6), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERESULT6 is not null) THEN
                    'OBJ_VARIABLERESULT6 contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_VARIABLERESULT6) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_WITHDRAW), '1900-01-01')) is null and 
                    src:OBJ_WITHDRAW is not null) THEN
                    'OBJ_WITHDRAW contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_WITHDRAW) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_XCOORDINATE), '0'), 38, 10) is null and 
                    src:OBJ_XCOORDINATE is not null) THEN
                    'OBJ_XCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_XCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_XLOCATION), '0'), 38, 10) is null and 
                    src:OBJ_XLOCATION is not null) THEN
                    'OBJ_XLOCATION contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_XLOCATION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_YCOORDINATE), '0'), 38, 10) is null and 
                    src:OBJ_YCOORDINATE is not null) THEN
                    'OBJ_YCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_YCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_YEARBUILT), '0'), 38, 10) is null and 
                    src:OBJ_YEARBUILT is not null) THEN
                    'OBJ_YEARBUILT contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_YEARBUILT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_YLOCATION), '0'), 38, 10) is null and 
                    src:OBJ_YLOCATION is not null) THEN
                    'OBJ_YLOCATION contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_YLOCATION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_ZCOORDINATE), '0'), 38, 10) is null and 
                    src:OBJ_ZCOORDINATE is not null) THEN
                    'OBJ_ZCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:OBJ_ZCOORDINATE) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_LASTSAVED), '1900-01-01')) is null and 
                src:OBJ_LASTSAVED is not null) THEN
                'OBJ_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBJ_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:OBJ_CODE,
                src:OBJ_ORG  ORDER BY 
            src:OBJ_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5OBJECTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_ACD), '0'), 38, 10) is null and 
                    src:OBJ_ACD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_AVAILABLECAPACITY), '0'), 38, 10) is null and 
                    src:OBJ_AVAILABLECAPACITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_BATHCOUNT), '0'), 38, 10) is null and 
                    src:OBJ_BATHCOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_BEDCOUNT), '0'), 38, 10) is null and 
                    src:OBJ_BEDCOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_BILLEVERYPERIOD), '0'), 38, 10) is null and 
                    src:OBJ_BILLEVERYPERIOD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_BLACKSWANCOST), '0'), 38, 10) is null and 
                    src:OBJ_BLACKSWANCOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CAPACITYRATING), '0'), 38, 10) is null and 
                    src:OBJ_CAPACITYRATING is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_COMMISS), '1900-01-01')) is null and 
                    src:OBJ_COMMISS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CONDITIONRATING), '0'), 38, 10) is null and 
                    src:OBJ_CONDITIONRATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CONDITIONSCORE), '0'), 38, 10) is null and 
                    src:OBJ_CONDITIONSCORE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_CONFIGAPPROVED), '1900-01-01')) is null and 
                    src:OBJ_CONFIGAPPROVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_CONFIGREQUESTED), '1900-01-01')) is null and 
                    src:OBJ_CONFIGREQUESTED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CONFIGREVISION), '0'), 38, 10) is null and 
                    src:OBJ_CONFIGREVISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CONFIGSEQLENGTH), '0'), 38, 10) is null and 
                    src:OBJ_CONFIGSEQLENGTH is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_CORRCONDITIONDATE), '1900-01-01')) is null and 
                    src:OBJ_CORRCONDITIONDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CORRCONDITIONSCORE), '0'), 38, 10) is null and 
                    src:OBJ_CORRCONDITIONSCORE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CORRCONDITIONUSAGE), '0'), 38, 10) is null and 
                    src:OBJ_CORRCONDITIONUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_COSTOFNEEDEDREPAIRS), '0'), 38, 10) is null and 
                    src:OBJ_COSTOFNEEDEDREPAIRS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_COUNTRY), '0'), 38, 10) is null and 
                    src:OBJ_COUNTRY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_CREATED), '1900-01-01')) is null and 
                    src:OBJ_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_CRITICALITYSCORE), '0'), 38, 10) is null and 
                    src:OBJ_CRITICALITYSCORE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_DESIREDCAPACITY), '0'), 38, 10) is null and 
                    src:OBJ_DESIREDCAPACITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_DEVICERANGEFROM), '0'), 38, 10) is null and 
                    src:OBJ_DEVICERANGEFROM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_DEVICERANGETO), '0'), 38, 10) is null and 
                    src:OBJ_DEVICERANGETO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_DEVICETOLFROM), '0'), 38, 10) is null and 
                    src:OBJ_DEVICETOLFROM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_DEVICETOLTO), '0'), 38, 10) is null and 
                    src:OBJ_DEVICETOLTO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_DORMEND), '1900-01-01')) is null and 
                    src:OBJ_DORMEND is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_DORMSTART), '1900-01-01')) is null and 
                    src:OBJ_DORMSTART is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_EFFLOSSPHASEIMBALANCE1), '0'), 38, 10) is null and 
                    src:OBJ_EFFLOSSPHASEIMBALANCE1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_EFFLOSSPHASEIMBALANCE2), '0'), 38, 10) is null and 
                    src:OBJ_EFFLOSSPHASEIMBALANCE2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_EFFLOSSPHASEIMBALANCE3), '0'), 38, 10) is null and 
                    src:OBJ_EFFLOSSPHASEIMBALANCE3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_EFFLOSSPHASEIMBALANCE4), '0'), 38, 10) is null and 
                    src:OBJ_EFFLOSSPHASEIMBALANCE4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_EFFLOSSPHASEIMBALANCE5), '0'), 38, 10) is null and 
                    src:OBJ_EFFLOSSPHASEIMBALANCE5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_ELECSUBMETERINTERVAL), '0'), 38, 10) is null and 
                    src:OBJ_ELECSUBMETERINTERVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_ELECUSAGETHRESHOLD), '0'), 38, 10) is null and 
                    src:OBJ_ELECUSAGETHRESHOLD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_ENDUSEFULLIFE), '1900-01-01')) is null and 
                    src:OBJ_ENDUSEFULLIFE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_ESTREVENUE), '0'), 38, 10) is null and 
                    src:OBJ_ESTREVENUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_FACILITYCONDITIONINDEX), '0'), 38, 10) is null and 
                    src:OBJ_FACILITYCONDITIONINDEX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_FAILUREPROBPERCENTAGE), '0'), 38, 10) is null and 
                    src:OBJ_FAILUREPROBPERCENTAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_FLOORAREA), '0'), 38, 10) is null and 
                    src:OBJ_FLOORAREA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_FROMPOINT), '0'), 38, 10) is null and 
                    src:OBJ_FROMPOINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_GISOBJID), '0'), 38, 10) is null and 
                    src:OBJ_GISOBJID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_GIS_SYNCCOUNT), '0'), 38, 10) is null and 
                    src:OBJ_GIS_SYNCCOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_GIS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:OBJ_GIS_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_GLTRANSFER), '1900-01-01')) is null and 
                    src:OBJ_GLTRANSFER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_GUESTPROFILEID), '0'), 38, 10) is null and 
                    src:OBJ_GUESTPROFILEID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_INCREMENT), '0'), 38, 10) is null and 
                    src:OBJ_INCREMENT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_INSTALLDATE), '1900-01-01')) is null and 
                    src:OBJ_INSTALLDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_INTERFACE), '1900-01-01')) is null and 
                    src:OBJ_INTERFACE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_INVENTORYVERIFICATIONDATE), '1900-01-01')) is null and 
                    src:OBJ_INVENTORYVERIFICATIONDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_LASTSAVED), '1900-01-01')) is null and 
                    src:OBJ_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:OBJ_LASTSTATUSUPDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_LATESTINSTALLDATE), '1900-01-01')) is null and 
                    src:OBJ_LATESTINSTALLDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_LATESTRECEIPTDATE), '1900-01-01')) is null and 
                    src:OBJ_LATESTRECEIPTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_LENGTH), '0'), 38, 10) is null and 
                    src:OBJ_LENGTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_LINEARCOSTWEIGHT), '0'), 38, 10) is null and 
                    src:OBJ_LINEARCOSTWEIGHT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_LINEARREFPRECISION), '0'), 38, 10) is null and 
                    src:OBJ_LINEARREFPRECISION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_LOTOREVIEWED), '1900-01-01')) is null and 
                    src:OBJ_LOTOREVIEWED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_LOTOREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:OBJ_LOTOREVIEWREQUIRED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_MINPENALTY), '0'), 38, 10) is null and 
                    src:OBJ_MINPENALTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_MTBFDAYS), '0'), 38, 10) is null and 
                    src:OBJ_MTBFDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_MTBFRATING), '0'), 38, 10) is null and 
                    src:OBJ_MTBFRATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_MTTRHRS), '0'), 38, 10) is null and 
                    src:OBJ_MTTRHRS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_MTTRRATING), '0'), 38, 10) is null and 
                    src:OBJ_MTTRRATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_MUBF), '0'), 38, 10) is null and 
                    src:OBJ_MUBF is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_MUBFRATING), '0'), 38, 10) is null and 
                    src:OBJ_MUBFRATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_NUMBEROFFAILURES), '0'), 38, 10) is null and 
                    src:OBJ_NUMBEROFFAILURES is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_ORDERLINE), '0'), 38, 10) is null and 
                    src:OBJ_ORDERLINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_ORIGCONFIGREV), '0'), 38, 10) is null and 
                    src:OBJ_ORIGCONFIGREV is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_ORIGINALINSTALLDATE), '1900-01-01')) is null and 
                    src:OBJ_ORIGINALINSTALLDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_ORIGINALRECEIPTDATE), '1900-01-01')) is null and 
                    src:OBJ_ORIGINALRECEIPTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_OUTPUTRANGEFROM), '0'), 38, 10) is null and 
                    src:OBJ_OUTPUTRANGEFROM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_OUTPUTRANGETO), '0'), 38, 10) is null and 
                    src:OBJ_OUTPUTRANGETO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PENALTYFACTOR), '0'), 38, 10) is null and 
                    src:OBJ_PENALTYFACTOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PERFORMANCE), '0'), 38, 10) is null and 
                    src:OBJ_PERFORMANCE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_PERFORMANCELASTUPDATED), '1900-01-01')) is null and 
                    src:OBJ_PERFORMANCELASTUPDATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_PERMITREVIEWED), '1900-01-01')) is null and 
                    src:OBJ_PERMITREVIEWED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_PERMITREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:OBJ_PERMITREVIEWREQUIRED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PRECISION), '0'), 38, 10) is null and 
                    src:OBJ_PRECISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PROCESSRANGEFROM), '0'), 38, 10) is null and 
                    src:OBJ_PROCESSRANGEFROM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PROCESSRANGETO), '0'), 38, 10) is null and 
                    src:OBJ_PROCESSRANGETO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PROCESSTOLFROM), '0'), 38, 10) is null and 
                    src:OBJ_PROCESSTOLFROM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PROCESSTOLTO), '0'), 38, 10) is null and 
                    src:OBJ_PROCESSTOLTO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_PURCHASECOST), '0'), 38, 10) is null and 
                    src:OBJ_PURCHASECOST is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_PURCHASEDATE), '1900-01-01')) is null and 
                    src:OBJ_PURCHASEDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_RECORD), '1900-01-01')) is null and 
                    src:OBJ_RECORD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_RELIABILITYRANKINGREV), '0'), 38, 10) is null and 
                    src:OBJ_RELIABILITYRANKINGREV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_RELIABILITYRANKINGSCORE), '0'), 38, 10) is null and 
                    src:OBJ_RELIABILITYRANKINGSCORE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_REPLACEMENTVALUE), '0'), 38, 10) is null and 
                    src:OBJ_REPLACEMENTVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_RISKCONSEQUENCECOST), '0'), 38, 10) is null and 
                    src:OBJ_RISKCONSEQUENCECOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_RPN), '0'), 38, 10) is null and 
                    src:OBJ_RPN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_RRSURVEYLASTUPDATED), '1900-01-01')) is null and 
                    src:OBJ_RRSURVEYLASTUPDATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_RRVALUESLASTCALCULATED), '1900-01-01')) is null and 
                    src:OBJ_RRVALUESLASTCALCULATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_SAFETYREVIEWED), '1900-01-01')) is null and 
                    src:OBJ_SAFETYREVIEWED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_SAFETYREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:OBJ_SAFETYREVIEWREQUIRED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_SERVICELIFE), '0'), 38, 10) is null and 
                    src:OBJ_SERVICELIFE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_SERVICELIFEUSAGE), '0'), 38, 10) is null and 
                    src:OBJ_SERVICELIFEUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_SERVICEPERC), '0'), 38, 10) is null and 
                    src:OBJ_SERVICEPERC is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_SOLDDATE), '1900-01-01')) is null and 
                    src:OBJ_SOLDDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_SQFOOTAGE), '0'), 38, 10) is null and 
                    src:OBJ_SQFOOTAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_STARTBILLINGPERIOD), '1900-01-01')) is null and 
                    src:OBJ_STARTBILLINGPERIOD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_TARGETPEAKDEMAND), '0'), 38, 10) is null and 
                    src:OBJ_TARGETPEAKDEMAND is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_TARGETPOWERFACTOR), '0'), 38, 10) is null and 
                    src:OBJ_TARGETPOWERFACTOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_TOLERANCE_SIZE), '0'), 38, 10) is null and 
                    src:OBJ_TOLERANCE_SIZE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_TOPOINT), '0'), 38, 10) is null and 
                    src:OBJ_TOPOINT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_TRANSFERDATE), '1900-01-01')) is null and 
                    src:OBJ_TRANSFERDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE01), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE02), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE03), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE04), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE05), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE05 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE06), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE06 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE07), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE07 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE08), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE08 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE09), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE09 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UDFDATE10), '1900-01-01')) is null and 
                    src:OBJ_UDFDATE10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM01), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM02), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM03), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM04), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM05), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM06), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM06 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM07), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM07 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM08), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM08 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM09), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM09 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UDFNUM10), '0'), 38, 10) is null and 
                    src:OBJ_UDFNUM10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:OBJ_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_UPDATED), '1900-01-01')) is null and 
                    src:OBJ_UPDATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VALUE), '0'), 38, 10) is null and 
                    src:OBJ_VALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERATING1), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERATING1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERATING2), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERATING2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERATING3), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERATING3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERATING4), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERATING4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERATING5), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERATING5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERATING6), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERATING6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERESULT1), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERESULT1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERESULT2), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERESULT2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERESULT3), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERESULT3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERESULT4), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERESULT4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERESULT5), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERESULT5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_VARIABLERESULT6), '0'), 38, 10) is null and 
                    src:OBJ_VARIABLERESULT6 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_WITHDRAW), '1900-01-01')) is null and 
                    src:OBJ_WITHDRAW is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_XCOORDINATE), '0'), 38, 10) is null and 
                    src:OBJ_XCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_XLOCATION), '0'), 38, 10) is null and 
                    src:OBJ_XLOCATION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_YCOORDINATE), '0'), 38, 10) is null and 
                    src:OBJ_YCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_YEARBUILT), '0'), 38, 10) is null and 
                    src:OBJ_YEARBUILT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_YLOCATION), '0'), 38, 10) is null and 
                    src:OBJ_YLOCATION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBJ_ZCOORDINATE), '0'), 38, 10) is null and 
                    src:OBJ_ZCOORDINATE is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBJ_LASTSAVED), '1900-01-01')) is null and 
                src:OBJ_LASTSAVED is not null)