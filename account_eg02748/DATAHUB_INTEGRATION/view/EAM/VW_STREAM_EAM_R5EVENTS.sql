CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5EVENTS AS SELECT
                        src:EVT_ACCOUNTINGENTITY::varchar AS EVT_ACCOUNTINGENTITY, 
                        src:EVT_ACD::numeric(38, 10) AS EVT_ACD, 
                        src:EVT_ACTION::varchar AS EVT_ACTION, 
                        src:EVT_ADDITIONALINFO::varchar AS EVT_ADDITIONALINFO, 
                        src:EVT_AGREEMENT::varchar AS EVT_AGREEMENT, 
                        src:EVT_ALERT::varchar AS EVT_ALERT, 
                        src:EVT_BILLABLE::varchar AS EVT_BILLABLE, 
                        src:EVT_BUILDMAINTPROGRAM::varchar AS EVT_BUILDMAINTPROGRAM, 
                        src:EVT_BURNPERMIT::varchar AS EVT_BURNPERMIT, 
                        src:EVT_CALCULATEDPRIORITY::numeric(38, 10) AS EVT_CALCULATEDPRIORITY, 
                        src:EVT_CALSTATUS::varchar AS EVT_CALSTATUS, 
                        src:EVT_CAMPAIGN::varchar AS EVT_CAMPAIGN, 
                        src:EVT_CAMPAIGN_LINE::numeric(38, 10) AS EVT_CAMPAIGN_LINE, 
                        src:EVT_CAMPAIGN_ORG::varchar AS EVT_CAMPAIGN_ORG, 
                        src:EVT_CAMPAIGN_SURVEY::varchar AS EVT_CAMPAIGN_SURVEY, 
                        src:EVT_CASEMANAGEMENT::varchar AS EVT_CASEMANAGEMENT, 
                        src:EVT_CASEMANAGEMENTTASK::varchar AS EVT_CASEMANAGEMENTTASK, 
                        src:EVT_CAUSE::varchar AS EVT_CAUSE, 
                        src:EVT_CEILPERMIT::varchar AS EVT_CEILPERMIT, 
                        src:EVT_CLASS::varchar AS EVT_CLASS, 
                        src:EVT_CLASS_ORG::varchar AS EVT_CLASS_ORG, 
                        src:EVT_CN::varchar AS EVT_CN, 
                        src:EVT_CODE::varchar AS EVT_CODE, 
                        src:EVT_CODE_ALIAS::varchar AS EVT_CODE_ALIAS, 
                        src:EVT_COMPLETED::datetime AS EVT_COMPLETED, 
                        src:EVT_COMPLETED_TRUNC::datetime AS EVT_COMPLETED_TRUNC, 
                        src:EVT_CONFINEDSPACE::varchar AS EVT_CONFINEDSPACE, 
                        src:EVT_CONFLICT::varchar AS EVT_CONFLICT, 
                        src:EVT_CONFLICTDESC::varchar AS EVT_CONFLICTDESC, 
                        src:EVT_CONFLICTRESOLVED::varchar AS EVT_CONFLICTRESOLVED, 
                        src:EVT_CONTACTRECORD::varchar AS EVT_CONTACTRECORD, 
                        src:EVT_CONTACTRECORD_ORG::varchar AS EVT_CONTACTRECORD_ORG, 
                        src:EVT_CONTEMAIL::varchar AS EVT_CONTEMAIL, 
                        src:EVT_CONTNAME::varchar AS EVT_CONTNAME, 
                        src:EVT_CONTNOTES::varchar AS EVT_CONTNOTES, 
                        src:EVT_CONTPHONE::varchar AS EVT_CONTPHONE, 
                        src:EVT_COSTCODE::varchar AS EVT_COSTCODE, 
                        src:EVT_CREATED::datetime AS EVT_CREATED, 
                        src:EVT_CREATEDBY::varchar AS EVT_CREATEDBY, 
                        src:EVT_DATE::datetime AS EVT_DATE, 
                        src:EVT_DEPEND::varchar AS EVT_DEPEND, 
                        src:EVT_DESC::varchar AS EVT_DESC, 
                        src:EVT_DOWNTIME::numeric(38, 10) AS EVT_DOWNTIME, 
                        src:EVT_DOWNTIMEHRS::numeric(38, 10) AS EVT_DOWNTIMEHRS, 
                        src:EVT_DUE::datetime AS EVT_DUE, 
                        src:EVT_DUPCASEMANAGEMENT::varchar AS EVT_DUPCASEMANAGEMENT, 
                        src:EVT_DURATION::numeric(38, 10) AS EVT_DURATION, 
                        src:EVT_EARLYSTART::datetime AS EVT_EARLYSTART, 
                        src:EVT_EMAIL::varchar AS EVT_EMAIL, 
                        src:EVT_ENTEREDBY::varchar AS EVT_ENTEREDBY, 
                        src:EVT_EQUIPMENTUSABILITY::varchar AS EVT_EQUIPMENTUSABILITY, 
                        src:EVT_EQUIPMENTUSABILITY_ORG::varchar AS EVT_EQUIPMENTUSABILITY_ORG, 
                        src:EVT_FACILITY::varchar AS EVT_FACILITY, 
                        src:EVT_FACILITY_ORG::varchar AS EVT_FACILITY_ORG, 
                        src:EVT_FAILURE::varchar AS EVT_FAILURE, 
                        src:EVT_FAILUREMODE::varchar AS EVT_FAILUREMODE, 
                        src:EVT_FAILUREUSAGE::numeric(38, 10) AS EVT_FAILUREUSAGE, 
                        src:EVT_FIREINCIDENT::varchar AS EVT_FIREINCIDENT, 
                        src:EVT_FIRSTBILL::varchar AS EVT_FIRSTBILL, 
                        src:EVT_FIXED::varchar AS EVT_FIXED, 
                        src:EVT_FLOW::varchar AS EVT_FLOW, 
                        src:EVT_FOLLOWUP::varchar AS EVT_FOLLOWUP, 
                        src:EVT_FREQ::numeric(38, 10) AS EVT_FREQ, 
                        src:EVT_FROMGEOREF::varchar AS EVT_FROMGEOREF, 
                        src:EVT_FROMPOINT::numeric(38, 10) AS EVT_FROMPOINT, 
                        src:EVT_FROMREFDESC::varchar AS EVT_FROMREFDESC, 
                        src:EVT_FROM_HORIZONTAL_OFFSET::numeric(38, 10) AS EVT_FROM_HORIZONTAL_OFFSET, 
                        src:EVT_FROM_HORIZONTAL_OFFSETTYPE::varchar AS EVT_FROM_HORIZONTAL_OFFSETTYPE, 
                        src:EVT_FROM_HORIZONTAL_OFFSETUOM::varchar AS EVT_FROM_HORIZONTAL_OFFSETUOM, 
                        src:EVT_FROM_LATITUDE::numeric(38, 10) AS EVT_FROM_LATITUDE, 
                        src:EVT_FROM_LONGITUDE::numeric(38, 10) AS EVT_FROM_LONGITUDE, 
                        src:EVT_FROM_OFFSET::numeric(38, 10) AS EVT_FROM_OFFSET, 
                        src:EVT_FROM_OFFSET_DIRECTION::varchar AS EVT_FROM_OFFSET_DIRECTION, 
                        src:EVT_FROM_OFFSET_PERCENTAGE::numeric(38, 10) AS EVT_FROM_OFFSET_PERCENTAGE, 
                        src:EVT_FROM_REFERENCE::numeric(38, 10) AS EVT_FROM_REFERENCE, 
                        src:EVT_FROM_RELATIONSHIP_TYPE::varchar AS EVT_FROM_RELATIONSHIP_TYPE, 
                        src:EVT_FROM_VERTICAL_OFFSET::numeric(38, 10) AS EVT_FROM_VERTICAL_OFFSET, 
                        src:EVT_FROM_VERTICAL_OFFSETTYPE::varchar AS EVT_FROM_VERTICAL_OFFSETTYPE, 
                        src:EVT_FROM_VERTICAL_OFFSETUOM::varchar AS EVT_FROM_VERTICAL_OFFSETUOM, 
                        src:EVT_FROM_XCOORDINATE::numeric(38, 10) AS EVT_FROM_XCOORDINATE, 
                        src:EVT_FROM_YCOORDINATE::numeric(38, 10) AS EVT_FROM_YCOORDINATE, 
                        src:EVT_GENWINBEGVAL::numeric(38, 10) AS EVT_GENWINBEGVAL, 
                        src:EVT_GENWINBEGVAL2::numeric(38, 10) AS EVT_GENWINBEGVAL2, 
                        src:EVT_GENWINSTART::datetime AS EVT_GENWINSTART, 
                        src:EVT_GLTRANSFER::datetime AS EVT_GLTRANSFER, 
                        src:EVT_GLTRANSFERFLAG::varchar AS EVT_GLTRANSFERFLAG, 
                        src:EVT_GUESTEMAIL::varchar AS EVT_GUESTEMAIL, 
                        src:EVT_GUESTFIRSTNAME::varchar AS EVT_GUESTFIRSTNAME, 
                        src:EVT_GUESTLASTNAME::varchar AS EVT_GUESTLASTNAME, 
                        src:EVT_GUESTMIDDLENAME::varchar AS EVT_GUESTMIDDLENAME, 
                        src:EVT_GUESTPHONENUMBER::varchar AS EVT_GUESTPHONENUMBER, 
                        src:EVT_GUESTPROFILEID::numeric(38, 10) AS EVT_GUESTPROFILEID, 
                        src:EVT_GUESTSALUTATION::varchar AS EVT_GUESTSALUTATION, 
                        src:EVT_HAZMATINCIDENT::varchar AS EVT_HAZMATINCIDENT, 
                        src:EVT_HIPAACONFIDENTIALITY::varchar AS EVT_HIPAACONFIDENTIALITY, 
                        src:EVT_HUMANFACTOR::varchar AS EVT_HUMANFACTOR, 
                        src:EVT_HUMANOVERSIGHT::varchar AS EVT_HUMANOVERSIGHT, 
                        src:EVT_INCREMENT::numeric(38, 10) AS EVT_INCREMENT, 
                        src:EVT_INFECTCONTROL::varchar AS EVT_INFECTCONTROL, 
                        src:EVT_INSPECTIONDIRECTION::varchar AS EVT_INSPECTIONDIRECTION, 
                        src:EVT_INTERFACE::datetime AS EVT_INTERFACE, 
                        src:EVT_INVOICE::varchar AS EVT_INVOICE, 
                        src:EVT_INVOICELINE::numeric(38, 10) AS EVT_INVOICELINE, 
                        src:EVT_INVOICE_ORG::varchar AS EVT_INVOICE_ORG, 
                        src:EVT_INVOICE_REVISION::numeric(38, 10) AS EVT_INVOICE_REVISION, 
                        src:EVT_ISSTYPE::varchar AS EVT_ISSTYPE, 
                        src:EVT_JECATEGORY::varchar AS EVT_JECATEGORY, 
                        src:EVT_JESOURCE::varchar AS EVT_JESOURCE, 
                        src:EVT_JOBTYPE::varchar AS EVT_JOBTYPE, 
                        src:EVT_LABTOTAL::numeric(38, 10) AS EVT_LABTOTAL, 
                        src:EVT_LASTCAL::datetime AS EVT_LASTCAL, 
                        src:EVT_LASTPLAN::datetime AS EVT_LASTPLAN, 
                        src:EVT_LASTSAVED::datetime AS EVT_LASTSAVED, 
                        src:EVT_LASTSTATUSUPDATE::datetime AS EVT_LASTSTATUSUPDATE, 
                        src:EVT_LASTTIMEPB::datetime AS EVT_LASTTIMEPB, 
                        src:EVT_LASTTIMERB::datetime AS EVT_LASTTIMERB, 
                        src:EVT_LATEEND::datetime AS EVT_LATEEND, 
                        src:EVT_LATITUDE::numeric(38, 10) AS EVT_LATITUDE, 
                        src:EVT_LIFESAFETY::varchar AS EVT_LIFESAFETY, 
                        src:EVT_LOCATION::varchar AS EVT_LOCATION, 
                        src:EVT_LOCATION_ORG::varchar AS EVT_LOCATION_ORG, 
                        src:EVT_LOCKOUT::varchar AS EVT_LOCKOUT, 
                        src:EVT_LONGITUDE::numeric(38, 10) AS EVT_LONGITUDE, 
                        src:EVT_LTYPE::varchar AS EVT_LTYPE, 
                        src:EVT_MANAGERPROFILEID::varchar AS EVT_MANAGERPROFILEID, 
                        src:EVT_MASTERCAL::varchar AS EVT_MASTERCAL, 
                        src:EVT_MATAVAIL::datetime AS EVT_MATAVAIL, 
                        src:EVT_MATTOTAL::numeric(38, 10) AS EVT_MATTOTAL, 
                        src:EVT_MAXCOST::numeric(38, 10) AS EVT_MAXCOST, 
                        src:EVT_MEDICEQUIPINCIDENT::varchar AS EVT_MEDICEQUIPINCIDENT, 
                        src:EVT_METER::varchar AS EVT_METER, 
                        src:EVT_METERDUE::numeric(38, 10) AS EVT_METERDUE, 
                        src:EVT_METERDUE2::numeric(38, 10) AS EVT_METERDUE2, 
                        src:EVT_METERDUEDATE::datetime AS EVT_METERDUEDATE, 
                        src:EVT_METERDUEDATE2::datetime AS EVT_METERDUEDATE2, 
                        src:EVT_METERINTERVAL::numeric(38, 10) AS EVT_METERINTERVAL, 
                        src:EVT_METERINTERVAL2::numeric(38, 10) AS EVT_METERINTERVAL2, 
                        src:EVT_METERREADING::numeric(38, 10) AS EVT_METERREADING, 
                        src:EVT_METHODOFDETECTION::varchar AS EVT_METHODOFDETECTION, 
                        src:EVT_METUOM::varchar AS EVT_METUOM, 
                        src:EVT_METUOM2::varchar AS EVT_METUOM2, 
                        src:EVT_MINOR::varchar AS EVT_MINOR, 
                        src:EVT_MP::varchar AS EVT_MP, 
                        src:EVT_MPPROJ::varchar AS EVT_MPPROJ, 
                        src:EVT_MPPROJTYPE::varchar AS EVT_MPPROJTYPE, 
                        src:EVT_MP_ORG::varchar AS EVT_MP_ORG, 
                        src:EVT_MP_REV::numeric(38, 10) AS EVT_MP_REV, 
                        src:EVT_MP_SEQ::numeric(38, 10) AS EVT_MP_SEQ, 
                        src:EVT_MRC::varchar AS EVT_MRC, 
                        src:EVT_MULTIEQUIP::varchar AS EVT_MULTIEQUIP, 
                        src:EVT_MUSTEND::datetime AS EVT_MUSTEND, 
                        src:EVT_NEARWINBEGVAL::numeric(38, 10) AS EVT_NEARWINBEGVAL, 
                        src:EVT_NEARWINBEGVAL2::numeric(38, 10) AS EVT_NEARWINBEGVAL2, 
                        src:EVT_NEARWINSTART::datetime AS EVT_NEARWINSTART, 
                        src:EVT_NEWDUR::numeric(38, 10) AS EVT_NEWDUR, 
                        src:EVT_NEWRSTATUS::varchar AS EVT_NEWRSTATUS, 
                        src:EVT_NEWSTATUS::varchar AS EVT_NEWSTATUS, 
                        src:EVT_NEWTARGET::datetime AS EVT_NEWTARGET, 
                        src:EVT_OBJCRITICALITY::varchar AS EVT_OBJCRITICALITY, 
                        src:EVT_OBJECT::varchar AS EVT_OBJECT, 
                        src:EVT_OBJECT_ORG::varchar AS EVT_OBJECT_ORG, 
                        src:EVT_OBRTYPE::varchar AS EVT_OBRTYPE, 
                        src:EVT_OBTYPE::varchar AS EVT_OBTYPE, 
                        src:EVT_OKWINEND::datetime AS EVT_OKWINEND, 
                        src:EVT_OKWINENDVAL::numeric(38, 10) AS EVT_OKWINENDVAL, 
                        src:EVT_OKWINENDVAL2::numeric(38, 10) AS EVT_OKWINENDVAL2, 
                        src:EVT_OLDRSTATUS::varchar AS EVT_OLDRSTATUS, 
                        src:EVT_OLDSTATUS::varchar AS EVT_OLDSTATUS, 
                        src:EVT_ORG::varchar AS EVT_ORG, 
                        src:EVT_ORIGACT::numeric(38, 10) AS EVT_ORIGACT, 
                        src:EVT_ORIGCASEMANAGEMENT::varchar AS EVT_ORIGCASEMANAGEMENT, 
                        src:EVT_ORIGIN::varchar AS EVT_ORIGIN, 
                        src:EVT_ORIGWO::varchar AS EVT_ORIGWO, 
                        src:EVT_OUTPUTCALCTYPE::varchar AS EVT_OUTPUTCALCTYPE, 
                        src:EVT_PACKAGE::varchar AS EVT_PACKAGE, 
                        src:EVT_PARENT::varchar AS EVT_PARENT, 
                        src:EVT_PATIENTSAFETY::varchar AS EVT_PATIENTSAFETY, 
                        src:EVT_PCRA::varchar AS EVT_PCRA, 
                        src:EVT_PERFORMEDBY::varchar AS EVT_PERFORMEDBY, 
                        src:EVT_PERFORMONDAY::numeric(38, 10) AS EVT_PERFORMONDAY, 
                        src:EVT_PERFORMONWEEK::varchar AS EVT_PERFORMONWEEK, 
                        src:EVT_PERIODUOM::varchar AS EVT_PERIODUOM, 
                        src:EVT_PERMITREVIEWED::datetime AS EVT_PERMITREVIEWED, 
                        src:EVT_PERMITREVIEWEDBY::varchar AS EVT_PERMITREVIEWEDBY, 
                        src:EVT_PERMITREVIEWEDNAME::varchar AS EVT_PERMITREVIEWEDNAME, 
                        src:EVT_PERMITREVIEWEDTYPE::varchar AS EVT_PERMITREVIEWEDTYPE, 
                        src:EVT_PERSON::varchar AS EVT_PERSON, 
                        src:EVT_PERSONALPROTECTIVEEQUIP::varchar AS EVT_PERSONALPROTECTIVEEQUIP, 
                        src:EVT_PFI::varchar AS EVT_PFI, 
                        src:EVT_PFPROMISEDATE::datetime AS EVT_PFPROMISEDATE, 
                        src:EVT_PHONE::varchar AS EVT_PHONE, 
                        src:EVT_PIDDRAWING::varchar AS EVT_PIDDRAWING, 
                        src:EVT_PIDNO::varchar AS EVT_PIDNO, 
                        src:EVT_PLANPRIO::varchar AS EVT_PLANPRIO, 
                        src:EVT_PLANSTATUS::varchar AS EVT_PLANSTATUS, 
                        src:EVT_PPM::varchar AS EVT_PPM, 
                        src:EVT_PPMREV::numeric(38, 10) AS EVT_PPMREV, 
                        src:EVT_PPOPK::numeric(38, 10) AS EVT_PPOPK, 
                        src:EVT_PRECISION::numeric(38, 10) AS EVT_PRECISION, 
                        src:EVT_PRESERVECALCPRIORITY::varchar AS EVT_PRESERVECALCPRIORITY, 
                        src:EVT_PRINT::varchar AS EVT_PRINT, 
                        src:EVT_PRINTED::varchar AS EVT_PRINTED, 
                        src:EVT_PRIORITY::varchar AS EVT_PRIORITY, 
                        src:EVT_PRODORDER::varchar AS EVT_PRODORDER, 
                        src:EVT_PRODPRIORITY::varchar AS EVT_PRODPRIORITY, 
                        src:EVT_PRODUCTIONEND::datetime AS EVT_PRODUCTIONEND, 
                        src:EVT_PRODUCTIONREQUEST::varchar AS EVT_PRODUCTIONREQUEST, 
                        src:EVT_PRODUCTIONREQUESTREV::numeric(38, 10) AS EVT_PRODUCTIONREQUESTREV, 
                        src:EVT_PRODUCTIONREQUEST_ORG::varchar AS EVT_PRODUCTIONREQUEST_ORG, 
                        src:EVT_PRODUCTIONSTART::datetime AS EVT_PRODUCTIONSTART, 
                        src:EVT_PROJBUD::varchar AS EVT_PROJBUD, 
                        src:EVT_PROJECT::varchar AS EVT_PROJECT, 
                        src:EVT_PROPERTYDAMAGE::varchar AS EVT_PROPERTYDAMAGE, 
                        src:EVT_PROVIDER::varchar AS EVT_PROVIDER, 
                        src:EVT_PROVIDER_ORG::varchar AS EVT_PROVIDER_ORG, 
                        src:EVT_PSQPK::numeric(38, 10) AS EVT_PSQPK, 
                        src:EVT_RECALLNOTICE::varchar AS EVT_RECALLNOTICE, 
                        src:EVT_REJECTREASON::varchar AS EVT_REJECTREASON, 
                        src:EVT_RELATIONSHIP_TYPE::varchar AS EVT_RELATIONSHIP_TYPE, 
                        src:EVT_REOPENED::varchar AS EVT_REOPENED, 
                        src:EVT_REPORTED::datetime AS EVT_REPORTED, 
                        src:EVT_REQM::varchar AS EVT_REQM, 
                        src:EVT_REQUESTEND::datetime AS EVT_REQUESTEND, 
                        src:EVT_REQUESTSTART::datetime AS EVT_REQUESTSTART, 
                        src:EVT_RESERVATIONNUM::varchar AS EVT_RESERVATIONNUM, 
                        src:EVT_REVIEWEDBY::varchar AS EVT_REVIEWEDBY, 
                        src:EVT_ROUTE::varchar AS EVT_ROUTE, 
                        src:EVT_ROUTEDFROM::varchar AS EVT_ROUTEDFROM, 
                        src:EVT_ROUTEPARENT::varchar AS EVT_ROUTEPARENT, 
                        src:EVT_ROUTEREASON::varchar AS EVT_ROUTEREASON, 
                        src:EVT_ROUTEREV::numeric(38, 10) AS EVT_ROUTEREV, 
                        src:EVT_ROUTERSTATUS::varchar AS EVT_ROUTERSTATUS, 
                        src:EVT_ROUTESTATUS::varchar AS EVT_ROUTESTATUS, 
                        src:EVT_RSTATUS::varchar AS EVT_RSTATUS, 
                        src:EVT_RTYPE::varchar AS EVT_RTYPE, 
                        src:EVT_SAFETY::varchar AS EVT_SAFETY, 
                        src:EVT_SAFETYREVIEWED::datetime AS EVT_SAFETYREVIEWED, 
                        src:EVT_SAFETYREVIEWEDBY::varchar AS EVT_SAFETYREVIEWEDBY, 
                        src:EVT_SAFETYREVIEWEDNAME::varchar AS EVT_SAFETYREVIEWEDNAME, 
                        src:EVT_SAFETYREVIEWEDTYPE::varchar AS EVT_SAFETYREVIEWEDTYPE, 
                        src:EVT_SCHEDEND::datetime AS EVT_SCHEDEND, 
                        src:EVT_SCHEDGRP::varchar AS EVT_SCHEDGRP, 
                        src:EVT_SCHEDNO::numeric(38, 10) AS EVT_SCHEDNO, 
                        src:EVT_SCHEDULEDENDTIME::numeric(38, 10) AS EVT_SCHEDULEDENDTIME, 
                        src:EVT_SCHEDULEDSTARTTIME::numeric(38, 10) AS EVT_SCHEDULEDSTARTTIME, 
                        src:EVT_SCREENER::varchar AS EVT_SCREENER, 
                        src:EVT_SECURITYINCIDENT::varchar AS EVT_SECURITYINCIDENT, 
                        src:EVT_SEQ::numeric(38, 10) AS EVT_SEQ, 
                        src:EVT_SERVICECATEGORY::varchar AS EVT_SERVICECATEGORY, 
                        src:EVT_SERVICECATEGORY_ORG::varchar AS EVT_SERVICECATEGORY_ORG, 
                        src:EVT_SERVICEPERC::numeric(38, 10) AS EVT_SERVICEPERC, 
                        src:EVT_SERVICEPROBLEM::varchar AS EVT_SERVICEPROBLEM, 
                        src:EVT_SERVICEPROBLEM_ORG::varchar AS EVT_SERVICEPROBLEM_ORG, 
                        src:EVT_SERVICEREQUEST::varchar AS EVT_SERVICEREQUEST, 
                        src:EVT_SESSION::varchar AS EVT_SESSION, 
                        src:EVT_SHIFT::varchar AS EVT_SHIFT, 
                        src:EVT_SIGPB::varchar AS EVT_SIGPB, 
                        src:EVT_SIGRB::varchar AS EVT_SIGRB, 
                        src:EVT_SLACK::numeric(38, 10) AS EVT_SLACK, 
                        src:EVT_SMDA::varchar AS EVT_SMDA, 
                        src:EVT_SOP::varchar AS EVT_SOP, 
                        src:EVT_SOPEFFECTIVE::datetime AS EVT_SOPEFFECTIVE, 
                        src:EVT_SOURCECODE::varchar AS EVT_SOURCECODE, 
                        src:EVT_SOURCESYSTEM::varchar AS EVT_SOURCESYSTEM, 
                        src:EVT_SRQCALLNAME::varchar AS EVT_SRQCALLNAME, 
                        src:EVT_SRQCUSTOMER::varchar AS EVT_SRQCUSTOMER, 
                        src:EVT_SRQLEVEL1::varchar AS EVT_SRQLEVEL1, 
                        src:EVT_STAFFINJURY::varchar AS EVT_STAFFINJURY, 
                        src:EVT_STANDWORK::varchar AS EVT_STANDWORK, 
                        src:EVT_START::datetime AS EVT_START, 
                        src:EVT_STATEMENTOFCOND::varchar AS EVT_STATEMENTOFCOND, 
                        src:EVT_STATUS::varchar AS EVT_STATUS, 
                        src:EVT_STEP::numeric(38, 10) AS EVT_STEP, 
                        src:EVT_SUPPLIER::varchar AS EVT_SUPPLIER, 
                        src:EVT_SUPPLIER_ORG::varchar AS EVT_SUPPLIER_ORG, 
                        src:EVT_SYMPTOM::varchar AS EVT_SYMPTOM, 
                        src:EVT_SYSLEVEL::varchar AS EVT_SYSLEVEL, 
                        src:EVT_TACTICALCAUSE::varchar AS EVT_TACTICALCAUSE, 
                        src:EVT_TARGET::datetime AS EVT_TARGET, 
                        src:EVT_TFDATECOMPLETED::datetime AS EVT_TFDATECOMPLETED, 
                        src:EVT_TFPROMISEDATE::datetime AS EVT_TFPROMISEDATE, 
                        src:EVT_TOGEOREF::varchar AS EVT_TOGEOREF, 
                        src:EVT_TOPOINT::numeric(38, 10) AS EVT_TOPOINT, 
                        src:EVT_TOREFDESC::varchar AS EVT_TOREFDESC, 
                        src:EVT_TO_HORIZONTAL_OFFSET::numeric(38, 10) AS EVT_TO_HORIZONTAL_OFFSET, 
                        src:EVT_TO_HORIZONTAL_OFFSETTYPE::varchar AS EVT_TO_HORIZONTAL_OFFSETTYPE, 
                        src:EVT_TO_HORIZONTAL_OFFSETUOM::varchar AS EVT_TO_HORIZONTAL_OFFSETUOM, 
                        src:EVT_TO_LATITUDE::numeric(38, 10) AS EVT_TO_LATITUDE, 
                        src:EVT_TO_LONGITUDE::numeric(38, 10) AS EVT_TO_LONGITUDE, 
                        src:EVT_TO_OFFSET::numeric(38, 10) AS EVT_TO_OFFSET, 
                        src:EVT_TO_OFFSET_DIRECTION::varchar AS EVT_TO_OFFSET_DIRECTION, 
                        src:EVT_TO_OFFSET_PERCENTAGE::numeric(38, 10) AS EVT_TO_OFFSET_PERCENTAGE, 
                        src:EVT_TO_REFERENCE::numeric(38, 10) AS EVT_TO_REFERENCE, 
                        src:EVT_TO_RELATIONSHIP_TYPE::varchar AS EVT_TO_RELATIONSHIP_TYPE, 
                        src:EVT_TO_VERTICAL_OFFSET::numeric(38, 10) AS EVT_TO_VERTICAL_OFFSET, 
                        src:EVT_TO_VERTICAL_OFFSETTYPE::varchar AS EVT_TO_VERTICAL_OFFSETTYPE, 
                        src:EVT_TO_VERTICAL_OFFSETUOM::varchar AS EVT_TO_VERTICAL_OFFSETUOM, 
                        src:EVT_TO_XCOORDINATE::numeric(38, 10) AS EVT_TO_XCOORDINATE, 
                        src:EVT_TO_YCOORDINATE::numeric(38, 10) AS EVT_TO_YCOORDINATE, 
                        src:EVT_TRANSCODE::varchar AS EVT_TRANSCODE, 
                        src:EVT_TRANSGROUP::numeric(38, 10) AS EVT_TRANSGROUP, 
                        src:EVT_TRANSORGID::numeric(38, 10) AS EVT_TRANSORGID, 
                        src:EVT_TRIGEVENT::varchar AS EVT_TRIGEVENT, 
                        src:EVT_TYPE::varchar AS EVT_TYPE, 
                        src:EVT_TYPECATEGORY::varchar AS EVT_TYPECATEGORY, 
                        src:EVT_UDFCHAR01::varchar AS EVT_UDFCHAR01, 
                        src:EVT_UDFCHAR02::varchar AS EVT_UDFCHAR02, 
                        src:EVT_UDFCHAR03::varchar AS EVT_UDFCHAR03, 
                        src:EVT_UDFCHAR04::varchar AS EVT_UDFCHAR04, 
                        src:EVT_UDFCHAR05::varchar AS EVT_UDFCHAR05, 
                        src:EVT_UDFCHAR06::varchar AS EVT_UDFCHAR06, 
                        src:EVT_UDFCHAR07::varchar AS EVT_UDFCHAR07, 
                        src:EVT_UDFCHAR08::varchar AS EVT_UDFCHAR08, 
                        src:EVT_UDFCHAR09::varchar AS EVT_UDFCHAR09, 
                        src:EVT_UDFCHAR10::varchar AS EVT_UDFCHAR10, 
                        src:EVT_UDFCHAR11::varchar AS EVT_UDFCHAR11, 
                        src:EVT_UDFCHAR12::varchar AS EVT_UDFCHAR12, 
                        src:EVT_UDFCHAR13::varchar AS EVT_UDFCHAR13, 
                        src:EVT_UDFCHAR14::varchar AS EVT_UDFCHAR14, 
                        src:EVT_UDFCHAR15::varchar AS EVT_UDFCHAR15, 
                        src:EVT_UDFCHAR16::varchar AS EVT_UDFCHAR16, 
                        src:EVT_UDFCHAR17::varchar AS EVT_UDFCHAR17, 
                        src:EVT_UDFCHAR18::varchar AS EVT_UDFCHAR18, 
                        src:EVT_UDFCHAR19::varchar AS EVT_UDFCHAR19, 
                        src:EVT_UDFCHAR20::varchar AS EVT_UDFCHAR20, 
                        src:EVT_UDFCHAR21::varchar AS EVT_UDFCHAR21, 
                        src:EVT_UDFCHAR22::varchar AS EVT_UDFCHAR22, 
                        src:EVT_UDFCHAR23::varchar AS EVT_UDFCHAR23, 
                        src:EVT_UDFCHAR24::varchar AS EVT_UDFCHAR24, 
                        src:EVT_UDFCHAR25::varchar AS EVT_UDFCHAR25, 
                        src:EVT_UDFCHAR26::varchar AS EVT_UDFCHAR26, 
                        src:EVT_UDFCHAR27::varchar AS EVT_UDFCHAR27, 
                        src:EVT_UDFCHAR28::varchar AS EVT_UDFCHAR28, 
                        src:EVT_UDFCHAR29::varchar AS EVT_UDFCHAR29, 
                        src:EVT_UDFCHAR30::varchar AS EVT_UDFCHAR30, 
                        src:EVT_UDFCHAR31::varchar AS EVT_UDFCHAR31, 
                        src:EVT_UDFCHAR32::varchar AS EVT_UDFCHAR32, 
                        src:EVT_UDFCHAR33::varchar AS EVT_UDFCHAR33, 
                        src:EVT_UDFCHAR34::varchar AS EVT_UDFCHAR34, 
                        src:EVT_UDFCHAR35::varchar AS EVT_UDFCHAR35, 
                        src:EVT_UDFCHAR36::varchar AS EVT_UDFCHAR36, 
                        src:EVT_UDFCHAR37::varchar AS EVT_UDFCHAR37, 
                        src:EVT_UDFCHAR38::varchar AS EVT_UDFCHAR38, 
                        src:EVT_UDFCHAR39::varchar AS EVT_UDFCHAR39, 
                        src:EVT_UDFCHAR40::varchar AS EVT_UDFCHAR40, 
                        src:EVT_UDFCHAR41::varchar AS EVT_UDFCHAR41, 
                        src:EVT_UDFCHAR42::varchar AS EVT_UDFCHAR42, 
                        src:EVT_UDFCHAR43::varchar AS EVT_UDFCHAR43, 
                        src:EVT_UDFCHAR44::varchar AS EVT_UDFCHAR44, 
                        src:EVT_UDFCHAR45::varchar AS EVT_UDFCHAR45, 
                        src:EVT_UDFCHKBOX01::varchar AS EVT_UDFCHKBOX01, 
                        src:EVT_UDFCHKBOX02::varchar AS EVT_UDFCHKBOX02, 
                        src:EVT_UDFCHKBOX03::varchar AS EVT_UDFCHKBOX03, 
                        src:EVT_UDFCHKBOX04::varchar AS EVT_UDFCHKBOX04, 
                        src:EVT_UDFCHKBOX05::varchar AS EVT_UDFCHKBOX05, 
                        src:EVT_UDFCHKBOX06::varchar AS EVT_UDFCHKBOX06, 
                        src:EVT_UDFCHKBOX07::varchar AS EVT_UDFCHKBOX07, 
                        src:EVT_UDFCHKBOX08::varchar AS EVT_UDFCHKBOX08, 
                        src:EVT_UDFCHKBOX09::varchar AS EVT_UDFCHKBOX09, 
                        src:EVT_UDFCHKBOX10::varchar AS EVT_UDFCHKBOX10, 
                        src:EVT_UDFDATE01::datetime AS EVT_UDFDATE01, 
                        src:EVT_UDFDATE02::datetime AS EVT_UDFDATE02, 
                        src:EVT_UDFDATE03::datetime AS EVT_UDFDATE03, 
                        src:EVT_UDFDATE04::datetime AS EVT_UDFDATE04, 
                        src:EVT_UDFDATE05::datetime AS EVT_UDFDATE05, 
                        src:EVT_UDFDATE06::datetime AS EVT_UDFDATE06, 
                        src:EVT_UDFDATE07::datetime AS EVT_UDFDATE07, 
                        src:EVT_UDFDATE08::datetime AS EVT_UDFDATE08, 
                        src:EVT_UDFDATE09::datetime AS EVT_UDFDATE09, 
                        src:EVT_UDFDATE10::datetime AS EVT_UDFDATE10, 
                        src:EVT_UDFNOTE01::varchar AS EVT_UDFNOTE01, 
                        src:EVT_UDFNOTE02::varchar AS EVT_UDFNOTE02, 
                        src:EVT_UDFNUM01::numeric(38, 10) AS EVT_UDFNUM01, 
                        src:EVT_UDFNUM02::numeric(38, 10) AS EVT_UDFNUM02, 
                        src:EVT_UDFNUM03::numeric(38, 10) AS EVT_UDFNUM03, 
                        src:EVT_UDFNUM04::numeric(38, 10) AS EVT_UDFNUM04, 
                        src:EVT_UDFNUM05::numeric(38, 10) AS EVT_UDFNUM05, 
                        src:EVT_UDFNUM06::numeric(38, 10) AS EVT_UDFNUM06, 
                        src:EVT_UDFNUM07::numeric(38, 10) AS EVT_UDFNUM07, 
                        src:EVT_UDFNUM08::numeric(38, 10) AS EVT_UDFNUM08, 
                        src:EVT_UDFNUM09::numeric(38, 10) AS EVT_UDFNUM09, 
                        src:EVT_UDFNUM10::numeric(38, 10) AS EVT_UDFNUM10, 
                        src:EVT_UPDATECOUNT::numeric(38, 10) AS EVT_UPDATECOUNT, 
                        src:EVT_UPDATED::datetime AS EVT_UPDATED, 
                        src:EVT_UPDATEDBY::varchar AS EVT_UPDATEDBY, 
                        src:EVT_UTILITYINCIDENT::varchar AS EVT_UTILITYINCIDENT, 
                        src:EVT_VIPSTATUS::varchar AS EVT_VIPSTATUS, 
                        src:EVT_VISITORINJURY::varchar AS EVT_VISITORINJURY, 
                        src:EVT_WARRANTY::varchar AS EVT_WARRANTY, 
                        src:EVT_WORKADDRESS::varchar AS EVT_WORKADDRESS, 
                        src:EVT_WORKMANSHIP::varchar AS EVT_WORKMANSHIP, 
                        src:EVT_WORKSPACE::varchar AS EVT_WORKSPACE, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:EVT_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:EVT_CODE  ORDER BY 
            src:EVT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5EVENTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_ACD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_CALCULATEDPRIORITY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_CAMPAIGN_LINE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_COMPLETED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_COMPLETED_TRUNC), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_DATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_DOWNTIME), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_DOWNTIMEHRS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_DUE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_DURATION), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_EARLYSTART), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_FAILUREUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_FREQ), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_FROMPOINT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_FROM_HORIZONTAL_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_FROM_LATITUDE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_FROM_LONGITUDE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_FROM_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_FROM_OFFSET_PERCENTAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_FROM_REFERENCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_FROM_VERTICAL_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_FROM_XCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_FROM_YCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_GENWINBEGVAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_GENWINBEGVAL2), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_GENWINSTART), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_GLTRANSFER), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_GUESTPROFILEID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_INCREMENT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_INTERFACE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_INVOICELINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_INVOICE_REVISION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_LABTOTAL), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_LASTCAL), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_LASTPLAN), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_LASTSTATUSUPDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_LASTTIMEPB), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_LASTTIMERB), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_LATEEND), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_LATITUDE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_LONGITUDE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_MATAVAIL), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_MATTOTAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_MAXCOST), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_METERDUE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_METERDUE2), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_METERDUEDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_METERDUEDATE2), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_METERINTERVAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_METERINTERVAL2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_METERREADING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_MP_REV), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_MP_SEQ), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_MUSTEND), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_NEARWINBEGVAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_NEARWINBEGVAL2), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_NEARWINSTART), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_NEWDUR), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_NEWTARGET), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_OKWINEND), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_OKWINENDVAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_OKWINENDVAL2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_ORIGACT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_PERFORMONDAY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_PERMITREVIEWED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_PFPROMISEDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_PPMREV), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_PPOPK), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_PRECISION), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_PRODUCTIONEND), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_PRODUCTIONREQUESTREV), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_PRODUCTIONSTART), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_PSQPK), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_REPORTED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_REQUESTEND), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_REQUESTSTART), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_ROUTEREV), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_SAFETYREVIEWED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_SCHEDEND), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_SCHEDNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_SCHEDULEDENDTIME), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_SCHEDULEDSTARTTIME), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_SEQ), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_SERVICEPERC), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_SLACK), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_SOPEFFECTIVE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_START), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_STEP), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_TARGET), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_TFDATECOMPLETED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_TFPROMISEDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_TOPOINT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_TO_HORIZONTAL_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_TO_LATITUDE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_TO_LONGITUDE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_TO_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_TO_OFFSET_PERCENTAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_TO_REFERENCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_TO_VERTICAL_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_TO_XCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_TO_YCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_TRANSGROUP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_TRANSORGID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_UDFDATE06), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_UDFDATE07), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_UDFDATE08), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_UDFDATE09), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_UDFDATE10), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_UDFNUM06), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_UDFNUM07), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_UDFNUM08), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_UDFNUM09), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_UDFNUM10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVT_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EVT_LASTSAVED), '1900-01-01')) is not null