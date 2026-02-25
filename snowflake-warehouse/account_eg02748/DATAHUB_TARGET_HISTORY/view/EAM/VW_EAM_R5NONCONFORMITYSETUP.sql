
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5NONCONFORMITYSETUP
                   as                       
                    SELECT
                        t.NCP_ORG,
                        t.NCP_PROTECTHEADER,
                        t.NCP_PROTECTOBSDATAONHDR,
                        t.NCP_SYNCHRONIZE,
                        t.NCP_MERGERESTRICTIONS,
                        t.NCP_UPDATEDBY,
                        t.NCP_UPDATED,
                        t.NCP_USEOBSSTATUS,
                        t.NCP_SUPERSEDEDSTATUS,
                        t.NCP_REINSPECTSTATUS,
                        t.NCP_AUTOAPPROVESTATUS,
                        t.NCP_AUTOSKIPSTATUS,
                        t.NCP_INCLUDECONDITION,
                        t.NCP_PREVOBSCOUNT,
                        t.NCP_UPDATECOUNT,
                        t.NCP_CREATEFROMCHECKLIST,
                        t.NCP_LASTSAVED,
                        t.NCP_MASSACKNOWLEDGEALLOWED,
                        t.NCP_COPYSEVERITY,
                        t.NCP_COPYINTENSITY,
                        t.NCP_COPYSIZE,
                        t.NCP_COPYIMPORTANCE,
                        t.NCP_COPYNEXTINSPDATEOVERRIDE,
                        t.NCP_COPYOBSERVATIONUDFS,
                        t.NCP_COPYFROM,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5NONCONFORMITYSETUP as  t
					 union
					        SELECT
                        th.NCP_ORG,
                        th.NCP_PROTECTHEADER,
                        th.NCP_PROTECTOBSDATAONHDR,
                        th.NCP_SYNCHRONIZE,
                        th.NCP_MERGERESTRICTIONS,
                        th.NCP_UPDATEDBY,
                        th.NCP_UPDATED,
                        th.NCP_USEOBSSTATUS,
                        th.NCP_SUPERSEDEDSTATUS,
                        th.NCP_REINSPECTSTATUS,
                        th.NCP_AUTOAPPROVESTATUS,
                        th.NCP_AUTOSKIPSTATUS,
                        th.NCP_INCLUDECONDITION,
                        th.NCP_PREVOBSCOUNT,
                        th.NCP_UPDATECOUNT,
                        th.NCP_CREATEFROMCHECKLIST,
                        th.NCP_LASTSAVED,
                        th.NCP_MASSACKNOWLEDGEALLOWED,
                        th.NCP_COPYSEVERITY,
                        th.NCP_COPYINTENSITY,
                        th.NCP_COPYSIZE,
                        th.NCP_COPYIMPORTANCE,
                        th.NCP_COPYNEXTINSPDATEOVERRIDE,
                        th.NCP_COPYOBSERVATIONUDFS,
                        th.NCP_COPYFROM,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5NONCONFORMITYSETUP as  th ;
                     