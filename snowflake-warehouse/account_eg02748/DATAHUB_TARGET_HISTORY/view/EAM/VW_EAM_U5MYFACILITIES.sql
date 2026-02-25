
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5MYFACILITIES
                   as                       
                    SELECT
                        t.MFA_USER,
                        t.MFA_FACCODE,
                        t.MFA_FACDESC,
                        t.MFA_MRC,
                        t.MFA_MRCDESC,
                        t.MFA_FACORG,
                        t.CREATEDBY,
                        t.CREATED,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.LASTSAVED,
                        t.UPDATECOUNT,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_U5MYFACILITIES as  t
					 union
					        SELECT
                        th.MFA_USER,
                        th.MFA_FACCODE,
                        th.MFA_FACDESC,
                        th.MFA_MRC,
                        th.MFA_MRCDESC,
                        th.MFA_FACORG,
                        th.CREATEDBY,
                        th.CREATED,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.LASTSAVED,
                        th.UPDATECOUNT,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5MYFACILITIES as  th ;
                     