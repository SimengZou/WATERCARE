CREATE OR REPLACE SECURE VIEW ${buildvar.env}_WBP_SHARE.DATAHUB_SHARED_WBP.VWS_PLANT_BUILDINGS
AS
SELECT 
  A1."ACQTYPE" AS "Acquistion Type"
, A3."ADDRKEY" AS "Address"
, A2."UNITID" AS "Asset ID"
, A2."COMPKEY" AS "CompKey"
, A1."COSTCENTRE" AS "Cost Centre Code"
, 'TBD' AS "Cost Centre Description"
, A1."CRITICALITY" AS "Criticality Code"
, 'TBD' AS "Criticality Description"
, A2."DPTH" AS "Depth"
, A2."UNITDESC" AS "Description"
, A2."DIAM" AS "Diameter"
, A1."FORDISPOSAL" AS "Disposal Reason"
, A1."DRAWINGNO" AS "Drawing No"
, A1."DRAWINGNOREF" AS "Drawing No Ref"
, A2."AGENCYCHARACTER1" AS "Equipment No"
, A3."EXPDATE" AS "Expire Date"
, A2."AREA" AS "Facility Code"
, 'TBD' AS "Facility Description"
, A2."HT" AS "Height"
, A2."INSTDATE" AS "Installed"
, A2."ASBLT" AS "Legacy Key"
, A2."MATL" AS "Material Code"
, A2."LEN" AS "Length"
, 'TBD' AS "Material Description"
, A1."MODELNO" AS "Model #"
, 'TBD' AS "Operations Area"
, A2."OWN" AS "Ownership Code"
, 'TBD' AS "Ownership Description"
, A3."MAPNO" AS "Position"
, A2."SUBAREA" AS "Process Code"
, 'TBD' AS "Process Description"
, 'TBD' AS "Process Type"
, A1."SAFETYCRITICAL" AS "Safety Critical Element"
, A2."SERVSTAT" AS "Service Status"
, A2."LOC" AS "Set Code"
, 'TBD' AS "Set Description"
, A2."DISTRICT" AS "System Code"
, 'TBD' AS "System Description"
, A2."UNITTYPE" AS "Unit Type Code"
, 'TBD' AS "Unit Type Description"
, A2."WT" AS "Weight"
, A2."WID" AS "Width"

, ad."NZHOUSENO" AS "Street Number"
, CONCAT(ad."NZSTNAME",' ',ad."NZSTTYPE") AS "Street Name"
, ad."NZSUBURB" AS "City"
, ad."NZPOSTCODE" AS "Postal Code"

, A1.MAKE                 					AS "Make"
, A2.SERNO                                  AS "Serial Number"
, A1.ZONECATCHMENT        					AS "Water Zone/Sewer Catchment"
, A1.WARRANTYEND          					AS "Warranty End Date"
, A1.WARRANTYSTART        					AS "Warranty Start Date"
, XBUILDINGS.DESIGNRESILIENCE               AS "Design Resilience"
, XBUILDINGS.QUAKEDESIGN                    AS "Earthquake Design Level"
, XBUILDINGS.EXTCOAT                   		AS "External Coating"
, XBUILDINGS.FUNCAREA                     	AS "Functional Area"
, XBUILDINGS.GRDLEVEL                       AS "Ground Level"
, XBUILDINGS.PHOTO3D                        AS "Photo/3D Model"
, XBUILDINGS.AREA                           AS "Surface Area (m2)"

FROM
  "${buildvar.env}_WCC_DATAWAREHOUSE"."DATAHUB_TARGET"."IPS_ASSETMANAGEMENT_AGENCYDEFINED_COMPAGENCYSIMPLE" as A2
  INNER JOIN "${buildvar.env}_WCC_DATAWAREHOUSE"."DATAHUB_TARGET"."IPS_WSLASSETS_XALLASSETSDP01" as A1 ON A2.COMPKEY = A1.COMPKEY AND A1.BUSINESSAREA IN ('WBC','WBE','WBW')
  INNER JOIN "${buildvar.env}_WCC_DATAWAREHOUSE"."DATAHUB_TARGET"."IPS_ASSETMANAGEMENT_COMP" as A3 on A3.COMPKEY = A1.COMPKEY
  INNER JOIN "${buildvar.env}_WCC_DATAWAREHOUSE"."DATAHUB_TARGET"."IPS_ASSETMANAGEMENT_COMPTYPE" as A4 on A4.COMPTYPE = A3.COMPTYPE
  LEFT OUTER JOIN "${buildvar.env}_WCC_DATAWAREHOUSE"."DATAHUB_TARGET"."IPS_PROPERTY_ADDRESS" as ad ON ad.ADDRKEY = A3.ADDRKEY AND ad.OPTA = 'WBP'
  LEFT OUTER JOIN "${buildvar.env}_WCC_DATAWAREHOUSE"."DATAHUB_TARGET"."IPS_WSLASSETSCONFIGURED_XBUILDINGS" AS XBUILDINGS ON A2.COMPKEY = XBUILDINGS.COMPKEY 
  WHERE A2.OWN = 'WBP' and A4.COMPCODE = 'BLDS';


