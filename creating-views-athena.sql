CREATE VIEW vw_quicksight_bionbi 
AS 
  SELECT Date_parse(eventtime, '%Y-%m-%dT%H:%i:%SZ') AS "Event Time", 
         eventname  AS "Event Name", 
         awsregion  AS "AWS Region", 
         accountid  AS "Account ID", 
         username   AS "User Name", 
         analysisname AS "Analysis Name", 
         dashboardname AS "Dashboard Name", 
         Date_parse(date, '%Y%m%d') AS "Event Date" 
  FROM   "quicksightbionbi02"."aggregatedoutput"; 


  CREATE VIEW vw_users 
AS 
  SELECT usr.username "User Name", 
         usr.role     AS "Role", 
         usr.active   AS "Active" 
  FROM   (quicksightbionbi02.users 
          CROSS JOIN Unnest("users") t (usr));


CREATE VIEW vw_analysis 
AS 
  SELECT aly.analysisname "Analysis Name", 
         aly.analysisid   AS "Analysis ID" 
  FROM   (quicksightbionbi02.analysis 
          CROSS JOIN Unnest("analysis") t (aly)); 


CREATE VIEW vw_analysisdatasets 
AS 
  SELECT alyds.analysesname "Analysis Name", 
         alyds.analysisid   AS "Analysis ID", 
         alyds.datasetid    AS "Dataset ID", 
         alyds.datasetname  AS "Dataset Name" 
  FROM   (quicksightbionbi02.analysisdatasets 
          CROSS JOIN Unnest("analysisdatasets") t (alyds));
 
CREATE VIEW vw_datasets 
AS 
  SELECT ds.datasetname AS "Dataset Name", 
         ds.importmode  AS "Import Mode" 
  FROM   (quicksightbionbi02.datasets 
          CROSS JOIN Unnest("datasets") t (ds));