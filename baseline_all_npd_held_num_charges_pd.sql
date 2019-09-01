-- District Trial where ALL CHARGES NP'D and where Held before trial(num charges query)
-- This is the second subset  or the "innermost ring" query
-- Complex query for all NP'd cases where RELS=DISPO
select cdi.casenumber as casenumber, count(*) as num_charges
from case_information ci
inner join event_history_information rels on rels.CaseNumber = ci.CaseNumber 
inner join charge_and_disposition_information cdi on ci.CaseNumber = cdi.CaseNumber 
inner join custom_bail_set init on ci.casenumber = init.casenumber
where county in ('BALTIMORE CITY', 'BALTIMORE COUNTY', 'MONTGOMERY COUNTY', 'PRINCE GEORGE\'S COUNTY') and ci.IssuedDate between %s and %s
and init.code="INIT" and init.outcome in ("HWOB","HDOB")
and rels.Event = 'RELS' and cdi.Disposition = 'nolle prosequi' 
and rels.Date = cdi.DispositionDate 
and ci.casenumber in
(
-- ALL NP'd
select distinct(ci.casenumber)
from case_information ci
inner join  custom_bail_set cbs on ci.casenumber = cbs.casenumber
where county in ('BALTIMORE CITY', 'BALTIMORE COUNTY', 'MONTGOMERY COUNTY', 'PRINCE GEORGE\'S COUNTY')
and casetype = 'CRIMINAL'
and casestatus = 'CLOSED'and casedisposition='TRIAL'
and issueddate between %s and %s
and code = 'INIT'
and ci.casenumber not in 
(
select distinct(ci.casenumber)
from case_information ci
inner join  custom_bail_set cbs on ci.casenumber = cbs.casenumber
inner join  charge_and_disposition_information cdi on ci.casenumber = cdi.casenumber
where county in ('BALTIMORE CITY', 'BALTIMORE COUNTY', 'MONTGOMERY COUNTY', 'PRINCE GEORGE\'S COUNTY')
and casetype = 'CRIMINAL'
and casestatus = 'CLOSED' and casedisposition='TRIAL'
and issueddate between %s and %s
and code = 'INIT'
and cdi.Disposition <> 'NOLLE PROSEQUI'
)
)
and ci.casenumber not in
(select casenumber from
(select ci.casenumber, count(ci.casenumber) as total_rels
from case_information ci
inner join event_history_information ehs on ci.casenumber = ehs.casenumber
where county in ('BALTIMORE CITY', 'BALTIMORE COUNTY', 'MONTGOMERY COUNTY', 'PRINCE GEORGE\'S COUNTY')
and casetype = 'CRIMINAL'
and casestatus = 'CLOSED' and casedisposition='TRIAL'
and issueddate between %s and %s
and ehs.event = "RELS"
group by ci.casenumber) as boob
where total_rels>1
)
-- Add in FTA
-- See if it works
and ci.casenumber not in
(
select distinct(ci.casenumber)
from case_information ci
inner join event_history_information ehs on ci.casenumber = ehs.casenumber
where county in ('BALTIMORE CITY', 'BALTIMORE COUNTY', 'MONTGOMERY COUNTY', 'PRINCE GEORGE\'S COUNTY')
and casetype = 'CRIMINAL'
and casestatus = 'CLOSED' and casedisposition='TRIAL'
and issueddate between %s and %s
and ehs.event = "WARI" and ehs.comment like "%FTA%"
)
group by ci.casenumber