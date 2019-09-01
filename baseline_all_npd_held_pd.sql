-- A Complex query to get dates and charges for all NP'd cases where RELS=DISPO
-- Pull this into Pandas and run a series of different analyses
select ci.casenumber, ci.county as jurisdiction, ci.issueddate, init.outcome as init_outcome, init.date as init_date, rels.date as rels_date, di.race, di.sex, di.zipcode, di.dob, 
cdi.description as top_charge, cdi.disposition as top_disposition, cdi.dispositiondate as top_dispo_date
from case_information ci 
inner join event_history_information rels on rels.CaseNumber = ci.CaseNumber 
inner join defendant_information di on di.CaseNumber = ci.CaseNumber 
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