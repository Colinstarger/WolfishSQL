#Baseline District Info - WHERE ALL CHARGES NP'D
select ci.casenumber, ci.county as jurisdiction, ci.issueddate, cbs.outcome as init_outcome, cbs.date as init_date, di.race, di.sex, di.zipcode, di.dob, 
cdi.description as top_charge, cdi.disposition as top_disposition, cdi.dispositiondate as top_dispo_date
from case_information ci
inner join  custom_bail_set cbs on ci.casenumber = cbs.casenumber
inner join  defendant_information di on ci.casenumber = di.casenumber
inner join  charge_and_disposition_information cdi on ci.casenumber = cdi.casenumber
where county in ('BALTIMORE CITY', 'BALTIMORE COUNTY', 'MONTGOMERY COUNTY', 'PRINCE GEORGE\'S COUNTY')
and casetype = 'CRIMINAL'
and casestatus = 'CLOSED'and casedisposition='TRIAL'
and issueddate between %s and %s
and code = 'INIT'
and ci.casenumber not in
(
select distinct (ci.casenumber)
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
group by casenumber
