-- District Trial where ALL CHARGES NP'D (num chargesquery)
-- This is first subset or "middle ring" query
select cdi.casenumber as casenumber, count(*) as num_charges
from charge_and_disposition_information cdi
inner join  case_information ci on cdi.casenumber = ci.casenumber
where county in ('BALTIMORE CITY', 'BALTIMORE COUNTY', 'MONTGOMERY COUNTY', 'PRINCE GEORGE\'S COUNTY')
and issueddate between %s and %s
and casestatus = 'CLOSED' and casedisposition='TRIAL'
and casetype = 'CRIMINAL'
and ci.casenumber in
(
select ci.casenumber
from case_information ci
inner join  custom_bail_set cbs on ci.casenumber = cbs.casenumber
where county in ('BALTIMORE CITY', 'BALTIMORE COUNTY', 'MONTGOMERY COUNTY', 'PRINCE GEORGE\'S COUNTY')
and casetype = 'CRIMINAL'
and casestatus = 'CLOSED' 
and issueddate between %s and %s
and code = 'INIT'
)
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