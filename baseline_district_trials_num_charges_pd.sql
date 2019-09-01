-- True Baseline: Supplement to get num charges for district cases
-- This is the "outermost" data set
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
group by casenumber