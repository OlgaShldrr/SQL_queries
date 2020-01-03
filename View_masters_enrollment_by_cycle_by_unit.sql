-- !preview conn=DBI::dbConnect(RSQLite::SQLite())

SELECT        organization.characteristics.pf_unitid, organization.characteristics.business_school_name, organization.characteristics.class_control, organization.characteristics.accreditation_status, organization.characteristics.country, 
                         organization.characteristics.state, organization.characteristics.macro_region, SUM(enrollment.by_edlevel.enrollment) AS enrollment, enrollment.by_edlevel.edlevel, enrollment.by_edlevel.survey_cycle
FROM            enrollment.by_edlevel INNER JOIN
                         organization.characteristics ON enrollment.by_edlevel.pf_unit_id = organization.characteristics.pf_unitid
WHERE        (enrollment.by_edlevel.edlevel = 'Master''s')
GROUP BY enrollment.by_edlevel.edlevel, enrollment.by_edlevel.survey_cycle, organization.characteristics.pf_unitid, organization.characteristics.business_school_name, organization.characteristics.class_control, 
                         organization.characteristics.accreditation_status, organization.characteristics.country, organization.characteristics.state, organization.characteristics.macro_region
