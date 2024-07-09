SELECT 
	date(CONVERT_TZ(from_unixtime(contact_time),'+0:00','-8:00')) as 'Created Date',
	COUNT(DISTINCT(contact_uid)) as 'New Count'
FROM
	ezymerged_middletownvet.contact as contact
WHERE
	date(CONVERT_TZ(from_unixtime(contact_time),'+0:00','-8:00')) = '2024-07-05'
GROUP BY
	date(CONVERT_TZ(from_unixtime(contact_time),'+0:00','-8:00'))