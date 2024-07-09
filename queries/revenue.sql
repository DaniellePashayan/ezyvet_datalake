SELECT
	date(CONVERT_TZ(from_unixtime(invoicedata_duedate), '+00:00', '-08:00')) as "Date",
	SUM(invoicedata_amount) as "Revenue"
FROM 
	invoice
WHERE
	invoicedata_active = 1
	AND invoicedata_approvedat > 0
	AND date(CONVERT_TZ(from_unixtime(invoicedata_duedate), '+00:00', '-08:00'))  >= current_date() - interval 7 day
GROUP BY
	date(CONVERT_TZ(from_unixtime(invoicedata_duedate), '+00:00', '-08:00'))