"""
	upload 3 raw tables to snowflake via script
	run cleaning script -> save 3 clean tables to s3
	fetch 3 clean tables from s3 -> upload to snowflake via script
	merge_tables -> save final merged cleaned table to s3
	fetch final table -> upload to snowflake
	create features and run model on final table -> get prediction table -> save to s3
	fetch from s3 -> upload to snowflake
	upload prediction table to google sheets
	connect data studio to sheets -> get viz
"""

# access secrets -> os.environ.get(secret_name)