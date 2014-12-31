import os
import configparser
from MetaMiner import MetaMiner
from pathutils import make_sure_path_exists
from objects import Table

def main():
	config = configparser.ConfigParser()
	config.read('config.ini')
	if len(config.sections()) == 0:
		print('config.ini file not yet present, please copy from template (templace_config.ini) and fill in required properties')
		quit()

	# create db wrapper on to-be-analyzed database
	subjectdb = MetaMiner(type='pymssql').getInstance(
		db_catalog=config['subjectdb']['db_catalog'],
		db_host=config['subjectdb']['db_host'],
		db_user=config['subjectdb']['db_user'],
		db_password=config['subjectdb']['db_password'])
	# create db wrapper on analysis-results database
	resultdb = MetaMiner(type='pymssql').getInstance(
		db_catalog=config['analysisdb']['db_catalog'],
		db_host=config['analysisdb']['db_host'],
		db_user=config['analysisdb']['db_user'],
		db_password=config['analysisdb']['db_password'])
	log_folder = config['analysisdb']['log_folder']

	make_sure_path_exists(log_folder)

	print("tables")
	for t in subjectdb.getTables():
		print("foreign keys of '%s'" % t)
		for fk in subjectdb.getForeignKeys(t):
			print(fk)
		# create a flat view on this table using the explicit foreign keys
		destination_view = Table(db_catalog=resultdb.db_catalog,
								 db_schema='dbo',
								 tablename="{0}_flat".format(t.tablename))
		# drop current version to allow rerunning of this script
		resultdb.drop_view(destination_view)
		# create the new flat view on the single table
		view_query = resultdb.view_query_wrap(destination_view,
											  subjectdb.flat_table_query_full(t))
		resultdb.execute(view_query, log_file = os.path.join(log_folder, "{}_create.sql".format(destination_view.tablename)))
	print("Summary of all foreign keys") #note that this single call is much faster than the previous per-table call as a new connection is created per query
	for fk in subjectdb.getForeignKeys():
		print(fk)


if __name__ == '__main__':
	main()
