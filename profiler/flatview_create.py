import configparser
from optparse import OptionParser
from MetaMiner import MetaMiner

parser = OptionParser()
parser.add_option("-i", "--host", dest="db_host", help="", metavar="string")
parser.add_option("-u", "--user", dest="db_user", help="", metavar="string")
parser.add_option("-p", "--password", dest="db_password", help="", metavar="string")
parser.add_option("-c", "--catalog", dest="db_catalog", help="", metavar="string")
(options, args) = parser.parse_args()


def main():
	config = configparser.ConfigParser()
	config.read('config.ini')
	if len(config.sections()) == 0:
		print('config.ini file not yet present, please copy from template (templace_config.ini) and fill in required properties')
		quit()

	miner = MetaMiner(type='pymssql').getInstance(
		db_catalog=config['subjectdb']['db_catalog'],
		db_host=config['subjectdb']['db_host'],
		db_user=config['subjectdb']['db_user'],
		db_password=config['subjectdb']['db_password'])
	print("tables")
	for t in miner.getTables():
		print("foreign keys of '%s'" % t)
		for fk in miner.getForeignKeys(t):
			print(fk)
		print(miner.getQueryForFlatTable(t))
		miner.execute(miner.getQueryForFlatTable(t))
	print("alltogether now")
	for fk in miner.getForeignKeys():
		print(fk)



if __name__ == '__main__':
	main()
