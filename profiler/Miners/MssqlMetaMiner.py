import sys
sys.path.append("../../common")
from objects import ForeignKey, PrimaryKey, Table, Column

import pymssql

class MssqlMetaMiner():

	def __init__(self, db_catalog='', db_host='127.0.0.1', db_user='', db_password=''):
		self.db_catalog = db_catalog
		self.db_host = db_host
		self.db_user = db_user
		self.db_password = db_password

	def getTableRowCount(self, table):
		retval = float('nan')
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute(""" 
					SELECT 
						COUNT(*) 
					FROM [{0}].[{1}].[{2}]""".format(table.db_catalog, table.db_schema, table.tablename))
				retval = cursor.fetchone()[0]
		return retval

	def getColumnCountForTable(self, table=None):
		retval = float('nan')
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute(""" 
					SELECT 
						COUNT(*)
					FROM 
						INFORMATION_SCHEMA.COLUMNS 
					WHERE 
						TABLE_CATALOG = '{0}' 
					AND 
						TABLE_SCHEMA = '{1}' 
					AND 
						TABLE_NAME = '{2}'""".format(table.db_catalog, table.db_schema, table.tablename))
				retval = cursor.fetchone()[0]
		return retval

	def getDataForTable(self, table=None, verbose=False):
		if table is None:
			return

		retval = []
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.db_catalog) as conn:
			with conn.cursor(as_dict=True) as cursor:
				query = """
					SELECT 
						*
					FROM 
						[{0}].[{1}]
					""".format(table.db_catalog, table.tablename)

				if verbose:
					print(query)
					print('')

				cursor.execute(query)
				retval = [ d for d in cursor.fetchall() ]
		return retval

	def getDataForColumn(self, column=None, verbose=False, distinct=False, order=None):
		if column is None:
			return None

		# print('getting data for: {0}.{1} -- {2} '.format(column.db_schema, column.tablename, column.columnname))

		tdict = {}
		tdict['image'] = ' CAST(CAST([{0}] AS BINARY) AS NVARCHAR(MAX)) '
		tdict['text'] = ' CAST([{0}] AS NVARCHAR(MAX)) '
		tdict['ntext'] = ' CAST([{0}] AS NVARCHAR(MAX)) '

		selectclause = ''
		if distinct:
			selectclause += ' \nDISTINCT '

		if column.datatype in tdict:
			selectclause += tdict[column.datatype].format(column.columnname)
		else:
			selectclause += " [{0}] ".format(column.columnname)

		orderByStr = ''
		if order:

			if column.datatype in tdict:
				orderByStr = 'ORDER BY {0} {1} '.format(tdict[column.datatype].format(column.columnname), order)
			else:
				orderByStr = 'ORDER BY [{0}] {1} '.format(column.columnname, order)

		retval = []
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.db_catalog) as conn:
			with conn.cursor() as cursor:
				query = """
					SELECT 
						{0}
					FROM 
						[{1}].[{2}]
					{3}
					""".format(selectclause, column.db_schema, column.tablename, orderByStr)

				if verbose:
					print(query)
					print('')

				cursor.execute(query)
				retval = [ d[0] for d in cursor.fetchall() ]

		return retval;

	def getTables(self):
		retval = []
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT 
						TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
					FROM
						INFORMATION_SCHEMA.TABLES 
					WHERE
						TABLE_CATALOG = '{0}' AND TABLE_TYPE = 'BASE TABLE' 
				""".format(self.db_catalog))
				retval = [ Table(db_catalog=d[0], db_schema=d[1], tablename=d[2]) for d in cursor.fetchall() ]
		return retval

	def getColumns(self, table=None):
		retval = []
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.db_catalog) as conn:
			with conn.cursor() as cursor:
				query="""
					SELECT
						T.TABLE_CATALOG, C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME, C.DATA_TYPE, C.ORDINAL_POSITION
					FROM
						INFORMATION_SCHEMA.COLUMNS C
					JOIN
						INFORMATION_SCHEMA.TABLES T
					ON
						C.TABLE_CATALOG = T.TABLE_CATALOG and C.TABLE_SCHEMA = T.TABLE_SCHEMA and C.TABLE_NAME = T.TABLE_NAME
					WHERE
						T.TABLE_CATALOG = %s AND T.TABLE_TYPE = 'BASE TABLE'
						{0}
					"""
				if table == None:
					query=query.format("")
					cursor.execute(query, (self.db_catalog))
				else:
					query=query.format("AND C.TABLE_NAME = %s")
					cursor.execute(query, (self.db_catalog, table.tablename))
				retval = [ Column(db_catalog=d[0], db_schema=d[1], tablename=d[2], columnname=d[3], datatype=d[4], ordinal_position=d[5]) for d in cursor.fetchall() ]
		return retval

	def getPrimaryKeys(self, columnseparator='|'):
		retval = []
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					with cte1 as (
						SELECT
							Tab.TABLE_CATALOG,
							Tab.TABLE_SCHEMA,
							Tab.TABLE_NAME,
							Col.CONSTRAINT_NAME,
							Col.COLUMN_NAME 
						FROM 
							INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab, 
							INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col 
						WHERE 
							Col.Constraint_Name = Tab.Constraint_Name
							AND Col.Table_Name = Tab.Table_Name
							AND Constraint_Type = 'PRIMARY KEY'
					)    

					SELECT
						G.TABLE_CATALOG,
						G.TABLE_SCHEMA,
						G.TABLE_NAME,
						G.CONSTRAINT_NAME,
						stuff(
						(
							SELECT cast('{0}' as varchar(max)) + U.COLUMN_NAME
							FROM cte1 U
							WHERE U.CONSTRAINT_NAME = G.CONSTRAINT_NAME and U.TABLE_CATALOG = G.TABLE_CATALOG and U.TABLE_SCHEMA = G.TABLE_SCHEMA and U.TABLE_NAME = G.TABLE_NAME
							for xml path('')
						), 1, 1, '') AS PKCOLS
					FROM
						cte1 G
					WHERE
						G.TABLE_CATALOG = '{1}'
					GROUP BY
						G.CONSTRAINT_NAME, G.TABLE_CATALOG, G.TABLE_SCHEMA, G.TABLE_NAME
				""".format(columnseparator, self.db_catalog))
				retval = [ PrimaryKey(db_catalog=d[0], db_schema=d[1], tablename=d[2], keyname=d[3], db_columns=d[4].split(columnseparator), type='explicit') for d in cursor.fetchall() ]
		return retval

	def getForeignKeys(self, table=None, columnseparator='|'):
		retval = []
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.db_catalog) as conn:
			with conn.cursor() as cursor:
				query="""
					with cte1 as (
						SELECT
							obj.name AS FK_NAME,
							fk_schema.name AS [schema],
							ref_schema.name AS [referenced_schema],
							fk_table.name AS [table],
							fk_col.name AS [column],
							ref_table.name AS [referenced_table],
							ref_col.name AS [referenced_column]
							-- the foreign key from schema.table.column refers to referenced_schema.referenced_table.referenced_column
							-- the referenced colum(s) usually is the primary key on that table, but should at least be unique
						FROM sys.foreign_key_columns fkc
						INNER JOIN sys.objects obj
							ON obj.object_id = fkc.constraint_object_id
						INNER JOIN sys.tables fk_table  -- the table having the foreign key constraint
							ON fk_table.object_id = fkc.parent_object_id
						INNER JOIN sys.tables ref_table  -- the table being referenced by the foreign key constraint
							ON ref_table.object_id = fkc.referenced_object_id
						INNER JOIN sys.schemas fk_schema
							ON fk_table.schema_id = fk_schema.schema_id
						INNER JOIN sys.schemas ref_schema
							ON ref_table.schema_id = ref_schema.schema_id
						INNER JOIN sys.columns fk_col
							ON fk_col.column_id = parent_column_id AND fk_col.object_id = fk_table.object_id
						INNER JOIN sys.columns ref_col
							ON ref_col.column_id = referenced_column_id AND ref_col.object_id = ref_table.object_id
						{0}
					)

					SELECT
						G.[schema],
						G.[referenced_schema],
						G.[table],  -- the table having a foreign key constraint (the from)
						G.[referenced_table],  --the table with a primary key (being pointed to by foreign key constraint) (the to)
						G.FK_NAME,
						stuff(
						(
							select cast(%s as varchar(max)) + U.[column]
							from cte1 U
							WHERE U.FK_NAME = G.FK_NAME
							for xml path('')
						), 1, 1, '') AS fk_columns, -- the concatenation of all columns that together uniquely refer to a row in ref_table
						stuff(
						(
							select cast(%s as varchar(max)) + U.[referenced_column]
							from cte1 U
							WHERE U.FK_NAME = G.FK_NAME
							for xml path('')
						), 1, 1, '') AS ref_columns -- the concatenation of all columns that together unique define how rows in ref_table should be referred to
					FROM
						cte1 G
					GROUP BY
						G.FK_NAME, G.[schema], G.referenced_schema, G.[table], G.referenced_table
						"""
				if table==None:
					query=query.format("")
					cursor.execute(query, (columnseparator, columnseparator))
				else:
					query=query.format("WHERE fk_table.name=%s AND fk_schema.name=%s")
					cursor.execute(query, (table.tablename, table.db_schema, columnseparator, columnseparator))
				retval = [ ForeignKey(db_catalog=self.db_catalog,
									  schema=d[0],
									  ref_schema=d[1],
									  tablename=d[2],
									  ref_tablename=d[3],
									  keyname=d[4],
									  columns=d[5].split(columnseparator),
									  ref_columns=d[6].split(columnseparator), type='explicit'
				) for d in cursor.fetchall() ]
		return retval

	def getQueryForFlatTable(self, table):
		query_select = ["SELECT {0}.*".format(table.tablename)]
		query_from = ["FROM {0} as {1}".format(table.__str__(), table.tablename)]
		fks = self.getForeignKeys(table)
		for fk in fks:
			#TODO: only works for single columns
			ref_alias = "{0}_{1}".format(fk.columns, fk.ref_tablename)
			query_select.extend(["{0}.{1} AS {2}_{1}".format(ref_alias, c.columnname, fk.columns)
								 for c in self.getColumns(fk.reftable())])
			query_from.extend(["LEFT JOIN {0} AS {1} ON {2}.{3}={1}.{4}".format(fk.reftable().__str__(), ref_alias,
																				fk.tablename, fk.columns,
																				fk.ref_columns)])
		query="{0}\r\n{1}".format(",\r\n       ".join(query_select),
								"\r\n     ".join(query_from))
		return query

	def executefetch(self, query, *args):
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute(query, args)
				retval = [ d for d in cursor.fetchall() ]
		return retval

	def execute(self, query, *args):
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute(query, args)
			conn.commit()

	def getQueryForView(self, view_name, query):
		return "CREATE VIEW {0} AS \r\n{1};".format(view_name, query)