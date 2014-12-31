from sqlalchemy import Column, Integer, String, Float, DateTime, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import event
import datetime
import math

Base = declarative_base()

multistring_separator = '|' # used to combine multi column keys in fields

def validate_float(value):
	if isinstance(value, float):
		if math.isnan(value):
			value = None
	elif value is None:
		pass
	else:
		print('float property set to non-float value <{0}>; defaulting value to <None>'.format(value))
		value = None
	return value

def validate_int(value):
	if isinstance(value, int):
		pass
	elif value is None:
		pass
	else:
		print('int property set to non-int value <{0}>; defaulting value to <None>'.format(value))
		value = None
	return value

def validate_string(value):
	if isinstance(value, str):
		pass
	elif value is None:
		pass
	else:
		print('str property set to non-str value <{0}>; defaulting value to <None>'.format(value))
		value = None
	return value

def validate_datetime(value):
	if isinstance(value, datetime.datetime):
		pass
	elif value is None:
		pass
	else:
		print('datetime.datetime property set to non-datetime.datetime value <{0}>; defaulting value to <None>'.format(value))
		value = None
	return value

validators = {
	Integer:validate_int,
	String:validate_string,
	DateTime:validate_datetime,
	Float:validate_float
}

# this event is called whenever an attribute
# on a class is instrumented
@event.listens_for(Base, 'attribute_instrument')
def configure_listener(class_, key, inst):
	if not hasattr(inst.property, 'columns'):
		return
	# this event is called whenever a "set" 
	# occurs on that instrumented attribute
	@event.listens_for(inst, "set", retval=True)
	def set_(instance, value, oldvalue, initiator):
		validator = validators.get(inst.property.columns[0].type.__class__)
		if validator:
			return validator(value)
		else:
			return value

class PrimaryKey(Base):
	__tablename__ = 'mprimarykey'

	id = Column(Integer, primary_key=True, autoincrement=True)
	db_catalog = Column(String(512))
	db_schema = Column(String(512))
	tablename = Column(String(512))
	db_columns = Column(String(512))
	keyname = Column(String(512))
	type = Column(String(512))
	score = Column(Float)
	comment = Column(String(2048))
	tags = Column(String(2048))
	date_added = Column(DateTime)

	def __init__(self, db_catalog='', db_schema='', tablename='', db_columns=[], keyname='', type='explicit'):
		self.db_catalog = db_catalog
		self.db_schema = db_schema
		self.tablename = tablename
		self.db_columns = multistring_separator.join(db_columns) # can be single...
		self.keyname = keyname
		self.type = type
		self.date_added = datetime.datetime.now()

	def __str__(self):
		return "{0}.{1}.{2:100}{3}".format(self.db_catalog, self.db_schema, self.tablename, self.keyname)

class ForeignKey(Base):
	__tablename__ = 'mforeignkey'

	id = Column(Integer, primary_key=True, autoincrement=True)
	db_catalog = Column(String(512))
	db_schema = Column(String(512, convert_unicode=True))
	ref_db_schema = Column(String(512, convert_unicode=True))
	tablename = Column(String(512, convert_unicode=True))
	ref_tablename = Column(String(512, convert_unicode=True))
	columns = Column(String(512, convert_unicode=True))
	ref_columns = Column(String(512, convert_unicode=True))
	keyname = Column(String(512, convert_unicode=True))
	type = Column(String(512, convert_unicode=True))
	score = Column(Float)
	comment = Column(String(2048, convert_unicode=True))
	tags = Column(String(2048, convert_unicode=True))
	date_added = Column(DateTime)

	def __init__(self, db_catalog='', schema='', ref_schema='', tablename='', ref_tablename='', columns=[], ref_columns=[], keyname='', type='explicit'):
		self.db_catalog = db_catalog.__str__()
		self.db_schema = schema.__str__()
		self.ref_db_schema = ref_schema.__str__()
		self.tablename = tablename.__str__()
		self.ref_tablename = ref_tablename.__str__()
		self.columns = multistring_separator.join(columns).__str__() # can be single...
		self.ref_columns = multistring_separator.join(ref_columns).__str__() # can be single...
		self.keyname = keyname.__str__()
		self.type = type.__str__()
		self.date_added = datetime.datetime.now()

	def table(self):
		return Table(self.db_catalog, self.db_schema, self.tablename)

	def reftable(self):
		return Table(self.db_catalog, self.ref_db_schema, self.ref_tablename)

	def __str__(self):
		return "{0} {1:75} -->  {2:75}".format(self.db_catalog,
											   "{0}.{1}.{2}".format(self.db_schema, self.tablename, self.columns),
											   "{0}.{1}.{2}".format(self.ref_db_schema, self.ref_tablename, self.ref_columns))

class Table(Base):
	__tablename__ = 'mtable'

	id = Column(Integer, primary_key=True, autoincrement=True)
	db_catalog = Column(String(512))
	db_schema = Column(String(512))
	tablename = Column(String(512))
	num_rows = Column(Integer)
	num_columns = Column(Integer)
	num_explicit_inlinks = Column(Integer)
	num_explicit_outlinks = Column(Integer)
	num_implicit_outlinks = Column(Integer)
	num_implicit_inlinks = Column(Integer)
	num_emptycolumns = Column(Integer)
	tablefillscore = Column(Float)
	muts_per_day = Column(Float) # TBD: FB specific
	lifetime_in_days = Column(Integer) # TBD: FB specific
	first_modified = Column(DateTime) # TBD: FB specific
	last_modified = Column(DateTime) # TBD: FB specific
	comment = Column(String(2048))
	tags = Column(String(2048))
	date_added = Column(DateTime)

	def __init__(self, db_catalog='', db_schema='', tablename=''):
		self.db_catalog = db_catalog.__str__()
		self.db_schema = db_schema.__str__()
		self.tablename = tablename.__str__()

		self.num_rows = None
		self.num_columns = None

		self.date_added = datetime.datetime.now()

	def full_name(self):
		"""The fully qualified name (including catalog, db_schema and tablename)"""
		return ".".join(["[{}]".format(name) for name in [self.db_catalog,
						 self.db_schema,
						 self.tablename] if len(name) > 0 ])

	def __str__(self):
		return self.full_name()

class Column(Base):
	__tablename__ = 'mcolumn'

	id = Column(Integer, primary_key=True, autoincrement=True)
	db_catalog = Column(String(512))
	db_schema = Column(String(512))
	tablename = Column(String(512))
	columnname = Column(String(512))
	ordinal_position = Column(Integer)
	datatype = Column(String(512))
	num_nulls = Column(Integer)
	num_distinct_values = Column(Integer)
	
	# NUMERIC
	min = Column(Float)
	max = Column(Float)
	avg = Column(Float)
	stdev = Column(Float)
	variance = Column(Float)
	sum = Column(Float)
	median = Column(Float)
	mode = Column(Float)
	quantile0 = Column(String(512))
	quantile1 = Column(String(512))
	quantile2 = Column(String(512))
	quantile3 = Column(String(512))
	quantile4 = Column(String(512))
	num_negative = Column(Integer)
	num_positive = Column(Integer)
	num_zero = Column(Integer)

	# DATETIME
	start_date = Column(DateTime)
	end_date = Column(DateTime)
	lifespan_in_days = Column(Integer)
	workdays = Column(Integer)
	weekends = Column(Integer)
	holidays = Column(Integer)
	quarter1 = Column(Integer)
	quarter2 = Column(Integer)
	quarter3 = Column(Integer)
	quarter4 = Column(Integer)
	m1 = Column(Integer) # January
	m2 = Column(Integer)
	m3 = Column(Integer)
	m4 = Column(Integer)
	m5 = Column(Integer)
	m6 = Column(Integer)
	m7 = Column(Integer)
	m8 = Column(Integer)
	m9 = Column(Integer)
	m10 = Column(Integer)
	m11 = Column(Integer)
	m12 = Column(Integer)
	d1 = Column(Integer) # Monday
	d2 = Column(Integer)
	d3 = Column(Integer)
	d4 = Column(Integer)
	d5 = Column(Integer)
	d6 = Column(Integer)
	d7 = Column(Integer)

	# TEXT
	word_frequency = Column(String(2048))

	comment = Column(String(2048))
	tags = Column(String(2048))
	date_added = Column(DateTime)

	def __init__(self, db_catalog='', db_schema='', tablename='', columnname='', datatype=None, ordinal_position=None):
		self.db_catalog = db_catalog.__str__()
		self.db_schema = db_schema.__str__()
		self.tablename = tablename.__str__()
		self.columnname = columnname.__str__()
		self.datatype = datatype.__str__()
		self.ordinal_position = ordinal_position
		self.date_added = datetime.datetime.now()

		self.num_nulls = None
		self.num_distinct_values = None
		self.min = None
		self.max = None
		self.avg = None

	def __str__(self):
		return "{0}.{1}.{2:100}{3}".format(self.db_catalog, self.db_schema, self.tablename, self.columnname)