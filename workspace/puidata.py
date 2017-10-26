from __future__ import print_function
import io
import os
import sys
import glob
import zipfile

import pandas as pd
try:
	import geopandas as gpd
except:
	print("Geopandas can't be loaded. shpLoader is not available.")

from abc import ABCMeta, abstractmethod
from collections import OrderedDict as odict

# import requests
try:
	import urllib2 as urllib
except ImportError:
	import urllib.request as urllib

'''

# Usage

## CSV
df = csvLoader.load(
	url='http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv',
	filename='API_SP.POP.TOTL_DS2_en_csv_v2.csv', is_zip=True, skiprows=3
).df


## Excel
dl = xlsxLoader.load(
	url='Team assignments and Weekly Innovation Update group (1).xlsx', filename='vlah.xlsx'
).save_cache()


## Shapefile
df = shpLoader.load(
	url='https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/mn_mappluto_16v2.zip', filename='MNMapPLUTO.shp'
).df

## Custom Loader

@customLoader.parser
def loader(file):
	text = file.read().decode('utf-8')
	# Can return:
	#	string - is read by pd.read_csv( ... )
	#	file-like - also fed to pd.read_csv( ... )
	#	list/dict/odict - gets fed to pd.DataFrame( ... )
	#	dataframe - taken as is
	return file

# function works the same way as csvLoader.load( ... )
df = loader(
	url='https://github.com/bensteers/PUI2017_bs3639/raw/master/HW5_bs3639/data-pvLFI.csv'
).df

# Alternative Syntax

### This is identical to the first csv example.
df = csvLoader(filename='API_SP.POP.TOTL_DS2_en_csv_v2.csv').from_cache().download(
	'http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv', is_zip=True
).save_cache().df

### This syntax makes it more extensible. For example, you could check the cache, download if it's not there, but not save it, you could do:
df = csvLoader(filename='API_SP.POP.TOTL_DS2_en_csv_v2.csv').from_cache().download(
	'http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv', is_zip=True
).df

### To download the data from source everytime, just do:
df = csvLoader(filename='API_SP.POP.TOTL_DS2_en_csv_v2.csv').download(
	'http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv', is_zip=True
).df

'''



'''
TODO:

Loaders to add:
	GeoJSON



'''



# Abstract class
class BaseLoader(object):
	__metaclass__ = ABCMeta

	envvar = 'PUIDATA'
	default_dir = './'
	extension = ''
	directory = os.getenv(envvar, default_dir)
	basename = ''
	filename = ''
	url = ''

	def __init__(self, filename=None, url=None, df=None, **kw):
		'''
		Arguments:
			url (str, optional): The url to pull from. Can be specified in later.
			filename (str, optional): The filename to save the cache file to. Also used to select a file from a zip archive
			df (pd.DataFrame, optional): Initialize with a dataframe
			**kw: can use - basename, extension, envvar, default_dir
		'''
		self.setup(filename=filename, url=url, **kw)
		self.clear_df().set_df(df)
		self.directory = os.getenv(self.envvar, self.default_dir)


	# Functions you need to implement

	@abstractmethod
	def read(self, *a, **kw):
		'''Loads the data from file'''
		# self.df = pd.DataFrame( ... )
		pass

	@abstractmethod
	def save(self, *a, **kw):
		'''Saves the data to file'''
		# self.df.to_whatever( ... )
		pass


	# Basic user interface

	def download(self, url=None, filename=None, is_zip=False, ith=0, **kw):
		'''Load data from url
		Assigns the dataframe to `self.df`

		Arguments:
			url (str): The url to get the csv from. Can be remote or local
			filename (str): The filename to get from the zip file. If a previous filename was not specified,
				it will be used for saving the cache file too.
			is_zip (bool): Whether the file is in a zip archive
			ith (int): If a filename is not specified and it's a zip archive, the `ith` file will be selected from the archive.
			**kw: Arguments to pass to `pd.read_csv` or whatever loader

		Returns self (chainable)
		'''
		if self.has_df(): # return from cache if it exists
			return self

		socket = self.open_socket(url, filename, is_zip, ith)
		self.read(socket, **kw)
		return self

	def from_cache(self, filename=None, **kw):
		'''Load file from cache. Assumed that it exists

		Arguments:
			filename (str): The filename to look for in the data directory.
			**kw: arguments to pass to the read function. For csv this is `read_csv`, for xlsx, `pd.read_excel`, etc.
		Returns self (chainable)
		'''
		if self.is_cached(filename):
			print('Loaded from cache:', self.local_file(filename or self.filename))
			self.read(self.local_file(filename or self.filename), **kw)
		return self

	def save_cache(self, filename=None, overwrite=False, **kw):
		'''save file to PUIDATA directory

		Arguments:
			filename (str): The filename to look for in the data directory.
			overwrite (bool): Whether or not to save if it already exists.
			**kw: arguments to pass to the read function. For csv this is `read_csv`, for xlsx, `pd.read_excel`, etc.
		Returns self (chainable)
		'''
		if (overwrite or not self.is_cached()) and self.has_df():
			print('Saving to cache:', self.local_file(filename or self.filename))
			BaseLoader.ensure_directory(self, self.local_file())
			self.save(self.local_file(filename or self.filename), **kw)
		return self


	# convenience functions :D

	@classmethod
	def load(clas, filename=None, url=None, *a, **kw):
		'''Runs cache load, but defines url/filename up front. This allows you
			to omit the filename if you want to get it from the url.

		See self.download(...) for kw arguments.

		Usage:
		df = csvLoader.load(url='http://website.com/data.csv').df
		# cached as data.csv
		'''
		return clas(filename=filename, url=url)(*a, **kw)

	def __call__(self, filename=None, url=None, *a, **kw):
		'''Works the same way as load, but works on an already instantiated loader.
			Primarily exists to expose .load(...) to customLoader.

		Example:
		@customLoader.parser
		def loader(file):
			return file
		df = loader(url=url).df
		'''
		return self.setup(filename=filename, url=url).cached_load(*a, **kw)

	def cached_load(self, *a, **kw):
		'''Helper to:
			* check cache and load from there if it exists,
			* If not, download from url
			* Save to cache if it doesn't already exist

		See self.download(...) for arguments.

		Usage:
		df = csvLoader(filename).cached_load(url).df
		'''
		return self.from_cache().download(*a, **kw).save_cache()


	# Caching utilities

	def local_file(self, filename=''):
		'''Get the path for a cached file'''
		return os.path.join(self.directory, self.basename, filename or '')

	def is_cached(self, filename=None):
		'''Check if a cached file exists'''
		return os.path.isfile(self.local_file(filename or self.filename))



	# File loaders

	def open_file(self, url=None, as_b=False):
		'''Create a file buffer from a url or path'''
		self.url = url or self.url or self.local_file(self.filename)
		try: # assume is url
			socket = urllib.urlopen(self.url)
			# socket = requests.get(self.url)
			# socket = urllib.urlopen(urllib.Request(self.url, headers={ 'User-Agent': 'Mozilla/5.0' }))
		except ValueError:#ValueError: # is local
			socket = open(self.url, 'rb' if as_b else 'r')
		return socket

	def open_zip(self, url):
		'''Open a zip archive as a zipfile object'''
		return zipfile.ZipFile(io.BytesIO( self.open_file(url, as_b=True).read() ))

	def open_socket(self, url=None, filename=None, is_zip=False, ith=0, as_b=False):
		'''Create a file buffer from a url or path, expanding a zip if requested'''
		# Load from zipfile
		if is_zip:
			z = self.open_zip(url)
			filename = filename or self.filename # default to previously assigned filename
			filename = filename if filename in z.namelist() else z.namelist()[ith] # default to ith if filename not in zip
			self.filename = self.filename or filename # set default filename
			socket = z.open(filename)
		# Load file
		else:
			self.filename = self.filename or filename or os.path.basename(url)
			socket = self.open_file(url, as_b=as_b)
		return socket



	# DataFrame getters/setters - for convenience

	def has_df(self):
		'''Whether or not there is a df or a list of dfs available'''
		return len(self.dfs) or self.df is not None

	def clear_df(self):
		'''Clear the loader's dataframe.'''
		self.df, self.dfs = None, []
		return self

	def get_df(self):
		'''Get df or multiple dfs'''
		if self.df is not None:
			return self.df
		elif len(self.dfs):
			return self.dfs
		return None

	def set_df(self, df):
		'''Assign df or multiple dfs'''
		if isinstance(df, pd.DataFrame):
			self.df = df
		elif hasattr(df, '__getitem__'):
			self.dfs = df # Assumed to be an ordered dict, dict, or list.
		return self

	def setup(self, url=None, filename=None, ext=None, **kw):
		'''Assign class properties'''
		self.url = url or self.url
		self.filename = filename or self.filename
		self.extension = ext or self.extension
		self.__dict__.update(kw) # update any other properties that people want

		# Set default filename to url basename
		if not self.filename and self.url:
			parts = os.path.splitext(os.path.basename(self.url))
			self.extension = self.extension or parts[1]
			self.filename = parts[0] + self.extension
		return self

	def to(self, cls):
		'''Convert one dataloader to another.
		Useful if you want to save in a different format. (e.g. xlsx sheet to csv)

		Arguments:
			cls (str or BaseLoader subclass): pass either actual class, or string
				of the class name minus the 'Loader', (i.e. for csvLoader use 'csv')

		xlsxLoader(
			url=url, filename=filename
		).from_cache().load_sheet('Sheet 1').to('csv').save_cache()
		'''
		if isinstance(cls, str):
			subclasses = {c.__name__: c for c in self.__class__.__subclasses__()}
			cls = subclasses.get(cls + 'Loader')
		if issubclass(cls, BaseLoader):
			return cls(filename=self.filename, url=self.url, df=self.get_df())
		else:
			return None



	# Extra methods

	def ensure_directory(self, outdir):
		'''Helper to create a directory if it doesn't already exist'''
		if outdir and not os.path.isdir(outdir):
			os.mkdir(outdir)


	@classmethod
	def list_cache(cls, subdir='', ext=None, full_path=False):
		'''Lists files in cache. i.e. lists PUIDATA directory'''
		directory = os.getenv(cls.envvar, cls.default_dir)
		files = glob.glob(os.path.join(
			directory, subdir or '', '*' + (ext if ext is not None else cls.extension)
		))
		return files if full_path else [os.path.relpath(f, directory) for f in files]


	def __str__(self):
		return '<{} ({}) from {}. Note: access df via loader.df or dfs via loader.dfs.>'.format(
			self.__class__.__name__,
			self.filename or '-na-',
			self.url or '-na-',
		)





class csvLoader(BaseLoader):
	extension = '.csv'
	'''
	# All equivalent:

	df = csvLoader.load(
		filename='asdfasdfdata-pvLFI.csv', url='https://github.com/bensteers/PUI2017_bs3639/raw/master/HW5_bs3639/data-pvLFI.csv'
	).df

	#
	df = csvLoader(
		filename='asdfasdfdata-pvLFI.csv'
	).cached_load(url='https://github.com/bensteers/PUI2017_bs3639/raw/master/HW5_bs3639/data-pvLFI.csv').df

	# or
	df = csvLoader(
		url='https://github.com/bensteers/PUI2017_bs3639/raw/master/HW5_bs3639/data-pvLFI.csv', filename='asdfasdfdata-pvLFI.csv'
	).cached_load().df

	# or
	dl = csvLoader()
	df = dl(
		filename='asdfasdfdata-pvLFI.csv', url='https://github.com/bensteers/PUI2017_bs3639/raw/master/HW5_bs3639/data-pvLFI.csv'
	).df

	# or
	df = csvLoader(
		url='http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv', filename='API_SP.POP.TOTL_DS2_en_csv_v2.csv'
	).from_cache().download(is_zip=True, skiprows=3).save_cache().df
	'''

	def read(self, file, **kw):
		'''Loads dataframe from file or file-like object'''
		self.df = pd.read_csv(file, **kw)


	def save(self, file, **kw):
		'''Saves file to location'''
		self.df.to_csv(file, index=False, **kw)



class xlsxLoader(BaseLoader):
	extension = '.xlsx'

	def __init__(self, filename=None, url=None, sheets=None):
		BaseLoader.__init__(self, filename, url)
		self.sheets = sheets


	def download(self, url=None, filename=None, is_zip=False, ith=0, sheets=None, **kw):
		'''Load xlsx from either url or file
		Assigns an ordered dict of dataframes to `self.dfs`

		Arguments:
			url (str): The url to get the csv from. Can be remote or local
			filename (str): The filename to get from the zip file. If a previous filename was not specified,
				it will be used for saving the cache file too.
			is_zip (bool): Whether the file is in a zip archive
			ith (int): If a filename is not specified and it's a zip archive, the `ith` file will be selected from the archive.
			sheets (list): The sheets to get from the excel file. By default, it gets all of them.
			**kw: Arguments to pass to `pd.read_csv`

		Returns self (chainable)
		'''
		if self.has_df(): # return from cache if it exists
			return self

		socket = self.open_socket(url, filename, is_zip=is_zip, ith=ith, as_b=True)
		self.read(socket, sheets=sheets, **kw)
		return self

	def load_sheet(self, sheet=None, i=None, *a, **kw):
		'''Helper function to load a specific sheet from an excel file
			Arguments:
				Specify either:
					sheet (str): name of the sheet
					i (int): the index of the sheet
				*a, **kw: arguments to pass to .download(...)

		'''
		self.download(*a, **kw)

		# Support numerical indexing too - if sheet not specified/doesn't exist
		if i is not None and not (sheet and self.dfs.get(sheet)):
			sheet = list(self.dfs.keys())[i]
		# Get sheet if it exists
		if sheet and self.dfs.get(sheet):
			self.df = self.dfs.get(sheet)
			self.sheet_name = sheet
		return self

	def read(self, file, sheets=None, **kw):
		'''Read xlsx file'''
		reader = pd.ExcelFile(file)
		sheets = sheets or self.sheets or reader.sheet_names
		self.dfs = odict([(sh, pd.read_excel(reader, sheetname=sh, **kw)) for sh in sheets])

	def save(self, file, **kw):
		'''Write xlsx file'''
		writer = pd.ExcelWriter(file)
		for name, df in self.dfs.items():
			df.to_excel(writer, name, index=False, **kw)
		writer.save()



class shpLoader(BaseLoader):
	extension = '.shp'

	_basename = ''

	def __init__(self, *a, **kw):
		# Only allow class if geopandas is loaded
		if 'geopandas' not in sys.modules:
			raise ImportError('shpLoader depends on geopandas, which could not be loaded.')
		super(shpLoader, self).__init__(*a, **kw)


	def download(self, url=None, filename=None, **kw):
		'''Load xlsx from either url or file
		Assigns an ordered dict of dataframes to `self.dfs`

		Arguments:
			url (str): The url to get the csv from. Can be remote or local
			filename (str): The filename to get from the zip file. If a previous filename was not specified,
				it will be used for saving the cache file too. The file will be stored in
			**kw: Arguments to pass to `pd.read_csv`

		Returns self (chainable)
		'''
		if self.has_df(): # return from cache if it exists
			return self

		z = self.open_zip(url)
		z.extractall(self.local_file())
		self.read(self.local_file(filename or self.filename), **kw)
		return self

	def read(self, file, **kw):
		'''Load from file-like object'''
		self.df = gpd.GeoDataFrame.from_file(file, **kw)

	def save(self, file, **kw):
		'''Simplistic implementation. There could be errors with CRS and I'm not
		sure how it works with the auxilliary shapefile data (.dbf, .prj, .shx, ...).'''
		self.df.to_file(file, driver='ESRI Shapefile')


	def cached_load(self, *a, **kw):
		'''Helper to load csv checking and saving to cache. See `from_csv`'''
		return self.from_cache().download(*a, **kw)

	def setup(self, *a, **kw):
		'''Set class properties - add basename as well'''
		super(shpLoader, self).setup(*a, **kw)
		self.basename = self.basename or (
			os.path.splitext(os.path.basename(self.url))[0] if self.url else '')
		return self



class customLoader(csvLoader):
	extension = ''
	_parser = lambda file: file

	@classmethod
	def parser(cls, func):
		'''Enables custom parsing of a file'''
		instance = cls()
		instance._parser = func
		return instance


	def download(self, url=None, filename=None, is_zip=False, ith=0, **kw):
		if self.has_df(): # return from cache if it exists
			return self

		socket = self.open_socket(url, filename, is_zip)
		result = self._parser(socket) # run custom parser

		# Convert result to dataframe
		if isinstance(result, pd.DataFrame):
			df = result # already dataframe
		elif hasattr(result, 'read'):
			df = self.read(result) # is file-like
		elif isinstance(result, str):
			df = self.read(io.StringIO(result)) # is a string
		elif isinstance(result, [dict, list, odict]):
			df = pd.DataFrame(result) # is some form that DataFrame can take
		else:
			raise TypeError('Returned object could not be converted to dataframe.')
		self.df = df
		return self



if __name__ == '__main__':

	if sys.argv[1] == 'list':
		def disp_cache_list(dl, subdir=''):
			print('{}: {}'.format(dl.extension, os.path.join(subdir or '', '*' + dl.extension)))
			for f in dl.list_cache(subdir):
				print('  ', f)

		disp_cache_list(csvLoader)
		disp_cache_list(xlsxLoader)
		disp_cache_list(shpLoader, '*')


	if sys.argv[1] == 'test':
		# Running tests on assignment 5 data

		BaseLoader.default_dir = './test-' + BaseLoader.envvar # save to data folder
		BaseLoader.envvar = 'adkfasdkjfhkdsjfhasfasfdasdf44444' # force to go into current directory (unless you have a variable named this...)


		try:
			print('Testing shapefile...')
			dl = shpLoader.load(
				url='https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/mn_mappluto_16v2.zip', filename='MNMapPLUTO.shp'
			)

			print(dl.has_df(), len(dl.df), dl.df.columns[:3].values)
		except ImportError:
			print("Couldn't test shpLoader as geopandas is not installed.")

		print('Testing zipped csv...')
		dl = csvLoader(
			url='http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv', filename='API_SP.POP.TOTL_DS2_en_csv_v2.csv'
		).from_cache().download(is_zip=True, skiprows=3).save_cache()

		print(dl.has_df(), len(dl.df), dl.df.columns[:3].values)

		print('Testing zipped csv preferred syntax...')
		dl = csvLoader.load(
			url='http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv',
			filename='API_SP.POP.TOTL_DS2_en_csv_v2.csv', is_zip=True, skiprows=3
		)

		print(dl.has_df(), len(dl.df), dl.df.columns[:3].values)

		print('Testing csv...')
		dl = csvLoader(
			url='https://github.com/bensteers/PUI2017_bs3639/raw/master/HW5_bs3639/data-pvLFI.csv'
		).from_cache().download().save_cache()

		print(dl.has_df(), len(dl.df), dl.df.columns[:3].values)

		print('Testing csv (custom filename)...')
		dl = csvLoader().cached_load(
			url='https://github.com/bensteers/PUI2017_bs3639/raw/master/HW5_bs3639/data-pvLFI.csv', filename='asdfasdfdata-pvLFI.csv'
		)

		print(dl.has_df(), len(dl.df), dl.df.columns[:3].values)

		print('Testing local zipped csv...')
		dl = csvLoader(filename='balhah.csv').from_cache().download(
			'World firearms murders and ownership - Sheet 1.zip', is_zip=True
		).save_cache()

		print(dl.has_df(), len(dl.df), dl.df.columns[:3].values)

		print('Testing xlsx...')
		dl = xlsxLoader(filename='vlah.xlsx').from_cache().download(
			'Team assignments and Weekly Innovation Update group (1).xlsx'
		).save_cache()

		print(dl.has_df(), len(dl.dfs), list(dl.dfs.values())[0].columns[:3].values)


		print('Testing custom parser...')
		@customLoader.parser
		def load(file):
			# Stupid example of duplicating the last row in a csv
			lines = file.read().decode('utf-8').split('\n')
			lines += lines[-1]
			return '\n'.join(lines)

		# Like calling
		dl = load(
			url='https://github.com/bensteers/PUI2017_bs3639/raw/master/HW5_bs3639/data-pvLFI.csv'
		)

		print(dl.has_df(), len(dl.df), dl.df.columns[:3].values)
