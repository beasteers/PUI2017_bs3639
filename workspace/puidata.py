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

>Note: shpLoader doesn't have a save method because I haven't implemented saving a shapefile from a dataframe (I'm sure it's pretty simple),
so calling save_cache() doesn't currently do anything. The file is still saved to cache as geopandas (or fiona) needs to load from a file
via a passed filename, not a file-like object.



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
	Shapefile save

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

	def __init__(self, filename=None, url=None, df=None):
		'''
		Arguments:
			url (str, optional): The url to pull from. Can be specified in later.
			filename (str, optional): The filename to save the cache file to. Also used to select a file from a zip archive
			sheets (list, optional): A list of sheet names to get when calling `from_xlsx`
		'''
		self.clear()
		self.url = url
		self.set_df(df)
		self.filename = filename
		self.directory = os.getenv(self.envvar, self.default_dir)
		if not filename and url:
			parts = os.path.splitext(os.path.basename(url))
			self.extension = self.extension or parts[1]
			self.filename = parts[0] + self.extension



	@abstractmethod
	def download(self, url=None, filename=None, is_zip=False, ith=0, **kw):
		'''Loads data from url'''
		return self

	@abstractmethod
	def read(self, *a, **kw):
		'''Loads the data from file'''
		pass

	@abstractmethod
	def save(self, *a, **kw):
		'''Saves the data to file'''
		pass


	def from_cache(self, filename=None, **kw):
		'''Load file from cache. Assumed that it exists

		Arguments:
			filename (str): The filename to look for in the data directory.
			**kw: arguments to pass to the read function. For csv this is `read_csv`, for xlsx, `pd.read_excel`, etc.
		Returns self (chainable)
		'''
		if self.is_cached(filename):
			print('Loaded from cache:', self.local_file(filename))
			self.read(self.local_file(filename), **kw)
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
			print('Saving to cache:', self.local_file(filename))
			BaseLoader.ensure_directory(self, filename)
			self.save(self.local_file(filename), **kw)
		return self



	def cached_load(self, *a, **kw):
		'''Helper to load csv checking and saving to cache. See `from_csv`'''
		return self.from_cache().download(*a, **kw).save_cache()

	@classmethod
	def load(cls, filename=None, url=None, *a, **kw):
		'''Factory method'''
		return cls(filename, url).cached_load(*a, **kw)



	# Caching

	def local_file(self, filename=''):
		'''Get the path for a cached file'''
		return os.path.join(self.directory, filename or self.filename or '')

	def is_cached(self, filename=None):
		'''Check if a cached file exists'''
		return os.path.isfile(self.local_file(filename))

	def clear(self):
		'''Clear the loader's dataframe.'''
		self.df, self.dfs = None, odict()
		return self



	# Utilities

	def open_file(self, url=None, as_b=False):
		'''Create a file buffer from a url or path'''
		self.url = url or self.url or self.local_file()
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
			filename = filename or self.filename
			filename = filename if filename in z.namelist() else z.namelist()[ith]
			self.filename = self.filename or filename # set default filename
			socket = z.open(filename)
		# Load file
		else:
			self.filename = filename or self.filename or os.path.basename(url)
			socket = self.open_file(url, as_b=as_b)
		return socket



	# DataFrame getters/setters - for convenience

	def has_df(self):
		'''Whether or not there is a df or a list of dfs available'''
		return len(self.dfs) or self.df is not None

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


	def ensure_directory(self, filename=None):
		'''Helper to create a directory if it doesn't already exist'''
		outdir = os.path.dirname(self.local_file(filename))
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

	df = csvLoader(
		url='http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv', filename='API_SP.POP.TOTL_DS2_en_csv_v2.csv'
	).from_cache().download(is_zip=True, skiprows=3).save_cache().df

	# or
	df = csvLoader(
		url='https://github.com/bensteers/PUI2017_bs3639/raw/master/HW5_bs3639/data-pvLFI.csv', filename='asdfasdfdata-pvLFI.csv'
	).load().df

	# or
	df = csvLoader(
		filename='asdfasdfdata-pvLFI.csv'
	).load(url='https://github.com/bensteers/PUI2017_bs3639/raw/master/HW5_bs3639/data-pvLFI.csv').df


	'''
	def download(self, url=None, filename=None, is_zip=False, ith=0, **kw):
		'''Load csv from either url or file
		Assigns the dataframe to `self.df`

		Arguments:
			url (str): The url to get the csv from. Can be remote or local
			filename (str): The filename to get from the zip file. If a previous filename was not specified,
				it will be used for saving the cache file too.
			is_zip (bool): Whether the file is in a zip archive
			ith (int): If a filename is not specified and it's a zip archive, the `ith` file will be selected from the archive.
			**kw: Arguments to pass to `pd.read_csv`

		Returns self (chainable)
		'''
		if self.df is not None: # return from cache if it exists
			return self

		socket = self.open_socket(url, filename, is_zip)
		self.read(socket, **kw)
		return self

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
		if len(self.dfs): # return from cache if it exists
			return self

		socket = self.open_socket(url, filename, is_zip=is_zip, ith=ith, as_b=True)
		self.read(socket, sheets=sheets, **kw)
		return self

	def load_sheet(self, sheet, *a, **kw):
		'''Helper function to load a specific sheet from an excel file'''
		self.sheet_name = sheet
		self.download(*a, **kw)
		if self.dfs.get(sheet):
			self.df = self.dfs.get(sheet)
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

	def __init__(self, filename=None, url=None, basename=None):
		BaseLoader.__init__(self, filename, url)
		self.basename = basename

	@property
	def filename(self): # Get filename with the zip archive name as the directory
		return os.path.join(self.basename, self._filename)

	@filename.setter
	def filename(self, filename):
		self._filename = filename

	@property
	def basename(self):
		'''Basically the folder that the zip contents are stored to. Defaults to zipfile name.'''
		return self._basename or (os.path.splitext(os.path.basename(self.url))[0] if self.url else '')

	@basename.setter
	def basename(self, basename):
		self._basename = basename


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
		if self.df is not None: # return from cache if it exists
			return self

		z = self.open_zip(url)
		z.extractall(self.local_file(self.basename))
		self.read(self.local_file(filename), **kw)
		return self

	def read(self, file, **kw):
		'''Load from file-like object'''
		self.df = gpd.GeoDataFrame.from_file(file, **kw)

	def save(self, file, **kw):
		'''Simplistic implementation. There could be errors with CRS and I'm not sure how it works with the auxilliary shapefile data (.dbf, .prj, .shx, ...).'''
		self.df.to_file(file, driver='ESRI Shapefile')


	def cached_load(self, *a, **kw):
		'''Helper to load csv checking and saving to cache. See `from_csv`'''
		return self.from_cache().download(*a, **kw)


# Only have shapefiles defined if geopandas is loaded.
if 'geopandas' not in sys.modules:
	class shpLoader(shpLoader):
		def __init__(self, *a, **kw):
			# Prevent a shpLoader instance from being instantiated
			raise ImportError('shpLoader depends on geopandas, which encountered an error on import.')




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

		BaseLoader.envvar = 'adkfasdkjfhkdsjfhasfasfdasdf44444' # force to go into current directory (unless you have a variable named this...)
		BaseLoader.default_dir = './data' # save to data folder

		try:
			print('Testing shapefile...')
			dl = shpLoader.load(
				url='https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/mn_mappluto_16v2.zip', filename='MNMapPLUTO.shp'
			)

			print(dl.has_df())
		except ImportError:
			print("Couldn't test shpLoader as geopandas is not installed.")

		print('Testing zipped csv...')
		dl = csvLoader(
			url='http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv', filename='API_SP.POP.TOTL_DS2_en_csv_v2.csv'
		).from_cache().download(is_zip=True, skiprows=3).save_cache()

		print(dl.has_df())

		print('Testing zipped csv preferred syntax...')
		dl = csvLoader.load(
			url='http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv',
			filename='API_SP.POP.TOTL_DS2_en_csv_v2.csv', is_zip=True, skiprows=3
		)

		print(dl.has_df())

		print('Testing csv...')
		dl = csvLoader(
			url='https://github.com/bensteers/PUI2017_bs3639/raw/master/HW5_bs3639/data-pvLFI.csv'
		).from_cache().download().save_cache()

		print(dl.has_df())

		print('Testing csv (custom filename)...')
		dl = csvLoader().cached_load(
			url='https://github.com/bensteers/PUI2017_bs3639/raw/master/HW5_bs3639/data-pvLFI.csv', filename='asdfasdfdata-pvLFI.csv'
		)

		print(dl.has_df())

		print('Testing local zipped csv...')
		dl = csvLoader(filename='balhah.csv').from_cache().download(
			'World firearms murders and ownership - Sheet 1.zip', is_zip=True
		).save_cache()

		print(dl.has_df())

		print('Testing xlsx...')
		dl = xlsxLoader(filename='vlah.xlsx').from_cache().download(
			'Team assignments and Weekly Innovation Update group (1).xlsx'
		).save_cache()

		print(dl.has_df())
