import io
import os
import zipfile
import mimetypes
import pandas as pd
from collections import OrderedDict as odict

try:
    import urllib2 as urllib
except ImportError:
    import urllib.request as urllib



class DataLoader(object):
    env = 'PUIDATA'
    default_dir = './'

    '''Used to load data from a url and save it to your PUIDATA directory. Subsequent calls can then pull from the cache.


    # Get data - check if in cache, otherwise get from url and save to cache
    df = DataLoader('data.csv').from_cache().from_csv('http://website.com/data.zip', is_zip=True).save_cache().df
    df = DataLoader('data.csv').from_cache().from_csv('http://website.com/data.csv').save_cache().df
    
    
    # Load excel file
    dfs = DataLoader('data.xlsx').from_cache().from_xlsx('http://website.com/data.xlsx').save_cache().df
    
    # Use url basename as filename for csvs - need to specify url before from_cache
    df = DataLoader(url='http://website.com/data.csv').from_cache().from_csv().save_cache().df

    # get the second file in an archive
    df = DataLoader(url='http://website.com/data.zip').from_cache().from_csv(is_zip=True, ith=2).save_cache().df

    # Get data - load from url everytime, don't use cache
    df = DataLoader('data.csv').from_zip('http://website.com/data.zip').df
    df = DataLoader('data.csv').from_csv('http://website.com/data.csv').df
    '''

    def __init__(self, filename=None, url=None, sheets=None):
        '''
        Arguments:
            url (str, optional): The url to pull from. Can be specified in later.
            filename (str, optional): The filename to save the cache file to. Also used to select a file from a zip archive
            sheets (list, optional): A list of sheet names to get when calling `from_xlsx`
        '''
        self.clear()
        self.url = url
        self.sheets = sheets
        self.filename = filename
        self.directory = os.getenv(self.env, self.default_dir)
        if not filename and url: 
            self.filename = os.path.splitext(os.path.basename(url))[0] + '.csv'




    # Loading from URL

    def from_xlsx(self, url=None, filename=None, is_zip=False, ith=0, sheets=None, **kw):
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
        if self.dfs: # return from cache if it exists
            return self

        socket = self._open_socket(url, filename, is_zip=is_zip, ith=ith, as_b=True)
        reader = pd.ExcelFile(socket)
        sheets = sheets or self.sheets or reader.sheet_names
        self.dfs = odict([(sh, pd.read_excel(reader, sheetname=sh, **kw)) for sh in sheets])
        return self


    def from_csv(self, url=None, filename=None, is_zip=False, ith=0, **kw):
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

        socket = self._open_socket(url, filename, is_zip)
        self.df = pd.read_csv(socket, **kw)
        return self


    def cfrom_csv(self, *a, **kw):
        '''Helper to load csv checking and saving to cache. See `from_csv`'''
        return self.from_cache().from_csv(*a, **kw).save_cache()

    def cfrom_xlsx(self, *a, **kw):
        '''Helper to load xlsx checking and saving to cache. See `from_xlsx`'''
        return self.from_cache().from_xlsx(*a, **kw).save_cache()






    # Caching
        
    def local_file(self, filename=None):
        '''Get the path for a cached file'''
        filename = filename or self.filename
        return os.path.join(self.directory, self.filename)

    def is_cached(self, filename=None):
        '''Check if a cached file exists'''
        return os.path.isfile(self.local_file(filename))

    def clear(self):
        '''Clear the loader's dataframe.'''
        self.df, self.dfs = None, odict()
        return self
    
    def from_cache(self, filename=None, **kw):
        '''Load file from cache. Assumed that it exists'''
        path = self.local_file(filename)
        mime = mimetypes.guess_type(path)

        if self.is_cached():
            print('Loaded from cache:', path)
            # load csv
            if mime == 'text/csv': 
                self.df = pd.read_csv(path, **kw)
            # Load xlsx
            elif mime in ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/vnd.ms-excel']: 
                self.dfs = self.load_xlsx(path, **kw)

        return self

    def save_cache(self, filename=None, refresh=False):
        '''save file to PUIDATA directory'''
        if refresh or not self.is_cached():
            file = self.local_file(filename)

            # Ensure the directory exists
            outdir = os.path.dirname(file)
            if outdir and not os.path.isdir(outdir):
                os.mkdir(outdir)

            # Save as excel
            if len(self.dfs):
                writer = pd.ExcelWriter(file)
                for name, df in self.dfs.items():
                    df.to_excel(writer, name, index=False)
                writer.save()
            # Save as csv
            elif self.df is not None:
                self.df.to_csv(file, index=False)
        return self





    # Utilities

    def _open_file(self, url=None, as_b=False):
        self.url = url or self.url or self.local_file()
        try: # assume is url
            socket = urllib.urlopen(self.url)
        except ValueError: # is local
            socket = open(self.url, 'rb' if as_b else 'r')
        return socket

    def _open_socket(self, url=None, filename=None, is_zip=False, ith=0, as_b=False):
        # Load from zipfile
        if is_zip:
            z = zipfile.ZipFile(io.BytesIO( self._open_file(url, as_b=True).read() ))
            filename = filename or self.filename
            filename = filename if filename in z.namelist() else z.namelist()[ith]
            self.filename = self.filename or filename # set default filename
            socket = z.open(filename)
        # Load file
        else:
            self.filename = filename or self.filename or os.path.basename(url)
            socket = self._open_file(url, as_b=as_b)
        return socket


    def has_df(self):
        '''for making sure there's a df'''
        return len(self.dfs) or self.df is not None











if __name__ == '__main__':


    # Running tests on assignment 5 

    DataLoader.env = 'adkfasdkjfhkdsjfhasfasfdasdf44444' # force to go into current directory (unless you have a variable named this...)
    DataLoader.default_dir = './data' # save to data folder


    dl = DataLoader(filename='API_SP.POP.TOTL_DS2_en_csv_v2.csv').from_cache().from_csv(
        'http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv', is_zip=True, skiprows=3
    ).save_cache()

    print(dl.has_df())
    

    dl = DataLoader(filename='asdfasdfdata-pvLFI.csv').from_cache().from_csv(
        'https://github.com/bensteers/PUI2017_bs3639/raw/master/HW5_bs3639/data-pvLFI.csv'
    ).save_cache()

    print(dl.has_df())

    dl = DataLoader(url='https://github.com/bensteers/PUI2017_bs3639/raw/master/HW5_bs3639/data-pvLFI.csv').from_cache().from_csv().save_cache()

    print(dl.has_df())


    dl = DataLoader(filename='vlah.xlsx').from_cache().from_xlsx(
        'Team assignments and Weekly Innovation Update group (1).xlsx'
    ).save_cache()

    print(dl.has_df())

    dl = DataLoader(filename='balhah.csv').from_cache().from_csv(
        'World firearms murders and ownership - Sheet 1.zip', is_zip=True
    ).save_cache()

    print(dl.has_df())

