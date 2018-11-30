from __future__ import print_function, division
import argparse
import csv
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import httplib2
from oauth2client import client, file, tools
import pandas as pd

SECRET = 'ga_api_client_secret.json' 
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
SAMPLING_LEVEL = 'HIGHER_PRECISION'
OUTPUT = 'json'

class GoogleAnalytics:
  def __init__(self, request):
    '''Load API request.
    Args:
      request: GA API v3 request
    '''
    self.request = request
    self.first_date = self.request.get('start_date')  # In total.
    self.last_date = self.request.get('end_date')  # In total.
    self.start_date = self.request.get('start_date')  # Next iteration.
    self.end_date = self.request.get('start_date')  # Next iteration.
    self.service = None
    self.batches = []  # List of all dates to iterate on.
    self.pages = []  # List of all pages in the current batch.

  def __next__(self):
    self.first_page = True

  def get_service(self, api_name='analytics', api_version='v3', scope=SCOPES, 
    client_secrets_path=SECRET):
    '''Get a service that communicates to a Google API.

    Args:
      api_name: string The name of the api to connect to.
      api_version: string The api version to connect to.
      scope: A list of strings representing the auth scopes to authorize for the
        connection.
      client_secrets_path: string A path to a valid client secrets file.

    Returns:
      A service that is connected to the specified API.
    '''
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(
      formatter_class=argparse.RawDescriptionHelpFormatter,
      parents=[tools.argparser])
    flags = parser.parse_args([])

    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(client_secrets_path, scope=SCOPES,
      message=tools.message_if_missing(client_secrets_path))

    # Prepare credentials, and authorize HTTP object with them.
    # If the credentials don't exist or are invalid run through the native client
    # flow. The Storage object will ensure that if successful the good
    # credentials will get written back to a file.
    storage = file.Storage(api_name + '.dat')
    credentials = storage.get()
    if credentials is None or credentials.invalid:
      credentials = tools.run_flow(flow, storage, flags)
    http = credentials.authorize(http=httplib2.Http())

    # Build the service object.
    service = build(api_name, api_version, http=http)

    return service

  def get_max_results(self, start_index=1, max_results=1000):
    '''Use the Analytics Service Object to query the Core Reporting API. The 
    result may be subjected to pagination.

    Args:
      start_index: data row number to start from.
      max_results: output row limit, max: 10000.

    Returns:
      Full API response json
    '''
    if max_results is None:
      max_results=1000
    if int(max_results) < 0:
      raise ValueError('Argument max_value has to be larger than zero.')
    if int(max_results) > 10000:
      max_results = 10000
      print('Argument max_value has been rounded down to 10000.')

    if not self.service:
      self.service = self.get_service()

    results = self.service.data().ga().get(
      ids=self.request.get('ids'),
      start_date=self.start_date,
      end_date=self.end_date,
      dimensions=self.request.get('dimensions'),
      metrics=self.request.get('metrics'),
      filters=self.request.get('filters'),
      segment=self.request.get('segment'),
      sort=self.request.get('sort'),
      start_index=start_index,
      max_results=self.request.get('max_results'),
      samplingLevel=SAMPLING_LEVEL,
      output=OUTPUT
      ).execute()

    if self.first_page == True:
      self.first_page = False
      self.items_per_page = int(results.get('itemsPerPage'))
      self.total_results = int(results.get('totalResults'))

      if self.items_per_page != self.total_results:
        self.pages = range(1, self.total_results, self.items_per_page)
    
    last_index = min(start_index + self.items_per_page - 1, self.total_results)
    print('--Retrieved rows {} to {} out of {}.'.format(start_index, \
      last_index, self.total_results))

    return results

  def get_all_results(self):
    '''Get all rows for a given batch. If paging occurs, iterate through all 
    pages of this batch.
    
    Returns: 
      Pandas DataFrame
    '''
    self.first_page = True

    # Get first page of results incl. metadata.
    results = self.get_max_results(max_results=self.request.get('max_results'))

    # Check sampling.
    contains_sampled_data = results.get('containsSampledData')
    if contains_sampled_data:
      sampling_perc = float(results.get('sampleSize')) / float(results.get('sampleSpace')) * 100
      print('--WARNING: Results are based on sampled data ({:.1f}%)!'.format(sampling_perc))

    # Get column names and types.
    columns = results.get('columnHeaders')
    self.column_names = [c['name'].replace('ga:', '') for c in columns]
    self.column_types = [c['dataType'] for c in columns]

    # Get first page of results.
    rows = results.get('rows')

    # Get remaining pages of results if exist.
    [rows.extend(self.get_max_results(
      start_index=page_start, max_results=self.request.get('max_results')
      ).get('rows')) for page_start in self.pages[1:]]
    self.df = pd.DataFrame(data=rows, columns=self.column_names)

    return self.df

  def transform_results(self, df):
    ''' Transform data types.

    Arguments:
      df: Pandas DataFrame.

    Returns:
      Pandas DataFrame
    '''
    conversions = {
      'STRING': 'object', 
      'INTEGER': 'int', 
      'PERCENT': 'float', 
      'TIME': 'float',
      'CURRENCY': 'float', 
      'FLOAT': 'float'
      }

    for column in df:
      df[column] = df[column].astype(
        conversions.get(self.column_types[df.columns.get_loc(column)]))

    return df

  def print_summary(self, df):
    '''Prints out data preview and columns description.

    Args:
      df: Pandas DataFrame

    Returns:
      nothing
    '''
    print('\nData sample:')
    print(df.head(5))
    print('...')
    print(df.tail(5))
    print('\nSummary stats:')
    print(df.describe())
    print('\nData types:')
    print(df.info())

  def save_to_file(self, df, output_file):
    '''Saves DataFrame into a csv file.

    Args:
      df: Pandas DataFrame

    Returns:
      nothing
    '''
    df.to_csv(output_file, quoting=csv.QUOTE_NONNUMERIC)

  def batch_get(self, output_file=None, summary=True):
    '''Iterate through dates and request data in daily batches to avoid 
    sampling.

    Returns:
      Pandas DataFrame
    '''
    df = pd.DataFrame()
    start_datetime = datetime.strptime(self.first_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(self.last_date, '%Y-%m-%d')
    delta = end_datetime - start_datetime
    dates = [(start_datetime + timedelta(i)).strftime('%Y-%m-%d') \
      for i in range(delta.days + 1)]

    for d in dates:
      self.start_date = d
      self.end_date = d
      print('\n--Geting results for {} out of [{} - {}] date range.' \
        .format(d, self.first_date, self.last_date))
      batch = self.get_all_results()
      df = df.append(batch, ignore_index=True)

    if df.index.values.size > 0:
      df = self.transform_results(df)

    if summary:
      self.print_summary(df)

    if output_file:
      self.save_to_file(df, output_file=output_file)

    return df
