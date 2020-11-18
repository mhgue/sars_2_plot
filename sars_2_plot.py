#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# SW requirements:
#   Data structures for "relational" or "labeled" data
#     apt-get install python3-pandas python3-pandas-lib
#   Extract data from Microsoft Excel spreadsheet files
#     apt-get install python3-xlrd
#   HTTP library with thread-safe connection pooling for Python3
#     apt-get install python3-urllib3
#   Error-tolerant HTML parser for Python 3
#     apt-get install python3-bs4
#   RFC 3986 compliant replacement for urlparse (Python 3)
#     apt-get install python3-uritools
#
# Get new infections per day in germany:
#   https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Fallzahlen_Kum_Tab.xlsx
#   https://www.zdf.de/nachrichten/panorama/coronavirus-rki-neuinfektionen-106.html
#
# see
#   https://www.datacamp.com/community/tutorials/python-excel-tutorial
#   https://www.datacamp.com/community/tutorials/pandas-tutorial-dataframe-python
# Options for plotting:
#   * matplotlib
#   * mpld3 apt-get install python3-mplexporter
#   * pygal http://www.pygal.org apt-get install python3-pygal
#   * Bokeh
#   * HoloViews http://holoviews.org 
#   * Plotly https://plotly.com/python/ apt-get install python3-plotly
#

import sys,os,re,math,copy
import argparse
import pandas
# https://docs.python.org/3/library/datetime.html#datetime.datetime
import datetime
import dateutil.parser
import pytz
# https://urllib3.readthedocs.io/en/latest/
import urllib3
# https://www.crummy.com/software/BeautifulSoup/bs4/doc/
from bs4 import BeautifulSoup
# https://pypi.org/project/uritools/
import uritools
# https://docs.python.org/3/library/shutil.html
import shutil
# https://numpy.org/ https://www.python-kurs.eu/numpy.php
import numpy
# https://matplotlib.org/api/pyplot_api.html
import matplotlib.pyplot, mplexporter
# Plot
import pygal
#import plotly.express
#import holoviews
# Own little helpers (optional)
#import helper

# RKI data is handcrafted and thus sometimes inconsistent
# => do check and correct if possible
def check_date( date, last=None ):
  # Some fields are of type string and some are datetime
  if isinstance( date, str ):
    # Some date strings do contain ',' instead of '.'
    date = date.replace( ',', '.' )
    date = datetime.datetime.strptime( date+" 00:00:00", '%d.%m.%Y %H:%M:%S')
  # Now all shall be unique and isochron
  assert isinstance( date, datetime.datetime )
  assert date.time() == datetime.time(0,0,0)
  date = date.date()
  if last:
    assert (date-last) == datetime.timedelta(days=1)
  return date

def element_that_fit( ilist, regstr ):
  olist=[]
  pattern = re.compile( regstr )
  for element in ilist:
    if pattern.match( element ):
      olist.append( element )
  if len(olist) > 1:
    print( olist )
  assert len(olist) == 1
  return olist[0]

# Difference to preceding element
def diff_list( lin, step=1 ):
  lout = copy.deepcopy( lin )
  n = len(lout)-1
  while n > step:
    lout[n] = lout[n]-lout[n-step]
    n=n-1
  for n in range(step):
    lout[n] = 0
  return lout

# Mean of preceding elements
def mean_list( lin, step=2 ):
  lout = copy.deepcopy( lin )
  n = len(lout)-1
  while n > step:
    mean = 0
    for m in range(step):
      mean = mean + lin[n-m]
    lout[n] = mean/step
    n=n-1
  return lout

class classCovid:
  def __init__( self, verb ):
    self.verb = verb
    # Like an ordinary windows IE user
    self.user_agent = {'user-agent': 'IE 9/Windows: Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)'}
    self.http = urllib3.PoolManager( 10, headers=self.user_agent )
    self.utc = pytz.UTC
    self.xls = None

  # Sometimes RKI is lazy in updating XLS so let's check text table
  def get_latest_entry( self, uri ):
    try:
      request = self.http.urlopen('GET', uri )
    except urllib3.exceptions.SSLError as e:
      print( "TSL error", e.reason, uri )
      sys.exit(-1)
    except urllib3.exceptions.HTTPError as e:
      print( e.reason, uri )
      sys.exit(-1)
    html =  request.data
    parsed_html = BeautifulSoup( html, "lxml", from_encoding="UTF-8" )
    # Inside the main text
    div = parsed_html.find( 'div', id='main' )
    # There are paragraphs with dates
    pat = re.compile( '^Stand: *([0-9]+\.[0-9]+\.[0-9]+).*' )
    for p in div.find_all( 'p' ):
      m = pat.search( p.text )
      if m: break
    # Under the first date there is a table
    s = p.next_sibling
    while s:
      if s.name == 'table': break
      s = s.next_sibling
    assert s != None
    # Body of that table
    tbody = s.find( 'tbody' )
    # Last row is total counts
    for tr in tbody.children: pass
    tds = tr.find_all( 'td' )
    # Check that this is the row with the "totals"
    assert tds[0].text == 'Gesamt'
    row = [t.text for t in tds]
    # Convert the date
    date = dateutil.parser.parse( m.group(1) )
    assert date.time() == datetime.time(0,0,0)
    date = date.date()
    # Check if we can append this latest data
    if (date - self.dates[-1]) == datetime.timedelta(days=1):
      if self.verb: print( "Found new data record in text table. XLS has not been updated." )
      self.dates.append( date )
      count = int(row[1].replace('.',''))
      assert count > self.counts[-1]
      self.counts.append( count )
      death = int(row[5].replace('.',''))
      assert death > self.deaths[-1]
      self.deaths.append( death )
      if self.verb: print( self.dates[-1], self.counts[-1], self.counts[-1]-self.counts[-2], 
                 '(', self.deaths[-1], ',', self.deaths[-1]-self.deaths[-2], ')' )

  def get_rki_internal_link( self, uri ):
    if self.verb: print( "Read %s" % uri )
    try:
      request = self.http.urlopen('GET', uri )
    except urllib3.exceptions.SSLError as e:
      print( "TSL error", e.reason, uri )
      sys.exit(-1)
    except urllib3.exceptions.HTTPError as e:
      print( e.reason, uri )
      sys.exit(-1)
    html =  request.data
    # Parse using the lxml parser
    parsed_html = BeautifulSoup( html, "lxml", from_encoding="UTF-8" )
    # Get internal links of this side
    links = parsed_html.body.find_all( 'a', attrs={'class':'more downloadLink InternalLink'} )
    assert len(links) == 1
    link = links[0]
    assert link.has_attr( 'href' )
    return uritools.urijoin( uri, link['href'] )

  # Make time non naive
  def non_naive( self, time ):
    if not time.tzinfo:
      return self.utc.localize( time )
    return time

  # Get non naive file time
  def file_time( self, filename ):
    try:
      file_time = datetime.datetime.fromtimestamp(os.path.getmtime(filename))
    except FileNotFoundError:
      file_time = datetime.datetime.min
    return self.non_naive( file_time )

  def in_file_time( self ):
    if self.verb: print( "Check date of %s" % self.xls.io )
    sheet = element_that_fit( self.xls.sheet_names, '^Tageswerte.*' )
    dataframe = self.xls.parse( sheet )
    stand = dataframe.columns[0]
    date = re.search( '[0-9]+\.[0-9]+\.[0-9]+', stand )
    if date:
      date = date.group(0)
    else:
      return self.non_naive( datetime.datetime.min )
    time = re.search( '[0-9]+:[0-9]+:[0-9]+', stand )
    if time:
      date = date+' '+time.group(0)
    in_time = dateutil.parser.parse( date )
    return self.non_naive( in_time )

  def get_file( self, uri ):
    self.xls = None
    parts = uritools.urisplit( uri )
    filename = os.path.basename(parts.path).split( ';' )[0]
    # Check file date
    file_time = self.file_time( filename )
    # Check in file date (if file exists)
    if os.path.isfile( filename ):
      self.xls = pandas.ExcelFile( filename )
      in_time = self.in_file_time()
      if self.verb: print( "File as of: %s" % str(in_time) )
      # If date content is more that 1 day behind timestamp
      if (file_time - in_time) > datetime.timedelta(days=1):
        file_time = in_time
    # Check URI date
    reply = self.http.request( 'GET', uri, preload_content=False )
    uri_time = dateutil.parser.parse( reply.headers['last-modified'] )
    if file_time >= uri_time:
      if self.verb: print( "%s is already up to date" % filename )
    else:
      if self.verb:
        print( "File: %s, URI: %s" % ( str(file_time), str(uri_time) ) )
        print( "Download %s" % filename )
      with reply as response, open(filename, 'wb') as out:
        shutil.copyfileobj( response, out )
      response.release_conn()
      self.xls = None
    if not self.xls:
      self.xls = pandas.ExcelFile( filename )

  def parse_rki_xls( self ):
    if self.verb: print( "Parse %s" % self.xls.io )
    # Take the one sheet with "-gesamt"
    sheet = element_that_fit( self.xls.sheet_names, '.*-gesamt$' )
    dataframe = self.xls.parse( sheet )
    # We are interested in date and count (and do check diff)
    assert dataframe.iloc[0][0] == "Berichtsdatum"
    assert dataframe.iloc[0][1] == "Anzahl COVID-19-Fälle"
    assert dataframe.iloc[0][3] == "Differenz Vortag Fälle"
    assert dataframe.iloc[0][4] == "Todesfälle"
    # Extract accumulated infections
    n=1
    self.dates=[]
    self.counts=[]
    self.deaths=[]
    while n < dataframe.shape[0]:
      date = dataframe.iloc[n][0]
      count = dataframe.iloc[n][1]
      death = dataframe.iloc[n][4]
      # Check consistency of dates
      if self.dates:
        self.dates.append( check_date( date, self.dates[-1] ) )
      else:
        self.dates.append( check_date( date ) )
      if isinstance( death, float ) and math.isnan( death ):
        death = int(0)
      assert isinstance( count, int )
      assert isinstance( death, int )
      # Check diff for consistency
      assert (n == 1) or isinstance( dataframe.iloc[n][3], int )
      assert (n == 1) or (count == (self.counts[-1]+dataframe.iloc[n][3]))
      # Keep data
      self.counts.append( count )
      self.deaths.append( death )
      if self.verb:
        if len(self.counts) > 1:
          print( self.dates[-1], self.counts[-1], self.counts[-1]-self.counts[-2], 
                 '(', self.deaths[-1], ',', self.deaths[-1]-self.deaths[-2], ')' )
        else:
          print( self.dates[-1], self.counts[-1], '(', self.deaths[-1], ')' )
      n=n+1

  def plot_pyplot( self ):
    # Time as x axes
    x = numpy.array( self.dates )
    # Infections as y axes
    y1 = numpy.array( self.counts )
    y2 = diff_list( y1 )
    y3 = diff_list( y2, 7 )
    
    ## Difference in step width 1 ( https://numpy.org/doc/stable/reference/generated/numpy.diff.html )
    #y = numpy.diff( y, 1 ) 
    ## Prepend ( https://docs.scipy.org/doc/numpy-1.10.0/reference/generated/numpy.insert.html )
    #y = numpy.insert( y, 0, 0, axis=0 )
    #y = numpy.diff( y, 7 )
    #y = numpy.insert( y, [0,0,0,0,0,0,0], 0, axis=0 )
    matplotlib.pyplot.plot( x, y2, 'r+' )
    #matplotlib.pyplot.show()
    mplexporter.show()

# For render_to_browser do
#   update-alternatives --get-selections | grep www
#   sudo update-alternatives --config x-www-browser
# May still fail due to SVG content in HTML file.
# => Do set default in file browser.
  def plot_pygal( self ):
    y1 = self.counts
    y2 = diff_list( y1 )     # infections per day
    y3 = diff_list( y2, 7 )  # change of infections per day within one week
    y4 = mean_list( y2, 7 )  # 7 day mean of infections per day
    y5 = mean_list( y3, 7 )  # 7 day mean of change

    chart = pygal.Line()
    chart.title = "SARS-CoV-2 Infektionen Deutschland"
    chart.x_labels = self.dates
    chart.add( '1. Δ inf./day',   y2 )
    chart.add( '2. 7 day Ø of 1', y4 )
    chart.add( '3. week Δ of 1',  y3 )
    chart.add( '4. 7 day Ø of 3', y5 )
    chart.render_to_file( 'covid.html' )
    chart.render_in_browser()

  def plot_plotly( self ):
    chart = plotly.express.line( self.counts, 
        x="date", y="new", title="SARS-CoV-2 Infektionen Deutschland" )
    chart.show()

def main():
  """
  Main function to initiate all other actions
  """
  # https://docs.python.org/3/library/argparse.html
  parser = argparse.ArgumentParser( description='A template for python' )
  #parser.add_argument( "excel", help="Microsoft excel file", type=str )
  parser.add_argument( '-v', '--verbose', type=int, default=0, 
                       help='Level of verbose output.' )
  args = parser.parse_args()

  covid = classCovid( args.verbose )
  xlsx_link = covid.get_rki_internal_link(
      'https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Fallzahlen_Kum_Tab.xlsx' )
  xlsx_file = covid.get_file( xlsx_link )
  covid.parse_rki_xls()
  covid.get_latest_entry( 'https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Fallzahlen.html' )
  #covid.plot_pyplot()
  covid.plot_pygal()
  #covid.plot_plotly()

if __name__ == '__main__':
  main()

#EOF
