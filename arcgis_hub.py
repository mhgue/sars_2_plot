#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#
# Read data from feature servers as e.g. used by:
#   https://experience.arcgis.com/experience/478220a4c454480e823b17327b2bf1d4
#
# ArcGIS
# Query operation:
#   http://ec2-54-204-216-109.compute-1.amazonaws.com:6080/arcgis/sdk/rest/ms_dyn_query.html
#

import copy,json,sys
import urllib3
import uritools
import pprint
import datetime
from collections.abc import Iterable
from numbers import Number

def eprint(*args, **kwargs):
  print(*args, file=sys.stderr, **kwargs)

def are_values_equal( case, vals, err = 3 ):
  assert isinstance( vals, Iterable )
  assert len(vals) >= 2
  assert isinstance( vals[0], Number )
  for v in vals:
    if (v != vals[0]):
      eprint( "Inconsistent %s: %d != %d" % (case,v,vals[0]) )
    assert abs(v-vals[0]) < err

# Read data from ArcGIS feature servers
class arcgis_hub:
  def __init__( self ):
    # URIs of feature servers:
    self.uri_dict={
        # https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0
        'rki covid19':'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query',
        # ['AnzahlFall', 'AnzahlTodesfall', 'AnzahlGenesen', 'NeuerFall', 'NeuerTodesfall', 'NeuGenesen',
        #  'ObjectId', 'Datenstand', 'Meldedatum', 'Bundesland', 'IdBundesland', 'Landkreis', 'IdLandkreis',
        #  'Altersgruppe', 'Altersgruppe2', 'Geschlecht', 'Refdatum', 'IstErkrankungsbeginn']
        # https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/917fc37a709542548cc3be077a786c17_0
        'rki landkreis':'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_Landkreisdaten/FeatureServer/0/query',
        # https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/ef4b445a53c1406892257fe63129a8ea_0
        'rki bundesland':'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/Coronafälle_in_den_Bundesländern/FeatureServer/0/query',
        # https://npgeo-corona-npgeo-de.hub.arcgis.com/app/e6acbf22cc4f4b85949f59734244ba71
        'who europa':'https://services.arcgis.com/5T5nSi527N4F7luB/arcgis/rest/services/EURO_COVID19_Current_Ancillary/FeatureServer/0/query',
        # ['LastCaseDate',  'CasesTotal',  'CasesN1',  'CasesN2',  'CasesN3',  'CasesN4',  'CasesN5', 
        #  'LastDeathDate', 'DeathsTotal', 'DeathsN1', 'DeathsN2', 'DeathsN3', 'DeathsN4', 'DeathsN5',
        #  'OBJECTID', 'WHO_CODE', 'LAT', 'LON', 'NAME_ENG', 'NAME_RUS' ]
        'rki covid19 refdate':'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_Covid19_Refdate/FeatureServer/0/query',
        # ['Landkreis', 'Bundesland', 'Datenstand', 'FID', 'Datum', 'AnzahlFall', 'IstErkrankungsbeginn']
        'rki covid19 recovered':'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19_Recovered_BL/FeatureServer/0/query',
        # ['Bundesland', 'FID', 'Genesen', 'DiffVortag', 'Datenstand', 'IdBundesland']
        'rki covid19 sums':'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/Covid19_RKI_Sums/FeatureServer/0/query'
        # ['AnzahlFall', 'AnzahlTodesfall', 'AnzahlGenesen', 'SummeFall', 'SummeTodesfall', 'SummeGenesen',
        #  'ObjectId', 'Datenstand', 'Meldedatum', 'Bundesland', 'IdBundesland', 'Landkreis', 'IdLandkreis']
        }
    self.esriTypes = {  'esriFieldTypeInteger':int,        # Integer value
                        'esriFieldTypeDouble':[float,int], # Floating point or integer (e.g. statistic result)
                        'esriFieldTypeOID':int,       # Integer identifier / index
                        'esriFieldTypeDate':int,      # Timestamp UNIX epoche in msec
                        'esriFieldTypeString':str     # General string
                    }
    self.user_agent = {'user-agent': 'IE 9/Windows: Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)'}
    self.http = urllib3.PoolManager( 10, headers=self.user_agent )
    self.__default_query()
    self.pp = pprint.PrettyPrinter(indent=4)
  
  # Request the query
  def __get( self, uri_label, query=None ):
    assert uri_label in self.uri_dict
    # Concatenate URI and parameters
    uri_parts = uritools.urisplit( self.uri_dict[uri_label] )
    if not query: query=self.query
    uri = uritools.uricompose( scheme=uri_parts.scheme, host=uri_parts.host,
        port=uri_parts.port, path=uri_parts.path, query=query, fragment=None )
    #print( uri )
    try:
      request = self.http.urlopen('GET', uri )
    except urllib3.exceptions.SSLError as e:
      print( "TSL error", e.reason, uri )
      sys.exit(-1)
    except urllib3.exceptions.HTTPError as e:
      print( e.reason, uri )
      sys.exit(-1)
    self.reply = None
    self.fields = None
    self.values = None
    try:
      self.reply = json.loads( request.data )
    except json.JSONDecodeError as e:
      print( e.reason, uri )
      # May be HTML error e.g. <title>404 - File or directory not found.</title>
      sys.exit(-1)
    if 'error' in self.reply:
      e = self.reply['error']
      eprint( "ERROR: %d %s %s" % ( e['code'], " ".join( e['details'] ), " ".join( e['message'] ) ) )
      sys.exit(-1)

  # The base query of all others
  def __default_query( self ):
    self.query = {  'f':'json',         # output format JSON
                    'where':'1=1',      # SQL request anything
                    'returnGeometry':'false',   # No geometry needed
                    'spatialRel':'esriSpatialRelIntersects',
                    'outFields':'*',            # output any field
                    'resultType':'standard',
                    'cacheHint':'true' }

  # Do any kind of statistics query
  def __query_statistics( self, stat ):
    self.query['outStatistics']=json.dumps(stat, separators=(',', ':'))

  # Do statistics of one type on one field.
  def __query_statistics_type_field( self, stype, sfield, sname = 'value' ):
    stat=[{'statisticType':stype, 'onStatisticField':sfield, 'outStatisticFieldName':sname}]
    self.__query_statistics( stat )

  # Accumulate fields by sum over group
  def __query_sum_per_field( self, field_to_sum, field_to_group ):
    self.query['where']="%s<>0" % field_to_sum
    self.query['groupByFieldsForStatistics']=field_to_group
    self.query['orderByFields']="%s asc" % field_to_group
    self.__query_statistics_type_field( 'sum', field_to_sum )

  # Do request a part only
  def __query_part( self, offset=0, count=1 ):
    self.query['resultRecordCount']=count
    if (offset > 0):
      self.query['resultOffset']=offset

  # Add new condition to where
  def __query_where_and( self, condition=None ):
    if condition:
      self.query['where']='%s AND %s' % (self.query['where'], condition )

  # Check and parse the result of a single value statistics query
  def __parse_sigle_statistics( self ):
    assert 'features' in self.reply
    assert isinstance( self.reply['features'], list )
    assert 1 == len(self.reply['features'])
    assert 'attributes' in self.reply['features'][0]
    assert 'value' in self.reply['features'][0]['attributes']
    return int(self.reply['features'][0]['attributes']['value'])

  def __parse_fields( self ):
    if self.fields: return self.fields
    assert 'fields' in self.reply
    assert isinstance( self.reply['fields'], list )
    self.fields = dict()
    for f in self.reply['fields']:
      self.fields[f['alias']] = f
      self.fields[f['name']] = f
    return self.fields

  def __parse_values( self ):
    if self.values: return self.values
    self.__parse_fields()
    assert 'features' in self.reply
    assert isinstance( self.reply['features'], list )
    self.values = list()
    self.totals = dict()
    for f in self.reply['features']:
      d = dict()
      assert 'attributes' in f
      for a in f['attributes']:
        val = f['attributes'][a]
        esriType = self.fields[a]['type']
        if esriType == 'esriFieldTypeDate':
          assert isinstance( val, int )  # epoche in msec
          d[a] = datetime.datetime.utcfromtimestamp(val/1000)
        elif esriType == 'esriFieldTypeInteger':
          assert isinstance( val, int )
          self.totals[a] = self.totals.setdefault(a,0) + val
          d[a] = val
        elif esriType == 'esriFieldTypeDouble':
          assert isinstance( val, float ) or isinstance( val, int )
          self.totals[a] = self.totals.setdefault(a,0) + val
          d[a] = val
        elif esriType == 'esriFieldTypeOID':
          assert isinstance( val, int )
          d[a] = val
        else:
          d[a] = val
      self.values.append( d )
    return self.values

  def print_fields( self, spacing=20 ):
    self.__parse_fields()
    fmt = "{:<%d}" % spacing
    for name in self.fields:
      out = list()
      out.append( name )
      out.append( self.fields[name]['type'] )
      if name != self.fields[name]['alias']:
        out.append( self.fields[name]['alias'] )
      print( ";".join( [ fmt.format(s) for s in out ] ) )

  def print_data_table( self, spacing=8 ):
    self.__parse_values()
    # header with names
    print( ";".join( self.values[0] ) )
    # values
    fmt = "{:<%d}" % spacing
    for line in self.values:
      out = list()
      for name in self.values[0]:
        val = line[name]
        if self.fields[name]['type'] == 'esriFieldTypeDate':
          if (val.time() == datetime.time(0,0)):
            out.append( str( val.date() ) )
          else:
            out.append( str( val ))
        else:
          out.append( str( val ) )
      print( ";".join( [ fmt.format(s) for s in out ] ) )
    print( self.totals )

  def print( self ):
    # RAW output without interpretation
    #self.pp.pprint( self.reply )
    # Formatted output as a table
    self.print_data_table()

  ###################################################################
  # Templates of requests:

  # Get the total over the complete database (scalar result)
  def get_total( self, counter, newcase, base ):
    self.__default_query()
    if newcase:
      self.query['where']='%s IN(0, 1)' % newcase
    self.__query_statistics_type_field( 'sum', counter )
    self.__get( base )
    return self.__parse_sigle_statistics()
  
  # Get the maximum over the complete database (scalar result)
  def get_max( self, counter, base ):
    self.__default_query()
    self.__query_statistics_type_field( 'max', counter )
    self.__get( base )
    return self.__parse_sigle_statistics()
  
  # Get the current new (scalar result)
  def get_current_new( self, counter, newcase, base ):
    assert newcase
    self.__default_query()
    self.query['where']='%s IN(1,-1)' % newcase
    self.__query_statistics_type_field( 'sum', counter )
    self.__get( base )
    return self.__parse_sigle_statistics()

  # Get the totals of each day
  # e.g. 
  def get_total_per_day( self, counter, timestamp, newcase, base ):
    self.__default_query()
    self.__query_sum_per_field( counter, timestamp )
    if newcase:
      self.__query_where_and( '%s IN(0, 1)' % newcase )
    self.__get( base )
    self.__parse_values()

  ###################################################################
  # Examples of concrete requests:

  # Total amount of cases reported until now (differnet ways)
  def get_current_total_cases_01( self ):
    return self.get_total( 'Fallzahl', None, 'rki bundesland' )
  def get_current_total_cases_02( self ):
    return self.get_total( 'cases', None, 'rki landkreis' )
  def get_current_total_cases_03( self ):
    return self.get_total( 'AnzahlFall', 'NeuerFall', 'rki covid19' )
  def get_current_total_cases_04( self ):
    self.get_total_per_day( 'AnzahlFall', 'Meldedatum', 'NeuerFall', 'rki covid19' )
    return self.totals['value']
  def get_current_total_cases_05( self ):
    self.get_total_per_day( 'AnzahlFall', 'Datum', None, 'rki covid19 refdate' )
    return self.totals['value']
  def get_current_total_cases_06( self ):
    self.get_total_per_day( 'AnzahlFall', 'Meldedatum', None, 'rki covid19 sums' )
    return self.totals['value']

  # New cases (delta within 24h)
  def get_current_new_cases( self ):
    return self.get_current_new( 'AnzahlFall', 'NeuerFall', 'rki covid19' )

  # Total amount of deaths reported until now
  def get_current_total_deaths_01( self ):
    return self.get_total( 'Death', None, 'rki bundesland' )
  def get_current_total_deaths_02( self ):
    return self.get_total( 'deaths', None, 'rki landkreis' )
  def get_current_total_deaths_03( self ):
    return self.get_total( 'AnzahlTodesfall', None, 'rki covid19' )
  def get_current_total_deaths_04( self ):
    self.get_total_per_day( 'AnzahlTodesfall', 'Meldedatum', 'NeuerFall', 'rki covid19' )
    return self.totals['value']
  def get_current_total_deaths_05( self ):
    self.get_total_per_day( 'AnzahlTodesfall', 'Meldedatum', None, 'rki covid19 sums' )
    return self.totals['value']

  # New deaths (delta within 24h)
  def get_current_new_deaths( self ):
    return self.get_current_new( 'AnzahlTodesfall', 'NeuerTodesfall', 'rki covid19' )
  
  # Total amount of cases recovered reported until now
  def get_current_total_recovered_01( self ):
    return self.get_total( 'AnzahlGenesen', 'NeuGenesen', 'rki covid19' )
  def get_current_total_recovered_02( self ):
    return self.get_max( 'Genesen', 'rki covid19 recovered' )
  def get_current_total_recovered_03( self ):
    return self.get_total( 'AnzahlGenesen', None, 'rki covid19 sums' )

  # New recovered (delta within 24h) ???
  def get_current_new_recovered( self ):
    return self.get_current_new( 'AnzahlGenesen', 'NeuerGenesen', 'rki covid19' )


#####################################################################
# Experimental
####################################################################

  def get_total_by_age_and_sex( self ):
    self.__default_query()
    self.query['where']="Geschlecht<>'unbekannt' AND Altersgruppe<>'unbekannt' AND NeuerFall IN(0, 1)"
    self.query['groupByFieldsForStatistics']='Altersgruppe,Geschlecht'
    self.query['orderByFields']='Altersgruppe asc'
    self.__query_statistics_type_field( 'sum', 'AnzahlFall' )
    self.__get( 'rki covid19' )
    self.print()

  # Count of entries per bundesland ?
  def get_BL_per_bundesland( self ):
    self.__default_query()
    self.query['groupByFieldsForStatistics']='BL'
    self.query['orderByFields']='BL asc'
    self.__query_statistics_type_field( 'count', 'BL' )
    self.__get( 'rki landkreis' )
    self.print()

  # Accumulated cases per 100000 population
  def get_cases_per_100000_per_bundesland( self ):
    self.__default_query()
    self.query['groupByFieldsForStatistics']='LAN_ew_GEN'
    self.query['orderByFields']='value desc'
    self.__query_statistics_type_field( 'max', 'faelle_100000_EW' )
    self.__get( 'rki bundesland' )
    self.print()


  def get_cases_per_day_corrected(self ):
    self.__default_query()
    self.query['where']="AnzahlFall<>0" #" AND Datum>timestamp '2020-03-01 22:59:59'"
    self.query['outFields']='AnzahlFall,Datum,IstErkrankungsbeginn'
    self.query['orderByFields']='Datum asc'
    self.query['groupByFieldsForStatistics']='Datum,IstErkrankungsbeginn'
    self.__query_statistics_type_field( 'sum', 'AnzahlFall' )
    self.__get( 'rki covid19 refdate' )
    self.__parse_values()

    common = dict()
    total = 0
    for v in self.values:
      date = v['Datum'].date()
      if not date in common:
        common[date] = { 'sum':0, 'erkrankt':0, 'gemeldet':0, 'total':0 }

      val = v['value']
      total += val
      if v['IstErkrankungsbeginn'] > 0:
        common[date]['erkrankt'] += val
      else:
        common[date]['gemeldet'] += val
      common[date]['sum'] += val
      common[date]['total'] = total
    
    dates = sorted(list(common.keys()))
    counts = [ common[d]['total'] for d in dates ]
    for d in sorted(list(common.keys())):
      print( d, common[d]['total'], common[d]['sum'] )
    return { 'dates':dates, 'counts':counts }

  def get_04(self ):
    self.__default_query()
    self.query['where']="AnzahlFall<>0 AND Datum>timestamp '2020-03-01 22:59:59'"
    self.query['outFields']='FID,AnzahlFall,Datum,IstErkrankungsbeginn'
    self.query['orderByFields']='Datum asc'
    self.__query_part( 0, 320 )
    self.__get( 'rki covid19 refdate' )
    self.print()

  def get_03( self ):
    self.__default_query()
    self.query['where']="Meldedatum>timestamp '2020-03-01 22:59:59' AND Meldedatum NOT BETWEEN timestamp '2020-12-10 23:00:00' AND timestamp '2020-12-11 22:59:59'"
    self.query['outFields']="ObjectId,SummeFall,Meldedatum"
    self.query['orderByFields']="Meldedatum asc"
    self.__query_part( 0, 10000 )
    self.__get( 'rki covid19 sums' )
    self.print()

  def get_02( self ):
    self.__default_query()
    self.query['where']="Datum>timestamp '2020-03-01 22:59:59' AND AnzahlFall<>0"
    self.query['outFields']='FID,AnzahlFall,Datum,IstErkrankungsbeginn'
    self.query['orderByFields']='Datum asc'
    self.__query_part( 0, 10 )
    self.__get( 'rki covid19 refdate' )
    self.print()

  def get_01( self ):
    self.__default_query()
    self.query['groupByFieldsForStatistics']='BL'
    self.query['orderByFields']='BL asc'
    self.__query_statistics_type_field( 'count', 'BL', 'count_result' )
    self.query['outSR']='102100'  # wkid
    self.__get( 'rki landkreis' )
    self.print()

  def get_total_cases_until( self ):
    self.__default_query()
    #self.query['where']="NeuerFall IN(0,1) AND Refdatum<timestamp '2020-12-01 22:59:59'"
    self.query['where']="NeuerFall IN(0,1) AND Meldedatum<timestamp '2020-12-01 22:59:59'"
    self.__query_statistics_type_field( 'sum', 'AnzahlFall' )
    self.__get( 'rki covid19' )
    return self.__parse_sigle_statistics()

  # Get field names by requesting 1 record
  def get_fields( self, base='rki covid19' ):
    self.__default_query()
    self.__query_part(0,1)
    self.__get( base )
    self.__parse_fields()
    #self.print_fields()
    print( list(self.fields) )

  def check( self ):
    cases = [ self.get_current_total_cases_01(), 
              self.get_current_total_cases_02(),
              self.get_current_total_cases_03(),
              self.get_current_total_cases_04(),
              self.get_current_total_cases_05(),
              self.get_current_total_cases_06() ]
    are_values_equal( "total cases", cases )
    deaths = [ self.get_current_total_deaths_01(),
               self.get_current_total_deaths_02(),
               self.get_current_total_deaths_03(),
               self.get_current_total_deaths_04(),
               self.get_current_total_deaths_05() ]
    are_values_equal( "total deaths", deaths )
    revovered = [ self.get_current_total_recovered_01(),
                  self.get_current_total_recovered_02(),
                  self.get_current_total_recovered_03() ]
    are_values_equal( "total recovered", revovered )

# Test cases
def main():
  arcgis = arcgis_hub()
  arcgis.check()
  #print( "Current cases: ", arcgis.get_current_total_cases() )
  #print( "Current new cases: ", arcgis.get_current_new_cases() )
  #print( "Current deaths: ", arcgis.get_current_total_deaths() )
  #print( "Current new deaths: ", arcgis.get_current_new_deaths() )
  #print( "Current recovered: ", arcgis.get_current_total_recovered() )
  #print( "Current new recovered: ", arcgis.get_current_new_recovered() )
  #print( "Cases until 1.12.: ", arcgis.get_total_cases_until() )

  #arcgis.get_BL_per_bundesland()
  #arcgis.get_cases_per_100000_per_bundesland()
  #arcgis.get_fields( 'rki covid19' )
  #arcgis.get_fields( 'rki landkreis' )
  #arcgis.get_fields( 'rki bundesland' )
  #arcgis.get_fields( 'rki covid19 refdate' )
  #arcgis.get_fields( 'rki covid19 recovered' )
  #arcgis.get_fields( 'rki covid19 sums' )
  #arcgis.get_fields( 'who europa' )
  #arcgis.get_total_by_age_and_sex()
  #arcgis.get_unknown_01()
  #arcgis.get_cases_per_day_corrected()

if __name__ == '__main__':
  main()

#EOF
