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
    
def eprint(*args, **kwargs):
  print(*args, file=sys.stderr, **kwargs)

def are_values_equal( case, val1, val2 ):
  if (val1 != val2):
    eprint( "Inconsistent %s: %d != %d" % (case,val1,val2) )

# Read data from ArcGIS feature servers
class arcgis_hub:
  def __init__( self ):
    # URIs of feature servers:
    self.uri_dict={
        # https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0
        'rki covid19':'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query',
        # https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/917fc37a709542548cc3be077a786c17_0
        'rki landkreis':'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_Landkreisdaten/FeatureServer/0/query',
        # https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/ef4b445a53c1406892257fe63129a8ea_0
        'rki bundesland':'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/Coronafälle_in_den_Bundesländern/FeatureServer/0/query',
        # https://npgeo-corona-npgeo-de.hub.arcgis.com/app/e6acbf22cc4f4b85949f59734244ba71
        'who europa':'https://services.arcgis.com/5T5nSi527N4F7luB/arcgis/rest/services/EURO_COVID19_Current_Ancillary/FeatureServer/0/query',
        
        'rki covid19 refdate':'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_Covid19_Refdate/FeatureServer/0/query',
        'rki covid19 recovered':'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19_Recovered_BL/FeatureServer/0/query'
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
    try:
      self.reply = json.loads( request.data )
    except json.JSONDecodeError as e:
      print( e.reason, uri )
      # May be HTML error e.g. <title>404 - File or directory not found.</title>
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
  def __statistics_query( self, stat ):
    self.query['outStatistics']=json.dumps(stat, separators=(',', ':'))

  # Do statistics of one type on one field.
  def __statistics_query_type_field( self, stype, sfield ):
    stat=[{'statisticType':stype, 'onStatisticField':sfield, 'outStatisticFieldName':'value'}]
    self.__statistics_query( stat )

  # Check and parse the result of a single value statistics query
  def __parse_sigle_statistics( self ):
    assert 'features' in self.reply
    assert isinstance( self.reply['features'], list )
    assert 1 == len(self.reply['features'])
    assert 'attributes' in self.reply['features'][0]
    assert 'value' in self.reply['features'][0]['attributes']
    return int(self.reply['features'][0]['attributes']['value'])

  ###################################################################
  # Examples of concrete requests:

  # Total amount of cases reported until now
  def get_current_total_cases( self ):
    self.__default_query()
    self.__statistics_query_type_field( 'sum', 'cases' )
    self.__get( 'rki landkreis' )
    return self.__parse_sigle_statistics()
  def get_current_total_cases_2( self ):
    self.__default_query()
    self.__statistics_query_type_field( 'sum', 'Fallzahl' )
    self.__get( 'rki bundesland' )
    return self.__parse_sigle_statistics()

  # New cases (delta within 24h)
  def get_current_new_cases( self ):
    self.__default_query()
    self.query['where']='NeuerFall IN(1, -1)'
    self.__statistics_query_type_field( 'sum', 'AnzahlFall' )
    self.__get( 'rki covid19' )
    return self.__parse_sigle_statistics()

  # Total amount of deaths reported until now
  def get_current_total_deaths( self ):
    self.__default_query()
    self.__statistics_query_type_field( 'sum', 'deaths' )
    self.__get( 'rki landkreis' )
    return self.__parse_sigle_statistics()
  def get_current_total_deaths_2( self ):
    self.__default_query()
    self.__statistics_query_type_field( 'sum', 'Death' )
    self.__get( 'rki bundesland' )
    return self.__parse_sigle_statistics()

  # New deaths (delta within 24h)
  def get_current_new_deaths( self ):
    self.__default_query()
    self.query['where']='NeuerTodesfall IN(1, -1)'
    self.__statistics_query_type_field( 'sum', 'AnzahlTodesfall' )
    self.__get( 'rki covid19' )
    return self.__parse_sigle_statistics()
  
  # Total amount of cases recovered reported until now
  def get_current_total_recovered( self ):
    self.__default_query()
    self.__statistics_query_type_field( 'max', 'Genesen' )
    self.__get( 'rki covid19 recovered' )
    return self.__parse_sigle_statistics()

  # New recovered (delta within 24h) ???
  def get_current_new_recovered( self ):
    self.__default_query()
    self.query['where']='NeuerGenesen IN(1, -1)'
    self.__statistics_query_type_field( 'sum', 'AnzahlGenesen' )
    self.__get( 'rki covid19' )
    return self.__parse_sigle_statistics()
  
  def get_age_sex( self ):
    self.__default_query()
    self.query['where']="Geschlecht<>'unbekannt' AND Altersgruppe<>'unbekannt' AND NeuerFall IN(0, 1)"
    self.query['groupByFieldsForStatistics']='Altersgruppe,Geschlecht'
    self.query['orderByFields']='Altersgruppe asc'
    self.__statistics_query_type_field( 'sum', 'AnzahlFall' )
    self.__get( 'rki covid19' )
    self.print()

  # Count of entries per bundesland ?
  def get_BL_per_bundesland( self ):
    self.__default_query()
    self.query['groupByFieldsForStatistics']='BL'
    self.query['orderByFields']='BL asc'
    self.__statistics_query_type_field( 'count', 'BL' )
    self.__get( 'rki landkreis' )
    self.print()

  # Accumulated cases per 100000 population
  def get_cases_per_100000_per_bundesland( self ):
    self.__default_query()
    self.query['groupByFieldsForStatistics']='LAN_ew_GEN'
    self.query['orderByFields']='value desc'
    self.__statistics_query_type_field( 'max', 'faelle_100000_EW' )
    self.__get( 'rki bundesland' )
    self.print()

  def get_unknown_01( self ):
    self.__default_query()
    self.query['where']="Datum>timestamp '2020-03-01 22:59:59' AND AnzahlFall<>0"
    self.query['outFields']='FID,AnzahlFall,Datum,IstErkrankungsbeginn'
    self.query['orderByFields']='Datum asc'
    self.query['resultRecordCount']=10
    self.__get( 'rki covid19 refdate' )
    self.print()

  # Get field names by requesting 10 complete records
  def get_record_names( self ):
    self.__default_query()
    self.query['resultRecordCount']=10
    self.query['orderByFields']='cases_per_100k desc'
    self.__get( 'rki landkreis' )
    self.print()
  
  def get_record_names_rki_covid19( self ):
    self.__default_query()
    self.query['resultRecordCount']=1
    self.__get( 'rki covid19' )
    self.print()

  def check( self ):
    are_values_equal( "total cases", self.get_current_total_cases(), self.get_current_total_cases_2() )
    are_values_equal( "total deaths", self.get_current_total_deaths(), self.get_current_total_deaths_2() )
    assert abs(self.get_current_total_cases() - self.get_current_total_cases_2()) < 10
    assert abs(self.get_current_total_deaths() - self.get_current_total_deaths_2()) < 10

  def print( self ):
    self.pp.pprint( self.reply )


# Test cases
def main():
  arcgis = arcgis_hub()
  arcgis.check()
  print( "Current cases: ", arcgis.get_current_total_cases() )
  print( "Current new cases: ", arcgis.get_current_new_cases() )
  print( "Current deaths: ", arcgis.get_current_total_deaths() )
  print( "Current new deaths: ", arcgis.get_current_new_deaths() )
  print( "Current recovered: ", arcgis.get_current_total_recovered() )
  #print( "Current new recovered: ", arcgis.get_current_new_recovered() )
  
  #arcgis.get_BL_per_bundesland()
  #arcgis.get_cases_per_100000_per_bundesland()
  #arcgis.get_record_names()
  #arcgis.get_record_names_rki_covid19()
  arcgis.get_age_sex()

if __name__ == '__main__':
  main()

#EOF
