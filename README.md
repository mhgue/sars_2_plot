# sars_2_plot
A small python tool to read german SARS-CoV-2 counts from [RKI](https://www.rki.de) servers and plot them.

 1. Load and parse a HTML page to get link to spreadsheet file.
 2. Load and parse spreadsheet file to get time series of SARS-2 infections and deaths in Germany.
 3. Load latest values from different HTML page (if needed).
 4. Calculate differences and means per day and week
 5. Show plot as SVG in local browser

## SW requirements
* Data structures for "relational" or "labeled" data
    * `apt-get install python3-pandas python3-pandas-lib`
* Extract data from Microsoft Excel spreadsheet files
    * `apt-get install python3-xlrd`
* HTTP library with thread-safe connection pooling for Python3
    * `apt-get install python3-urllib3`
* Error-tolerant HTML parser for Python 3
    * `apt-get install python3-bs4`
* RFC 3986 compliant replacement for urlparse (Python 3)
    * `apt-get install python3-uritools`
* Plot to SVG
    * `apt-get install python3-pygal`

Tested and written using ubuntu linux 18.04

## Sources of data
* [RKI.de](https://www.rki.de):
    * RKI: [Fallzahlen_Kum_Tab.xlsx](https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Fallzahlen_Kum_Tab.xlsx)
    * RKI: [COVID-19: Fallzahlen in Deutschland](https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Fallzahlen.html)
    * [SurvStat@RKI](https://www.rki.de/DE/Content/Infekt/SurvStat/survstat_node.html)
* [ArcGIS Online](https://www.arcgis.com):
    * [NPGEO Corona Hub 2020](https://npgeo-corona-npgeo-de.hub.arcgis.com/)
        * [RKI COVID19](https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0)
        * [RKI Corona Landkreise](https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/917fc37a709542548cc3be077a786c17_0)
        * [RKI Corona Bundesländer](https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/ef4b445a53c1406892257fe63129a8ea_0)
        * [COVID-19 situation in the WHO European Region](https://npgeo-corona-npgeo-de.hub.arcgis.com/app/e6acbf22cc4f4b85949f59734244ba71)

----
Keywords: SARS-CoV-2 Covid-19 Corona epidemic pandemic spreadsheet html parsing plotting python BeautifulSoup pandas germany
