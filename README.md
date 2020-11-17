# sars_2_plot
A small python tool to read german SARS-CoV-2 counts from RKI servers and plot them.

1.) Load and parse a HTML page to get link to spreadsheet file.
2.) Load and parse spreadsheet file to get time series of SARS-2 infections and deaths in Germany.
3.) Load latest values from different HTML page (if needed).
4.) Calculate differences and means per day and week
5.) Show plot as SVG in local browser

## SW requirements
* Data structures for "relational" or "labeled" data
   apt-get install python3-pandas python3-pandas-lib
* Extract data from Microsoft Excel spreadsheet files
   apt-get install python3-xlrd
* HTTP library with thread-safe connection pooling for Python3
   apt-get install python3-urllib3
* Error-tolerant HTML parser for Python 3
   apt-get install python3-bs4
* RFC 3986 compliant replacement for urlparse (Python 3)
   apt-get install python3-uritools
* Plot to SVG
   apt-get install python3-pygal

Tested and written using ubuntu linux 18.04

----
Keywords: SARS-CoV-2 Covid-19 Corona epidemic pandemic spreadsheet html parsing plotting
