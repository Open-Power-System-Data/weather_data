import os
import json
import yaml
import hashlib


country_map = {
    'BE': 'Belgium',
    'BG': 'Bulgaria',
    'CZ': 'Czech Republic',
    'DK': 'Denmark',
    'DE': 'Germany',
    'EE': 'Estonia',
    'IE': 'Ireland',
    'GR': 'Greece',
    'ES': 'Spain',
    'FR': 'France',
    'IT': 'Italy',
    'CY': 'Cyprus',
    'LV': 'Latvia',
    'LT': 'Lithuania',
    'LU': 'Luxembourg',
    'HU': 'Hungary',
    'MT': 'Malta',
    'NL': 'Netherlands',
    'AT': 'Austria',
    'PL': 'Poland',
    'PT': 'Portugal',
    'RO': 'Romania',
    'SI': 'Slovenia',
    'SK': 'Slovakia',
    'FI': 'Finland',
    'SE': 'Sweden',
    'GB': 'Great Britain',
    'CH': 'Switzerland',
    'GR': 'Greece',
    'NO': 'Norway',
    'ME': 'Montenegro',
    'MD': 'Moldova',
    'RS': 'Serbia',
    'HR': 'Croatia',
    'AL': 'Albania',
    'MK': 'Macedonia',
    'BA': 'Bosnia and Herzegovina',
}


metadata_head = '''
profile: tabular-data-package
name: opsd_weather_data
title: Weather Data
description: Hourly country-aggregated weather data for Europe
longDescription: 'This data package contains weather data relevant for power system modeling, at hourly resolution, for all European countries, from the NASA MERRA-2 reanalysis, aggregated to countries by Renewables.ninja, using population-weighted mean across all MERRA-2 grid cells within the given country.'
homepage: 'https://data.open-power-system-data.org/weather_data/{version}'
documentation: 'https://github.com/Open-Power-System-Data/datapackage_weather_data/blob/{version}/main.ipynb'
version: '{version}'
created: '{version}'
lastChanges: '{changes}'
# license:
#     name:
#     path:
#     title:
keywords:
  - Open Power System Data
  - time series
  - power systems
  - weather
  - MERRA-2
  - Renewables.ninja
geographicalScope: Europe
temporalScope:
    start: "1980-01-01"
    end: "2016-12-31"
contributors:
  - web: https://www.pfenninger.org/
    name: Stefan Pfenninger
    email: stefan.pfenninger@usys.ethz.ch
    organization: ETH Zürich
    role: author
  - web: https://www.imperial.ac.uk/people/i.staffell
    name: Iain Staffell
    email: i.staffell@imperial.ac.uk
    organization: Imperial College London
    role: author
sources:
  - name: NASA
    web: https://gmao.gsfc.nasa.gov/reanalysis/MERRA-2/
  - name: Renewables.ninja
    web: https://www.renewables.ninja/#/country
resources:
'''

metadata_resource = '''
profile: tabular-data-resource
name: opsd_weather_data_countries
title: Weather Data - country-aggregated
description: All available country-aggregated weather data
path: weather_data_singleindex.csv
format: csv
mediatype: text/csv
encoding: UTF8
bytes: {bytes}
hash: {hash}
dialect:
    csvddfVersion: 1.0
    delimiter: ","
    lineTerminator: "\\n"
    header: true
alternativeFormats:
  - path: weather_data_singleindex.csv
    stacking: Singleindex
    format: csv
  - path: weather_data_multiindex.csv
    stacking: Multiindex
    format: csv
schema:
    primaryKey: time
    missingValues: ""
    fields:
      - name: time
        description: Start of time period in Coordinated Universal Time
        type: datetime
        format: "fmt:%Y-%m-%dT%H%M%SZ"
        opsdContentfilter: true
'''


def get_field(column):
    """``column`` is a tuple of the form (countrycode, subcountrycode, variablename)"""
    countrycode, subcountrycode, variable = column

    field_template = '''
    name: {region}_{variable}_{attribute}
    description: {variable} weather variable for {attribute} in {country}
    type: number (float)
    opsdProperties:
        Region: {region}
        Variable: {variable}
        Attribute: {attribute}
    '''.format(
        region=countrycode,
        country=country_map[countrycode],
        variable=variable,
        attribute=subcountrycode,
    )

    return yaml.load(field_template)


def generate_json(df, version, changes):
    '''
    Creates a datapackage.json file that complies with the Frictionless
    data JSON Table Schema from the information in the column MultiIndex.

    Parameters
    ----------
    df: pandas.DataFrame
        A dict with keys '15min' and '60min' and values the respective
        DataFrames
    version: str
        Version tag of the Data Package
    changes : str
        Desription of the changes from the last version to this one.

    Returns
    ----------
    None

    '''
    md_head = yaml.load(
        metadata_head.format(version=version, changes=changes)
    )

    md_resource_path = os.path.join(version, 'weather_data_singleindex.csv')
    filesize_bytes = os.path.getsize(md_resource_path)
    with open(md_resource_path, 'rb') as f:
        file_md5_hash = hashlib.md5(f.read()).hexdigest()

    md_resource = yaml.load(
        metadata_resource.format(bytes=filesize_bytes, hash=file_md5_hash)
    )

    fields = [get_field(col) for col in df.columns]

    metadata = md_head
    metadata['resources'] = [md_resource]
    metadata['resources'][0]['schema']['fields'] += fields

    out_path = os.path.join(version, 'datapackage.json')
    os.makedirs(version, exist_ok=True)

    datapackage_json = json.dumps(metadata, indent=4, separators=(',', ': '))
    with open(out_path, 'w') as f:
        f.write(datapackage_json)
