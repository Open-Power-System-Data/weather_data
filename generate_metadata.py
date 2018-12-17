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
description: Hourly geographically aggregated weather data for Europe
longDescription: "This data package contains weather data relevant for power system modeling, at hourly resolution, for Europe, aggregated by Renewables.ninja from the NASA MERRA-2 reanalysis. It covers the European countries using a population-weighted mean across all MERRA-2 grid cells within the given country. It also covers Germany's NUTS-2 zones."
homepage: 'https://data.open-power-system-data.org/weather_data/{version}'
documentation: 'https://github.com/Open-Power-System-Data/weather_data/blob/{version}/main.ipynb'
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

metadata_resource_singleindex_csv = '''
profile: tabular-data-resource
name: opsd_weather_data
title: Weather Data
description: Geographically aggregated weather data
path: weather_data.csv
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
  - path: weather_data.csv
    stacking: Singleindex
    format: csv
  - path: weather_data_multiindex.csv
    stacking: Multiindex
    format: csv
#   - path: weather_data.xlsx
#     stacking: Multiindex
#     format: xlsx
schema:
    primaryKey: utc_timestamp
    missingValues: ""
    fields:
      - name: utc_timestamp
        description: Start of time period in Coordinated Universal Time
        type: datetime
        format: "fmt:%Y-%m-%dT%H%M%SZ"
        opsdContentfilter: true
'''

metadata_resource_xlsx = '''
name: opsd_weather_data
title: Weather Data (Excel file)
description: Geographically aggregated weather data (Excel file)
path: weather_data.xlsx
format: xlsx
mediatype: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
bytes: {bytes}
hash: {hash}
'''


def get_field(column):
    """``column`` is a tuple of the form (geography, variable)"""
    # HACK: units hardcoded here
    units = {
        'temperature': 'degrees C',
        'windspeed_10m': 'm/s',
        'radiation_direct_horizontal': 'W/m2',
        'radiation_diffuse_horizontal': 'W/m2',
    }

    geography, variable = column

    country = geography[0:2]

    if len(geography) == 2:
        resolution = 'Country'
    else:
        resolution = 'NUTS-2'

    unit = units[variable]

    field_template = '''
    name: {geography}_{variable}
    description: {variable} weather variable for {geography} in {unit}
    type: number (float)
    opsdProperties:
        Variable: {variable}
        Country: {country}
        Resolution: {resolution}
    '''.format(
        geography=geography,
        variable=variable,
        country=country,
        resolution=resolution,
        unit=unit
    )

    return yaml.load(field_template)


def get_resource_data(template, file_path):
    filesize_bytes = os.path.getsize(file_path)
    with open(file_path, 'rb') as f:
        file_md5_hash = hashlib.md5(f.read()).hexdigest()

    return yaml.load(
        template.format(bytes=filesize_bytes, hash=file_md5_hash)
    )


def generate_json(df, version, changes):
    '''
    Creates a datapackage.json file that complies with the Frictionless
    data JSON Table Schema from the information in the column MultiIndex.

    Parameters
    ----------
    df: pandas.DataFrame
    version: str
        Version tag of the Data Package
    changes : str
        Desription of the changes from the last version to this one.

    Returns
    -------
    None

    '''
    md_head = yaml.load(
        metadata_head.format(version=version, changes=changes)
    )

    md_resource_singleindex_csv = get_resource_data(
        metadata_resource_singleindex_csv,
        os.path.join(version, 'weather_data.csv')
    )

    # md_resource_xlsx = get_resource_data(
    #     metadata_resource_xlsx,
    #     os.path.join(version, 'weather_data.xlsx')
    # )

    fields = [get_field(col) for col in df.columns]

    metadata = md_head
    metadata['resources'] = [md_resource_singleindex_csv]  # , md_resource_xlsx]
    metadata['resources'][0]['schema']['fields'] += fields

    out_path = os.path.join(version, 'datapackage.json')
    os.makedirs(version, exist_ok=True)

    datapackage_json = json.dumps(metadata, indent=4, separators=(',', ': '))
    with open(out_path, 'w') as f:
        f.write(datapackage_json)
