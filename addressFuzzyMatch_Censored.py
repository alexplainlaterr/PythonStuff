import pandas as pd
from fuzzywuzzy import process, fuzz
from pathlib import Path
from datetime import datetime

# Import and created data frames
directory = Path(r'C:\Users\AlexWong')
file = directory/'Addresses and Locations.xlsx'
dfAddress = pd.read_excel(file, sheet_name='Addresses')
dfLocations = pd.read_excel(file, sheet_name='Locations')
dfLocations = dfLocations.loc[:, ~dfLocations.columns.str.contains('^Unnamed')]
dfLocationsDistinct = pd.DataFrame()

# Create key fields
dfAddress['CityStateZip'] = dfAddress['City'].astype(str) + dfAddress['State'].astype(str) + dfAddress['Zip'].astype(str)
dfLocationsDistinct['CityStateZip'] = dfLocations['City'].astype(str) + dfLocations['State'].astype(str) + dfLocations['Zip'].astype(str)
dfLocationsDistinct = dfLocationsDistinct.drop_duplicates()

# Remove Address that do not map to Locations sheet (via Join)
dfAddress = dfAddress.join(dfLocationsDistinct.set_index('CityStateZip'), on='CityStateZip', how='inner', rsuffix='_right')
dfAddress.reset_index(drop=True, inplace=True) # reset index for fuzzy for loop

# Fuzzy matching
dfAddress['CityStateZipStreet'] = dfAddress['City'].astype(str) + dfAddress['State'].astype(str) + dfAddress['Zip'].astype(str) + ' ' + dfAddress['Street 1'].astype(str)
dfLocations['CityStateZipStreet'] = dfLocations['City'].astype(str) + dfLocations['State'].astype(str) + dfLocations['Zip'].astype(str) + ' ' + dfLocations['Street'].astype(str)

NormalizedStreet = []
Similarity = []
row=0
for i in dfAddress['CityStateZipStreet']:
    ratio = process.extract( i, dfLocations['CityStateZipStreet'], limit=1, scorer=fuzz.token_set_ratio)
    NormalizedStreet.append(ratio[0][0])
    Similarity.append(ratio[0][1])
dfAddress['NormalizedStreet'] = pd.Series(NormalizedStreet)
dfAddress['Similarity'] = pd.Series(Similarity)

# Output results file
name = 'Addresses Normalized.xlsx'
fileOut = str(name)[:str(name).find('.')] + '_' + datetime.today().strftime('%Y%m%d_%H%M%S') + '.xlsx'
dfAddress.to_excel(directory/fileOut, index=False)