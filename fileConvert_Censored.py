import pandas as pd
import numpy as np
import sys
from pathlib import Path
from datetime import datetime

### Load data into dataframe
directory = Path(str(sys.argv[1]))
file = directory/str(sys.argv[2])
df = pd.read_csv(file)
df_og = df.copy()
loca = pd.DataFrame({'City': ['Mississauga', 'Milton', 'Toronto'],
                     'Code': ['MI', 'ML', 'TO']})

### Measure tracking number length
df['TrackingLen'] = df['TrackingNumber'].astype(str).map(len)

### Get first 2 characters of tracking number
df['Carrier2'] = df['TrackingNumber'].astype(str).str[:2]

### Assign correct carrier
df.loc[df['Carrier2'] == '1Z', 'CarrierNew'] = 'UPS'
df.loc[(df['TrackingLen'] >= 20) & (df['TrackingLen'] <= 22), 'CarrierNew'] = 'USP'
df.loc[(df['TrackingLen'] >= 12) & (df['TrackingLen'] <= 15), 'CarrierNew'] = 'FDX'
df.loc[(df['TrackingLen'] == 16), 'CarrierNew'] = 'CNP'

### Update static names for PLD_SOURCE and SHIPPER_CUSTCODE
df['PldSourceNew'] = 'COMPANY'
df['ShipperCustcodeNew'] = 'CMPY'

### Update ID_SHIPPER
df = df.join(loca.set_index('City'), on='SHIPPER_CITY')
df['ShipperIdNew'] = df['ID_SHIPPER']
if len(sys.argv) > 3:
    df.loc[df['ID_SHIPPER'].isna(), 'ShipperIdNew'] = 'CMPY' + df['CarrierNew'] + str(sys.argv[3])
    df.loc[df['ID_SHIPPER'].astype(str).map(len) <= 4, 'ShipperIdNew'] = 'CMPY' + df['CarrierNew'] + str(sys.argv[3])
else:
    df.loc[df['ID_SHIPPER'].isna(), 'ShipperIdNew'] = 'CMPY' + df['CarrierNew'] + df['Code']
    df.loc[df['ID_SHIPPER'].astype(str).map(len) <= 4, 'ShipperIdNew'] = 'CMPY' + df['CarrierNew'] + df['Code']

### Update columns
df['CARRIER'] = df['CarrierNew']
df['PLD_SOURCE'] = df['PldSourceNew']
df['SHIPPER_CUSTCODE'] = df['ShipperCustcodeNew']
df['ID_SHIPPER'] = df['ShipperIdNew']

### Drop columns
df = df.drop(columns=['TrackingLen', 'CarrierNew', 'Carrier2', 'PldSourceNew',
                      'ShipperCustcodeNew', 'Code', 'ShipperIdNew'])
fileOut = str(sys.argv[2])[:str(sys.argv[2]).find('.')] + '_NEW_' + datetime.today().strftime('%Y%m%d') + '.ILX'
df.to_csv(directory/fileOut, index=False)

### Validation - Validate the the process didn't break anything
validation = pd.DataFrame(index=['Records', 'Columns', 'Unique Tracking Numbers',
                                'Carrier Distinct', 'PLD Source Distinct',
                                'Shipper Custcode Distinct', 'ID Shipper Distinct'],
                        columns=['Original', 'New'])
validation.loc['Records', 'Original'] = len(df_og)
validation.loc['Records', 'New'] = len(df)
validation.loc['Columns', 'Original'] = len(df_og.columns)
validation.loc['Columns', 'New'] = len(df.columns)
validation.loc['Unique Tracking Numbers', 'Original'] = len(df_og['TrackingNumber'].unique())
validation.loc['Unique Tracking Numbers', 'New'] = len(df['TrackingNumber'].unique())
validation.loc['Carrier Distinct', 'Original'] = df_og['CARRIER'].unique()
validation.loc['Carrier Distinct', 'New'] = df['CARRIER'].unique()
validation.loc['PLD Source Distinct', 'Original'] = df_og['PLD_SOURCE'].unique()
validation.loc['PLD Source Distinct', 'New'] = df['PLD_SOURCE'].unique()
validation.loc['Shipper Custcode Distinct', 'Original'] = df_og['SHIPPER_CUSTCODE'].unique()
validation.loc['Shipper Custcode Distinct', 'New'] = df['SHIPPER_CUSTCODE'].unique()
validation.loc['ID Shipper Distinct', 'Original'] = df_og['ID_SHIPPER'].unique()
validation.loc['ID Shipper Distinct', 'New'] = df['ID_SHIPPER'].unique()

valFileOut = str(sys.argv[2])[:str(sys.argv[2]).find('.')] + '_VALIDATION_' + datetime.today().strftime('%Y%m%d') + '.txt'
validation.to_csv(directory/valFileOut, sep='\t', index=False)
with open(directory/valFileOut,'w') as outfile:
    validation.to_string(outfile, columns=validation.columns)