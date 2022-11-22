import requests
from datetime import date
def download_file(url, filename=''):
    req = requests.get(downloadUrl)
    today = date.today()
    try:
        if filename:
            pass
        else:
            filename = req.url[downloadUrl.rfind('/')+1:-4] + '_' + str(today) + '.csv'

        with requests.get(url) as req:
            with open(filename, 'wb') as f:
                for chunk in req.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return filename
    except Exception as e:
        print(e)
        return None

downloadUrl = 'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv'   
download_file(downloadUrl)