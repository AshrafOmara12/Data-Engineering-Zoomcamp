import wget
import os



def download_files(url: str, service: str):

    filename = wget.download(url, out=f'{service}')
    return filename

def check_file(input: str, file_name):
    if os.path.isfile(f"{input}/{file_name}"):
        print(f'file already exit: {file_name}')
        return True
    else:
        print(input, year, month)
        print(f'start downloading file: {file_name}')
        try:
            download_files(url, input)
        except:
            print(f"{file_name} Not Found")
        return False

def data_to_local():
    data = ['fhv', 'yellow', 'green']
    for input in data:
        for year in range(2019, 2022):
            for month in range(1,13):
                file_name = f"{input}_tripdata_{year}-{month:02d}.csv.gz"
                url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{input}/{file_name}"
                if not os.path.isdir(f'{input}'):
                    os.mkdir(f'{input}')
                    if check_file(input, file_name):
                        continue
                    else:
                        check_file(input, file_name)
                else:
                    check_file(input, file_name)
