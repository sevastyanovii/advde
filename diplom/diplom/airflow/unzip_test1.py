import os
import shutil
import zipfile
import tempfile

# my_dir = r"C:\Users\vanio\temp"
my_zip = r"C:\Users\vanio\temp\JC-202109-citibike-tripdata.csv.zip"


def create_temp_file():
    tmpfile = tempfile.NamedTemporaryFile(prefix='aws')
    tmpfile.close()
    return tmpfile.name


def unzip_file(zf):
    if zipfile.is_zipfile(zf):
        with zipfile.ZipFile(zf) as zip_file:
            for member in zip_file.namelist():
                filename = os.path.basename(member)
                # skip directories
                if not filename:
                    continue

                if member.endswith(".csv") and not member.startswith(".") and member.find("/") < 0:
                    # copy file (taken from zipfile's extract)
                    source = zip_file.open(member)
                    unzipped = create_temp_file()
                    target = open(unzipped, "wb")
                    with source, target:
                        shutil.copyfileobj(source, target)
                    print(f'unzipped {member} into {unzipped}')
                    return unzipped
                print(f'File "{zf}" is not contain data. Will be skipped...')
            else:
                print(f'File "{zf}" is not zipped. Will be skipped...')
            raise Air


unzipped = unzip_file(my_zip)

final_target = r'C:\Users\vanio\temp\unzipped-copy.csv'
source = open(unzipped, 'rb')

if os.path.exists(final_target):
    target = open(final_target, 'wb')
else:
    target = open(final_target, 'xb')

with source, target:
    shutil.copyfileobj(source, target)
