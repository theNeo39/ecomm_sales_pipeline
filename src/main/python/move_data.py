import time,glob,os
import shutil
import re
src_path='/mnt/e/output/**/*'
trg_path='/mnt/e/ecomm_sales_pipline/data/'

days = glob.glob(src_path,recursive='true')
count=0
for day in days:
    if re.search('.csv$',day):
        print('logging sales data- {} to data folder'.format(count))
        shutil.copy(day,trg_path+str(count)+'.csv')
        count=count+1
        time.sleep(60)

