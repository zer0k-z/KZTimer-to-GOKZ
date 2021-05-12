import os
from os import walk
import glob
import shutil

# Variables
replay_folder = 'D:/coding/renamer'
destination = 'D:/coding/destination'

# Constants
PRO_PREFIX = '0_KZT_NRM_PRO'
NUB_PREFIX = '0_KZT_NRM_NUB'
SUFFIX = '.replay'
PRO_REPLAY_NAME = '0_KZT_NRM_PRO.replay'
NUB_REPLAY_NAME = '0_KZT_NRM_NUB.replay'

# Code
_, dirnames, _ = next(walk(replay_folder))

for map_name in dirnames:
    try:
        pro_format = '{0}/{1}/{2}*{3}'.format(replay_folder, map_name, PRO_PREFIX, SUFFIX)        
        pro_path = min(glob.glob(pro_format), key=os.path.getsize)
        os.makedirs('{0}/{1}'.format(destination, map_name), exist_ok=True)        
        shutil.copy2(pro_path,'{0}/{1}/{2}'.format(destination, map_name, PRO_REPLAY_NAME))
    except ValueError:
        pass # If there is no file, don't worry
    except shutil.SameFileError:
        pass # If it is the same file, don't worry


    try: 
        nub_format = '{0}/{1}/{2}*{3}'.format(replay_folder, map_name, NUB_PREFIX, SUFFIX)    
        nub_path = min(glob.glob(nub_format), key=os.path.getsize)
        os.makedirs('{0}/{1}'.format(destination, map_name), exist_ok=True)
        shutil.copy2(nub_path,'{0}/{1}/{2}'.format(destination, map_name, NUB_REPLAY_NAME))        
    except ValueError:
        pass
    except shutil.SameFileError:
        pass

print('Done!')