#!/usr/bin/python3
#-*-coding:utf-8-*-

import logging
import sys 

# print(sys.executable)

display ('input current date: ')
time = input()

log_settings = {
    'filename': '../log/log_{}'.format(time),
    'format': '%(asctime)s - %(pathname)s:%(lineno)d - ' \
        '%(levelname)s: %(message)s',
#    'mode':'a',
    'level': logging.INFO,
}


logging.basicConfig(**log_settings)



        
        
        
        
        