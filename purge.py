"""
Purge completed jobs and their associated cloud storage (AWS S3) files

@author yohanes.gultom@gmail.com
"""

import configparser
import logging
import argparse
from run import AmazonTranscribeJob

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Purge completed AWS Transcribe jobs and files')
    parser.add_argument('--log_level', help='Log level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
    args = parser.parse_args()
    log_level_dict = {
        'DEBUG': logging.DEBUG, 
        'INFO': logging.INFO, 
        'WARNING': logging.WARNING, 
        'ERROR': logging.ERROR, 
        'CRITICAL': logging.CRITICAL,
    }
    log_level = log_level_dict.get(args.log_level, logging.INFO)
    logging.basicConfig(level=log_level)

    job = AmazonTranscribeJob()
    job.purge()
    logging.info('DONE')    