"""
Automatic audio transcription job using AWS Transcribe service https://aws.amazon.com/transcribe/

@author yohanes.gultom@gmail.com
"""

import configparser
import boto3
import os
import logging
import argparse
import time
import functools
import json
import csv


class AmazonTranscribeJob:
    def __init__(self, config=None):        
        self.config = self.parse_config() if not config else config
        self.session = boto3.session.Session(
            aws_access_key_id=self.config['aws']['access_key_id'], 
            aws_secret_access_key=self.config['aws']['secret_access_key'],
            region_name=self.config['aws']['region']
        )
        self.s3 = self.session.client('s3')
        self.transcribe = self.session.client('transcribe')

    def parse_config(self):        
        config = configparser.ConfigParser()        
        config.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'settings.conf'))
        return config

    def slugify(self, text):
        non_url_safe = ['"', '#', '$', '%', '&', '+',',', '/', ':', ';', '=', '?','@', '[', '\\', ']', '^', '`','{', '|', '}', '~', "'"]
        translate_table = {ord(char): u'' for char in non_url_safe}
        text = text.translate(translate_table)
        text = u'_'.join(text.split())
        return text

    def get_latest_audio_files(self, user_id):
        '''
        Get two audio files {user_id}.mp3 and {user_id}_p.mp3
        from {ecap_users_dir}/{user_id}/{last_modified_subdir}
        and return their path
        '''
        # get user dir
        users_dir = self.config['default']['ecap_users_dir']
        user_dir = os.path.join(users_dir, user_id)
        subdirs = []
        for d in os.listdir(user_dir):  
            subdir = os.path.join(user_dir, d)            
            if os.path.isdir(subdir):
                subdirs.append(subdir)
        # get last modified dir by mtime        
        latest_subdir = max(subdirs, key=os.path.getmtime)
        latest_subdir_slug = self.slugify(latest_subdir.split(os.path.sep)[-1])
        expected_files = {
            f'{latest_subdir_slug}_{user_id}.mp3': os.path.join(user_dir, latest_subdir, f'{user_id}.mp3'),
            f'{latest_subdir_slug}_{user_id}_p.mp3': os.path.join(user_dir, latest_subdir, f'{user_id}_p.mp3'),
        }
        for f in expected_files.values():
            if not os.path.isfile(f):
                raise FileNotFoundError(f)
        return expected_files, latest_subdir

    def upload_to_s3(self, files):
        '''
        Upload files to s3 server {s3_bucket_name} bucket        
        and return the responses
        '''
        s3_urls = []
        bucket_name = self.config['aws']['s3_bucket_name']
        for file_name, file_path in files.items():
            logging.info(f'Uploading {file_path} as {file_name}..')
            res = self.s3.upload_file(file_path, bucket_name, file_name)
            s3_urls.append(f's3://{bucket_name}/{file_name}')
        return s3_urls

    def start_transcribe_jobs(self, s3_urls, lang='en-US'):
        '''
        Start amazon transcribe jobs for given s3 URL files
        '''
        job_names = []
        output_bucket_name = self.config['aws']['s3_bucket_name']
        for url in s3_urls:
            file_name = url.split('/')[-1]            
            res = self.transcribe.start_transcription_job(
                TranscriptionJobName=file_name, 
                LanguageCode=lang, 
                Media={'MediaFileUri': url}, 
                OutputBucketName=output_bucket_name
            )
            logging.debug(res)
            status = res['TranscriptionJob']['TranscriptionJobStatus']
            job_name = res['TranscriptionJob']['TranscriptionJobName']
            if status.upper() != 'IN_PROGRESS':
                raise RuntimeError(f'IN_PROGRESS status expected but {status} found for job named {job_name}')
                logging.error(res)
            job_names.append(job_name)
        return job_names

    def wait_until_transcribe_jobs_completed(self, job_names, retry_wait=5, max_results=10):
        '''
        Get list of submitted AWS Transcribe jobs filtered by shortest job name without extension
        If any of the job's status is not COMPLETED, wait for {retry_wait} seconds and repeat the process
        This method will only return if all jobs are COMPLETED or any job is FAILED
        '''
        completed = False
        shortest_job_name = min(job_names)
        partial_job_name = shortest_job_name.split('.')[0]
        while not completed:
            res = self.transcribe.list_transcription_jobs(
                JobNameContains=partial_job_name, 
                MaxResults=max_results
            )            
            logging.debug(res)
            completed = True
            for j in res['TranscriptionJobSummaries']:                
                name = j['TranscriptionJobName']
                status = j['TranscriptionJobStatus']
                if j['TranscriptionJobStatus'] == 'FAILED':
                    logging.error(j)
                    raise RuntimeError(f'Amazon Transcribe job {name} is {status}')
                elif j['TranscriptionJobStatus'] != 'COMPLETED':
                    logging.info(f'Job {name} is not yet completed. Current status: {status}')
                    completed = False
                    break
            if not completed:
                logging.info(f'Not yet completed. Waiting for {retry_wait} seconds...')
                time.sleep(retry_wait)
        logging.info('All jobs are completed!')
        return job_names

    def download_transcribe_results(self, results, user_subdir):
        '''
        Download result files form S3 and save them in user subdir with different name
        '''
        output_files = []
        user_subdir_slug = self.slugify(user_subdir.split(os.path.sep)[-1])
        output_bucket_name = self.config['aws']['s3_bucket_name']
        for job_name in results:
            # expected output_name: "{USER_ID}_AT.json" and "{USER_ID}_P_AT.json"
            output_name = job_name.replace(user_subdir_slug + '_', '').replace('.mp3', '').upper() + '_AT.json'
            output_path = os.path.join(user_subdir, output_name)
            self.s3.download_file(
                output_bucket_name, 
                f'{job_name}.json', 
                output_path
            )
            output_files.append(output_path)
            logging.info(f'Transcribe result successfully downloaded {output_path}')
        return output_files

    def generate_reports(self, output_files):
        '''
        Generate reports:
        1. NNNN_transcript.txt: transcript from {NNNN}_AT.json        
        2. AI pronunciation.csv:
            1. pron_transcript average (average confidence score from {NNNN}_AT.json)
            2. pron_transcript score (SMF scaled value of pron_transcript average)
            3. pron_words average (average confidence score from {NNNN}_P_AT.json)
            4. pron_words score (SMF scaled value of pron_words average)
        '''
        result_file = min(output_files)
        result_p_file = max(output_files)
        result_dir = os.path.dirname(result_p_file)

        # transcript text
        user_id = os.path.basename(result_file).split('_')[0]
        transcript_file = os.path.join(result_dir, f'{user_id}_transcript_AT.TXT')
        with open(result_file) as infile, open(transcript_file, mode='w+') as outfile:
            data = json.load(infile)
            for res in data['results']['transcripts']:
                outfile.write(res['transcript'] + '\n')

        def smf(x, a, b):
            '''
            S-shaped membership function starting to climb at a and leveling off at b
            https://www.mathworks.com/help/fuzzy/smf.html
            '''
            assert a <= b, 'a <= b is required.'
            if x <= a:
                return 0.0
            elif a <= x and x <= (a+b)/2.0:
                return 2 * ((x-a)/(b-a)) ** 2
            elif (a+b)/2.0 <= x and x <= b:
                return 1.0 - 2.0 * ((x-b)/(b-a)) ** 2
            else: # x >= b
                return 1.0
            return y

        def get_confidence_average(data):
            total = 0
            for item in data['results']['items']:
                confidence = [float(alt['confidence']) for alt in item['alternatives']]                                
                total += (sum(confidence) / len(confidence))
            return total / len(data['results']['items'])

        # metrics report        
        report_path = os.path.join(result_dir, 'AI pronunciation.csv')        
        with open(result_file) as transcript_file, open(result_p_file) as words_file, open(report_path, mode='w+') as f:
            # compute transcript score           
            transcript_data = json.load(transcript_file)
            transcript_average = get_confidence_average(transcript_data)
            ta = float(self.config['report']['pron_transcript_score_smf_a'])
            tb = float(self.config['report']['pron_transcript_score_smf_b'])
            transcript_score = smf(transcript_average, ta, tb) * 100.0

            # compute words score
            words_data = json.load(words_file)
            words_average = get_confidence_average(words_data)
            wa = float(self.config['report']['pron_words_score_smf_a'])
            wb = float(self.config['report']['pron_words_score_smf_b'])
            words_score = smf(words_average, wa, wb) * 100.0

            # write scores
            w = csv.writer(f, delimiter=',', quotechar='"')
            w.writerow(['pron_transcript average', 'pron_transcript score', 'pron_words average', 'pron_words score'])
            w.writerow([transcript_average, transcript_score, words_average, words_score])

    def purge(self):
        '''
        Delete completed jobs and files
        '''
        output_bucket_name = self.config['aws']['s3_bucket_name']
        res = self.transcribe.list_transcription_jobs(Status='COMPLETED')
        logging.debug(res)
        if not res['TranscriptionJobSummaries']:
            logging.info('No completed job found')
        for j in res['TranscriptionJobSummaries']:                
            job_name = j['TranscriptionJobName']
            logging.info(f'Deleting job and file {job_name}..')
            delete_job_res = self.transcribe.delete_transcription_job(TranscriptionJobName=job_name)
            deletes = {'Objects': [{'Key': job_name}, {'Key': job_name+'.json'}]}
            delete_files_res = self.s3.delete_objects(Bucket=output_bucket_name, Delete=deletes)

    def start(self, user_id):
        '''
        Run automatic transcription job and generate reports
        '''
        audio_files, user_subdir = self.get_latest_audio_files(user_id)
        s3_urls = self.upload_to_s3(audio_files)
        job_names = self.start_transcribe_jobs(s3_urls)

        # estimate waiting time
        max_size_mb = max([os.stat(p).st_size / 1000000.0 for n, p in audio_files.items()])
        retry_wait = 22.0 * max_size_mb
        logging.info(f'Biggest file size is {max_size_mb} MB. Waiting for {retry_wait} seconds before checking job status..')
        time.sleep(retry_wait)

        completed_jobs = self.wait_until_transcribe_jobs_completed(job_names)
        output_files = self.download_transcribe_results(completed_jobs, user_subdir)
        users_dir = self.config['default']['ecap_users_dir']
        self.generate_reports(output_files)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run automatic transcription using AWS Transcribe')
    parser.add_argument('user_id', help='ECAP User ID')
    parser.add_argument('--log_level', help='Log level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
    args = parser.parse_args()
    log_level_dict = {
        'DEBUG': logging.DEBUG, 
        'INFO': logging.INFO, 
        'WARNING': logging.WARNING, 
        'ERROR': logging.ERROR, 
        'CRITICAL': logging.CRITICAL,
    }
    user_id = args.user_id
    log_level = log_level_dict.get(args.log_level, logging.INFO)
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # run job
    job = AmazonTranscribeJob()
    job.start(user_id)
    logging.info('DONE')    
