import unittest
import configparser
import os
import datetime
from unittest import mock
from run import AmazonTranscribeJob
 
class TestAmazonTranscribeJob(unittest.TestCase):

    def setUp(self):
        config = configparser.ConfigParser()
        config['aws'] = {
            'region': 'us-east-2',
            'access_key_id': 'ZZZZZZZZZZZZZZZZZZZZ',
            'secret_access_key': 'SSSSSSSSSSSSSSSSSSSSSSSSSS',
            's3_bucket_name': 'ecap-transcribe',
        }
        config['default'] = {
            'ecap_users_dir': os.path.join('C:\\', 'ECAP', 'USERS'),
        }
        self.app = AmazonTranscribeJob(config)
        self.config = config

    @mock.patch('run.os')    
    def test_get_audio_files(self, mock_os):
        # data
        user_id = '1000'
        users_dir = self.config['default']['ecap_users_dir']
        subdirs = ['2020_01_01', '2020_01_02']
        subdirs_mtimes = [1, 2]
        latest_subdir = subdirs[subdirs_mtimes.index(max(subdirs_mtimes))]        
        expected = {
            f'{latest_subdir}_{user_id}.mp3': os.path.join(users_dir, user_id, latest_subdir, f'{user_id}.mp3'), 
            f'{latest_subdir}_{user_id}_p.mp3': os.path.join(users_dir, user_id, latest_subdir, f'{user_id}_p.mp3'),
        }
        # mock
        mock_os.path.sep = os.path.sep
        mock_os.listdir.return_value = subdirs
        mock_os.path.join.side_effect = os.path.join
        mock_os.path.isdir.return_value = True
        mock_os.path.getmtime.side_effect = subdirs_mtimes
        mock_os.path.isfile.side_effect = [True, True]
        actual, actual_subdir = self.app.get_latest_audio_files(user_id)
        self.assertEqual(actual, expected)
        self.assertEqual(actual_subdir, os.path.join(users_dir, user_id, latest_subdir))

    def test_upload_to_s3(self):
        user_id = '0001'
        users_dir = self.config['default']['ecap_users_dir']
        user_dir = os.path.join(users_dir, user_id)
        files = {
            f'2020_01_01_{user_id}.mp3': os.path.join(user_dir, f'{user_id}.mp3'),
            f'2020_01_01_{user_id}_p.mp3': os.path.join(user_dir, f'{user_id}_p.mp3'),
        }
        bucket_name = self.config['aws']['s3_bucket_name']
        expected = [f's3://{bucket_name}/{file_name}' for file_name in files.keys()]
        self.app.s3 = mock.Mock()
        self.app.s3.upload_file.return_value = None
        actual = self.app.upload_to_s3(files)
        self.assertEqual(actual, expected)


    def test_start_transcribe_jobs(self):
        responses = [{'TranscriptionJob': {'TranscriptionJobName': '4_07_12_20_3486.mp3', 'TranscriptionJobStatus': 'IN_PROGRESS', 'LanguageCode': 'en-US', 'Media': {'MediaFileUri': 's3://ecap-transcribe/4_07_12_20_3486.mp3'}, 'StartTime': datetime.datetime(2020, 4, 8, 20, 25, 48, 691000), 'CreationTime': datetime.datetime(2020, 4, 8, 20, 25, 48, 658000)}, 'ResponseMetadata': {'RequestId': 'a7a38c7f-8e8e-4f55-a8b7-28271b9f4426', 'HTTPStatusCode': 200, 'HTTPHeaders': {'content-type': 'application/x-amz-json-1.1', 'date': 'Wed, 08 Apr 2020 13:25:48 GMT', 'x-amzn-requestid': 'a7a38c7f-8e8e-4f55-a8b7-28271b9f4426', 'content-length': '258', 'connection': 'keep-alive'}, 'RetryAttempts': 0}}, {'TranscriptionJob': {'TranscriptionJobName': '4_07_12_20_3486_p.mp3', 'TranscriptionJobStatus': 'IN_PROGRESS', 'LanguageCode': 'en-US', 'Media': {'MediaFileUri': 's3://ecap-transcribe/4_07_12_20_3486_p.mp3'}, 'StartTime': datetime.datetime(2020, 4, 8, 20, 25, 49, 86000), 'CreationTime': datetime.datetime(2020, 4, 8, 20, 25, 49, 68000)}, 'ResponseMetadata': {'RequestId': '004efdf1-8391-49d9-9146-017cb62bd684', 'HTTPStatusCode': 200, 'HTTPHeaders': {'content-type': 'application/x-amz-json-1.1', 'date': 'Wed, 08 Apr 2020 13:25:49 GMT', 'x-amzn-requestid': '004efdf1-8391-49d9-9146-017cb62bd684', 'content-length': '262', 'connection': 'keep-alive'}, 'RetryAttempts': 0}}]
        self.app.transcribe.start_transcription_job = mock.Mock(side_effect=responses)
        s3_urls = [res['TranscriptionJob']['Media']['MediaFileUri'] for res in responses]
        actual = self.app.start_transcribe_jobs(s3_urls)
        expected = [res['TranscriptionJob']['TranscriptionJobName'] for res in responses]
        self.assertEqual(actual, expected)

    def test_wait_until_transcribe_jobs_completed(self):
        response = {'TranscriptionJobSummaries': [{'TranscriptionJobName': '4_07_12_20_3486_p.mp3', 'CreationTime': datetime.datetime(2020, 4, 8, 20, 25, 49, 68000), 'StartTime': datetime.datetime(2020, 4, 8, 20, 25, 49, 86000), 'CompletionTime': datetime.datetime(2020, 4, 8, 20, 27, 29, 836000), 'LanguageCode': 'en-US', 'TranscriptionJobStatus': 'COMPLETED', 'OutputLocationType': 'CUSTOMER_BUCKET'}, {'TranscriptionJobName': '4_07_12_20_3486.mp3', 'CreationTime': datetime.datetime(2020, 4, 8, 20, 25, 48, 658000), 'StartTime': datetime.datetime(2020, 4, 8, 20, 25, 48, 691000), 'CompletionTime': datetime.datetime(2020, 4, 8, 20, 27, 26, 842000), 'LanguageCode': 'en-US', 'TranscriptionJobStatus': 'COMPLETED', 'OutputLocationType': 'CUSTOMER_BUCKET'}], 'ResponseMetadata': {'RequestId': '7c2cec46-e584-4779-8e51-812fe22ffc0d', 'HTTPStatusCode': 200, 'HTTPHeaders': {'content-type': 'application/x-amz-json-1.1', 'date': 'Thu, 09 Apr 2020 12:12:52 GMT', 'x-amzn-requestid': '7c2cec46-e584-4779-8e51-812fe22ffc0d', 'content-length': '515', 'connection': 'keep-alive'}, 'RetryAttempts': 0}}
        self.app.transcribe.list_transcription_jobs = mock.Mock(return_value=response)
        job_names = ['4_07_12_20_3486.mp3', '4_07_12_20_3486_p.mp3']        
        actual = self.app.wait_until_transcribe_jobs_completed(job_names)        
        self.assertEqual(actual, job_names)

    def test_download_transcribe_results(self):
        completed_jobs = ['4_07_12_20_3486.mp3', '4_07_12_20_3486_p.mp3']
        users_dir = self.config['default']['ecap_users_dir']
        user_subdir = os.path.join(users_dir, '4_07_12_20')
        self.app.s3.download_file = mock.Mock(side_effect=[True, True])
        actual = self.app.download_transcribe_results(completed_jobs, user_subdir)    
        expected = [
            os.path.join(user_subdir, '3486_AT.json'),
            os.path.join(user_subdir, '3486_P_AT.json'),
        ]
        self.assertEqual(actual, expected)

if __name__ == '__main__':
    unittest.main()