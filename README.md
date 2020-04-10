# ecap-aws-transcribe

Automatic transcription job with Amazon Web Service (AWS) Transcribe

Dependencies:
* Python >= 3.7 

Setup:
1. Clone repo and enter project's root directory
1. Copy `settings.conf.example` to `settings.conf` then adjust its values accordingly
1. Install dependencies `pip install -r requirements.txt`
1. Run unit test `python -m unittest discover -v`
1. To start automatic transcription job, run command `python run.py {user_id}` (logs will be streamed to `stdout`)
1. To purge completed jobs and their associated s3 files run command `python purge.py`
