"""
    upload_file_from_url.py

    MediaWiki API Demos
    Demo of `Upload` module: Post request to upload a file from a URL

    MIT license
"""
# https://www.mediawiki.org/wiki/API:Upload

import requests

S = requests.Session()
URL = "http://localhost:8371/api.php"

# Step 1: Retrieve a login token
PARAMS_1 = {
    "action": "query",
    "meta": "tokens",
    "type": "login",
    "format": "json"
}

R = S.get(url=URL, params=PARAMS_1)
DATA = R.json()

LOGIN_TOKEN = DATA["query"]["tokens"]["logintoken"]

# Step 2: Send a post request to login. Use of main account for login is not
# supported. Obtain credentials via Special:BotPasswords
# (https://www.mediawiki.org/wiki/Special:BotPasswords) for lgname & lgpassword
PARAMS_2 = {
    "action": "login",
    "lgname": "bot_username",
    "lgpassword": "bot_password",
    "format": "json",
    "lgtoken": LOGIN_TOKEN
}

R = S.post(URL, data=PARAMS_2)

# Step 3: While logged in, retrieve a CSRF token
PARAMS_3 = {
    "action": "query",
    "meta":"tokens",
    "format":"json"
}

R = S.get(url=URL, params=PARAMS_3)
DATA = R.json()

CSRF_TOKEN = DATA["query"]["tokens"]["csrftoken"]

# Step 4: Post request to upload a file from a URL
PARAMS_4 = {
    "action": "upload",
    "filename": "new_image.jpg",
    "url": "https://farm9.staticflickr.com/8213/8300206113_374c017fc5.jpg",
    "format": "json",
    "token": CSRF_TOKEN,
    "ignorewarnings": 1
}

R = S.post(URL, data=PARAMS_4)
DATA = R.json()
print(DATA)

