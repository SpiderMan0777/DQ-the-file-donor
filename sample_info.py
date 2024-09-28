from info import DATABASE_URI

# Bot information
SESSION = 'Media_search'
USER_SESSION = 'Spidey_gaming_official_bot'
API_ID = 28519661
API_HASH = 'd47c74c8a596fd3048955b322304109d'
BOT_TOKEN = '6728052880:AAHxKrRYBp2GRH_bXbybZZZolktFRwupKVI'
USERBOT_STRING_SESSION = ''

# Bot settings
CACHE_TIME = 300
USE_CAPTION_FILTER = False

# Admins, Channels & Users
ADMINS = [5518489725, 'SPIDEY_OFFICIAL_777', 5518489725]
CHANNELS = [-1001959922658, -1001959922658, 'SPIDEY OFFICIAL']
AUTH_USERS = []
AUTH_CHANNEL = None

# MongoDB information
DATABASE_NAME = 'Telegram'
COLLECTION_NAME = 'channel_files'  # If you are using the same database, then use different collection name for each bot

#temp dict for storing the db uri which will be used for storing user, chat and file infos
tempDict = {'indexDB': DATABASE_URI}
