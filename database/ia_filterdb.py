import logging
import re
import base64
from struct import pack
from pyrogram.file_id import FileId
from pymongo.errors import DuplicateKeyError
from umongo import Instance, Document, fields
from motor.motor_asyncio import AsyncIOMotorClient
from marshmallow.exceptions import ValidationError
from info import DATABASE_URI, DATABASE_NAME, COLLECTION_NAME, USE_CAPTION_FILTER, MAX_B_TN, SECONDDB_URI
from utils import get_settings, save_group_settings
from sample_info import tempDict 

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Global variable for selected media database
saveMedia = None

# Primary database setup
primary_client = AsyncIOMotorClient(DATABASE_URI)
primary_db = primary_client[DATABASE_NAME]
primary_instance = Instance.from_db(primary_db)

# Secondary database setup
secondary_client = AsyncIOMotorClient(SECONDDB_URI)
secondary_db = secondary_client[DATABASE_NAME]
secondary_instance = Instance.from_db(secondary_db)

# Media document class for primary database
@primary_instance.register
class Media(Document):
    file_id = fields.StrField(attribute='_id')
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)

    class Meta:
        indexes = ('$file_name',)
        collection_name = COLLECTION_NAME

# Media document class for secondary database
@secondary_instance.register
class Media2(Document):
    file_id = fields.StrField(attribute='_id')
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)

    class Meta:
        indexes = ('$file_name',)
        collection_name = COLLECTION_NAME

async def choose_mediaDB():
    """
    Chooses which database to use based on the value of indexDB key in tempDict.
    Sets the global saveMedia variable to either Media (primary) or Media2 (secondary).
    """
    global saveMedia
    if tempDict['indexDB'] == DATABASE_URI:
        logger.info("Using primary database (Media)")
        saveMedia = Media
    else:
        logger.info("Using secondary database (Media2)")
        saveMedia = Media2

async def save_file(media):
    """
    Saves a media file in the appropriate database after checking for duplicates.
    """
    file_id, file_ref = unpack_new_file_id(media.file_id)
    file_name = re.sub(r"(_|\-|\.|\+)", " ", str(media.file_name))
    
    # Check if the file already exists in the primary database
    if await Media.count_documents({'file_id': file_id}, limit=1):
        logger.warning(f'{getattr(media, "file_name", "NO_FILE")} is already saved in the primary database!')
        return False, 0

    file_data = saveMedia(
        file_id=file_id,
        file_ref=file_ref,
        file_name=file_name,
        file_size=media.file_size,
        file_type=media.file_type,
        mime_type=media.mime_type,
        caption=media.caption.html if media.caption else None,
    )
    
    try:
        await file_data.commit()
    except DuplicateKeyError:
        logger.warning(f'{getattr(media, "file_name", "NO_FILE")} is already saved in the database')
        return False, 0
    except ValidationError:
        logger.exception('Validation error occurred while saving file in the database')
        return False, 2
    else:
        logger.info(f'{getattr(media, "file_name", "NO_FILE")} is saved to the database')
        return True, 1

async def get_search_results(chat_id, query, file_type=None, max_results=10, offset=0, filter=False):
    """
    Search for media files based on the query.
    Supports pagination and filtering by file type.
    """
    if chat_id is not None:
        settings = await get_settings(int(chat_id))
        max_results = int(MAX_B_TN) if not settings.get('max_btn', False) else 10

    query = query.strip()
    raw_pattern = r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])' if ' ' not in query else query.replace(' ', r'.*[\s\.\+\-_()]')
    
    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error:
        return []

    filter_conditions = {'file_name': regex}
    if USE_CAPTION_FILTER:
        filter_conditions = {'$or': [{'file_name': regex}, {'caption': regex}]}
    if file_type:
        filter_conditions['file_type'] = file_type

    total_results = (await Media.count_documents(filter_conditions)) + (await Media2.count_documents(filter_conditions))

    if max_results % 2 != 0:
        max_results += 1

    cursor = Media.find(filter_conditions).sort('$natural', -1)
    cursor2 = Media2.find(filter_conditions).sort('$natural', -1).skip(offset).limit(max_results)
    fileList2 = await cursor2.to_list(length=max_results)

    if len(fileList2) < max_results:
        next_offset = offset + len(fileList2)
        cursor_skip = max(0, next_offset - await Media2.count_documents(filter_conditions))
        cursor.skip(cursor_skip).limit(max_results - len(fileList2))
        fileList1 = await cursor.to_list(length=max_results - len(fileList2))
        files = fileList2 + fileList1
        next_offset += len(fileList1)
    else:
        files = fileList2
        next_offset = offset + max_results

    return files, next_offset if next_offset < total_results else '', total_results

async def get_bad_files(query, file_type=None, filter=False):
    """
    Retrieve files that match a certain query, typically used to identify problematic files.
    """
    query = query.strip()
    raw_pattern = r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])' if ' ' not in query else query.replace(' ', r'.*[\s\.\+\-_()]')
    
    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error:
        return []

    filter_conditions = {'file_name': regex}
    if USE_CAPTION_FILTER:
        filter_conditions = {'$or': [{'file_name': regex}, {'caption': regex}]}
    if file_type:
        filter_conditions['file_type'] = file_type

    cursor = Media.find(filter_conditions).sort('$natural', -1)
    cursor2 = Media2.find(filter_conditions).sort('$natural', -1)
    files = await cursor2.to_list(length=await Media2.count_documents(filter_conditions)) + await cursor.to_list(length=await Media.count_documents(filter_conditions))

    return files, len(files)

async def get_file_details(query):
    """
    Retrieve details of a file by its file_id.
    """
    filter_conditions = {'file_id': query}
    filedetails = await Media.find(filter_conditions).to_list(length=1)
    if not filedetails:
        filedetails = await Media2.find(filter_conditions).to_list(length=1)
    return filedetails

def encode_file_id(s: bytes) -> str:
    """
    Encodes a byte sequence into a file ID using base64.
    """
    r = b""
    n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0
            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def encode_file_ref(file_ref: bytes) -> str:
    """
    Encodes a byte sequence into a file reference using base64.
    """
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")

def unpack_new_file_id(new_file_id):
    """
    Decodes a new file ID into its components: file_id and file_ref.
    """
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(pack("<iiqq", int(decoded.file_type), decoded.dc_id, decoded.media_id, decoded.access_hash))
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref
