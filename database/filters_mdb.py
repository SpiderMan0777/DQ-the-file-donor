import pymongo
from info import DATABASE_URI, DATABASE_NAME, SECONDDB_URI
from pyrogram import enums
import logging
from sample_info import tempDict

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# Initialize MongoDB clients and databases
myclient = pymongo.MongoClient(DATABASE_URI)
mydb = myclient[DATABASE_NAME]

myclient2 = pymongo.MongoClient(SECONDDB_URI)
mydb2 = myclient2[DATABASE_NAME]


# Function to add a filter to the database
async def add_filter(grp_id, text, reply_text, btn, file, alert):
    """Adds or updates a filter for a group."""
    
    # Determine which DB to use based on the indexDB value
    if tempDict['indexDB'] == DATABASE_URI:
        mycol = mydb[str(grp_id)]
    else:
        mycol = mydb2[str(grp_id)]

    # Filter data to be inserted/updated
    data = {
        'text': str(text),
        'reply': str(reply_text),
        'btn': str(btn),
        'file': str(file),
        'alert': str(alert)
    }

    try:
        # Update the filter or insert if it doesn't exist
        mycol.update_one({'text': str(text)}, {"$set": data}, upsert=True)
    except Exception as e:
        logger.exception(f"Error adding filter for group {grp_id}: {e}", exc_info=True)


# Function to find a filter in the database
async def find_filter(group_id, name):
    """Finds a filter by its name in the group."""
    mycol = mydb[str(group_id)]
    mycol2 = mydb2[str(group_id)]
    
    try:
        query = mycol.find_one({"text": name})
        if query:
            return query['reply'], query['btn'], query.get('alert', None), query['file']
    except Exception as e:
        logger.exception(f"Error finding filter in {group_id} (DB1): {e}", exc_info=True)

    try:
        query2 = mycol2.find_one({"text": name})
        if query2:
            return query2['reply'], query2['btn'], query2.get('alert', None), query2['file']
    except Exception as e:
        logger.exception(f"Error finding filter in {group_id} (DB2): {e}", exc_info=True)

    return None, None, None, None


# Function to get all filters for a group
async def get_filters(group_id):
    """Retrieves all filters for a group."""
    mycol = mydb[str(group_id)]
    mycol2 = mydb2[str(group_id)]

    texts = []
    try:
        for file in mycol.find():
            texts.append(file['text'])
    except Exception as e:
        logger.exception(f"Error retrieving filters from DB1 for group {group_id}: {e}", exc_info=True)
    
    try:
        for file in mycol2.find():
            texts.append(file['text'])
    except Exception as e:
        logger.exception(f"Error retrieving filters from DB2 for group {group_id}: {e}", exc_info=True)

    return texts


# Function to delete a specific filter from the database
async def delete_filter(message, text, group_id):
    """Deletes a specific filter for a group."""
    mycol = mydb[str(group_id)]
    mycol2 = mydb2[str(group_id)]
    
    myquery = {'text': text}
    try:
        if mycol.count_documents(myquery) == 1:
            mycol.delete_one(myquery)
            await message.reply_text(
                f"'`{text}`' deleted. I'll not respond to that filter anymore.",
                quote=True,
                parse_mode=enums.ParseMode.MARKDOWN
            )
        elif mycol2.count_documents(myquery) == 1:
            mycol2.delete_one(myquery)
            await message.reply_text(
                f"'`{text}`' deleted. I'll not respond to that filter anymore.",
                quote=True,
                parse_mode=enums.ParseMode.MARKDOWN
            )
        else:
            await message.reply_text("Couldn't find that filter!", quote=True)
    except Exception as e:
        logger.exception(f"Error deleting filter '{text}' in group {group_id}: {e}", exc_info=True)


# Function to delete all filters for a group
async def del_all(message, group_id, title):
    """Deletes all filters for a group."""
    if str(group_id) not in mydb.list_collection_names() and str(group_id) not in mydb2.list_collection_names():
        await message.edit_text(f"Nothing to remove in {title}!")
        return

    mycol = mydb[str(group_id)]
    mycol2 = mydb2[str(group_id)]
    try:
        mycol.drop()
        mycol2.drop()
        await message.edit_text(f"All filters from {title} have been removed.")
    except Exception as e:
        logger.exception(f"Error removing all filters from {group_id}: {e}", exc_info=True)
        await message.edit_text("Couldn't remove all filters from the group!")


# Function to count the total filters for a group
async def count_filters(group_id):
    """Counts the total number of filters for a group."""
    mycol = mydb[str(group_id)]
    mycol2 = mydb2[str(group_id)]

    try:
        count = mycol.count_documents({}) + mycol2.count_documents({})
        return False if count == 0 else count
    except Exception as e:
        logger.exception(f"Error counting filters in group {group_id}: {e}", exc_info=True)
        return False


# Function to retrieve filter statistics
async def filter_stats():
    """Returns the total number of collections and filters."""
    collections = mydb.list_collection_names()
    collections2 = mydb2.list_collection_names()

    # Ignore "CONNECTION" collection
    if "CONNECTION" in collections:
        collections.remove("CONNECTION")
    if "CONNECTION" in collections2:
        collections2.remove("CONNECTION")

    totalcount = 0
    try:
        for collection in collections:
            mycol = mydb[collection]
            totalcount += mycol.count_documents({})
        for collection in collections2:
            mycol2 = mydb2[collection]
            totalcount += mycol2.count_documents({})
    except Exception as e:
        logger.exception(f"Error retrieving filter stats: {e}", exc_info=True)

    totalcollections = len(collections) + len(collections2)
    return totalcollections, totalcount
qqqq
