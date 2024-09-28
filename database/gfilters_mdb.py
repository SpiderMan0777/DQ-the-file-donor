import pymongo
from info import DATABASE_URI, DATABASE_NAME, SECONDDB_URI
from pyrogram import enums
from sample_info import tempDict
import logging

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# Initialize MongoDB clients and databases
myclient = pymongo.MongoClient(DATABASE_URI)
mydb = myclient[DATABASE_NAME]

myclient2 = pymongo.MongoClient(SECONDDB_URI)
mydb2 = myclient2[DATABASE_NAME]


# Function to add a global filter to the database
async def add_gfilter(gfilters, text, reply_text, btn, file, alert):
    """Adds or updates a global filter."""
    
    # Determine which DB to use based on the indexDB value
    if tempDict['indexDB'] == DATABASE_URI:
        mycol = mydb[str(gfilters)]
    else:
        mycol = mydb2[str(gfilters)]

    # Filter data to be inserted/updated
    data = {
        'text': str(text),
        'reply': str(reply_text),
        'btn': str(btn),
        'file': str(file),
        'alert': str(alert)
    }

    try:
        # Update the global filter or insert if it doesn't exist
        mycol.update_one({'text': str(text)}, {"$set": data}, upsert=True)
    except Exception as e:
        logger.exception(f"Error adding global filter for {gfilters}: {e}", exc_info=True)


# Function to find a global filter in the database
async def find_gfilter(gfilters, name):
    """Finds a global filter by its name."""
    mycol = mydb[str(gfilters)]
    mycol2 = mydb2[str(gfilters)]
    
    try:
        # First, search in the primary database
        query = mycol.find_one({"text": name})
        if not query:
            # If not found, search in the secondary database
            query = mycol2.find_one({"text": name})
        
        if query:
            return query['reply'], query['btn'], query.get('alert', None), query['file']
    except Exception as e:
        logger.exception(f"Error finding global filter in {gfilters}: {e}", exc_info=True)

    return None, None, None, None


# Function to get all global filters for a group
async def get_gfilters(gfilters):
    """Retrieves all global filters for a group."""
    mycol = mydb[str(gfilters)]
    mycol2 = mydb2[str(gfilters)]

    texts = []
    
    try:
        # Get all filters from the first database
        for file in mycol.find():
            texts.append(file['text'])
    except Exception as e:
        logger.exception(f"Error retrieving filters from DB1 for {gfilters}: {e}", exc_info=True)
    
    try:
        # Get all filters from the second database
        for file in mycol2.find():
            texts.append(file['text'])
    except Exception as e:
        logger.exception(f"Error retrieving filters from DB2 for {gfilters}: {e}", exc_info=True)

    return texts


# Function to delete a specific global filter
async def delete_gfilter(message, text, gfilters):
    """Deletes a specific global filter."""
    mycol = mydb[str(gfilters)]
    mycol2 = mydb2[str(gfilters)]

    myquery = {'text': text}

    try:
        # Check if the filter exists in the first database
        if mycol.count_documents(myquery) == 1:
            mycol.delete_one(myquery)
            await message.reply_text(
                f"'`{text}`' deleted. I'll not respond to that gfilter anymore.",
                quote=True,
                parse_mode=enums.ParseMode.MARKDOWN
            )
        # If not found, check the second database
        elif mycol2.count_documents(myquery) == 1:
            mycol2.delete_one(myquery)
            await message.reply_text(
                f"'`{text}`' deleted. I'll not respond to that gfilter anymore.",
                quote=True,
                parse_mode=enums.ParseMode.MARKDOWN
            )
        else:
            await message.reply_text("Couldn't find that gfilter!", quote=True)
    except Exception as e:
        logger.exception(f"Error deleting global filter '{text}' in {gfilters}: {e}", exc_info=True)


# Function to delete all global filters for a group
async def del_allg(message, gfilters):
    """Deletes all global filters for a group."""
    if str(gfilters) not in mydb.list_collection_names() and str(gfilters) not in mydb2.list_collection_names():
        await message.edit_text("Nothing to remove!")
        return

    mycol = mydb[str(gfilters)]
    mycol2 = mydb2[str(gfilters)]

    try:
        mycol.drop()
        mycol2.drop()
        await message.edit_text(f"All gfilters have been removed!")
    except Exception as e:
        logger.exception(f"Error removing all global filters in {gfilters}: {e}", exc_info=True)
        await message.edit_text("Couldn't remove all gfilters!")


# Function to count the total global filters for a group
async def count_gfilters(gfilters):
    """Counts the total number of global filters for a group."""
    mycol = mydb[str(gfilters)]
    mycol2 = mydb2[str(gfilters)]

    try:
        count = mycol.count_documents({}) + mycol2.count_documents({})
        return False if count == 0 else count
    except Exception as e:
        logger.exception(f"Error counting global filters in {gfilters}: {e}", exc_info=True)
        return False


# Function to retrieve global filter statistics
async def gfilter_stats():
    """Returns the total number of collections and global filters."""
    collections = mydb.list_collection_names()
    collections2 = mydb2.list_collection_names()

    # Remove the "CONNECTION" collection if it exists
    if "CONNECTION" in collections:
        collections.remove("CONNECTION")
    if "CONNECTION" in collections2:
        collections2.remove("CONNECTION")

    totalcount = 0

    try:
        # Count the filters in the first database
        for collection in collections:
            mycol = mydb[collection]
            totalcount += mycol.count_documents({})
        
        # Count the filters in the second database
        for collection in collections2:
            mycol2 = mydb2[collection]
            totalcount += mycol2.count_documents({})
    except Exception as e:
        logger.exception(f"Error retrieving global filter stats: {e}", exc_info=True)

    totalcollections = len(collections) + len(collections2)
    return totalcollections, totalcount
    
