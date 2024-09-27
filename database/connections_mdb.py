import pymongo
from sample_info import tempDict
from info import DATABASE_URI, DATABASE_NAME, SECONDDB_URI
import logging

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# Connect to primary and secondary MongoDB databases
myclient = pymongo.MongoClient(DATABASE_URI)
mydb = myclient[DATABASE_NAME]
mycol = mydb['CONNECTION']

myclient2 = pymongo.MongoClient(SECONDDB_URI)
mydb2 = myclient2[DATABASE_NAME]
mycol2 = mydb2['CONNECTION']


async def add_connection(group_id, user_id):
    """Adds a connection if it doesn't already exist for the user in the group."""
    query = mycol.find_one({"_id": user_id}, {"_id": 0, "active_group": 0}) or mycol2.find_one({"_id": user_id}, {"_id": 0, "active_group": 0})

    if query:
        group_ids = [x["group_id"] for x in query.get("group_details", [])]
        if group_id in group_ids:
            return False

    group_details = {"group_id": group_id}
    data = {
        '_id': user_id,
        'group_details': [group_details],
        'active_group': group_id,
    }

    try:
        if not query:
            if tempDict['indexDB'] == DATABASE_URI:
                mycol.insert_one(data)
            else:
                mycol2.insert_one(data)
        else:
            update_col = mycol if mycol.find_one({"_id": user_id}) else mycol2
            update_col.update_one(
                {'_id': user_id},
                {"$push": {"group_details": group_details}, "$set": {"active_group": group_id}}
            )
        return True
    except Exception as e:
        logger.exception('Error while adding connection', exc_info=True)
        return False


async def active_connection(user_id):
    """Returns the active group ID for a user, if it exists."""
    query = mycol.find_one({"_id": user_id}, {"_id": 0, "group_details": 0}) or mycol2.find_one({"_id": user_id}, {"_id": 0, "group_details": 0})
    
    if not query:
        return None
    return int(query.get('active_group', None)) if query['active_group'] is not None else None


async def all_connections(user_id):
    """Returns a list of all group IDs a user is connected to."""
    query = mycol.find_one({"_id": user_id}, {"_id": 0, "active_group": 0}) or mycol2.find_one({"_id": user_id}, {"_id": 0, "active_group": 0})

    if query:
        return [x["group_id"] for x in query.get("group_details", [])]
    return None


async def if_active(user_id, group_id):
    """Checks if a specific group is active for a user."""
    query = mycol.find_one({"_id": user_id}, {"_id": 0, "group_details": 0}) or mycol2.find_one({"_id": user_id}, {"_id": 0, "group_details": 0})
    
    return query is not None and query['active_group'] == group_id


async def make_active(user_id, group_id):
    """Sets a specific group as the active group for a user."""
    update = mycol.update_one({'_id': user_id}, {"$set": {"active_group": group_id}})
    if update.modified_count == 0:
        update = mycol2.update_one({'_id': user_id}, {"$set": {"active_group": group_id}})
    return update.modified_count != 0


async def make_inactive(user_id):
    """Makes the active group for a user inactive."""
    update = mycol.update_one({'_id': user_id}, {"$set": {"active_group": None}})
    if update.modified_count == 0:
        update = mycol2.update_one({'_id': user_id}, {"$set": {"active_group": None}})
    return update.modified_count != 0


async def delete_connection(user_id, group_id):
    """Deletes a specific group connection for a user."""
    try:
        update = mycol.update_one(
            {"_id": user_id},
            {"$pull": {"group_details": {"group_id": group_id}}}
        )
        if update.modified_count == 0:
            update = mycol2.update_one(
                {"_id": user_id},
                {"$pull": {"group_details": {"group_id": group_id}}}
            )
            if update.modified_count == 0:
                return False
            else:
                query = mycol2.find_one({"_id": user_id})
                _handle_after_deletion(query, user_id, group_id, mycol2)
        else:
            query = mycol.find_one({"_id": user_id})
            _handle_after_deletion(query, user_id, group_id, mycol)

        return True
    except Exception as e:
        logger.exception(f'Error while deleting connection: {e}', exc_info=True)
        return False


def _handle_after_deletion(query, user_id, group_id, collection):
    """Handles setting the active group after a deletion."""
    if query and query.get("group_details"):
        if query['active_group'] == group_id:
            previous_group_id = query["group_details"][-1]["group_id"]
            collection.update_one({'_id': user_id}, {"$set": {"active_group": previous_group_id}})
    else:
        collection.update_one({'_id': user_id}, {"$set": {"active_group": None}})
