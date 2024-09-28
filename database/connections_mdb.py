import pymongo
from sample_info import tempDict
from info import DATABASE_URI, DATABASE_NAME, SECONDDB_URI

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# MongoDB client setup
myclient = pymongo.MongoClient(DATABASE_URI)
mydb = myclient[DATABASE_NAME]
mycol = mydb['CONNECTION']

myclient2 = pymongo.MongoClient(SECONDDB_URI)
mydb2 = myclient2[DATABASE_NAME]
mycol2 = mydb2['CONNECTION']

# Add a connection for a user to a group
async def add_connection(group_id, user_id):
    query = mycol.find_one(
        {"_id": user_id},
        {"_id": 0, "active_group": 0}
    )
    
    if query is not None:
        group_ids = [x["group_id"] for x in query["group_details"]]
        if group_id in group_ids:
            return False

    group_details = {
        "group_id": group_id
    }

    data = {
        "_id": user_id,
        "group_details": [group_details],
        "active_group": group_id
    }

    if mycol.count_documents({"_id": user_id}) == 0 and mycol2.count_documents({"_id": user_id}) == 0:
        try:
            if tempDict['indexDB'] == DATABASE_URI:
                mycol.insert_one(data)
            else:
                mycol2.insert_one(data)
            return True
        except Exception as e:
            logger.exception(f'Error adding connection: {e}', exc_info=True)
            return False
    else:
        try:
            if mycol.count_documents({"_id": user_id}) == 0:
                mycol2.update_one(
                    {"_id": user_id},
                    {
                        "$push": {"group_details": group_details},
                        "$set": {"active_group": group_id}
                    }
                )
            else:
                mycol.update_one(
                    {"_id": user_id},
                    {
                        "$push": {"group_details": group_details},
                        "$set": {"active_group": group_id}
                    }
                )
            return True
        except Exception as e:
            logger.exception(f'Error updating connection: {e}', exc_info=True)
            return False

# Get the active connection for a user
async def active_connection(user_id):
    query = mycol.find_one(
        {"_id": user_id},
        {"_id": 0, "group_details": 0}
    )
    query2 = mycol2.find_one(
        {"_id": user_id},
        {"_id": 0, "group_details": 0}
    )
    
    if not query and not query2:
        return None
    elif query:
        group_id = query.get("active_group", None)
        return int(group_id) if group_id is not None else None
    else:
        group_id = query2.get("active_group", None)
        return int(group_id) if group_id is not None else None

# Get all connections of a user
async def all_connections(user_id):
    query = mycol.find_one(
        {"_id": user_id},
        {"_id": 0, "active_group": 0}
    )
    query2 = mycol2.find_one(
        {"_id": user_id},
        {"_id": 0, "active_group": 0}
    )
    
    if query is not None:
        return [x["group_id"] for x in query["group_details"]]
    elif query2 is not None:
        return [x["group_id"] for x in query2["group_details"]]
    else:
        return None

# Check if a user has an active connection in a specific group
async def if_active(user_id, group_id):
    query = mycol.find_one(
        {"_id": user_id},
        {"_id": 0, "group_details": 0}
    )
    
    if query is None:
        query = mycol2.find_one(
            {"_id": user_id},
            {"_id": 0, "group_details": 0}
        )
    
    return query is not None and query['active_group'] == group_id

# Make a group the active connection for a user
async def make_active(user_id, group_id):
    update = mycol.update_one(
        {"_id": user_id},
        {"$set": {"active_group": group_id}}
    )
    
    if update.modified_count == 0:
        update = mycol2.update_one(
            {"_id": user_id},
            {"$set": {"active_group": group_id}}
        )
    
    return update.modified_count != 0

# Make a user inactive (no active group)
async def make_inactive(user_id):
    update = mycol.update_one(
        {"_id": user_id},
        {"$set": {"active_group": None}}
    )
    
    if update.modified_count == 0:
        update = mycol2.update_one(
            {"_id": user_id},
            {"$set": {"active_group": None}}
        )
    
    return update.modified_count != 0

# Delete a user's connection to a group
async def delete_connection(user_id, group_id):
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
                query = mycol2.find_one({"_id": user_id}, {"_id": 0})
                if len(query["group_details"]) >= 1 and query['active_group'] == group_id:
                    prvs_group_id = query["group_details"][-1]["group_id"]
                    mycol2.update_one({"_id": user_id}, {"$set": {"active_group": prvs_group_id}})
                else:
                    mycol2.update_one({"_id": user_id}, {"$set": {"active_group": None}})
                return True
        else:
            query = mycol.find_one({"_id": user_id}, {"_id": 0})
            if len(query["group_details"]) >= 1 and query['active_group'] == group_id:
                prvs_group_id = query["group_details"][-1]["group_id"]
                mycol.update_one({"_id": user_id}, {"$set": {"active_group": prvs_group_id}})
            else:
                mycol.update_one({"_id": user_id}, {"$set": {"active_group": None}})
            return True
    except Exception as e:
        logger.exception(f'Error deleting connection: {e}', exc_info=True)
        return False
