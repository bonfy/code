# coding: utf-8

from pymongo import MongoClient

def get_collection(db_name, collection_name, db_url='localhost', db_port=27017):
    """ Get Collection
    :param db_name         string:  db name
    :param collection_name string:  collection name
    :param db_url          string:  url
    :param db_port         int   :  port
    :rtype:                         pymongo.collection.Collection
    """
    collection = None
    try:
        client = MongoClient(db_url, db_port)
        db = client[db_name]
        collection = db[collection_name]
    except Exception as e:
        print(e)
    finally:
        return collection


class CollectionFactory:
    """ Collection Factory
        Make you use collection easily
    """

    def __init__(self, collection, identity_field=None):
        self.collection = collection
        # 能够标识唯一性的字段名称
        self.identity_field = identity_field

    def insert_one(self, dct):
        """ Insert one dict to collection
        :param dct     dict:      dct
        :rtype:                   ObjectId / None
        """
        if not self._isInCollectionbyDictTag(dct):
            result = self.collection.insert_one(dct)  # return cursor
            return result.inserted_id                 # retrun Object_id

    def insert_many(self, lst):
        """ Insert many dicts to collection
        :param lst     List[dict]:   list of dict
        :rtype:                      List[ObjectId] / None
        """
        lst_new = [dct for dct in lst if not self._isInCollectionbyDictTag(dct)]
        if lst_new:
            result = self.collection.insert_many(lst_new)
            return result.inserted_ids

    def drop(self):
        """ Drop the collection
        :rtype:                       boolean  dropped for True , not for False
        """
        return self.collection.drop()

    def count(self):
        """ return the count
        """
        return self.collection.count()


    def _isInCollectionbyQuery(self, query):
        """ 判断是否 Collection 中是否已经含有此query
        :param query dict:
        :rtype:                      boolean
        """

        result = self.collection.find_one(query)
        if not result:
            return False
        return True

    def _isInCollectionbyDictTag(self, dct):
        """ 判断是否 Collection 中已含有此dict
        :param dct dict:
        :param tag string:
        :rtype:                       boolean
        """
        if not self.identity_field:
            query = dct
        else:
            query = {
                self.identity_field: dct[self.identity_field]
            }
        print(query)
        return self._isInCollectionbyQuery(query)


def main():

    collection = get_collection('test', 'test')
    Tst = CollectionFactory(collection, identity_field='tag')

    dct = dict(content='hello', tag='tag_01')
    Tst.insert_one(dct)

if __name__ == '__main__':
    main()
