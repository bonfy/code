from b_mongo import CollectionFactory, get_collection
from pymongo import MongoClient

class TestMongo:

    def setup(self):
        print('---------------setup---------------')
        collection = get_collection('test', 'test')
        self.tc = CollectionFactory(collection)

    def teardown(self):
        print('---------------tear down--------------')
        if self.tc.drop():
            print('droped the database')


    def test_insert_one(self):
        dct = dict(content='insert one', tag='tag_01')
        result = self.tc.insert_one(dct)
        assert result
        # 重复的插入
        result = self.tc.insert_one(dct)
        assert not result

    def test_insert_many(self):
        num = self.tc.count()
        lst = [
            dict(content='insert many 1', tag='tag_many_01'),
            dict(content='insert many 2', tag='tag_many_02'),
            dict(content='insert many 3', tag='tag_many_03')
        ]
        result = self.tc.insert_many(lst)
        assert len(result) == 3
        assert self.tc.count() == num + 3

        lst = [
            dict(content='insert many 1', tag='tag_many_01'),
            dict(content='insert many 2', tag='tag_many_02')
        ]
        result = self.tc.insert_many(lst)
        assert result == None

        lst = [
            dict(content='insert many 1', tag='tag_many_01'),
            dict(content='insert many 2', tag='tag_many_02'),
            dict(content='insert many 4', tag='tag_many_04')
        ]
        result = self.tc.insert_many(lst)
        assert len(result) == 1
        assert self.tc.count() == num + 4
