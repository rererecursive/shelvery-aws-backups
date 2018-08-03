import datetime
import os
import sys
import unittest

pwd = os.path.dirname(os.path.abspath(__file__))
sys.path.append(f"{pwd}/../shelvery")
sys.path.append(f"{pwd}/shelvery")
sys.path.append(f"{pwd}/lib")
sys.path.append(f"{pwd}/../lib")

from shelvery.backup_resource import BackupResource
from shelvery.entity_resource import EntityResource


class ShelveryFactoryTestCase(unittest.TestCase):

    def setUp(self):
        print(f"Setting up unit backup_test")
        self.date = datetime.datetime.strptime('Jun 2 2018 1:23PM', '%b %d %Y %I:%M%p')

    def tearDown(self):
        print(f"Tear down unit backup_test")


    def test_LongBackupName(self):
        original_name = 'this_is_a_very_very_very_very_very_very_very_very_very_very_very_very_very_very_very_very_long_resource_name'
        new_name = 'this_is_a_very_very_very_very_very_very_very_very_very_very_very_very_very_very_ver-vol-0aa973e53da322192-2018-06-02-1323-daily'
        entity = EntityResource(
            resource_id='vol-0aa973e53da322192',
            resource_region='ap-southeast-2',
            date_created=self.date,
            tags={'Name':original_name}
        )
        resource = BackupResource(
            tag_prefix='shelvery',
            entity_resource=entity,
            date_created=self.date
        )

        print("Resource name is '%s'." % (resource.name))
        self.assertTrue(resource.name == new_name)

    def test_BackupName(self):
        original_name = 'this_is_a_resource_name'
        new_name = 'this_is_a_resource_name-vol-0aa973e53da322192-2018-06-02-1323-daily'
        entity = EntityResource(
            resource_id='vol-0aa973e53da322192',
            resource_region='ap-southeast-2',
            date_created=self.date,
            tags={'Name':original_name}
        )
        resource = BackupResource(
            tag_prefix='shelvery',
            entity_resource=entity,
            date_created=self.date
        )
        print("Resource name is '%s'." % (resource.name))
        self.assertTrue(resource.name == new_name)

if __name__ == '__main__':
    unittest.main()
