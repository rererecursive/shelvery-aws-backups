from functools import reduce
from typing import List

import boto3

from shelvery.backup_resource import BackupResource
from shelvery.entity_resource import EntityResource
from shelvery.ec2_backup import ShelveryEC2Backup

from shelvery.engine import SHELVERY_DO_BACKUP_TAGS


class ShelveryEC2AMIBackup(ShelveryEC2Backup):
    def delete_backup(self, backup_resource: BackupResource):
        regional_client = boto3.client('ec2', region_name=backup_resource.region)
        ami = regional_client.describe_images(ImageIds=[backup_resource.backup_id])['Images'][0]

        # delete image
        regional_client.deregister_image(ImageId=backup_resource.backup_id)
        snapshots = []
        for bdm in ami['BlockDeviceMappings']:
            if 'Ebs' in bdm and 'SnapshotId' in bdm['Ebs']:
                snapshots.append(bdm['Ebs']['SnapshotId'])

        # delete related snapshots
        for snapshot in snapshots:
            regional_client.delete_snapshot(SnapshotId=snapshot)

    def get_existing_backups(self, backup_tag_prefix: str) -> List[BackupResource]:
        amis = self.ec2client.describe_images(Filters=[
            {'Name': f"tag:{backup_tag_prefix}:{BackupResource.BACKUP_MARKER_TAG}", 'Values': ['true']}
        ])['Images']
        backups = []
        instances = dict(map(
            lambda x: (x.resource_id, x),
            self._get_all_entities()
        ))
        for ami in amis:
            backup = BackupResource.construct(backup_tag_prefix,
                                              ami['ImageId'],
                                              dict(map(lambda x: (x['Key'], x['Value']), ami['Tags'])))

            if backup.entity_id in instances:
                backup.entity_resource = instances[backup.entity_id]

            backups.append(backup)

        return backups

    def get_resource_type(self) -> str:
        return 'Amazon Machine Image'

    def get_engine_type(self) -> str:
        return 'ec2ami'

    def backup_resource(self, backup_resource: BackupResource):
        regional_client = boto3.client('ec2', region_name=backup_resource.region)
        ami = regional_client.create_image(
            NoReboot=True,
            Name=backup_resource.name,
            Description=f"Shelvery created backup for {backup_resource.entity_id}",
            InstanceId=backup_resource.entity_id,

        )
        backup_resource.backup_id = ami['ImageId']
        return backup_resource

    def _get_all_entities(self)-> List[EntityResource]:
        instances = self.ec2client.describe_instances()
        while 'NextToken' in instances:
            instances += self.ec2client.describe_instances(
                NextToken=instances['NextToken']
            )
        return self._convert_instances_to_entities(instances)

    def get_entities_to_backup(self, tag_name: str) -> List[EntityResource]:
        instances = self.ec2client.describe_instances(
            Filters=[
                {
                    'Name': f"tag:{tag_name}",
                    'Values': SHELVERY_DO_BACKUP_TAGS
                }
            ]
        )
        while 'NextToken' in instances:
            instances += self.ec2client.describe_instances(
                Filters=[
                    {
                        'Name': f"tag:{tag_name}",
                        'Values': SHELVERY_DO_BACKUP_TAGS
                    }
                ],
                NextToken=instances['NextToken']
            )

        return self._convert_instances_to_entities(instances)

    @staticmethod
    def _convert_instances_to_entities(instances):
        """
        Params:
            instances: a list of Reservations (i.e. the response from `aws ec2 describe-instances`)
        """
        local_region = boto3.session.Session().region_name

        entities = []
        for reservation in instances['Reservations']:
            for instance in reservation['Instances']:
                tags = {}
                if 'Tags' in instance:
                    tags = dict(map(lambda tag: (tag['Key'], tag['Value']), instance['Tags']))
                entities.append(EntityResource(resource_id=instance['InstanceId'], resource_region=local_region, date_created=instance['LaunchTime'], tags=tags))

        return entities

    def is_backup_available(self, backup_region: str, backup_id: str) -> bool:
        regional_client = boto3.client('ec2', region_name=backup_region)
        ami = regional_client.describe_images(ImageIds=[backup_id])
        if len(ami['Images']) > 0:
            return ami['Images'][0]['State'] == 'available'

        return False

    def copy_backup_to_region(self, backup_id: str, region: str) -> str:
        local_region = boto3.session.Session().region_name
        local_client = boto3.client('ec2', region_name=local_region)
        regional_client = boto3.client('ec2', region_name=region)
        ami = local_client.describe_images(ImageIds=[backup_id])['Images'][0]
        idempotency_token = f"shelverycopy{backup_id.replace('-','')}to{region.replace('-','')}"
        return regional_client.copy_image(Name=ami['Name'],
                                          ClientToken=idempotency_token,
                                          Description=f"Shelvery copy of {backup_id} to {region} from {local_region}",
                                          SourceImageId=backup_id,
                                          SourceRegion=local_region
                                          )['ImageId']

    def get_backup_resource(self, region: str, backup_id: str) -> BackupResource:
        ami = self.ec2client.describe_images(ImageIds=[backup_id])['Images'][0]

        d_tags = dict(map(lambda x: (x['Key'], x['Value']), ami['Tags']))
        backup_tag_prefix = d_tags['shelvery:tag_name']

        backup = BackupResource.construct(backup_tag_prefix, backup_id, d_tags)
        return backup

    def share_backup_with_account(self, backup_region: str, backup_id: str, aws_account_id: str):
        ec2 = boto3.session.Session(region_name=backup_region).resource('ec2')
        image = ec2.Image(backup_id)
        image.modify_attribute(Attribute='launchPermission',
                               LaunchPermission={
                                   'Add': [{'UserId': aws_account_id}]
                               },
                               UserIds=[aws_account_id],
                               OperationType='add')
