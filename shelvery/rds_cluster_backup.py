import boto3

from shelvery.runtime_config import RuntimeConfig
from shelvery.backup_resource import BackupResource
from shelvery.engine import ShelveryEngine, SHELVERY_DO_BACKUP_TAGS
from shelvery.entity_resource import EntityResource

from typing import Dict, List
from botocore.errorfactory import ClientError


class ShelveryRDSClusterBackup(ShelveryEngine):
    def is_backup_available(self, backup_region: str, backup_id: str) -> bool:
        rds_client = boto3.client('rds', region_name=backup_region)
        snapshots = rds_client.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=backup_id)
        return snapshots['DBClusterSnapshots'][0]['Status'] == 'available'

    def get_resource_type(self) -> str:
        return 'RDS Cluster'

    def backup_resource(self, backup_resource: BackupResource) -> BackupResource:
        if RuntimeConfig.get_rds_mode(backup_resource.entity_resource.tags, self) == RuntimeConfig.RDS_CREATE_SNAPSHOT:
            return self.backup_from_cluster(backup_resource)
        if RuntimeConfig.get_rds_mode(backup_resource.entity_resource.tags,
                                      self) == RuntimeConfig.RDS_COPY_AUTOMATED_SNAPSHOT:
            return self.backup_from_latest_automated(backup_resource)

        raise Exception(f"Only {RuntimeConfig.RDS_COPY_AUTOMATED_SNAPSHOT} and "
                        f"{RuntimeConfig.RDS_CREATE_SNAPSHOT} rds backup "
                        f"modes supported - set rds backup mode using rds_backup_mode configuration option ")

    def backup_from_latest_automated(self, backup_resource: BackupResource):
        rds_client = boto3.client('rds')
        auto_snapshots = rds_client.describe_db_cluster_snapshots(
            DBClusterIdentifier=backup_resource.entity_id,
            SnapshotType='automated',
            # API always returns in date descending order, and we only need last one
            MaxRecords=20
        )
        auto_snapshots = sorted(auto_snapshots['DBClusterSnapshots'], key=lambda k: k['SnapshotCreateTime'],
                                reverse=True)

        if len(auto_snapshots) == 0:
            self.logger.error(f"There is no latest automated backup for cluster {backup_resource.entity_id},"
                              f" fallback to RDS_CREATE_SNAPSHOT mode. Creating snapshot directly on cluster...")
            return self.backup_from_cluster(backup_resource)

        # TODO handle case when there are no latest automated backups
        automated_snapshot_id = auto_snapshots[0]['DBClusterSnapshotIdentifier']
        rds_client.copy_db_cluster_snapshot(
            SourceDBClusterSnapshotIdentifier=automated_snapshot_id,
            TargetDBClusterSnapshotIdentifier=backup_resource.name,
            CopyTags=False
        )
        backup_resource.backup_id = backup_resource.name
        return backup_resource

    def backup_from_cluster(self, backup_resource):
        rds_client = boto3.client('rds')
        rds_client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=backup_resource.name,
            DBClusterIdentifier=backup_resource.entity_id
        )
        backup_resource.backup_id = backup_resource.name
        return backup_resource

    def delete_backup(self, backup_resource: BackupResource):
        rds_client = boto3.client('rds')
        rds_client.delete_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=backup_resource.backup_id
        )

    def tag_backup_resource(self, backup_resource: BackupResource):
        regional_rds_client = boto3.client('rds', region_name=backup_resource.region)
        snapshots = regional_rds_client.describe_db_cluster_snapshots(
            DBClusterSnapshotIdentifier=backup_resource.backup_id)
        snapshot_arn = snapshots['DBClusterSnapshots'][0]['DBClusterSnapshotArn']
        tags = list(map(lambda k: {'Key': k, 'Value': backup_resource.tags[k].replace(',', ' ')}, backup_resource.tags))
        regional_rds_client.add_tags_to_resource(
            ResourceName=snapshot_arn,
            Tags=tags
        )

    def get_existing_backups(self, backup_tag_prefix: str) -> List[BackupResource]:
        rds_client = boto3.client('rds')

        # collect all snapshots
        all_snapshots = self.collect_all_snapshots(rds_client)

        # filter ones backed up with shelvery
        all_backups = self.get_shelvery_backups_only(all_snapshots, backup_tag_prefix, rds_client)

        return all_backups

    def share_backup_with_account(self, backup_region: str, backup_id: str, aws_account_id: str):
        rds_client = boto3.client('rds', region_name=backup_region)
        rds_client.modify_db_cluster_snapshot_attribute(
            DBClusterSnapshotIdentifier=backup_id,
            AttributeName='restore',
            ValuesToAdd=[aws_account_id]
        )

    def copy_backup_to_region(self, backup_id: str, region: str) -> str:
        local_region = boto3.session.Session().region_name
        client_local = boto3.client('rds')
        rds_client = boto3.client('rds', region_name=region)
        snapshots = client_local.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=backup_id)
        snapshot = snapshots['DBClusterSnapshots'][0]
        rds_client.copy_db_cluster_snapshot(
            SourceDBClusterSnapshotIdentifier=snapshot['DBClusterSnapshotArn'],
            TargetDBClusterSnapshotIdentifier=backup_id,
            SourceRegion=local_region,
            # tags are created explicitly
            CopyTags=False
        )
        return backup_id

    def get_backup_resource(self, backup_region: str, backup_id: str) -> BackupResource:
        rds_client = boto3.client('rds', region_name=backup_region)
        snapshots = rds_client.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=backup_id)
        snapshot = snapshots['DBClusterSnapshots'][0]
        tags = rds_client.list_tags_for_resource(ResourceName=snapshot['DBClusterSnapshotArn'])['TagList']
        d_tags = dict(map(lambda t: (t['Key'], t['Value']), tags))
        return BackupResource.construct(d_tags['shelvery:tag_name'], backup_id, d_tags)

    def get_engine_type(self) -> str:
        return 'rds_cluster'

    def get_entities_to_backup(self, tag_name: str) -> List[EntityResource]:
        # region and api client
        local_region = boto3.session.Session().region_name
        rds_client = boto3.client('rds')

        # list of models returned from api
        db_cluster_entities = []

        db_clusters = self.get_all_clusters(rds_client)

        # collect tags in check if instance tagged with marker tag

        for instance in db_clusters:
            tags = rds_client.list_tags_for_resource(ResourceName=instance['DBClusterArn'])['TagList']

            # convert api response to dictionary
            d_tags = dict(map(lambda t: (t['Key'], t['Value']), tags))

            # check if marker tag is present
            if tag_name in d_tags and d_tags[tag_name] in SHELVERY_DO_BACKUP_TAGS:
                resource = EntityResource(instance['DBClusterIdentifier'],
                                          local_region,
                                          instance['ClusterCreateTime'],
                                          d_tags)
                db_cluster_entities.append(resource)

        return db_cluster_entities

    def get_all_clusters(self, rds_client):
        """
        Get all RDS clusters within region for given boto3 client
        :param rds_client: boto3 rds service
        :return: all RDS instances within region for given boto3 client
        """
        # list of resource models
        db_clusters = []
        # temporary list of api models, as calls are batched
        temp_clusters = rds_client.describe_db_clusters()
        db_clusters.extend(temp_clusters['DBClusters'])
        # collect database instances
        while 'Marker' in temp_clusters:
            temp_clusters = rds_client.describe_db_clusters(Marker=temp_clusters['Marker'])
            db_clusters.extend(temp_clusters['DBClusters'])

        return db_clusters

    def get_shelvery_backups_only(self, all_snapshots, backup_tag_prefix, rds_client):
        """
        :param all_snapshots: all snapshots within region
        :param backup_tag_prefix:  prefix of shelvery backup system
        :param rds_client:  amazon boto3 rds client
        :return: snapshots created using shelvery
        """
        all_backups = []
        marker_tag = f"{backup_tag_prefix}:{BackupResource.BACKUP_MARKER_TAG}"
        for snap in all_snapshots:
            tags = rds_client.list_tags_for_resource(ResourceName=snap['DBClusterSnapshotArn'])['TagList']
            self.logger.info(f"Checking RDS Snap {snap['DBClusterSnapshotIdentifier']}")
            d_tags = dict(map(lambda t: (t['Key'], t['Value']), tags))
            if marker_tag in d_tags:
                if d_tags[marker_tag] in SHELVERY_DO_BACKUP_TAGS:
                    backup_resource = BackupResource.construct(backup_tag_prefix, snap['DBClusterSnapshotIdentifier'],
                                                               d_tags)
                    backup_resource.entity_resource = snap['EntityResource']
                    backup_resource.entity_id = snap['EntityResource'].resource_id

                    all_backups.append(backup_resource)

        return all_backups

    def collect_all_snapshots(self, rds_client):
        """
        :param rds_client:
        :return: All snapshots within region for rds_client
        """
        all_snapshots = []
        tmp_snapshots = rds_client.describe_db_cluster_snapshots(SnapshotType='manual')
        all_snapshots.extend(tmp_snapshots['DBClusterSnapshots'])
        while 'Marker' in tmp_snapshots:
            tmp_snapshots = rds_client.describe_db_cluster_snapshots()
            all_snapshots.extend(tmp_snapshots['DBClusterSnapshots'])

        self.populate_snap_entity_resource(all_snapshots)

        return all_snapshots

    def populate_snap_entity_resource(self, all_snapshots):
        cluster_ids = []
        for snap in all_snapshots:
            if snap['DBClusterIdentifier'] not in cluster_ids:
                cluster_ids.append(snap['DBClusterIdentifier'])
        entities = {}
        rds_client = boto3.client('rds')
        local_region = boto3.session.Session().region_name

        for cluster_id in cluster_ids:
            try:
                rds_instance = rds_client.describe_db_clusters(DBClusterIdentifier=cluster_id)['DBClusters'][0]
                tags = rds_client.list_tags_for_resource(ResourceName=rds_instance['DBClusterArn'])['TagList']
                d_tags = dict(map(lambda t: (t['Key'], t['Value']), tags))
                rds_entity = EntityResource(cluster_id,
                                            local_region,
                                            rds_instance['ClusterCreateTime'],
                                            d_tags)
                entities[cluster_id] = rds_entity
            except ClientError as e:
                if 'DBClusterNotFoundFault' in str(type(e)):
                    entities[cluster_id] = EntityResource.empty()
                    entities[cluster_id].resource_id = cluster_id
                else:
                    raise e

        for snap in all_snapshots:
            if snap['DBClusterIdentifier'] in entities:
                snap['EntityResource'] = entities[snap['DBClusterIdentifier']]


    def do_copy_shared_backups(self):
        """Make a copy of the RDS snapshots that have been shared with this account.
        Note that a resource's tags are only visible to the account that owns it; the tags
        are therefore not accessible on shared resources.

        # TODO: do we want to copy them to the DR regions as well?
        # TODO: do we want to share the copies with other accounts once created?
        # TODO: append the account ID on the end of the snapshot
        # TODO: parse the ARN into an object

        Problems:
            - if a snapshot with the same name is shared by prod AND dev, one will be ignored.
            Solution:
            - append the account's ID on the snapshot name

            - the 'shelvery:name' tag uses the snapshot's create time instead of the snapshot's name.
            Solution:
            - parse the snapshot's date and add this to the 'shelvery:name' tag.
        """
        copied = 0
        ignored = 0

        rds_client = boto3.client('rds')
        region = boto3.session.Session().region_name
        shared_snapshots = rds_client.describe_db_cluster_snapshots(SnapshotType='shared', IncludeShared=True)['DBClusterSnapshots']
        existing_snapshots = rds_client.describe_db_cluster_snapshots()['DBClusterSnapshots']

        if not len(shared_snapshots):
            print("No shared RDS cluster snapshots were found.")
            return

        # Get a list of snapshot
        existing_snapshot_ids = []
        for snap in existing_snapshots:
            tokens = snap['DBClusterSnapshotArn'].split(':')
            # Remove the account ID from the ARN
            snapshot_id = ":".join(tokens[:4] + tokens[5:])
            existing_snapshot_ids.append(snapshot_id)

        # To compare the snapshots, compare the ARNS without the IDs.

        print("Found %s shared cluster snapshots." % (len(shared_snapshots)))

        for shared_snapshot in shared_snapshots:
            # TODO: move this mess into an ARN object
            shared_snapshot_arn = shared_snapshot['DBClusterSnapshotArn']
            shared_tokens = shared_snapshot_arn.split(':')
            account_id = shared_tokens[4]
            shared_snapshot_name = ":".join(shared_tokens[6:])
            shared_snapshot_name_with_account_id = shared_snapshot_name + '-' + account_id
            # Remove the account ID from the ARN
            shared_snapshot_id = ":".join(shared_tokens[:4] + shared_tokens[5:]) + '-' + account_id

            # Compare the ARNs (without the region)
            if shared_snapshot_id not in existing_snapshot_ids:
                entity = EntityResource(
                    shared_snapshot['DBClusterIdentifier'],
                    region,
                    shared_snapshot['SnapshotCreateTime'],
                    {}
                )

                # Add the tags to the resource
                backup_resource = BackupResource(
                    tag_prefix=RuntimeConfig.get_tag_prefix(),
                    entity_resource=entity
                )

                # Override the 'name' and 'retention type' tags to be based on the original snapshot's name plus the account ID.
                # This is pretty hacky and should be refactored.
                retention_type = shared_snapshot_name.split('-')[-1]

                backup_resource.tags[f"{RuntimeConfig.get_tag_prefix()}:retention_type"] = retention_type
                backup_resource.tags[f"{RuntimeConfig.get_tag_prefix()}:name"] = shared_snapshot_name_with_account_id
                backup_resource.tags['Name'] = shared_snapshot_name_with_account_id
                backup_resource.tags[f"{RuntimeConfig.get_tag_prefix()}:account_id"] = account_id

                # Construct the ARN for the target db snapshot name.
                print("Copying shared RDS cluster snapshot '%s' to this account as '%s'..." % (shared_snapshot_name, shared_snapshot_name_with_account_id))
                copied_snapshot = rds_client.copy_db_cluster_snapshot(
                    SourceDBClusterSnapshotIdentifier=shared_snapshot_arn,
                    TargetDBClusterSnapshotIdentifier=shared_snapshot_name_with_account_id,
                    CopyTags=False
                )

                copied_snapshot_arn = copied_snapshot['DBClusterSnapshot']['DBClusterSnapshotArn']
                rds_client.add_tags_to_resource(
                    ResourceName=copied_snapshot_arn,
                    Tags=list(map(lambda k: {'Key': k, 'Value': backup_resource.tags[k].replace(',', ' ')}, backup_resource.tags))
                )

                copied += 1
            else:
                ignored += 1

        print("Copied %d. Ignored %d." % (copied, ignored))
