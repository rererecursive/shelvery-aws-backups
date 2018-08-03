from datetime import datetime
from typing import Dict

from dateutil.relativedelta import relativedelta
from datetime import timedelta

from shelvery.entity_resource import EntityResource
from shelvery.runtime_config import RuntimeConfig


class BackupResource:
    """Model representing single backup"""

    BACKUP_MARKER_TAG = 'backup'
    MAXIMUM_STRING_LENGTH = 127
    TIMESTAMP_FORMAT = '%Y-%m-%d-%H%M'
    TIMESTAMP_FORMAT_LEGACY = '%Y%m%d-%H%M'

    RETENTION_DAILY = 'daily'
    RETENTION_WEEKLY = 'weekly'
    RETENTION_MONTHLY = 'monthly'
    RETENTION_YEARLY = 'yearly'

    def __init__(self, tag_prefix, entity_resource: EntityResource, construct=False, date_created=datetime.utcnow()):
        """Construct new backup resource out of entity resource (e.g. ebs volume)."""
        # if object manually created
        if construct:
            return

        self.date_created = date_created
        date_formatted = date_created.strftime(self.TIMESTAMP_FORMAT)

        self.retention_type = self.determine_retention_type(date_created)
        self.name = self.determine_backup_name(date_formatted, entity_resource, self.retention_type)

        self.entity_id = entity_resource.resource_id
        self.entity_resource = entity_resource
        self.__region = entity_resource.resource_region

        self.tags = {
            'Name': self.name,
            "shelvery:tag_name": tag_prefix,
            f"{tag_prefix}:date_created": date_formatted,
            f"{tag_prefix}:name": self.name,
            f"{tag_prefix}:region": entity_resource.resource_region,
            f"{tag_prefix}:retention_type": self.retention_type,
            f"{tag_prefix}:entity_id": entity_resource.resource_id,
            f"{tag_prefix}:{self.BACKUP_MARKER_TAG}": 'true'
        }
        self.backup_id = None
        self.expire_date = None

    def determine_backup_name(self, date: datetime, entity: EntityResource, retention_type: str):
        """Determine the backup name. Make it meaningful (using tags) and unique (from its ID).
        If the name is more than 127 characters long, reduce it.
        """
        if 'Name' in entity.tags:
            name = f"{entity.tags['Name']}-{entity.resource_id}"
            name_length = len(f"{name}-{date}-{retention_type}")

            if name_length > self.MAXIMUM_STRING_LENGTH:
                # If the backup name is too long, reduce the characters from the tag.
                print(f"The backup name is larger than {self.MAXIMUM_STRING_LENGTH}. Reducing to 127 characters...")
                to_trim = name_length - self.MAXIMUM_STRING_LENGTH
                name = entity.tags['Name'][:-to_trim] + '-' + entity.resource_id
        else:
            name = entity.resource_id

        return f"{name}-{date}-{retention_type}"

    def determine_retention_type(self, date: datetime):
        """Determine the retention type from a date.
        """
        if date.day == 1:
            if date.month == 1:
                return self.RETENTION_YEARLY
            else:
                return self.RETENTION_MONTHLY
        elif date.weekday() == 6:
            return self.RETENTION_WEEKLY
        else:
            return self.RETENTION_DAILY

    @classmethod
    def construct(cls,
                  tag_prefix: str,
                  backup_id: str,
                  tags: Dict):
        """
        Construct BackupResource object from object id and aws tags stored by shelvery
        """

        obj = BackupResource(None, None, True)
        obj.entity_resource = None
        obj.entity_id = None
        obj.backup_id = backup_id
        obj.tags = tags

        # read properties from tags
        obj.retention_type = tags[f"{tag_prefix}:retention_type"]
        obj.name = tags[f"{tag_prefix}:name"]

        if f"{tag_prefix}:entity_id" in tags:
            obj.entity_id = tags[f"{tag_prefix}:entity_id"]

        try:
            obj.date_created = datetime.strptime(tags[f"{tag_prefix}:date_created"], cls.TIMESTAMP_FORMAT)
        except Exception as e:
            if 'does not match format' in str(e):
                str_date = tags[f"{tag_prefix}:date_created"]
                print(f"Failed to read {str_date} as date, trying legacy format {cls.TIMESTAMP_FORMAT_LEGACY}")
                obj.date_created = datetime.strptime(tags[f"{tag_prefix}:date_created"], cls.TIMESTAMP_FORMAT_LEGACY)


        obj.region = tags[f"{tag_prefix}:region"]
        return obj

    def entity_resource_tags(self):
        return self.entity_resource.tags if self.entity_resource is not None else {}

    def calculate_expire_date(self, engine):
        """Determine expire date, based on 'retention_type' tag"""
        if self.retention_type == BackupResource.RETENTION_DAILY:
            expire_date = self.date_created + timedelta(
                days=RuntimeConfig.get_keep_daily(self.entity_resource_tags(), engine))
        elif self.retention_type == BackupResource.RETENTION_WEEKLY:
            expire_date = self.date_created + relativedelta(
                weeks=RuntimeConfig.get_keep_weekly(self.entity_resource_tags(), engine))
        elif self.retention_type == BackupResource.RETENTION_MONTHLY:
            expire_date = self.date_created + relativedelta(
                months=RuntimeConfig.get_keep_monthly(self.entity_resource_tags(), engine))
        elif self.retention_type == BackupResource.RETENTION_YEARLY:
            expire_date = self.date_created + relativedelta(
                years=RuntimeConfig.get_keep_yearly(self.entity_resource_tags(), engine))
        else:
            # in case there is no retention tag on backup, we want it kept forever
            expire_date = datetime.utcnow() + relativedelta(years=10)

        self.expire_date = expire_date

    def is_stale(self, engine):
        self.calculate_expire_date(engine)
        now = datetime.now(self.date_created.tzinfo)
        return now > self.expire_date

    @property
    def region(self):
        return self.__region

    @region.setter
    def region(self, region: str):
        self.__region = region
