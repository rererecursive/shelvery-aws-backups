
#import shelvery
# shelvery.factory import ShelveryFactory

import argparse
import sys

__version__ = '0.0'

#print("Usage: shelvery <backup_type> <action>\n\nBackup types: rds ebs rds_cluster ec2ami\nActions: create_backups clean_backups")

class ShelveryCLI():
    def __init__(self):
        parser = argparse.ArgumentParser(
            usage='''shelvery <backup> <action> [<args>]

Shelvery is a tool for managing AWS backups.

Backups:
  ebs               an Elastic Block Store volume
  ec2ami            an Amazon Machine Image
  rds               a snapshot of an RDS instance
  rds_cluster       a snapshot of an RDS cluster

Actions:
  create_backups        create backups from tagged resources
  clean_backups         remove any expired backups
  copy_shared_backups   make a copy of any backups shared with this account
  tag_resources         add shelvery tags to a resource

Optional arguments:
      --resource-ids    the resource identifiers to back up, space-separated
  -h, --help            show this help message and exit
  -v, --version         print version information


''')
        parser.add_argument('backup')
        parser.add_argument('action')
        parser.add_argument('-v', '--version', action='version', version='Shelvery v%s' % (__version__))
        parser.add_argument('--resource-ids', help='resource identifiers to back up, comma-separated', nargs='*')

        args = parser.parse_args(sys.argv[1:])

        if args.backup not in ['ebs', 'ec2ami', 'rds', 'rds_cluster']:
            parser.print_usage()
            print ('%s: error: unknown backup type.' % (__file__))
            exit(1)

        if args.action not in ['create_backups', 'clean_backups', 'copy_shared_backups', 'tag_resources']:
            parser.print_usage()
            print ('%s: error: unknown action type.' % (__file__))
            exit(1)

        # Call the method on the resource.
        #backup_engine = ShelveryFactory.get_shelvery_instance(backup_type)
        #method = backup_engine.__getattribute__(action)
        #method(vars(args))


if __name__ == "__main__":
    ShelveryCLI()
