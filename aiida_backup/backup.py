"""
Performs an incremental backup of the AiiDA repository for the last N days.
Instead of the existing script at aiida.common.additions.backup_script,
it tars two levels down. At most, 65536 tarfiles will be made.
"""
from aiida.common.folders import RepositoryFolder
from aiida.orm.node import Node
from aiida.orm.querybuilder import QueryBuilder
from aiida.utils import timezone

import datetime, os

def get_query(node_ids, past_days):
    if node_ids:
        return QueryBuilder().append(Node, filters={'id':{'in':node_ids}})
    elif past_days is not None:
        if past_days < 1:
            raise ValueError('past days has to be at least one')

        n_days_ago = timezone.now() - datetime.timedelta(days=past_days)
        return QueryBuilder().append(Node, filters={'mtime':{'>=': n_days_ago}})
    else:
        raise RuntimeError("Shouldn't get here, node_ids={} past_days={}".format(
                node_ids, past_days))

def make_repo(query, location):
    paths_to_tar = set()
    for node, in query.iterall():
        path = RepositoryFolder(
                section=Node._section_name,
                uuid=node.uuid).abspath
        # path: [REPO]/repository/node/24/64/fd5a-749e-4572-b390-53efb766256a
        path_LevelUp = '/'.join(path.split('/')[:-1])
        # path_LevelUp: [REPO]/repository/node/24/64
        paths_to_tar.add(path_LevelUp)


if __name__=='__main__':
    from argparse import ArgumentParser
    # command line arguments:
    parser = ArgumentParser()
    parser.add_argument('location', help='Location for the backup')
    # User must specify nodes or past days, but not both!
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-n', '--node-ids', nargs='+', type=int, help='Specific node ids to backup')
    group.add_argument('-p', '--past-days', type=int, help='Number of days to backup, cannot be used if node ids are specified')
    # parsing the arguments
    parsed = parser.parse_args()

    qb = get_query(node_ids=parsed.node_ids, past_days=parsed.past_days)
    make_repo(query=qb, location=parsed.location)
    
