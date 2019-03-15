"""
Performs an incremental backup of the AiiDA repository for the last N days.
Instead of the existing script at aiida.common.additions.backup_script,
it tars two levels down. At most, 65536 tarfiles will be made.
"""
from aiida.common.folders import RepositoryFolder
from aiida.orm import Node
from aiida.orm.querybuilder import QueryBuilder
from aiida.utils import timezone
from aiida.backends.utils import load_dbenv, is_dbenv_loaded

import datetime, os


def mkdir(dirname):
    """
    Utility function that creates a directory if it doesn't exist yet, but doesn't raise
    if it does already.
    :param dirname: the name of the directory
    """
    if os.path.exists(dirname):
        return
    else:
        os.mkdir(dirname)


def get_query(full, node_ids, past_days):
    """
    Construct the query
    :param bool full: if True, returns the full database.
    :param list node_ids: If this is set, and full is False, reurns the Node that match the idea
    :param int past_days: If this is set, and none of the above, returnes the nodes modified in
        the last past_days days.
    :returns: A querybuilder instance.
    """
    if full:
        # Returning a query that searches all nodes.
        return QueryBuilder().append(Node)
    elif node_ids:
        # Filter by id:
        return QueryBuilder().append(Node, filters={'id':{'in':node_ids}})
    elif past_days is not None:
        if past_days < 1:
            raise ValueError('past days has to be at least one')
        # Filtering by days before now:
        n_days_ago = timezone.now() - datetime.timedelta(days=past_days)
        return QueryBuilder().append(Node, filters={'mtime':{'>=': n_days_ago}})
    else:
        raise RuntimeError("Shouldn't get here, node_ids={} past_days={}".format(
                node_ids, past_days))

def get_paths_to_tar(query):
    """
    Constucting a set that contains all the paths that have to be tarred
    :param query: The querybuilder instance
    :returns: a set of filenames
    """
    paths_to_tar = set()
    res = query.all()
    print '{} items to back up'.format(len(res))
    for node, in res:
        print node
        path = RepositoryFolder(
                section='node', #Node._section_name, # hardcoded to be independent of AiiDA Version
                uuid=node.uuid).abspath
        # path: [REPO]/repository/node/24/64/fd5a-749e-4572-b390-53efb766256a
        path_LevelUp = '/'.join(path.split('/')[:-1])
        # path_LevelUp: [REPO]/repository/node/24/64
        paths_to_tar.add(path_LevelUp)
    return sorted(paths_to_tar)

def tar_paths(paths_to_tar, location, ignore_files=[]):
    """
    This functions tars all the paths given to us, leaving in the tar in an appropriate subdirectory
    (respecting the same directory hierarchy) in the set location. 
    :param set paths_to_tar: The set of path names
    :param str location: The parent directory where the tars are stored.
    :param list ignore_files: option to ignore files that won't be included in the archive.
    """
    NODE_SUBFOLDER = 'node'
    LOC = [location]
    mkdir(location) #, ignore_existing=True)
    mkdir('/'.join((location, NODE_SUBFOLDER))) #, ignore_existing=True)
    ignore_pattern = ' '.join(['--exclude={}'.format(f) for f in ignore_files])
    for path in paths_to_tar:
        # Taking the last 2 items in the path:
        splitted_path = path.split('/')
        mkdir('/'.join(LOC+splitted_path[-3:-1]))
        tar_file_name = '/'.join(LOC+splitted_path[-3:])
        if os.path.exists(tar_file_name):
            os.remove(tar_file_name)
        cmd = 'tar cf {}.tar -C {} {} {}'.format(tar_file_name,
                '/'.join(splitted_path[:-1]), splitted_path[-1], ignore_pattern)
        os.system(cmd)


if __name__=='__main__':
    from argparse import ArgumentParser
    # command line arguments:
    parser = ArgumentParser("sddf")
    parser.add_argument('location', help='Location for the backup')
    parser.add_argument('--ignore-files', nargs='+', help='names of files to ignore', default=[])
    # User must specify nodes or past days, but not both!
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-n', '--node-ids', nargs='+', type=int, help='Specific node ids to backup')
    group.add_argument('-p', '--past-days', type=int, help='Number of days to backup, cannot be used if node ids are specified')
    group.add_argument('-f', '--full', action='store_true', help='Backup everything')
    group.add_argument('-t', '--timestamp', action='store_true', help='Backup everything stored after timestamp')

    # parsing the arguments
    parsed = parser.parse_args()

    if not is_dbenv_loaded():
        load_dbenv()

    qb = get_query(full=parsed.full, node_ids=parsed.node_ids, past_days=parsed.past_days)
    paths_to_tar = get_paths_to_tar(query=qb)
    tar_paths(paths_to_tar, location=parsed.location, ignore_files=parsed.ignore_files)
    
