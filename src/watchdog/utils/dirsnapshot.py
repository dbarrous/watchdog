# Copyright 2011 Yesudeep Mangalapilly <yesudeep@gmail.com>
# Copyright 2012 Google, Inc & contributors.
# Copyright 2014 Thomas Amland <thomas.amland@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
:module: watchdog.utils.dirsnapshot
:synopsis: Directory snapshots and comparison.
:author: yesudeep@google.com (Yesudeep Mangalapilly)
:author: contact@tiger-222.fr (Mickaël Schoentgen)

.. ADMONITION:: Where are the moved events? They "disappeared"

        This implementation does not take partition boundaries
        into consideration. It will only work when the directory
        tree is entirely on the same file system. More specifically,
        any part of the code that depends on inode numbers can
        break if partition boundaries are crossed. In these cases,
        the snapshot diff will represent file/directory movement as
        created and deleted events.

Classes
-------
.. autoclass:: DirectorySnapshot
   :members:
   :show-inheritance:

.. autoclass:: DirectorySnapshotDiff
   :members:
   :show-inheritance:

.. autoclass:: EmptyDirectorySnapshot
   :members:
   :show-inheritance:

"""

from __future__ import annotations

import errno
import os
from stat import S_ISDIR
import sqlite3


class DirectorySnapshotDiff:
    def __init__(self, previous_dirsnap, current_dirsnap):
        self.previous_dirsnap = previous_dirsnap
        self.current_dirsnap = current_dirsnap
        self.added, self.removed, self.modified = self.compute_diff()

    def compute_diff(self):
        previous_paths = self.previous_dirsnap.paths
        current_paths = self.current_dirsnap.paths

        added = current_paths - previous_paths
        removed = previous_paths - current_paths

        modified = set()
        for path in previous_paths.intersection(current_paths):
            prev_inode, prev_device = self.previous_dirsnap.inode(path)
            cur_inode, cur_device = self.current_dirsnap.inode(path)

            if prev_inode != cur_inode or prev_device != cur_device:
                modified.add(path)
            else:
                prev_mtime = self.previous_dirsnap.mtime(path)
                cur_mtime = self.current_dirsnap.mtime(path)
                if prev_mtime != cur_mtime:
                    modified.add(path)

        return added, removed, modified

    def __repr__(self):
        return f"<{type(self).__name__} added={len(self.added)} removed={len(self.removed)} modified={len(self.modified)}>"

    def __str__(self):
        return self.__repr__()

    def __getitem__(self, item):
        if item == "added":
            return self.added
        elif item == "removed":
            return self.removed
        elif item == "modified":
            return self.modified
        else:
            raise KeyError(f"Invalid key: {item}")

    def __iter__(self):
        yield "added", self.added
        yield "removed", self.removed
        yield "modified", self.modified


class DirectorySnapshot:
    def __init__(self, path, db_path, recursive=True, stat=os.stat, listdir=os.scandir):
        self.recursive = recursive
        self.stat = stat
        self.listdir = listdir

        self.db_path = db_path

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshot
                (path TEXT PRIMARY KEY, inode INTEGER, device INTEGER, is_dir INTEGER, mtime REAL, size INTEGER)
                """
            )

            st = self.stat(path)
            self.add_entry_to_db(conn, path, st)

            for p, st in self.walk(path):
                self.add_entry_to_db(conn, p, st)

    def add_entry_to_db(self, conn, path, st):
        conn.execute(
            """
            INSERT OR REPLACE INTO snapshot (path, inode, device, is_dir, mtime, size)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                path,
                st.st_ino,
                st.st_dev,
                int(S_ISDIR(st.st_mode)),
                st.st_mtime,
                st.st_size,
            ),
        )
        conn.commit()

    def walk(self, root):
        try:
            paths = [os.path.join(root, entry.name) for entry in self.listdir(root)]
        except OSError as e:
            if e.errno in (errno.ENOENT, errno.ENOTDIR, errno.EINVAL):
                return
            else:
                raise

        entries = []
        for p in paths:
            try:
                entry = (p, self.stat(p))
                entries.append(entry)
                yield entry
            except OSError:
                continue

        if self.recursive:
            for path, st in entries:
                try:
                    if S_ISDIR(st.st_mode):
                        for entry in self.walk(path):
                            yield entry
                except PermissionError:
                    pass

    @property
    def paths(self):
        with sqlite3.connect(self.db_path) as conn:
            return {row[0] for row in conn.execute("SELECT path FROM snapshot")}

    def path(self, inode, device):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT path FROM snapshot WHERE inode = ? AND device = ?",
                (inode, device),
            ).fetchone()
            return row[0] if row else None

    def inode(self, path):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT inode, device FROM snapshot WHERE path = ?", (path,)
            ).fetchone()
            return row if row else (None, None)

    def isdir(self, path):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT is_dir FROM snapshot WHERE path = ?", (path,)
            ).fetchone()
            return bool(row[0]) if row else False

    def mtime(self, path):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT mtime FROM snapshot WHERE path = ?", (path,)
            ).fetchone()
            return row[0] if row else None

    def size(self, path):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT size FROM snapshot WHERE path = ?", (path,)
            ).fetchone()

            return row[0] if row else None

    def delete_snapshot(self):
        os.remove(self.db_path)

    def __sub__(self, previous_dirsnap):
        """Allow subtracting a DirectorySnapshot object instance from
        another.
        :returns:
            A :class:`DirectorySnapshotDiff` object.
        """
        return DirectorySnapshotDiff(previous_dirsnap, self)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        with sqlite3.connect(self.db_path) as conn:
            return f"<{type(self).__name__} db_path={self.db_path} entries={conn.execute('SELECT COUNT(*) FROM snapshot').fetchone()[0]}>"


class EmptyDirectorySnapshot:
    """Class to implement an empty snapshot. This is used together with
    DirectorySnapshot and DirectorySnapshotDiff in order to get all the files/folders
    in the directory as created.
    """

    @staticmethod
    def path(_):
        """Mock up method to return the path of the received inode. As the snapshot
        is intended to be empty, it always returns None.

        :returns:
            None.
        """
        return None

    @property
    def paths(self):
        """Mock up method to return a set of file/directory paths in the snapshot. As
        the snapshot is intended to be empty, it always returns an empty set.

        :returns:
            An empty set.
        """
        return set()
