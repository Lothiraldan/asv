# -*- coding: utf-8 -*-
# Licensed under a 3-clause BSD style license - see LICENSE.rst

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os

from . import Command
from ..results import Results
from ..console import log
from .. import util
from .run import Run
from ..migrations import (
    Executor,
    NoMigrationFilesFound,
    MigrationDoesNotExist,
    UnknownTarget,
)


class Migrate(Command):
    EXCLUDED_FILES = util.OCTOBUS_MIGRATIONS_EXCLUDED_FILES

    @classmethod
    def setup_arguments(cls, subparsers):
        parser = subparsers.add_parser(
            "migrate", help="Apply or unapply migrations")

        parser.add_argument("target", type=str, nargs='?', default=None, help="Target migration")
        parser.set_defaults(func=cls.run_from_args)

        return parser

    @classmethod
    def run_from_conf_args(cls, conf, args, _machine_file=None):
        return cls.run(conf, target=args.target)

    @classmethod
    def run(cls, conf, target=None):
        print_target = target if target is not None else "latest"
        log.info("Migrating results data to {}...".format(print_target))

        benchmarks_json_path = os.path.join(conf.results_dir, 'benchmarks.json')
        benchmarks_data = util.load_json(benchmarks_json_path)
        executor = Executor(os.getcwd(), benchmarks_data, target)

        for root, dirs, files in os.walk(conf.results_dir):
            for filename in files:
                if not filename.endswith('.json'):
                    continue
                path = os.path.join(root, filename)
                if filename not in cls.EXCLUDED_FILES:
                    data = util.load_json(path)
                    try:
                        new_data = executor.migrate_file(path, data)
                        util.write_json(path, new_data, Results.api_version,
                                        compact=True)
                    except (NoMigrationFilesFound, MigrationDoesNotExist, UnknownTarget) as err:
                        log.error(str(err))
                        exit(1)
                    except util.UserError as err:
                        # Conversion failed: just skip the file
                        log.warning("{}: {}".format(path, err))
                        continue

        log.info("Updating benchmarks.json...")
        # We can assume regenerating it is always needed after a migration
        with log.indent():
            Run.run(conf, bench=['just-discover'], pull=False)
