"""
Migrations system for ASV results.

Following the evolution of both the benchmark target and our knowledge of it,
we sometimes introduce new parameters into our benchmarks, may it be an
environment variable, the number of concurrent threads used, etc., and all the
previous results need to be updated.
Previously, this proved to be a very cumbersome task, as the current result
format is row based instead of "object-oriented" (or "relational" as in SQL
powered systems). Ad-hoc scripts needed to be written to accommodate this very
non-human-friendly format, crawling iteration to a halt.

This module leverages a new format system that more closely resembles what a
traditional relational schema would look like from a human perspective:
information is repeated wherever needed, matrices are used instead of arrays,
leading to less fragmentation and mental overhead.

MIGRATIONS
==========

Note: the migrations system is heavily inspired by the one from Django.

INTRO
-----

Migrations are python files within an ASV project, located in the
`asv_migrations` folder. They represent a set of operations to apply to result
files, in order.

Migrations files follow the following naming scheme: `<index>_<name>`, with
`<index>` usually 0-left-padded to 4 digits (e.g `0002`), and name any valid
identifier for a python module. Indexes need to be contiguous as a safety
measure, and conflicting indexes result in an error; it is your responsibility
to handle them.
Unlike the Django system, there is no "merge" system, and dependencies cannot be
expressed.

Example:

    In `0031_complete_the_burger.py`

    ```
    from asv import migrations

    class Migration(migrations.Migration):
        operations = [
            migrations.AddBenchParam(
                name="benchparam",
            ),
            migrations.AddBenchParam(
                name="benchparam2",
                default=1,
                targets=r"^status\.mard\..*"
            ),
            migrations.AddBenchParam(
                name="fried_potato",
                default="delicious",
                targets=r"^status\.mardu.*",
                insert_after="top_bun"
            ),
            migrations.AddBenchParam(
                name="sauce",
                default=1.034,
                targets=r"^status\.mardu.*",
                insert_before="fried_potato"
            ),
            migrations.AddBenchParam(
                name="lettuce",
                targets=r"^status\.mardu.*",
                insert_after="top_bun",
                insert_before="sauce"
            ),
        ]
    ```

Warning:

    A migration with index `1` needs to be present on the filesystem. It is the
    initial migration that could be used in the future if a Django-like
    introspection system is implemented, i.e. if migrations could be
    automatically inferred from changes in the benchmarks code.
    As of now, it is a marker that results files have been "readied" for
    migrations. Only operations in subsequent migrations will be applied.

For now, only one operation exists: `AddBenchParam`, but more will be added as
needed.

USAGE
-----

Running `asv migrate` will look through all results files and migrate them on a
per file basis. You an specify a target and it will move forwards (backwards
is not yet supported) up to that target.

Example:

    ```
    $ asv migrate
    $ asv migrate 0004_name  # Files at index 0005 or more will be migrated back
    $ asv migrate 0010_whatever
    ```

Warning:

    As the current migration system is not officially supported by ASV, be
    careful of only manipulating parameters that were not already present, as
    a backwards migration will delete the parameter since the migration system
    had no record of it existing before.

    For example, if you already have a `foobar` parameter for some of your tests
    and create a migration with a `AddBenchParam` operation for `foobar`,
    targeted results missing the `foobar` param will be updated with its default
    value, but running the migration backwards will remove both these default
    values *and* the ones that were there before.
"""
from __future__ import unicode_literals, print_function, absolute_import

import glob
import os
import re
import sys
import itertools

from collections import OrderedDict, defaultdict
from copy import deepcopy

from .benchmark import _repr_no_address
from .util import UserError
from .console import log

MIGRATIONS_KEY = 'octobus_migration_version'
NO_MIGRATION_INDEX = '0000'
INITIAL_MIGRATION_INDEX = '0001'
INITIAL_MIGRATION = '{}_initial'.format(INITIAL_MIGRATION_INDEX)


class NotReadyForMigrationsError(Exception):
    pass


class NoMigrationFilesFound(Exception):
    pass


class MigrationDoesNotExist(Exception):
    pass


class UnknownTarget(Exception):
    pass


class Executor(object):
    def __init__(self, project_root, all_benchmarks_data=None, target=None):
        self.project_root = project_root

        migrations_folder = os.path.join(project_root, 'asv_migrations')
        self.migrations_folder = os.path.abspath(migrations_folder)

        glob_pattern = os.path.join(self.migrations_folder, '*.py')
        self.all_migrations = (
            (os.path.basename(f).replace(".py", ""), f)
            for f in glob.glob(glob_pattern)
        )
        # Sort migrations numerically
        key = lambda x: get_migration_index(x[0])
        self.all_migrations = OrderedDict(sorted(self.all_migrations, key=key))

        if not self.all_migrations:
            raise NoMigrationFilesFound("No migrations file found, aborting.")
        if target is None:
            self.target = list(self.all_migrations.keys())[-1]
        elif target not in self.all_migrations:
            raise UnknownTarget("Unknown target migration `{}`".format(target))
        else:
            self.target = target

        # Check for conflicting indexes
        seen = {}
        last_index = 0
        for name in self.all_migrations:
            index = get_migration_index(name)

            conflict = seen.get(index)
            if conflict is not None:
                msg = "Conflicting migrations index: `{}` and `{}`"
                raise UserError(msg.format(conflict, name))
            seen[index] = name

            if last_index != index - 1:
                msg = "Migration indexes should be contiguous: `{}` and `{}`"
                raise UserError(msg.format(last_index, index))
            last_index = index


        self.all_benchmarks_data = all_benchmarks_data
        self._migrations_modules = {}

    @property
    def all_migrations_names(self):
        return self.all_migrations

    @property
    def all_migrations_indexes(self):
        indexes = [get_migration_index(n) for n in self.all_migrations]
        return indexes

    @property
    def current_migration_index(self):
        return get_migration_index_from_path(self.current_migration_name)

    @property
    def current_migration_name(self):
        if not len(self.all_migrations):
            return int(NO_MIGRATION_INDEX)
        return list(self.all_migrations)[-1]

    def get_migration_instance(self, name):
        cached = self._migrations_modules.get(name)
        if cached is not None:
            return cached

        path = self.all_migrations[name]

        # FIXME there probably is a namespacing issue
        if sys.version_info < (3, 5):
            import imp
            module = imp.load_source(name, path)
        else:
            import importlib.util
            spec = importlib.util.spec_from_file_location(name, path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

        result = module.Migration(name)

        # Cache the import
        self._migrations_modules[name] = result

        return result

    def get_pending_migrations_names(self, file_contents):
        file_migration_idx = get_file_migration_index(file_contents)

        if file_migration_idx == 0:
            raise NotReadyForMigrationsError()

        if file_migration_idx not in self.all_migrations_indexes:
            msg = "`{}` is an unknown migration. " \
                  "Please synchronize your migrations files."
            raise MigrationDoesNotExist(
                msg.format(file_migration_idx, self.current_migration_index)
            )

        target_index = get_migration_index(self.target)

        pending = []
        for migration in self.all_migrations:
            index = get_migration_index(migration)
            if target_index >= index > file_migration_idx:
                pending.append(migration)

        return pending

    def get_pending_migrations(self, file_contents):
        pending = []

        pending_names = self.get_pending_migrations_names(file_contents)
        for name in pending_names:
            pending.append(self.get_migration_instance(name))

        return pending

    def migrate_file(self, path, file_contents):
        try:
            pending = self.get_pending_migrations(file_contents)
        except NotReadyForMigrationsError:
            # File hasn't had its initial migration done
            file_contents = to_octobus_results(self.all_benchmarks_data, file_contents)
            file_contents[MIGRATIONS_KEY] = INITIAL_MIGRATION
            pending = self.get_pending_migrations(file_contents)

        if not len(pending):
            log.debug("No migrations to apply for {}.".format(path))
            return file_contents

        for migration in pending:
            migration.apply(file_contents)

        file_contents[MIGRATIONS_KEY] = self.target

        # Keep compat with asv
        new_contents = octobus_results_to_asv_results(file_contents)
        return new_contents


class Migration(object):
    # Operations to apply during this migration, in order.
    operations = []

    def __init__(self, name):
        self.name = name
        # Copy operations as we might mutate them at runtime
        self.operations = list(self.__class__.operations)

    def __eq__(self, other):
        return isinstance(other, Migration) and self.name == other.name

    def __repr__(self):
        return "<Migration %s>" % self.name

    def __str__(self):
        return "%s" % self.name

    def __hash__(self):
        return hash("%s" % self.name)

    def apply(self, state):
        log.debug("Applying migration {!r}".format(self.name))
        for operation in self.operations:
            operation.forwards(state)

    def unapply(self, state):
        log.debug("Unapplying migration {!r}".format(self.name))
        for operation in reversed(self.operations):
            operation.backwards(state)


class Operation(object):
    """
    Largely stolen from django's migration model.
    """

    def __new__(cls, *args, **kwargs):
        # We capture the arguments to make returning them trivial
        self = object.__new__(cls)
        self._constructor_args = (args, kwargs)
        return self

    def deconstruct(self):
        """
        Return a 3-tuple of class import path (or just name if it lives
        under asv_migrations), positional arguments, and keyword arguments.
        """
        return (
            self.__class__.__name__,
            self._constructor_args[0],
            self._constructor_args[1],
        )

    def forwards(self, state):
        """
        Take the state from the previous migration, and mutate it
        so that it matches what this migration would perform.
        """
        msg = 'subclasses of Operation must provide a forwards() method'
        raise NotImplementedError(msg)

    def describe(self):
        """
        Output a brief summary of what the action does.
        """
        return "%s: %s" % (self.__class__.__name__, self._constructor_args)

    def __repr__(self):
        return "<%s %s%s>" % (
            self.__class__.__name__,
            ", ".join(map(repr, self._constructor_args[0])),
            ",".join(" %s=%r" % x for x in self._constructor_args[1].items()),
        )


class AddBenchParam(Operation):
    """
    Adds a parameter to targeted benchmarks.

    - `name` is the name of the new parameter
    - `default` is an optional default value for filling old results
    - `targets` is an optional Python regex string that specifies the names of
      the benchmarks to which to apply this operation. Defaults to `".*"`
    - `insert_after` is an optional name of the parameter that should precede
      this one
    - `insert_before` is an optional name of the parameter that should follow
      this one

    If both `insert_after` and `insert_before` are given, they need to be
    contiguous. If none of them are given, the parameter will be appended at the
    end of the list.
    """

    def __init__(self, name, values, default=None, targets='.*', insert_after=None, insert_before=None):
        self.name = name
        if len(set(values)) < len(values):
            msg = "Values must be unique: `{}`"
            raise UserError(msg.format(values))
        if default is not None and default not in values:
            msg = "Wrong default value `{}`, must be in `{}`"
            raise UserError(msg.format(default, values))
        self.values = [_repr_no_address(item) for item in values]
        self.default = _repr_no_address(default)
        self.targets = re.compile(targets)
        self.insert_after = insert_after
        self.insert_before = insert_before

    def is_benchmark_targeted(self, benchmark_name):
        return self.targets.match(benchmark_name)

    def forwards(self, state):
        log.debug("Applying operation {!r}".format(self.deconstruct()))
        param_names = state["bench_param_names"]
        octobus_results = state['octobus_results']
        new_octobus_results = {}

        for name, bench in octobus_results.items():
            if not self.is_benchmark_targeted(name):
                new_octobus_results[name] = octobus_results[name]
                continue

            param_names.setdefault(name, [])
            bench_param_names = param_names[name]

            self.update_param_names(bench_param_names)
            new_octobus_results.setdefault(name, [])

            for result in bench:
                for value in self.values:
                    new_result = deepcopy(result)
                    bench_params = new_result['params']

                    if value == self.default:
                        bench_params.setdefault(self.name, self.default)
                    else:
                        bench_params[self.name] = value
                        new_result['result'] = None
                        useless_columns = [
                            "stats_ci_99_a",
                            "stats_ci_99_b",
                            "stats_q_25",
                            "stats_q_75",
                            "stats_number",
                            "stats_repeat",
                            "samples",
                            "profile"
                        ]
                        for column in useless_columns:
                            new_result.pop(column, None)
                    new_result['version'] = None
                    new_octobus_results[name].append(new_result)

        state["octobus_results"] = new_octobus_results

    def update_param_names(self, bench_param_names):
        # TODO refactor, this is probably redundant
        if not self.insert_after and not self.insert_before and self.name not in bench_param_names:
            bench_param_names.append(self.name)
        elif self.insert_after and self.insert_before:
            for index, param_name in enumerate(bench_param_names):
                if param_name == self.name:
                    break
                if param_name == self.insert_after:
                    if bench_param_names[index + 1] == self.insert_before:
                        bench_param_names.insert(index + 1, self.name)
                        break
                    else:
                        msg = "Cannot insert param `{}`: `insert_after` ({}) and " \
                              "`insert_before` ({}) are not adjacent (and not null)"
                        msg = msg.format(self.name, self.insert_after,
                                         self.insert_before)
                        raise UserError(msg)
            else:
                msg = "Cannot insert param `{}`: `insert_after` ({}) param not found"
                raise UserError(msg.format(self.name, self.insert_after))
        elif self.insert_after:
            for index, param_name in enumerate(bench_param_names):
                if param_name == self.name:
                    break
                if param_name == self.insert_after:
                    bench_param_names.insert(index + 1, self.name)
                    break
            else:
                msg = "Cannot insert param `{}`: `insert_after` ({}) param not found"
                raise UserError(msg.format(self.name, self.insert_after))

        elif self.insert_before:
            for index, param_name in enumerate(bench_param_names):
                if param_name == self.name:
                    break
                if param_name == self.insert_before:
                    bench_param_names.insert(index, self.name)
                    break
            else:
                msg = "Cannot insert param `{}`: `insert_before` ({}) param not found"
                raise UserError(msg.format(self.name, self.insert_before))


def to_octobus_results(all_benchmarks_data, old_format_data):
    new = deepcopy(old_format_data)
    new["octobus_results"] = defaultdict(list)

    results = old_format_data.get('results')
    if results is None:
        return new

    new["bench_param_names"] = {}

    for bench_name, result_lines in results.items():
        try:
            current_bench_data = all_benchmarks_data[bench_name]
        except KeyError:
            log.warning(
                "Test {} does not exist in benchmarks.json,"
                " skipping".format(
                    bench_name
                ))
            continue
        param_names = current_bench_data['param_names']
        new["bench_param_names"][bench_name] = param_names

        # Columns x lines
        result_columns = old_format_data['result_columns']
        assert len(result_columns) >= len(result_lines)
        results_as_object = OrderedDict(zip(result_columns, result_lines))

        assert_tuple = (
            bench_name,
            param_names,
            results_as_object['params']
        )

        assert len(param_names) >= len(results_as_object['params']), assert_tuple

        # Columns x lines for params as well
        results_as_object['params'] = OrderedDict(
            zip(param_names, results_as_object['params']))

        # Generate cartesian product of all params
        explosion = []
        for x in itertools.product(*results_as_object['params'].values()):
            explosion.append(OrderedDict(zip(results_as_object['params'], x)))

        for index, combo in enumerate(explosion):
            new_result = {}
            new_result["params"] = combo

            # Match each combination with its results/stats, etc.
            for field in result_columns:
                if field == "params":
                    continue

                try:
                    corresponding_values = results_as_object[field]
                except KeyError:
                    # A field is missing (no stats, for instance)
                    continue
                if isinstance(corresponding_values, list):
                    try:
                        new_result[field] = corresponding_values[index]
                    except IndexError:
                        new_result[field] = None
                else:
                    new_result[field] = corresponding_values

            new["octobus_results"][bench_name].append(new_result)

    return new

def octobus_results_to_asv_results(data):
    octobus_results = data.get('octobus_results')

    if not octobus_results:
        msg = "Results data should contains non-empty `octobus_results`"
        raise UserError(msg)

    SCALAR_COLUMNS = {'version', 'started_at', 'duration'}
    out_data = deepcopy(data)
    out_data['results'] = {}

    result_columns = data['result_columns']
    bench_param_names = data['bench_param_names']

    for benchmark, results in octobus_results.items():
        asv_results = OrderedDict()
        asv_params = OrderedDict()

        for result in results:
            for column in result_columns:
                if column == "params":
                    for name in bench_param_names[benchmark]:
                        asv_params.setdefault(name, OrderedDict())
                        param_value = result['params'][name]

                        # There is no `OrderedSet` in Python < 3.7
                        # so we use the keys of an `OrderedDict` instead
                        asv_params[name][param_value] = None
                if column in SCALAR_COLUMNS:
                    # Not a list, same result for all results
                    asv_results[column] = result.get(column)
                    continue
                try:
                    corresponding_value = result[column]
                except KeyError:
                    # this field is missing, so are the rest since it's sorted
                    break
                else:
                    asv_results.setdefault(column, [])
                    asv_results[column].append(corresponding_value)

        params = (list(v.keys()) for v in asv_params.values())
        asv_results['params'] = list(params)
        out_data['results'][benchmark] = list(asv_results.values())

    return out_data


def get_migration_index(migration_name):
    return int(migration_name.split('_')[0])


def get_migration_index_from_path(migration_path):
    migration_name = os.path.basename(migration_path)
    return get_migration_index(migration_name)


def get_file_migration_index(data):
    migration_name = data.get(MIGRATIONS_KEY, NO_MIGRATION_INDEX)
    return get_migration_index(migration_name)


def check_migration_version(current_migration_idx, path, data, is_update=False):
    file_migration_index = get_file_migration_index(data)

    if not is_update:
        if file_migration_index < current_migration_idx:
            msg = "`{}` is at migration `{}` and needs to be migrated to " \
                  "`{}`. Run `asv update` to update it."
            raise UserError(
                msg.format(path,
                           file_migration_index,
                           current_migration_idx))
    if file_migration_index > current_migration_idx:
        msg = "`{}` is at a newer migration `{}` than is avaible in " \
              "migrations files `{}`. Please synchronize your migrations files."
        raise UserError(
            msg.format(path,
                       file_migration_index,
                       current_migration_idx))

    return (current_migration_idx, file_migration_index)
