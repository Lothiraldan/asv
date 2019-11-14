from __future__ import unicode_literals

import json
from collections import OrderedDict
from copy import deepcopy

import pytest

from asv.migrations import (
    Executor, MIGRATIONS_KEY,
    octobus_results_to_asv_results,
    to_octobus_results,
    AddBenchParam,
    NotReadyForMigrationsError,
    MigrationDoesNotExist,
    UnknownTarget,
)
from asv.util import UserError

basic_migrations = OrderedDict((
    ("0001_initial", """
from asv import migrations

class Migration(migrations.Migration):
    operations = []
    """),
    ("0002_whatever", """
from asv import migrations

class Migration(migrations.Migration):
    operations = [
        migrations.AddBenchParam(name="benchparam", values=[1], default=1),
    ]
    """),
    ("0003_whatever", """
from asv import migrations

class Migration(migrations.Migration):
    operations = [
        migrations.AddBenchParam(name="benchparam2", values=[None],),
        migrations.AddBenchParam(name="benchparam3", values=["whatever"], default="whatever"),
    ]
    """),
))

migrations_with_regex = OrderedDict((
    ("0001_initial", """
from asv import migrations

class Migration(migrations.Migration):
    operations = []
    """),
    ("0002_whatever", """
from asv import migrations

class Migration(migrations.Migration):
    operations = [
        migrations.AddBenchParam(name="benchparam", values=[1], default=1, targets=r"^status\.mard\..*"),
        migrations.AddBenchParam(name="benchparam2", values=[None], targets=r"^status\.mardu.*"),
    ]
    """),
))

migrations_with_regex_and_position = OrderedDict((
    ("0001_initial", """
from asv import migrations

class Migration(migrations.Migration):
    operations = []
    """),
    ("0002_whatever", """
from asv import migrations

class Migration(migrations.Migration):
    operations = [
        migrations.AddBenchParam(name="benchparam", values=[1], default=1, targets=r"^status\.mard\..*"),
        migrations.AddBenchParam(name="fried_potato", values=["delicious"], default="delicious", targets=r"^status\.mardu.*", insert_after="top_bun"),
        migrations.AddBenchParam(name="sauce", values=[1.034], default=1.034, targets=r"^status\.mardu.*", insert_before="fried_potato"),
        migrations.AddBenchParam(name="lettuce", values=[None], targets=r"^status\.mardu.*", insert_after="top_bun", insert_before="sauce"),
    ]
    """),
))

migrations_with_multiple_values = OrderedDict((
    ("0001_initial", """
from asv import migrations

class Migration(migrations.Migration):
    operations = []
    """),
    ("0002_whatever", """
from asv import migrations

class Migration(migrations.Migration):
    operations = [
        migrations.AddBenchParam(name="max_worker_count", values=[1, 2], default=1,),
    ]
    """),
))

conflicting_migrations = OrderedDict((
    ("0001_initial", ""),
    ("0002_whatever", ""),
    ("0002_conflict", ""),
))

non_contiguous_migrations = OrderedDict((
    ("0001_initial", ""),
    ("0002_whatever", ""),
    ("0004_non_contiguous", ""),
))


@pytest.fixture
def project_dir(request, tmp_path):
    migrations_dir = tmp_path / 'asv_migrations'
    migrations_dir.mkdir()
    migrations = request.param if hasattr(request,
                                          'param') else basic_migrations

    for name, migration_code in migrations.items():
        with (migrations_dir / '{}.py'.format(name)).open(mode='w') as f:
            f.write(migration_code)

    return tmp_path


@pytest.mark.parametrize(
    [
        'file_contents',
        'target',
        'expected_pending',
    ],
    [
        [{MIGRATIONS_KEY: "0002_whatever"}, None, ['0003_whatever']],
        [{MIGRATIONS_KEY: "0003_whatever"}, None, []],
        [{MIGRATIONS_KEY: "0001_initial"}, "0002_whatever", ["0002_whatever"]],
        [{MIGRATIONS_KEY: "0002_whatever"}, "0001_initial", []],
        [{MIGRATIONS_KEY: "0001_initial"}, "0001_initial", []],
    ]
)
def test_pending_migrations(project_dir, file_contents, target,
                            expected_pending):
    executor = Executor(str(project_dir), target=target)

    expected = ['0001_initial', '0002_whatever', '0003_whatever']
    assert list(executor.all_migrations) == expected

    pending = executor.get_pending_migrations_names(file_contents)
    assert pending == expected_pending


def test_unknown_migration(project_dir):
    executor = Executor(str(project_dir))

    expected = ['0001_initial', '0002_whatever', '0003_whatever']
    assert list(executor.all_migrations) == expected

    with pytest.raises(MigrationDoesNotExist):
        executor.get_pending_migrations_names({MIGRATIONS_KEY: "0007_whatever"})


def test_no_migrations(project_dir):
    executor = Executor(str(project_dir))

    expected = ['0001_initial', '0002_whatever', '0003_whatever']
    assert list(executor.all_migrations) == expected

    with pytest.raises(NotReadyForMigrationsError):
        executor.get_pending_migrations_names({})


@pytest.mark.parametrize(["project_dir"], [
    [conflicting_migrations]
], indirect=['project_dir'])
def test_migrations_conflict(project_dir):
    with pytest.raises(UserError) as e:
        Executor(str(project_dir))

    assert "Conflicting migrations" in str(e)


@pytest.mark.parametrize(["project_dir"], [
    [non_contiguous_migrations]
], indirect=['project_dir'])
def test_non_contiguous_migrations(project_dir):
    with pytest.raises(UserError) as e:
        Executor(str(project_dir))

    assert "Migration indexes should be contiguous" in str(e)


def test_unknown_target(project_dir):
    with pytest.raises(UnknownTarget) as e:
        Executor(str(project_dir), target="unknown")

    assert "Unknown target migration" in str(e)

run_migration_parameters = [
[
        basic_migrations,
        {
            'octobus_results': {
                "bench_whatever": [
                    {
                        'result': 0.001,
                        'params': {

                        }
                    },
                    {
                        'result': 0.002,
                        'params': {
                            "benchparam": "2"
                        }
                    },
                    {
                        'result': 0.003,
                        'params': {
                            "benchparam": "2",
                            "benchparam2": "'test'",
                        }
                    }
                ]
            },
            MIGRATIONS_KEY: "0001_initial",
            "result_columns": ['result', 'params', 'version'],
            "bench_param_names":
                {"bench_whatever": ["benchparam", "benchparam2"]},
        },
        {
            'octobus_results': {
                "bench_whatever": [
                    {
                        'result': 0.001,
                        'params': {
                            "benchparam": "1",
                            "benchparam2": "None",
                            "benchparam3": "'whatever'",
                        },
                        "version": None
                    },
                    {
                        'result': 0.002,
                        'params': {
                            "benchparam": "2",
                            "benchparam2": "None",
                            "benchparam3": "'whatever'",
                        },
                        "version": None
                    },
                    {
                        'result': 0.003,
                        'params': {
                            "benchparam": "2",
                            "benchparam2": "'test'",
                            "benchparam3": "'whatever'",
                        },
                        "version": None
                    }
                ]
            },
            "bench_param_names": {
                "bench_whatever": [
                    "benchparam",
                    "benchparam2",
                    "benchparam3",
                ]
            },
            "results": {
                "bench_whatever": [
                    [
                        0.001,
                        0.002,
                        0.003,
                    ],
                    [
                        ["1", "2"],
                        ["None", "'test'"],
                        ["'whatever'"]
                    ],
                    None
                ]
            }
        },
    ],
    [
        migrations_with_regex,
        {
            'octobus_results': {
                "status.mard.track": [
                    {
                        'result': 0.001,
                        'params': {}
                    },
                ],
                "status.mardu.time": [
                    {
                        'result': 0.002,
                        'params': {}
                    },
                ],
            },
            MIGRATIONS_KEY: "0001_initial",
            "result_columns": ['result', 'params', 'version'],
            "bench_param_names": {}
        },
        {
            'octobus_results': {
                "status.mard.track": [
                    {
                        'result': 0.001,
                        'params': {
                            "benchparam": "1"
                        },
                        "version": None
                    },
                ],
                "status.mardu.time": [
                    {
                        'result': 0.002,
                        'params': {
                            "benchparam2": "None"
                        },
                        "version": None
                    },
                ],
            },
            "bench_param_names": {
                "status.mard.track": [
                    "benchparam",
                ],
                "status.mardu.time": [
                    "benchparam2",
                ]
            },
            "results": {
                "status.mard.track": [
                    [
                        0.001,
                    ],
                    [["1"]],
                    None
                ],
                "status.mardu.time": [
                    [
                        0.002,
                    ],
                    [["None"]],
                    None
                ]
            }
        }
    ],
    [
        migrations_with_regex_and_position,
        {
            'octobus_results': {
                "status.mard.track": [
                    {
                        'result': 0.001,
                        'params': {}
                    },
                ],
                "status.mardu.time": [
                    {
                        'result': 0.002,
                        'params': {
                            "bottom_bun": "1",  # In reverse order, should be fine
                            "top_bun": "'See, it's a burger.'",
                        }
                    },
                ],
            },
            MIGRATIONS_KEY: "0001_initial",
            "result_columns": ['result', 'params', 'version'],
            "bench_param_names": {
                "status.mard.track": [],
                "status.mardu.time": [
                    "top_bun",
                    "bottom_bun",
                ]
            }
        },
        {
            'octobus_results': {
                "status.mard.track": [
                    {
                        'result': 0.001,
                        'params': {
                            "benchparam": "1"
                        },
                        "version": None
                    },
                ],
                "status.mardu.time": [
                    {
                        'result': 0.002,
                        'params': {
                            # Out of order
                            "fried_potato": "'delicious'",
                            "bottom_bun": "1",
                            "top_bun": "'See, it's a burger.'",
                            "sauce": "1.034",
                            "lettuce": "None",
                        },
                        "version": None
                    },
                ],
            },
            "bench_param_names": {
                "status.mard.track": [
                    "benchparam",
                ],
                "status.mardu.time": [
                    "top_bun",
                    "lettuce",
                    "sauce",
                    "fried_potato",
                    "bottom_bun",
                ]
            },
            "results": {
                "status.mard.track": [
                    [
                        0.001,
                    ],
                    [["1"]],
                    None
                ],
                "status.mardu.time": [
                    [0.002],
                    [
                        ["'See, it's a burger.'"],  # top_bun
                        ["None"],  # lettuce
                        ["1.034"],  # sauce
                        ["'delicious'"],  # fried_potato
                        ["1"]  # bottom_bun
                    ],
                    None
                ]
            }
        }
    ],
    [
        migrations_with_multiple_values,
        {
            'octobus_results': {
                "status.mard.track": [
                    {
                        'result': 0.001,
                        'params': {
                            "repo": "'test'"
                        }
                    },
                    {
                        'result': 0.002,
                        'params': {
                            "repo": "'test2'"
                        }
                    },
                ],
            },
            MIGRATIONS_KEY: "0001_initial",
            "result_columns": ['result', 'params', 'version'],
            "bench_param_names": {
                "status.mard.track": ["repo"],
            }
        },
        {
            'octobus_results': {
                "status.mard.track": [
                    {
                        'result': 0.001,
                        'params': {
                            "repo": "'test'",
                            "max_worker_count": "1"
                        },
                        "version": None
                    },
                    {
                        'result': None,
                        'params': {
                            "repo": "'test'",
                            "max_worker_count": "2"
                        },
                        "version": None
                    },
                    {
                        'result': 0.002,
                        'params': {
                            "repo": "'test2'",
                            "max_worker_count": "1"
                        },
                        "version": None
                    },
                    {
                        'result': None,
                        'params': {
                            "repo": "'test2'",
                            "max_worker_count": "2"
                        },
                        "version": None
                    },
                ],
            },
            "bench_param_names": {
                "status.mard.track": [
                    "repo",
                    "max_worker_count",
                ],
            },
            "results": {
                "status.mard.track": [
                    [
                        0.001,
                        None,
                        0.002,
                        None,
                    ],
                    [
                        ["'test'", "'test2'"],
                        ["1", "2"]
                    ],
                    None
                ],
            }
        }
    ]
]


@pytest.mark.parametrize(
    ["project_dir", 'contents', 'expected'],
    run_migration_parameters,
    indirect=['project_dir'])
def test_run_migrations(project_dir, contents, expected):
    executor = Executor(str(project_dir))

    result = executor.migrate_file("whatever", deepcopy(contents))
    assert result['octobus_results'] == expected['octobus_results']
    assert result['bench_param_names'] == expected['bench_param_names']
    assert result['results'] == expected['results']


def test_run_migration_with_initial(project_dir):
    benchmarks_data = {
        "bench_whatever": {
            "param_names": ["wew"],

        }
    }
    executor = Executor(str(project_dir), all_benchmarks_data=benchmarks_data)

    contents = {
        "results": {
            "bench_whatever": [
                [
                    0.001,
                    0.002,
                    0.003,
                ],
                [
                    ["'lad'", "'m8'", "'thing'"]
                ]
            ],
        },
        "result_columns": ['result', 'params', 'version'],
    }

    expected = {
        'octobus_results': {
            "bench_whatever": [
                {
                    'result': 0.001,
                    'params': {
                        "wew": "'lad'",
                        "benchparam": "1",
                        "benchparam2": "None",
                        "benchparam3": "'whatever'",
                    },
                    "version": None,
                },
                {
                    'result': 0.002,
                    'params': {
                        "wew": "'m8'",
                        "benchparam": "1",
                        "benchparam2": "None",
                        "benchparam3": "'whatever'",
                    },
                    "version": None,
                },
                {
                    'result': 0.003,
                    'params': {
                        "wew": "'thing'",
                        "benchparam": "1",
                        "benchparam2": "None",
                        "benchparam3": "'whatever'",
                    },
                    "version": None,
                }
            ]
        },
        "bench_param_names": {
            "bench_whatever": [
                "wew",
                "benchparam",
                "benchparam2",
                "benchparam3",
            ]
        },
        "results": {
            "bench_whatever": [
                [
                    0.001,
                    0.002,
                    0.003,
                ],
                [
                    ["'lad'", "'m8'", "'thing'"],
                    ["1"],
                    ["None"],
                    ["'whatever'"]
                ],
                None
            ]
        }
    }
    result = executor.migrate_file("whatever", contents)
    assert dict(result['octobus_results']) == expected['octobus_results']
    assert result['bench_param_names'] == expected['bench_param_names']
    assert result['results'] == expected['results']


@pytest.mark.parametrize(['operation_params', 'error_message'], [
    (
            {"name": "b", "values": [None], "insert_after": "doesnotexist"},
            "`insert_after` (doesnotexist) param not found"
    ),
    (
            {"name": "b",  "values": [None], "insert_before": "doesnotexist"},
            "`insert_before` (doesnotexist) param not found"
    ),
    (
            {
                "name": "b",
                "values": [None],
                "insert_after": "a",
                "insert_before": "d"
            },
            "`insert_after` (a) and `insert_before` (d) are not adjacent (and not null)"
    ),
])
def test_invalid_bench_param_operation(operation_params, error_message):
    bench_param_names = ["a", "c", "d"]
    operation = AddBenchParam(**operation_params)

    with pytest.raises(UserError) as e:
        operation.update_param_names(bench_param_names)

    assert str(error_message) in str(e)


def test_converters_reciprocity():
    """
    Make sure that
    `asv_results -> octobus_results -> asv_results -> octobus_results` works
    """

    data = {
        "octobus_results": {
            "simple_command.read.diff.empty.time_bench": [
                {
                    "stats_ci_99_b": 5,
                    "stats_q_75": 7,
                    "stats_number": 8,
                    "stats_ci_99_a": 4,
                    "version": "version",
                    "params": {
                        "repo-format-dotencode": "True",
                        "repo-format-plain-cl-delta": "True",
                        "repo-format-compression-level": "'default'",
                        "repo-format-generaldelta": "True",
                        "repo-format-sparserevlog": "False",
                        "repo": "'mercurial-2018-08-01'",
                        "repo-format-compression": "'zlib'",
                        "repo-format-fncache": "True",
                        "max_worker_count": "1",
                    },
                    "result": 1,
                    "duration": 3,
                    "stats_q_25": 6,
                    "started_at": 2,
                    "stats_repeat": 9
                },
                {
                    "stats_ci_99_b": None,
                    "stats_q_75": None,
                    "stats_number": None,
                    "stats_ci_99_a": None,
                    "version": "version",
                    "params": {
                        "repo-format-dotencode": "True",
                        "repo-format-plain-cl-delta": "True",
                        "repo-format-compression-level": "'default'",
                        "repo-format-generaldelta": "True",
                        "repo-format-sparserevlog": "False",
                        "repo": "'mercurial-2018-08-01'",
                        "repo-format-compression": "'zlib'",
                        "repo-format-fncache": "True",
                        "max_worker_count": "2",
                    },
                    "result": None,
                    "duration": 3,
                    "stats_q_25": None,
                    "started_at": 2,
                    "stats_repeat": None
                },
                {
                    "stats_ci_99_b": 50,
                    "stats_q_75": 70,
                    "stats_number": 80,
                    "stats_ci_99_a": 40,
                    "version": "version",
                    "params": {
                        "repo-format-dotencode": "True",
                        "repo-format-plain-cl-delta": "True",
                        "repo-format-compression-level": "'default'",
                        "repo-format-generaldelta": "True",
                        "repo-format-sparserevlog": "False",
                        "repo": "'mercurial-2018-08-01'",
                        "repo-format-compression": "'zstd'",
                        "repo-format-fncache": "True",
                        "max_worker_count": "1",
                    },
                    "result": 10,
                    "duration": 3,
                    "stats_q_25": 60,
                    "started_at": 2,
                    "stats_repeat": 90
                },
                {
                    "stats_ci_99_b": None,
                    "stats_q_75": None,
                    "stats_number": None,
                    "stats_ci_99_a": None,
                    "version": "version",
                    "params": {
                        "repo-format-dotencode": "True",
                        "repo-format-plain-cl-delta": "True",
                        "repo-format-compression-level": "'default'",
                        "repo-format-generaldelta": "True",
                        "repo-format-sparserevlog": "False",
                        "repo": "'mercurial-2018-08-01'",
                        "repo-format-compression": "'zstd'",
                        "repo-format-fncache": "True",
                        "max_worker_count": "2",
                    },
                    "result": None,
                    "duration": 3,
                    "stats_q_25": None,
                    "started_at": 2,
                    "stats_repeat": None
                },
            ],
        },
        "bench_param_names": {
            "simple_command.read.diff.empty.time_bench": [
                "repo",
                "repo-format-compression",
                "repo-format-compression-level",
                "repo-format-dotencode",
                "repo-format-fncache",
                "repo-format-generaldelta",
                "repo-format-plain-cl-delta",
                "repo-format-sparserevlog",
                "max_worker_count"
            ]
        },
        "result_columns": [
            "result",
            "params",
            "version",
            "started_at",
            "duration",
            "stats_ci_99_a",
            "stats_ci_99_b",
            "stats_q_25",
            "stats_q_75",
            "stats_number",
            "stats_repeat",
            "samples",
            "profile"
        ],
    }

    expected = {
        "simple_command.read.diff.empty.time_bench": [
            [
                1,
                None,
                10,
                None
            ],
            [
                [
                    "'mercurial-2018-08-01'",
                ],
                [
                    "'zlib'",
                    "'zstd'"
                ],
                [
                    "'default'"
                ],
                [
                    "True"
                ],
                [
                    "True"
                ],
                [
                    "True"
                ],
                [
                    "True"
                ],
                [
                    "False",
                ],
                [
                    "1",
                    "2"
                ]
            ],
            u'version',
            2,
            3,
            [4, None, 40, None],
            [5, None, 50, None],
            [6, None, 60, None],
            [7, None, 70, None],
            [8, None, 80, None],
            [9, None, 90, None]
        ]
    }
    all_benchmarks_data = {
        "simple_command.read.diff.empty.time_bench": {
            "param_names": [
                "repo",
                "repo-format-compression",
                "repo-format-compression-level",
                "repo-format-dotencode",
                "repo-format-fncache",
                "repo-format-generaldelta",
                "repo-format-plain-cl-delta",
                "repo-format-sparserevlog",
                "max_worker_count"
            ],
            "params": [
                [
                    "'mercurial-2018-08-01'",
                ],
                [
                    "'zlib'",
                    "'zstd'"
                ],
                [
                    "'default'"
                ],
                [
                    "True"
                ],
                [
                    "True"
                ],
                [
                    "True"
                ],
                [
                    "True"
                ],
                [
                    "False",
                ],
                [
                    "1",
                    "2"
                ]
            ],
        }
    }
    new_data = octobus_results_to_asv_results(data)
    assert new_data['results'] == expected
    res = to_octobus_results(all_benchmarks_data, new_data)
    new_octobus_results = res["octobus_results"]

    for bench, results in new_octobus_results.items():
        assert results == data['octobus_results'][bench]
