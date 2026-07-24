def pytest_addoption(parser):
    parser.addoption('--qmt-live', action='store_true', default=False,
                     help='Run QMT Mini live integration tests')
