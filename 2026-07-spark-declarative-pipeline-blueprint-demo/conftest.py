
def pytest_addoption(parser):
    parser.addoption("--job-name", action="store", default=None, help="Databricks job name")
    parser.addoption("--pipeline-name", action="store", default=None, help="Databricks pipeline name")
