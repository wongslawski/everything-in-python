# -*- coding: utf-8 -*-

import os
from enum import Enum

SPARK_HOME = None
if os.environ.get("SPARK_HOME", "") != "":
    SPARK_HOME = os.environ["SPARK_HOME"].strip()
assert SPARK_HOME, "error! SPARK_HOME not set!"


class DeployMode(Enum):
    Client = 1
    Cluster = 2


class SparkSubmitWrapper(object):
    def __init__(self):
        """
        --master
            spark://host:port, mesos://host:port, yarn,
            k8s://https://host:port, or local (Default: local[*]).
        """
        self._master = ""

        """
        --deploy-mode
            Whether to launch the driver program locally ("client") or 
            on one of the worker machines inside the cluster ("cluster")
            (Default: client).
        """
        self._deploy_model = DeployMode.Client

        """
        --class
            Your application's main class (for Java / Scala apps).
        """
        self._class = ""

        """
        --name
            A name of your application.
        """
        self._name = ""

        """
        --jars
            list of jars to include on the driver and executor classpaths.
        """
        self._jars = []

        """
        --packages
            list of maven coordinates of jars to include
            on the driver and executor classpaths. Will search the local
            maven repo, then maven central and any additional remote
            repositories given by --repositories. The format for the
            coordinates should be groupId:artifactId:version.
        """
        self._packages = []

        """
        --exclude-packages
            list of groupId:artifactId, to exclude while
            resolving the dependencies provided in --packages to avoid
            dependency conflicts.
        """
        self._exclude_packages = []

        """
        --repositories
            Comma-separated list of additional remote repositories to
            search for the maven coordinates given with --packages.
        """
        self._repositories = []

        """
        --py-files
            list of .zip, .egg, or .py files to place
            on the PYTHONPATH for Python apps.
        """
        self._py_files = []

        """
        --files
            list of files to be placed in the working
            directory of each executor. File paths of these files
            in executors can be accessed via SparkFiles.get(fileName).
        """
        self._files = []

        """
        --conf
            PROP=VALUE. Arbitrary Spark configuration property.
        """
        self._conf = {}

        """
        --properties-file
            Path to a file from which to load extra properties. If not
            specified, this will look for conf/spark-defaults.conf.
        """
        self._properties_file = ""

        """
        --driver-memory
            Memory for driver (e.g. 1000M, 2G) (Default: 1024M).
        """
        self._driver_memory = ""

        """
        --driver-java-options
            Extra Java options to pass to the driver.
        """
        self._driver_java_options = ""

        """
        --driver-library-path
            Extra library path entries to pass to the driver.
        """
        self._driver_library_path = ""

        """
        --driver-class-path
            Extra class path entries to pass to the driver. Note that
            jars added with --jars are automatically included in the classpath.
        """
        self._driver_class_path = ""

        """
        --executor-memory
            Memory per executor (e.g. 1000M, 2G) (Default: 1G).
        """
        self._executor_memory = ""

        """
        --proxy-user
            User to impersonate when submitting the application.
            This argument does not work with --principal / --keytab.
        """
        self._proxy_user = ""

        """
        --driver-cores (Cluster deploy mode only)
            Number of cores used by the driver, only in cluster mode (Default: 1).
        """
        self._driver_cores = -1

        """
        --total-executor-cores (Spark standalone and Mesos only)
            Total cores for all executors.
        """
        self._total_executor_cores = -1

        """
        --executor-cores (Spark standalone and YARN only)
            Number of cores per executor. (Default: 1 in YARN mode,
            or all available cores on the worker in standalone mode)
        """
        self._executor_cores = -1

        """
        --queue (YARN-only)
            The YARN queue to submit to (Default: "default").
        """
        self._queue = ""

        """
        --num-executors (YARN-only)
            Number of executors to launch (Default: 2).
            If dynamic allocation is enabled, the initial number of
            executors will be at least NUM.
        """
        self._num_executors = -1

        """
        --archives (YARN-only)
            list of archives to be extracted into the
            working directory of each executor.
        """
        self._archives = []

        """
        --principal (YARN-only)
            Principal to be used to login to KDC, while running on secure HDFS.
        """
        self._principal = ""

        """
        --keytab (YARN-only)
            The full path to the file that contains the keytab for the
            principal specified above. This keytab will be copied to
            the node running the Application Master via the Secure
            Distributed Cache, for renewing the login tickets and the
            delegation tokens periodically.
        """
        self._keytab = ""

    def set_master(self, master):
        assert master != ""
        self._master = master
        return self

    def set_deploy_mode(self, deploy_mode):
        assert isinstance(deploy_mode, DeployMode), "param must be DeployMode enum"
        self._deploy_model = deploy_mode
        return self

    def set_class(self, param_class):
        assert param_class != ""
        self._class = param_class
        return self

    def set_name(self, name):
        assert name != ""
        self._name = name
        return self

    def add_jar(self, *jars):
        assert len(jars) > 0
        self._jars.extend(jars)
        return self

    def add_package(self, *packages):
        assert len(packages) > 0
        self._packages.extend(packages)
        return self

    def add_exclude_package(self, *packages):
        assert len(packages) > 0
        self._exclude_packages.extend(packages)
        return self

    def add_repository(self, *repositories):
        assert len(repositories) > 0
        self._repositories.extend(repositories)
        return self

    def add_py_file(self, *py_files):
        assert len(py_files) > 0
        self._py_files.extend(py_files)
        return self

    def add_file(self, *files):
        assert len(files) > 0
        self._files.extend(files)
        return self

    def add_conf(self, key, value):
        assert key != ""
        assert value != ""
        self._conf[key] = value
        return self

    def set_properties_file(self, properties_file):
        assert properties_file != ""
        self._properties_file = properties_file
        return self

    def set_driver_memory(self, driver_memory):
        assert isinstance(driver_memory, str)
        assert driver_memory.endswith("M") or driver_memory.endswith("G"), "memory value must end with M/G"
        self._driver_memory = driver_memory
        return self

    def set_driver_java_options(self, driver_java_options):
        assert driver_java_options != ""
        self._driver_java_options = driver_java_options
        return self

    def set_driver_library_path(self, driver_library_path):
        assert driver_library_path != ""
        self._driver_class_path = driver_library_path
        return self

    def set_driver_class_path(self, driver_class_path):
        assert driver_class_path != ""
        self._driver_class_path = driver_class_path
        return self

    def set_executor_memory(self, executor_memory):
        assert isinstance(executor_memory, str)
        assert executor_memory.endswith("M") or executor_memory.endswith("G"), "memory value must end with M/G"
        self._executor_memory = executor_memory
        return self

    def set_proxy_user(self, proxy_user):
        assert proxy_user != ""
        self._proxy_user = proxy_user
        return self

    def set_driver_cores(self, driver_cores):
        assert isinstance(driver_cores, int) and driver_cores > 0
        self._driver_cores = driver_cores
        return self

    def set_total_executor_cores(self, total_executor_cores):
        assert isinstance(total_executor_cores, int)
        assert total_executor_cores > 0
        self._total_executor_cores = total_executor_cores
        return self

    def set_executor_cores(self, executor_cores):
        assert isinstance(executor_cores, int)
        assert executor_cores > 0
        self._executor_cores = executor_cores
        return self

    def set_queue(self, queue):
        assert queue != ""
        self._queue = queue
        return self

    def set_num_executors(self, num_executors):
        assert isinstance(num_executors, int)
        assert num_executors > 0
        self._num_executors = num_executors
        return self

    def add_archive(self, *archives):
        assert len(archives) > 0
        self._archives.extend(archives)
        return self

    def set_principal(self, principal):
        assert principal != ""
        self._principal = principal
        return self

    def set_keytab(self, keytab):
        assert keytab != ""
        self._keytab = keytab
        return self

    def build_cmd(self):
        pass


if __name__ == '__main__':
    submit = SparkSubmitWrapper()