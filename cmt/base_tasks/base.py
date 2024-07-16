# coding: utf-8

"""
Base tasks.
"""

__all__ = [
    "Task", "ConfigTask", "ConfigTaskWithCategory", "DatasetTask", "DatasetTaskWithCategory",
    "DatasetWrapperTask", "HTCondorWorkflow", "SGEWorkflow", "SlurmWorkflow" "InputData",
]


import re
import os
import math
from collections import OrderedDict

import warnings
with warnings.catch_warnings():
    warnings.filterwarnings('ignore')
    import cppyy

import luigi
import law

from law.util import merge_dicts
from law.contrib.htcondor.job import HTCondorJobFileFactory
from cmt.sge.job import SGEJobFileFactory
from cmt.sge.workflow import SGEWorkflow as SGEWorkflowTmp
# from cmt.condor_tools.htcondor import HTCondorWorkflowExt

from abc import abstractmethod

from analysis_tools import ObjectCollection
from analysis_tools.utils import import_root

law.contrib.load("cms", "git", "htcondor", "slurm", "root", "tasks", "telegram", "tensorflow", "wlcg")


class Target():
    def __init__(self, path, *args, **kwargs):
        self.path = path


class Task(law.Task):

    version = luigi.Parameter(description="version of outputs to produce")
    request_cpus = luigi.IntParameter(default=1, description="number of cpus requested, default: 1")
    notify = law.telegram.NotifyTelegramParameter()

    default_store = "$CMT_STORE"
    default_wlcg_fs = "wlcg_fs"

    # law related configs
    exclude_params_req = {"notify"}
    exclude_params_branch = {"notify"}
    exclude_params_workflow = {"notify"}
    exclude_params_repr = {"notify"}
    output_collection_cls = law.SiblingFileCollection
    workflow_run_decorators = [law.decorator.notify]
    local_workflow_require_branches = False
    message_cache_size = 20

    @classmethod
    def vreq(cls, inst, **kwargs):
        # try to insert cls version when not already set
        if "version" not in kwargs:
            config = getattr(inst, "config", None)
            if config:
                version = config.versions.get(cls.task_family)
                if version:
                    kwargs["version"] = version

        # ensure that values from the cli always have precedence
        _prefer_cli = law.util.make_list(kwargs.get("_prefer_cli", []))
        if "version" not in _prefer_cli:
            _prefer_cli.append("version")
        kwargs["_prefer_cli"] = _prefer_cli

        return cls.req(inst, **kwargs)

    def store_parts(self):
        parts = OrderedDict()
        parts["task_family"] = self.task_family
        return parts

    def store_parts_ext(self):
        parts = OrderedDict()
        if self.version is not None:
            parts["version"] = self.version
        return parts

    def local_path(self, *path, **kwargs):
        store = kwargs.get("store") or self.default_store
        store = os.path.expandvars(os.path.expanduser(store))
        parts = tuple(self.store_parts().values()) + tuple(self.store_parts_ext().values()) + path
        return os.path.join(store, *[str(p) for p in parts])

    def local_target(self, *args, **kwargs):
        cls = law.LocalDirectoryTarget if kwargs.pop("dir", False) else law.LocalFileTarget
        return cls(self.local_path(*args, store=kwargs.pop("store", None)), **kwargs)

    def wlcg_path(self, *path, **kwargs):
        if "InputData" in str(type(self)):
            parts = list(path)[0]
            return "/".join([parts])
        else:
            parts = tuple(self.store_parts().values()) + tuple(self.store_parts_ext().values()) + path
            return os.path.join(*[str(p) for p in parts])

    def wlcg_target(self, *args, **kwargs):
        kwargs.setdefault("fs", self.default_wlcg_fs)
        if "InputData" in str(type(self)):
            cls = Target
        else:
            cls = (law.wlcg.WLCGDirectoryTarget if kwargs.pop("dir", False)
                else law.wlcg.WLCGFileTarget)
        path = self.wlcg_path(*args, **kwargs)
        kwargs.pop("avoid_store", False)
        return cls(path, **kwargs)

    def dynamic_target(self, *args, **kwargs):
        if (
            "InputData" in str(type(self)) or
            ("PreprocessRDF" in str(type(self)) and os.getenv("CMT_REMOTE_PREPROCESSING", "0") == "1")
        ):
            return self.wlcg_target(*args, **kwargs)
        else:
            return self.local_target(*args, **kwargs)

    def retrieve_file(self, filename):
        filenames = [
            filename,
            os.path.expandvars("$CMT_BASE/../{}".format(filename)),
            os.path.expandvars("$CMT_BASE/cmt/{}".format(filename))
        ]
        for f in filenames:
            if os.path.isfile(f):
                return f
        return ""
        # raise ValueError("File %s could not be found under the default paths" % filename)


class ConfigTask(Task):

    config_name = luigi.Parameter(default="base_2018", description="name of the config file to "
        "load, default: base_2018")

    def __init__(self, *args, **kwargs):
        super(ConfigTask, self).__init__(*args, **kwargs)

        # load the config
        try:
            config = __import__("config." + self.config_name)
            self.config = getattr(config, self.config_name).config
        except ModuleNotFoundError:
            cmt = __import__("cmt.config." + self.config_name)
            self.config = getattr(cmt.config, self.config_name).config

    def store_parts(self):
        parts = super(ConfigTask, self).store_parts()
        parts["config_name"] = self.config_name
        return parts


class ConfigTaskWithCategory(ConfigTask):

    category_name = luigi.Parameter(default="baseline_even", description="the name of a category "
        "whose selection rules are applied, default: baseline_even")
    # use_base_category = luigi.BoolParameter(default=False, description="use the base category of "
    #     "the requested category for requirements, apply the selection on-the-fly, default: False")
    use_base_category = False  # currently disabled

    allow_composite_category = False

    def __init__(self, *args, **kwargs):
        super(ConfigTaskWithCategory, self).__init__(*args, **kwargs)

        self.category = self.config.categories.get(self.category_name)

        if self.category.subcategories and not self.allow_composite_category:
            raise Exception("category '{}' is composite, prohibited by task {}".format(
                self.category.name, self))

    def store_parts(self):
        parts = super(ConfigTaskWithCategory, self).store_parts()
        parts["category_name"] = "cat_" + self.category_name
        return parts

    def expand_category(self):
        if self.category.subcategories:
            return self.category.subcategories
        else:
            return [self.category]

    def get_data_category(self, category=None):
        if category is None:
            category = self.category

        # if self.use_base_category and category.x.base_category:
            # return self.config.categories.get(category.x.base_category)
        # else:
            # return category
        return category


class ConfigTaskWithRegion(ConfigTask):

    region_name = luigi.Parameter(default=law.NO_STR, description="an optional name of a region "
        "to apply live, default: empty")

    def __init__(self, *args, **kwargs):
        super(ConfigTaskWithRegion, self).__init__(*args, **kwargs)

        self.region = None
        if self.region_name and self.region_name != law.NO_STR:
            self.region = self.config.regions.get(self.region_name)


class DatasetTask(ConfigTask):

    dataset_name = luigi.Parameter(default="hh_ggf", description="name of the dataset to process, "
        "default: hh_ggf")
    tree_name = luigi.Parameter(default=law.NO_STR, description="name of the tree inside "
        "the root file, default: Events (nanoAOD)")

    default_tree_name = "Events"

    def __init__(self, *args, **kwargs):
        super(DatasetTask, self).__init__(*args, **kwargs)

        # store a reference to the dataset object
        self.dataset = self.config.datasets.get(self.dataset_name)

        # store a reference to the main process
        self.process = self.dataset.process

        if self.tree_name == law.NO_STR:
            self.tree_name = getattr(self.config, "tree_name", self.default_tree_name)

    def store_parts(self):
        parts = super(DatasetTask, self).store_parts()
        parts["dataset_name"] = self.dataset_name
        return parts


class DatasetTaskWithCategory(ConfigTaskWithCategory, ConfigTaskWithRegion, DatasetTask):

    def __init__(self, *args, **kwargs):
        super(DatasetTaskWithCategory, self).__init__(*args, **kwargs)

        if self.dataset.get_aux("merging", None):
            self.n_files_after_merging = self.dataset.get_aux("merging").get(self.category.name, 1)
        else:
            self.n_files_after_merging = 1


class DatasetWrapperTask(ConfigTask):

    dataset_names = law.CSVParameter(default=(), description="names or name "
        "patterns of datasets to use, uses all datasets when empty, default: ()")
    process_names = law.CSVParameter(default=(), description="names or name "
        "patterns of processes to use, default: ()")
    dataset_tags = law.CSVParameter(default=(), description="list of tags for "
        "filtering datasets selected via dataset_names, default: ()")
    skip_dataset_names = law.CSVParameter(default=(), description="names or name pattern of "
        "datasets to skip, default: ()")
    skip_dataset_tags = law.CSVParameter(default=(), description="list of tags of datasets to "
        "skip, default: ()")

    def _find_datasets(self, names, tags):
        datasets = []
        for pattern in names:
            for dataset in self.config.datasets:
                if law.util.multi_match(dataset.name, pattern):
                    datasets.append(dataset)
        for tag in tags:
            for dataset in self.config.datasets:
                if dataset.has_tag(tag) and dataset not in datasets:
                    datasets.append(dataset)
        return datasets

    def __init__(self, *args, **kwargs):
        super(DatasetWrapperTask, self).__init__(*args, **kwargs)

        if getattr(self, "run_period", False):
            assert self.run_period in self.config.get_run_periods()
            self.skip_dataset_tags = list(self.skip_dataset_tags) \
                + [elem for elem in self.config.get_run_periods() if elem != self.run_period]

        if getattr(self, "run_era", False):
            assert self.config.get_run_period_from_run_era(self.run_era) != None
            self.skip_dataset_tags = list(self.skip_dataset_tags) \
                + [elem for elem in self.config.get_run_eras() if elem != self.run_era] \
                + [elem for elem in self.config.get_run_periods()
                    if elem != self.config.get_run_period_from_run_era(self.run_era)]

        # first get datasets to skip
        skip_datasets = self._find_datasets(self.skip_dataset_names, self.skip_dataset_tags)

        # then get actual datasets and filter
        dataset_names = list(self.dataset_names)
        if not dataset_names and self.process_names:
            for dataset in self.config.datasets:
                if any([self.config.is_process_from_dataset(process, dataset=dataset)
                        for process in self.process_names]):
                    dataset_names.append(dataset.name)

        if not dataset_names and not self.dataset_tags:
            dataset_names = self.get_default_dataset_names()
        self.datasets = [
            dataset for dataset in self._find_datasets(dataset_names, self.dataset_tags)
            if dataset not in skip_datasets
        ]

    def get_default_dataset_names(self):
        return list(self.config.datasets.names())


class HTCondorWorkflow(law.htcondor.HTCondorWorkflow):

    only_missing = luigi.BoolParameter(default=True, significant=False, description="skip tasks "
        "that are considered complete, default: True")
    max_runtime = law.DurationParameter(default=2.0, unit="h", significant=False,
        description="maximum runtime, default unit is hours, default: 2")
    htcondor_central_scheduler = luigi.BoolParameter(default=False, significant=False,
        description="whether or not remote tasks should connect to the central scheduler, default: "
        "False")
    # custom_condor_tag = law.CSVParameter(default=(),
       # description="Custom condor attributes to add to submit file ('as is', strings separated by commas)")
    custom_output_tag = luigi.Parameter(default="",
       description="Custom output tag for submission and status files")
    request_memory = luigi.IntParameter(
        default=-1,
        description="required amount of memory in MiB that this job needs, default: -1"
    )
    logs_to_eos = luigi.BoolParameter(
        default=False,
        description="Save job logs to same location as outputs, default: False"
    )

    exclude_params_branch = {"max_runtime", "htcondor_central_scheduler", "custom_condor_tag"}

    def htcondor_output_directory(self):
        # the directory where submission meta data should be stored
        # at the CERN HTCondor system, this cannot be eos so force using the local store
        if self.logs_to_eos:
            return law.LocalDirectoryTarget(self.local_path(store=self.default_store))
        else:
            return law.LocalDirectoryTarget(self.local_path(store="$CMT_STORE_LOCAL"))

    def htcondor_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        return os.path.expandvars("$CMT_BASE/cmt/files/cern_htcondor_bootstrap.sh")

    def htcondor_output_postfix(self):
        return self.custom_output_tag + super(HTCondorWorkflow, self).htcondor_output_postfix()

    def htcondor_job_config(self, config, job_num, branches):
        # render variables
        config.render_variables["cmt_base"] = os.environ["CMT_BASE"]
        config.render_variables["cmt_env_path"] = os.environ["PATH"]

        # custom job file content
        # config.custom_content.append(("requirements", "(OpSysAndVer =?= \"CentOS7\")"))
        config.custom_content.append(("getenv", "true"))
        config.custom_content.append(("log", "/dev/null"))
        config.custom_content.append(("+MaxRuntime", int(math.floor(self.max_runtime * 3600)) - 1))
        config.custom_content.append(("RequestCpus", self.request_cpus))
        if self.request_memory > 0:
            config.custom_content.append(("request_memory", self.request_memory))
        if self.custom_condor_tag:
            for elem in self.custom_condor_tag:
                config.custom_content.append((elem, None))

        # print "{}/x509up".format(os.getenv("HOME"))
        # config.custom_content.append(("Proxy_path", "{}/x509up".format(os.getenv("CMT_BASE"))))
        #config.custom_content.append(("arguments", "$(Proxy_path)"))

        return config

    def htcondor_create_job_file_factory(self, **kwargs):
        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts(self.htcondor_job_file_factory_defaults, kwargs)

        return HTCondorJobFileFactory(**kwargs)

    def htcondor_use_local_scheduler(self):
        return not self.htcondor_central_scheduler


class SlurmWorkflow(law.slurm.SlurmWorkflow):
    """
    Batch systems are typically very heterogeneous by design, and so is Slurm. Law does not aim
    to "magically" adapt to all possible Slurm setups which would certainly end in a mess.
    Therefore we have to configure the base Slurm workflow in law.contrib.slurm to work with
    the Maxwell cluster environment. In most cases, like in this example, only a minimal amount of
    configuration is required.
    """

    slurm_partition = luigi.Parameter(
        default="long",
        significant=False,
        description="target queue partition; default: long",
    )
    max_runtime = law.DurationParameter(
        default=72.0,
        unit="h",
        significant=False,
        description="the maximum job runtime; default unit is hours; default: 72h",
    )
    request_memory = luigi.IntParameter(
        default=-1,
        description="required amount of memory in MiB that this job needs, default: -1"
    )
    logs_to_eos = luigi.BoolParameter(
        default=False,
        description="Save job logs to same location as outputs, default: False"
    )

    def slurm_output_directory(self):
        # the directory where submission meta data should be stored
        if self.logs_to_eos:
            return law.LocalDirectoryTarget(self.local_path(store=self.default_store))
        else:
            return law.LocalDirectoryTarget(self.local_path(store="$CMT_JOB_META_DIR"))

    def slurm_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # configure it to be shared across jobs and rendered as part of the job itself
        bootstrap_file = os.path.expandvars("$CMT_BASE/cmt/slurm_tools/bootstrap.sh")
        return law.JobInputFile(bootstrap_file, share=True, render_job=True)

    def slurm_job_config(self, config, job_num, branches):
        # render_variables are rendered into all files sent with a job
        config.render_variables["cmt_base"] = os.environ["CMT_BASE"]
        config.render_variables["cmt_env_path"] = os.environ["PATH"]

        # useful defaults
        job_time = law.util.human_duration(
            seconds=law.util.parse_duration(self.max_runtime, input_unit="h") - 1,
            colon_format=True,
        )
        config.custom_content.append(("time", job_time))
        config.custom_content.append(("nodes", 1))
        if self.request_memory > 0:
            config.custom_content.append(f"#SBATCH --mem{self.request_memory}M")
        else:
            config.custom_content.append(f"#SBATCH --mem=3000M")

        # replace default slurm-SLURM_JOB_ID.out and slurm-SLURM_JOB_ID.err;
        # %x is a job-name (or script name when there is no job-name)
        config.custom_content.append("#SBATCH -o %x-%j.out")
        config.custom_content.append("#SBATCH -e %x-%j.err")

        return config


class SGEWorkflow(SGEWorkflowTmp):

    only_missing = luigi.BoolParameter(default=True, significant=False, description="skip tasks "
        "that are considered complete, default: True")
    max_runtime = law.DurationParameter(default=2.0, unit="h", significant=False,
        description="maximum runtime, default unit is hours, default: 2")
    max_memory = luigi.Parameter(default="", description="Max virtual memory to be used by each job")
    sge_central_scheduler = luigi.BoolParameter(default=False, significant=False,
        description="whether or not remote tasks should connect to the central scheduler, default: "
        "False")
    custom_condor_tag = law.CSVParameter(default=(),
       description="Custom condor attributes to add to submit file ('as is', strings separated by commas)")
    custom_output_tag = luigi.Parameter(default="",
       description="Custom output tag for submission and status files")

    exclude_params_branch = {"max_runtime", "sge_central_scheduler", "custom_condor_tag"}

    def sge_output_directory(self):
        return law.LocalDirectoryTarget(self.local_path(store="$CMT_STORE_LOCAL"))

    def sge_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        return os.path.expandvars("$CMT_BASE/cmt/sge/ic_sge_bootstrap.sh")

    def sge_output_postfix(self):
        return self.custom_output_tag + super(SGEWorkflow, self).sge_output_postfix()

    def sge_job_config(self, config, job_num, branches):
        # render variables
        config.render_variables["cmt_base"] = os.environ["CMT_BASE"]
        config.render_variables["cmt_env_path"] = os.environ["PATH"]

        # custom job file content
        # config.custom_content.append(("requirements", "(OpSysAndVer =?= \"CentOS7\")"))
        # config.custom_content.append(("getenv", "true"))
        # config.custom_content.append(("log", "/dev/null"))
        config.custom_content.append(("max_runtime", int(math.floor(self.max_runtime * 3600)) - 1))
        config.custom_content.append(("max_memory", self.max_memory))
        config.custom_content.append(("request_cpus", self.request_cpus))
        # config.custom_content.append(("RequestCpus", self.request_cpus))
        # if self.custom_condor_tag:
            # for elem in self.custom_condor_tag:
                # config.custom_content.append((elem, None))

        # print "{}/x509up".format(os.getenv("HOME"))
        # config.custom_content.append(("Proxy_path", "{}/x509up".format(os.getenv("CMT_BASE"))))
        #config.custom_content.append(("arguments", "$(Proxy_path)"))

        return config

    def sge_create_job_file_factory(self, **kwargs):
        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts(self.sge_job_file_factory_defaults, kwargs)

        return SGEJobFileFactory(**kwargs)

    def sge_use_local_scheduler(self):
        return not self.sge_central_scheduler


class CategoryWrapperTask(DatasetWrapperTask, law.WrapperTask):
    category_names = law.CSVParameter(default=(), description="names of categories "
        "to run, default: none")

    @abstractmethod
    def atomic_requires(self, category_name):
        return None

    def requires(self):
        return OrderedDict(
            (category_name, self.atomic_requires(category_name))
            for category_name in self.category_names
        )


class SplittedTask():
    @abstractmethod
    def get_splitted_branches(self):
        return


class RDFModuleTask(DatasetTask):
    allow_redefinition = luigi.BoolParameter(default=False, description="whether to allow "
        "redefinition of variables already in the RDataFrame, default: False")

    class RDataFrame():
        def __init__(self, *args, **kwargs):
            ROOT = import_root()
            if len(args) != 1 or isinstance(args[0], ROOT.TChain):
                self.rdf = ROOT.RDataFrame(*args)
            else:  # rdf coming from another df after some modification (e.g. Define)
                self.rdf = args[0]
            self.allow_redefinition = kwargs.pop("allow_redefinition", False)

        def Define(self, *args):
            try:
                return type(self)(self.rdf.Define(*args),
                    allow_redefinition=self.allow_redefinition)
            except cppyy.gbl.std.runtime_error:
            # except:
                if not self.allow_redefinition:
                    raise ValueError(f"Attempted a redefinition of variable {args[0]}. If you want "
                        "to proceed, please rerun with allow_redefinition = True")
                else:
                    print(50 * "-")
                    print(f"WARNING: Variable {args[0]} is already present in the RDataFrame and "
                        "will be redefined as allow_redefinition = True")
                    print(50 * "-")
                    return type(self)(self.rdf.Redefine(*args),
                        allow_redefinition=self.allow_redefinition)

        def Snapshot(self, *args):
            try:
                self.rdf.Snapshot(*args)
            # except cppyy.gbl.std.logic_error:
            except TypeError:
                # A redefinition has been performed, so we need to remove duplicated branches
                args = list(args)
                args[2] = tuple(set(args[2]))
                self.rdf.Snapshot(*args)

        # avoid redefining the rest of RDataFrame functions
        def __getattr__(self, name):
            def fn(*args, **kwargs):
                result = getattr(self.rdf, name)(*args, **kwargs)
                if "RInterface" in str(type(result)):  # A new RDataFrame has been created
                    return type(self)(result, allow_redefinition=self.allow_redefinition)
                return result
            return fn

    def get_feature_modules(self, filename, **kwargs):
        module_params = None

        # check for default modules file inside the config
        if filename == law.NO_STR:
            if self.config.default_module_files.get(self.task_family, None):
                filename = self.config.default_module_files[self.task_family]

        if filename != "" and filename != law.NO_STR:
            import yaml
            from cmt.utils.yaml_utils import ordered_load

            tmp_file = self.retrieve_file("config/{}.yaml".format(filename))

            with open(tmp_file) as f:
                module_params = ordered_load(f, yaml.SafeLoader)
        else:
            return []

        def _args(*_nargs, **_kwargs):
            return _nargs, _kwargs

        modules = []
        if module_params:
            for tag in module_params.keys():
                parameter_str = ""
                assert "name" in module_params[tag] and "path" in module_params[tag]
                name = module_params[tag]["name"]
                if "parameters" in module_params[tag]:
                    for param, value in module_params[tag]["parameters"].items():
                        if isinstance(value, str):
                            if "self" in value:
                                value = eval(value)
                        if isinstance(value, str):
                            parameter_str += param + " = '{}', ".format(value)
                        else:
                            parameter_str += param + " = {}, ".format(value)
                mod = module_params[tag]["path"]
                mod = __import__(mod, fromlist=[name])
                nargs, kwargs = eval('_args(%s)' % parameter_str)

                # include systematics
                systematic = kwargs.pop("systematic", getattr(self, "systematic", None))
                if systematic:
                    systematic = self.config.systematics.get(systematic)
                    systematic_direction = kwargs.pop("systematic_direction",
                        getattr(self, "systematic_direction", None))
                    module_syst_type = systematic.get_aux("module_syst_type")
                    if isinstance(module_syst_type, str) or isinstance(module_syst_type, list) :
                        # we remove the first underscore from the syst expression, as it is
                        # already included in the syst definition inside JetLepMetSyst
                        expression = systematic.expression[1:]
                        direction = eval(f"systematic.{systematic_direction}")
                        if isinstance(module_syst_type, str):
                            kwargs[module_syst_type] = f"{expression}{direction}"
                        else:
                            for syst_type in module_syst_type:
                                kwargs[syst_type] = f"{expression}{direction}"
                    elif isinstance(module_syst_type, dict):
                        # assuming structure
                        # module_syst_type={syst_name={up: up_exp, down: down_exp},}
                        for syst_type, syst_expr in module_syst_type.items():
                            kwargs[syst_type] = eval(f"syst_expr['{systematic_direction}']")

                modules.append(getattr(mod, name)(**kwargs)())

        return modules

    def get_branches_to_save(self, branchNames, keep_and_drop_file):
        tmp_filename = self.retrieve_file("config/{}.txt".format(keep_and_drop_file))
        if not os.path.isfile(tmp_filename):
            return branchNames
        comment = re.compile(r"#.*")
        ops = []
        with open(tmp_filename) as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip()
                if len(line) == 0 or line[0] == '#':
                    continue
                line = re.sub(comment, "", line)
                while line[-1] == "\\":
                    line = line[:-1] + " " + file.next().strip()
                    line = re.sub(comment, "", line)
                try:
                    (op, sel) = line.split()
                    if op == "keep":
                        ops.append((sel, 1))
                    elif op == "drop":
                        ops.append((sel, 0))
                    else:
                        raise ValueError("Error in file %s, line '%s': "% (filename, line)
                            + ": it's not (keep|drop) "
                        )
                except ValueError as e:
                    raise ValueError("Error in file %s, line '%s': " % (filename, line)
                        + "it's not a keep or drop pattern"
                    )

        branchStatus = [1 for branchName in branchNames]
        for pattern, stat in ops:
            for ib, b in enumerate(branchNames):
                # if re.match(pattern, str(b)):
                if re.fullmatch(pattern, str(b)) is not None:
                    branchStatus[ib] = stat

        return [branchName for (branchName, branchStatus) in zip(branchNames, branchStatus)
            if branchStatus == 1]

    def get_input(self):
        if not self.merging_factor and not self.threshold:
            return self.input()
        else:
            return tuple([f for f in self.input().values()])

    def get_path(self, inp, index=None):
        if not index:
            index = 0
        if not self.merging_factor and not self.threshold:
            return (inp[index].path,)
        else:
            return tuple([t[index].path for t in inp])

    def build_rdf(self):
        ROOT = import_root()
        inp = self.get_input()
        try:
            if len(inp[0]) == 1:
                return ROOT.RDataFrame(self.tree_name, self.get_path(inp))
        except:
            if len(inp) == 1:
                return ROOT.RDataFrame(self.tree_name, self.get_path(inp))
        # friend tree
        tchain = ROOT.TChain()
        # for elem in self.get_path(inp):
            # tchain.Add("{}/{}".format(elem, self.tree_name))
        tchain.Add("{}/{}".format(self.get_path(inp)[0], self.tree_name))

        # considering only one friend for now
        friend_tchain = ROOT.TChain()
        for elem in self.get_path(inp, 1):
            friend_tchain.Add("{}/{}".format(elem, self.tree_name))
        tchain.AddFriend(friend_tchain, "friend")
        return tchain


class InputData(DatasetTask, law.ExternalTask):

    file_index = luigi.IntParameter(default=law.NO_INT, description="index of the external file to "
        "refer to, points to the collection of all files when empty, default: empty")

    default_wlcg_fs = "wlcg_xrootd"
    # os.environ["CMT_REMOTE_JOB"] = "1"
    # os.environ["CMT_INPUT_DATA"] = "1"
    version = None

    def complete(self):
        return True

    def output(self):
        if self.file_index != law.NO_INT:
            out = [self.dynamic_target(
                self.dataset.get_files(
                    os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name),
                    index=self.file_index),
                avoid_store=True, check_empty=True)]
            if self.dataset.friend_datasets:
                if not isinstance(self.dataset.friend_datasets, list):
                    friend_dataset_names = [self.dataset.friend_datasets]
                else:
                    friend_dataset_names = self.dataset.friend_datasets
                for dataset_name in friend_dataset_names:
                    out.append(self.dynamic_target(
                        self.config.datasets.get(dataset_name).get_files(
                            os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name),
                            index=self.file_index, check_empty=True), avoid_store=True)
                    )
            return tuple(out)
        else:
            cls = law.SiblingFileCollection
            return cls([self.dynamic_target(file_path, avoid_store=True)
                for file_path in self.dataset.get_files(
                    os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name), add_prefix=False)])


class FitBase(ConfigTask):
    def convert_parameters(self, d):
        for param, val in d.items():
            if isinstance(val, str):
                val = val.strip()
                if val.startswith("("):
                    val = val[1:-1]
                if "," not in val:
                    d[param] = tuple([float(val)])
                else:
                    d[param] = tuple(map(float, val.split(',')))
            else:
                d[param] = val
        return d

    def get_x(self, x_range, blind_range=None, name="x"):
        from analysis_tools.utils import import_root
        ROOT = import_root()
        # fit range
        x_range = (float(x_range[0]), float(x_range[1]))
        x = ROOT.RooRealVar(name, name, x_range[0], x_range[1])

        # blinded range
        blind = False
        if not blind_range:
            return x, False
        if blind_range[0] != blind_range[1]:
            blind = True
            blind_range = (float(blind_range[0]), float(blind_range[1]))
            assert(blind_range[0] >= x_range[0] and blind_range[0] < blind_range[1] and
                blind_range[1] <= x_range[1])
            x.setRange("loSB", x_range[0], blind_range[0])
            x.setRange("hiSB", blind_range[1], x_range[1])
            x.setRange("full", x_range[0], x_range[1])
            fit_range = "loSB,hiSB"
        return x, blind

    def get_fit(self, name, parameters, x, **kwargs):
        from analysis_tools.utils import import_root
        ROOT = import_root()
        fit_parameters = {}
        params = OrderedDict()
        fit_name = kwargs.pop("fit_name", "model")

        if name == "voigtian":
            fit_parameters["mean"] = parameters.get("mean", (0, -100, 100))
            fit_parameters["gamma"] = parameters.get("gamma", (0.02, 0, 0.1))
            fit_parameters["sigma"] = parameters.get("sigma", (0.001, 0, 0.1))
            fit_parameters = self.convert_parameters(fit_parameters)

            try:
                params["mean"] = ROOT.RooRealVar('mean', 'Mean of Voigtian',
                    *fit_parameters["mean"])
                params["gamma"] = ROOT.RooRealVar('gamma', 'Gamma of Voigtian',
                    *fit_parameters["gamma"])
                params["sigma"] = ROOT.RooRealVar('sigma', 'Sigma of Voigtian',
                    *fit_parameters["sigma"])
            except TypeError:
                params["mean"] = ROOT.RooRealVar('mean', 'Mean of Voigtian',
                    fit_parameters["mean"])
                params["gamma"] = ROOT.RooRealVar('gamma', 'Gamma of Voigtian',
                    fit_parameters["gamma"])
                params["sigma"] = ROOT.RooRealVar('sigma', 'Sigma of Voigtian',
                    fit_parameters["sigma"])

            fun = ROOT.RooVoigtian(fit_name, fit_name, x,
                params["mean"], params["gamma"], params["sigma"])

        elif name == "gaussian":
            fit_parameters["mean"] = parameters.get("mean", (0, -100, 100))
            fit_parameters["sigma"] = parameters.get("sigma", (0.001, 0, 0.1))
            fit_parameters = self.convert_parameters(fit_parameters)

            try:
                params["mean"] = ROOT.RooRealVar('mean', 'Mean of Voigtian',
                    *fit_parameters["mean"])
                params["sigma"] = ROOT.RooRealVar('sigma', 'Sigma of Voigtian',
                    *fit_parameters["sigma"])
            except TypeError:
                params["mean"] = ROOT.RooRealVar('mean', 'Mean of Voigtian',
                    fit_parameters["mean"])
                params["sigma"] = ROOT.RooRealVar('sigma', 'Sigma of Voigtian',
                    fit_parameters["sigma"])

            fun = ROOT.RooGaussian(fit_name, fit_name, x, params["mean"], params["sigma"])

        elif name == "polynomial":
            order = int(parameters.get("polynomial_order", 1))
            for i in range(order):
                fit_parameters[f"p{i}"] = parameters.get(f"p{i}", (0, -5, 5))
            fit_parameters = self.convert_parameters(fit_parameters)
            for i in range(order):
                try:
                    params[f"p{i}"] = ROOT.RooRealVar(f'p{i}', f'p{i}', *fit_parameters[f"p{i}"])
                except TypeError:
                    params[f"p{i}"] = ROOT.RooRealVar(f'p{i}', f'p{i}', fit_parameters[f"p{i}"])
            fun = ROOT.RooPolynomial(fit_name, fit_name, x, ROOT.RooArgList(*list(params.values())))

        elif name == "constant":
            fun = ROOT.RooUniform(fit_name, fit_name, x)

        elif name == "exponential":
            # https://root.cern.ch/doc/master/classRooExponential.html
            fit_parameters["c"] = parameters.get("c", (0, -2, 2))
            fit_parameters = self.convert_parameters(fit_parameters)
            try:
                params["c"] = ROOT.RooRealVar('c', 'c', *fit_parameters["c"])
            except TypeError:
                params["c"] = ROOT.RooRealVar('c', 'c', fit_parameters["c"])
            fun = ROOT.RooExponential(fit_name, fit_name, x, params["c"])

        elif name == "powerlaw":
            order = int(fit_parameters.get("powerlaw_order", 1))
            for i in range(order):
                fit_parameters[f"a{i}"] = parameters.get(f"a{i}", (1, 0, 2))
                fit_parameters[f"b{i}"] = parameters.get(f"b{i}", (0, -2, 2))
            fit_parameters = self.convert_parameters(fit_parameters)

            for i in range(order):
                try:
                    params[f'a{i}'] = ROOT.RooRealVar(f'a{i}', f'a{i}', *fit_parameters[f"a{i}"])
                    params[f'b{i}'] = ROOT.RooRealVar(f'b{i}', f'b{i}', *fit_parameters[f"b{i}"])
                except TypeError:
                    params[f'a{i}'] = ROOT.RooRealVar(f'a{i}', f'a{i}', fit_parameters[f"a{i}"])
                    params[f'b{i}'] = ROOT.RooRealVar(f'b{i}', f'b{i}', fit_parameters[f"b{i}"])

            fit_fun = " + ".join([f"@{i + 1} * TMath::Power(@0, @{i + 2})"
                for i in range(0, order, 2)])
            fun = ROOT.RooGenericPdf(fit_name, fit_fun, ROOT.RooArgList(
                *([x] + list(params.values()))))

        return fun, params


class ProcessGroupNameTask(DatasetWrapperTask):

    process_group_name = luigi.Parameter(default="default", description="the name of the process "
        "grouping, only encoded into output paths when changed, default: default")
    default_process_group_name = luigi.Parameter(default="", description="the name of the "
        "process grouping, only encoded into output paths when changed, default: default")

    def __init__(self, *args, **kwargs):
        super(ProcessGroupNameTask, self).__init__(*args, **kwargs)
        try:
            processes_in_process_group_name = self.config.process_group_names[
                self.process_group_name]
        except KeyError:
            processes_in_process_group_name = self.config.process_group_names[
                self.default_process_group_name]

        self.processes_datasets = {}
        self.datasets_to_run = []

        def get_processes(dataset=None, process=None):
            processes = ObjectCollection()
            if dataset and not process:
                process = self.config.processes.get(dataset.process.name)
            processes.append(process)
            if process.parent_process:
                processes += get_processes(process=self.config.processes.get(
                    process.parent_process))
            return processes

        for dataset in self.datasets:
            processes = get_processes(dataset=dataset)
            filtered_processes = ObjectCollection()
            for process in processes:
                if process.name in processes_in_process_group_name:
                    filtered_processes.append(process)
            if len(filtered_processes) > 1:
                raise Exception("%s process group name includes not orthogonal processes %s"
                    % (self.process_group_name, ", ".join(filtered_processes.names)))
            elif len(filtered_processes) == 1:
                process = filtered_processes[0]
                if process not in self.processes_datasets:
                    self.processes_datasets[process] = []
                self.processes_datasets[process].append(dataset)
                self.datasets_to_run.append(dataset)
        if len(self.datasets_to_run) == 0:
            raise ValueError("No datasets were selected. Are you sure you want to use"
                " %s as process_group_name?" % self.process_group_name)


class QCDABCDTask(law.Task):

    """
    :param do_qcd: whether to estimate QCD using the ABCD method
    :type do_qcd: bool

    :param qcd_wp: working point to use for QCD estimation
    :type qcd_wp: str from choice list

    :param qcd_signal_region_wp: region to use as signal region for QCD estimation
    :type qcd_signal_region_wp: str

    :param shape_region: region to use as shape region for QCD estimation
    :type shape_region: str from choice list

    :param qcd_sym_shape: whether to symmetrise the shape coming from both possible shape regions
    :type qcd_sym_shape: bool

    :param qcd_category_name: category name used for the same sign regions in QCD estimation
    :type qcd_category_name: str
    """

    do_qcd = luigi.BoolParameter(default=False, description="whether to compute the QCD shape, "
        "default: False")
    qcd_wp = luigi.ChoiceParameter(default=law.NO_STR,
        choices=(law.NO_STR, "vvvl_vvl", "vvl_vl", "vl_l", "l_m"), significant=False,
        description="working points to use for qcd computation, default: empty (vvvl - m)")
    qcd_signal_region_wp = luigi.Parameter(default="os_iso", description="signal region wp, "
        "default: os_iso")
    shape_region = luigi.Parameter(default="os_inviso",
        significant=True, description="shape region default: os_inviso")
    qcd_sym_shape = luigi.BoolParameter(default=False, description="symmetrize QCD shape, "
        "default: False")
    qcd_category_name = luigi.Parameter(default="default", description="category use "
        "for qcd regions ss_iso and ss_inviso, default=default (same as category)")
    do_sideband = luigi.BoolParameter(default=False, description="whether to compute the background "
        "shape from sideband region, default: False")
