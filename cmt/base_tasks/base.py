# coding: utf-8

"""
Base tasks.
"""

__all__ = [
    "Task", "ConfigTask", "ConfigTaskWithCategory", "DatasetTask", "DatasetTaskWithCategory",
    "DatasetWrapperTask", "HTCondorWorkflow", "SGEWorkflow", "InputData",
]


import re
import os
import math
from collections import OrderedDict

import luigi
import law

from law.util import merge_dicts
from law.contrib.htcondor.job import HTCondorJobFileFactory
from cmt.sge.job import SGEJobFileFactory
from cmt.sge.workflow import SGEWorkflow as SGEWorkflowTmp
# from cmt.condor_tools.htcondor import HTCondorWorkflowExt

from abc import abstractmethod

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
        if kwargs.pop("avoid_store", False):
            parts = list(path)[0]
        else:
            parts = tuple(self.store_parts().values()) + tuple(self.store_parts_ext().values()) + path
        return "/".join([parts])

    def wlcg_target(self, *args, **kwargs):
        kwargs.setdefault("fs", self.default_wlcg_fs)
        # cls = law.wlcg.WLCGDirectoryTarget if kwargs.pop("dir", False) else law.wlcg.WLCGFileTarget
        cls = Target
        path = self.wlcg_path(*args, **kwargs)
        kwargs.pop("avoid_store", False)
        return cls(path, **kwargs)

    def dynamic_target(self, *args, **kwargs):
        if os.getenv("CMT_REMOTE_JOB", "0") == "1":
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
    region_name = luigi.Parameter(default=law.NO_STR, description="an optional name of a region "
        "to apply live, default: empty")
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

        self.region = None
        if self.region_name and self.region_name != law.NO_STR:
            self.region = self.config.regions.get(self.region_name)

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


class DatasetTaskWithCategory(ConfigTaskWithCategory, DatasetTask):

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

    exclude_params_branch = {"max_runtime", "htcondor_central_scheduler", "custom_condor_tag"}

    def htcondor_output_directory(self):
        # the directory where submission meta data should be stored
        # at the CERN HTCondor system, this cannot be eos so force using the local store
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
        default="standard",
        significant=False,
        description="target queue partition; default: standard",
    )
    max_runtime = law.DurationParameter(
        default=1.0,
        unit="h",
        significant=False,
        description="the maximum job runtime; default unit is hours; default: 1h",
    )

    def slurm_output_directory(self):
        # the directory where submission meta data should be stored
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


class RDFModuleTask():
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
    os.environ["CMT_REMOTE_JOB"] = "1"
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
            
