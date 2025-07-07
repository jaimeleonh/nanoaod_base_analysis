# coding: utf-8

"""
Preprocessing tasks.
"""

__all__ = []


import abc
import contextlib
import itertools
from collections import OrderedDict, defaultdict
import os
import sys
from subprocess import call
import json

import law
import luigi

from analysis_tools.utils import join_root_selection as jrs
from analysis_tools.utils import import_root, create_file_dir

from cmt.base_tasks.base import (
    DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, SGEWorkflow, SlurmWorkflow,
    InputData, ConfigTaskWithCategory, SplittedTask, DatasetTask, RDFModuleTask,
    fully_split_branch_map, get_categorization_merging_factor, categorization_branch_map,
    get_categorization_reduced_branch, snapshot_ensuring_output_tree
)

directions = ["up", "down"]


class DatasetSuperWrapperTask(DatasetWrapperTask, law.WrapperTask):

    exclude_index = True

    def __init__(self, *args, **kwargs):
        super(DatasetSuperWrapperTask, self).__init__(*args, **kwargs)

    @abc.abstractmethod
    def atomic_requires(self, dataset):
        return None

    def requires(self):
        return OrderedDict(
            (dataset.name, self.atomic_requires(dataset))
            for dataset in self.datasets
        )


class DatasetSystWrapperTask(DatasetSuperWrapperTask):
    systematic_names = law.CSVParameter(default=(), description="names of systematics "
        "to run, default: central only (empty string)")

    exclude_index = True

    @abc.abstractmethod
    def atomic_requires(self, dataset, systematic, direction):
        return None

    def requires(self):
        output = OrderedDict()
        for dataset in self.datasets:
            systematics = [("", "")]
            if not dataset.process.isData:
                systematics += list(itertools.product(self.systematic_names, directions))
            for syst, d in systematics:
                output[(dataset.name, syst, d)] = self.atomic_requires(dataset, syst, d)
        return output


class DatasetCategoryWrapperTask(DatasetWrapperTask, law.WrapperTask):
    category_names = law.CSVParameter(default=("baseline_even",), description="names of categories "
        "to run, default: (baseline_even,)")

    exclude_index = True

    def __init__(self, *args, **kwargs):
        super(DatasetCategoryWrapperTask, self).__init__(*args, **kwargs)

        # tasks wrapped by this class do not allow composite categories, so split them here
        self.categories = []
        for name in self.category_names:
            category = self.config.categories.get(name)
            if category.subcategories:
                self.categories.extend(category.subcategories)
            else:
                self.categories.append(category)

    @abc.abstractmethod
    def atomic_requires(self, dataset, category):
        return None

    def requires(self):
        return OrderedDict(
            ((dataset.name, category.name), self.atomic_requires(dataset, category))
            for dataset, category in itertools.product(self.datasets, self.categories)
            if not dataset.process.name in category.get_aux("skip_processes", [])
        )


class DatasetCategorySystWrapperTask(DatasetCategoryWrapperTask, law.WrapperTask):
    systematic_names = law.CSVParameter(default=(), description="names of systematics "
        "to run, default: central only (empty string)")

    exclude_index = True

    @abc.abstractmethod
    def atomic_requires(self, dataset, category, systematic, direction):
        return None

    def requires(self):
        output = OrderedDict()
        for dataset, category in itertools.product(self.datasets, self.categories):
            systematics = [("", "")]
            if not dataset.process.isData:
                systematics += list(itertools.product(self.systematic_names, directions))
            for syst, d in systematics:
                output[(dataset.name, category.name, syst, d)] = self.atomic_requires(dataset, category, syst, d)
        return output


class PreCounter(RDFModuleTask, law.LocalWorkflow, HTCondorWorkflow, SGEWorkflow, SlurmWorkflow,
        SplittedTask):
    """
    Performs a counting of the events with and without applying the necessary weights.
    Weights are read from the config file.
    In case they have to be computed, RDF modules can be run.

    Example command:

    ``law run PreCounter --version test  --config-name base_config --dataset-name ggf_sm \
--workflow htcondor --weights-file weight_file``

    :param weights_file: filename inside ``cmt/config/`` (w/o extension) with the RDF modules to run
    :type weights_file: str

    :param systematic: systematic to use for categorization.
    :type systematic: str

    :param systematic_direction: systematic direction to use for categorization.
    :type systematic_direction: str
    """

    weights_file = luigi.Parameter(description="filename with modules to run RDataFrame",
        default=law.NO_STR)
    systematic = luigi.Parameter(default="", description="systematic to use for categorization, "
        "default: None")
    systematic_direction = luigi.ChoiceParameter(default="", choices=("", "up", "down"),
        description="systematic direction to use for categorization, default: None")

    # regions not supported
    region_name = None

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"

    if os.getenv("CMT_STORE_EOS_PRECOUNTER"):
        default_store = "$CMT_STORE_EOS_PRECOUNTER"

    def __init__(self, *args, **kwargs):
        super(PreCounter, self).__init__(*args, **kwargs)
        self.addendum = self.get_addendum()
        self.custom_output_tag = "_%s" % self.addendum
        self.threshold = self.dataset.get_aux("event_threshold", None)
        self.merging_factor = self.dataset.get_aux("preprocess_merging_factor", None)
        self.additional_trees = self.dataset.get_aux("additional_trees", [])

    def get_addendum(self):
        if self.systematic:
            weights = self.config.weights.total_events_weights
            for weight in weights:
                try:
                    feature = self.config.features.get(weight)
                    if self.systematic in feature.systematics:
                        return f"{self.systematic}_{self.systematic_direction}_"
                except:
                    continue
        return ""

    def create_branch_map(self):
        """
        :return: number of files for the selected dataset
        :rtype: int
        """
        return fully_split_branch_map(self.config_name, self.dataset)

    def workflow_requires(self):
        """
        """
        return {"data": InputData.req(self)}

    def requires(self):
        """
        Each branch requires one input file
        """
        if not self.threshold and not self.merging_factor:
            return InputData.req(self, file_index=self.branch)
        elif self.threshold and not self.merging_factor:
            reqs = {}
            for i in self.dataset.get_file_groups(
                    path_to_look=os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name),
                    threshold=self.threshold)[self.branch]:
                reqs[str(i)] = InputData.req(self, file_index=i)
            return reqs
        elif not self.threshold and self.merging_factor:
            nfiles = len(self.dataset.get_files(
                os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name), add_prefix=False,
                check_empty=True))
            reqs = {}
            for i in range(self.merging_factor * self.branch, self.merging_factor * (self.branch + 1)):
                if i >= nfiles:
                    break
                reqs[str(i)] = InputData.req(self, file_index=i)
            return reqs
        else:
            raise ValueError("Both event_threshold and preprocess_merging_factor "
                "can't be set at once")

    def output(self):
        """
        :return: One file per input file
        :rtype: `.json`
        """
        return self.local_target(f"data_{self.addendum}{self.branch}.json")

    def get_weight(self, weights, syst_name, syst_direction, **kwargs):
        """
        Obtains the product of all weights depending on the category/channel applied.
        Returns "1" if it's a data sample.

        :return: Product of all weights to be applied
        :rtype: str
        """
        if self.config.processes.get(self.dataset.process.name).isData:
            return "1"
        return self.config.get_weights_expression(weights, syst_name, syst_direction)

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        """
        Creates one RDataFrame per input file, runs the desired RDFModules
        and counts the number of events w/ and w/o additional weights
        """

        from shutil import copy
        ROOT = import_root()
        ROOT.ROOT.EnableThreadSafety()
        ROOT.ROOT.EnableImplicitMT(self.request_cpus)

        # create RDataFrame
        inp = self.get_input()
        if not self.dataset.friend_datasets:
            df = self.RDataFrame(self.tree_name, self.get_path(inp),
                allow_redefinition=self.allow_redefinition)
            # Additional tree RDF
            additional_dfs = []
            for additional_tree in self.additional_trees:
                df_additional = self.RDataFrame(additional_tree, self.get_path(inp),
                    allow_redefinition=self.allow_redefinition)
                additional_dfs.append(df_additional)

        # friend tree
        else:
            tchain = ROOT.TChain()
            for elem in self.get_path(inp):
                tchain.Add("{}/{}".format(elem, self.tree_name))
            friend_tchain = ROOT.TChain()
            for elem in self.get_path(inp, 1):
                friend_tchain.Add("{}/{}".format(elem, self.tree_name))
            tchain.AddFriend(friend_tchain, "friend")
            df = self.RDataFrame(tchain, allow_redefinition=self.allow_redefinition)

        weight_modules = self.get_feature_modules(self.weights_file)
        if len(weight_modules) > 0:
            for module in weight_modules:
                try:
                    df, _ = module.run(df)
                    for idf in range(len(additional_dfs)):
                        additional_dfs[idf], _ = module.run(additional_dfs[idf])
                except Exception as e:
                    print("Exception: %s. Exiting" % e)
                    sys.exit(1)

        weight = self.get_weight(
            self.config.weights.total_events_weights, self.systematic, self.systematic_direction)

        hmodel = ("", "", 1, 1, 2)
        histo_noweight = df.Define("var", "1.").Histo1D(hmodel, "var")
        if not self.dataset.process.isData:
            histo_weight = df.Define("var", "1.").Define("weight", weight).Histo1D(
                hmodel, "var", "weight")
        else:
            histo_weight = df.Define("var", "1.").Histo1D(hmodel, "var")

        nevents_additional = 0.0
        nweightedevents_additional = 0.0
        for df_additional in additional_dfs:
            histo_noweight_additional = df_additional.Define("var", "1.").Histo1D(hmodel, "var")
            if not self.dataset.process.isData:
                histo_weight_additional = df_additional.Define("var", "1.").Define("weight", weight).Histo1D(
                    hmodel, "var", "weight")
            else:
                histo_weight_additional = df_additional.Define("var", "1.").Histo1D(hmodel, "var")
            nevents_additional += histo_noweight_additional.Integral()
            nweightedevents_additional += histo_weight_additional.Integral()

        d = {
            "nevents": histo_noweight.Integral() + nevents_additional,
            "nweightedevents": histo_weight.Integral() + nweightedevents_additional,
            "filenames": [str(self.get_path(inp))]
        }

        with open(create_file_dir(self.output().path), "w+") as f:
            json.dump(d, f, indent=4)


class PreCounterWrapper(DatasetSystWrapperTask):
    """
    Wrapper task to run the PreCounter task over several datasets in parallel.

    Example command:

    ``law run PreCounterWrapper --version test  --config-name base_config \
--dataset-names tt_dl,tt_sl --PreCounter-weights-file weight_file --workers 2``
    """
    def atomic_requires(self, dataset, systematic, direction):
        return PreCounter.req(self, dataset_name=dataset.name,
            systematic=systematic, systematic_direction=direction)


class PreprocessRDF(PreCounter, DatasetTaskWithCategory):
    """
    Performs the preprocessing step applying a preselection + running RDF modules

    See requirements in :class:`.PreCounter`.

    Example command:

    ``law run PreprocessRDF --version test  --category-name base_selection \
--config-name base_config --dataset-name ggf_sm --workflow htcondor \
--modules-file modulesrdf --workers 10 --max-runtime 12h``

    :param modules_file: filename inside ``cmt/config/`` or "../config/" (w/o extension)
        with the RDF modules to run
    :type modules_file: str

    :param keep_and_drop_file: filename inside ``cmt/config/`` or "../config/" (w/o extension)
        with the RDF columns to save in the output file
    :type keep_and_drop_file: str

    :param compute_filter_efficiency: compute efficiency of each filter applied
    :type compute_filter_efficiency: bool
    """

    modules_file = luigi.Parameter(description="filename with RDF modules", default=law.NO_STR)
    keep_and_drop_file = luigi.Parameter(description="filename with branches to save, empty: all",
        default="")
    compute_filter_efficiency = luigi.BoolParameter(description="compute efficiency of each filter "
        "applied, default: False", default=False)
    weights_file = None

    default_store = "$CMT_STORE_EOS_PREPROCESSING"
    default_wlcg_fs = "wlcg_fs_categorization"

    def get_addendum(self):
        if self.systematic:
            systematic = self.config.systematics.get(self.systematic)
            if self.category.name in systematic.get_aux("affected_categories", []):
                return f"{self.systematic}_{self.systematic_direction}_"
        return ""

    def output(self):
        """
        :return: One file per input file with the tree + additional branches
        :rtype: `.root`
        """
        out = {"root" : self.local_target(f"data_{self.addendum}{self.branch}.root")}
        #out = {"root" : self.dynamic_target(f"data_{self.addendum}{self.branch}.root")}
        if self.compute_filter_efficiency:
            out["cut_flow"] = self.local_target(f"cutflow_{self.addendum}{self.branch}.json")
        out = law.SiblingFileCollection(out)
        return out

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        """
        Creates one RDataFrame per input file, applies a preselection
        and runs the desired RDFModules
        """
        from shutil import copy
        ROOT = import_root()
        ROOT.ROOT.EnableThreadSafety()
        ROOT.ROOT.EnableImplicitMT(self.request_cpus)

        print(self.output()["root"].path)

        # create RDataFrame
        inp = self.get_input()
        if not self.dataset.friend_datasets:
            df = self.RDataFrame(self.tree_name, self.get_path(inp),
                allow_redefinition=self.allow_redefinition)

        # friend tree
        else:
            tchain = ROOT.TChain()
            for elem in self.get_path(inp):
                tchain.Add("{}/{}".format(elem, self.tree_name))
            friend_tchain = ROOT.TChain()
            for elem in self.get_path(inp, 1):
                friend_tchain.Add("{}/{}".format(elem, self.tree_name))
            tchain.AddFriend(friend_tchain, "friend")
            df = self.RDataFrame(tchain, allow_redefinition=self.allow_redefinition)

        outp = self.output()['root']
        # print(outp.path)

        selection = self.category.selection
        # dataset_selection = self.dataset.get_aux("selection")
        # if dataset_selection and dataset_selection != "1":
            # selection = jrs(dataset_selection, selection, op="and")

        branches = list(df.GetColumnNames())

        if selection != "":
            filtered_df = df.Define("selection", selection).Filter("selection", self.category.name)
        else:
            filtered_df = df

        modules = self.get_feature_modules(self.modules_file)
        if len(modules) > 0:
            for module in modules:
                try:
                    filtered_df, add_branches = module.run(filtered_df)
                except Exception as e:
                    print("Exception: %s. Exiting" % e)
                    sys.exit(1)
                branches += add_branches
        branches = self.get_branches_to_save(branches, self.keep_and_drop_file)
        if self.compute_filter_efficiency == True:
            report = filtered_df.Report()

        # Save ensuring presence of TTree in output file
        snapshot_ensuring_output_tree(filtered_df, self.tree_name, create_file_dir(outp.path), branches)

        if self.compute_filter_efficiency == True:
            json_res = {cutReport.GetName() : {
                "pass": cutReport.GetPass(), "all": cutReport.GetAll()}
                for cutReport in report.GetValue()
            }
            with open(create_file_dir(self.output()["cut_flow"].path), "w+") as f:
                json.dump(json_res, f, indent=4)


class PreprocessRDFWrapper(DatasetCategorySystWrapperTask):
    """
    Wrapper task to run the PreprocessRDF task over several datasets in parallel.

    Example command:

    ``law run PreprocessRDFWrapper --version test  --category-name base_selection \
--config-name ul_2018 --dataset-names tt_dl,tt_sl --PreprocessRDF-workflow htcondor \
--PreprocessRDF-max-runtime 48h --PreprocessRDF-modules-file modulesrdf  --workers 10``
    """
    def atomic_requires(self, dataset, category, systematic, direction):
        return PreprocessRDF.vreq(self, dataset_name=dataset.name, category_name=category.name,
            systematic=systematic, systematic_direction=direction)


class SystWorkflowBase(PreCounter, DatasetTaskWithCategory):
    systematic_names = law.CSVParameter(default=(), description="names of systematics "
        "to run, default: central only (empty string)")
    systematic_directions = ("up", "down")

    def __init__(self, *args, **kwargs):
        super(SystWorkflowBase, self).__init__(*args, **kwargs)

        self.systematics = [("", "")]
        if self.systematic_names and not self.dataset.process.isData:
            self.systematics += list(itertools.product(
                self.systematic_names, self.systematic_directions))

        self.workflow_data = {
            "require_branches": [],
        }

    def workflow_requires(self):
        return {
            b: self.trace_branch_requires(self.as_branch(b).requires())
            for b in self.workflow_data["require_branches"]
        }

    def trace_branch_requires(self, branch_req):
        return branch_req.requires()

    def output(self):
        return self.requires().output()

    def matching_branch_data(self, task_cls):
        assert(self.is_branch())
        param_names = task_cls.get_param_names()
        return {
            key: value for key, value in self.branch_data.items()
            if key in param_names
        }


class PreprocessRDFSyst(SystWorkflowBase):

    def requires(self):
        return {
            (name, d): PreprocessRDF.vreq(self, systematic=name, systematic_direction=d)
            for (name, d) in self.systematics
        }

    def output(self):
        return {key: req.output() for key, req in self.requires().items()}

    def run(self):
        pass


class PreprocessRDFSystWrapper(DatasetCategoryWrapperTask):
    def atomic_requires(self, dataset, category):
        return PreprocessRDFSyst.vreq(self, dataset_name=dataset.name, category_name=category.name)


class Preprocess(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow, SlurmWorkflow, SplittedTask):

    modules = luigi.DictParameter(default=None)
    modules_file = luigi.Parameter(description="filename with modules to run on nanoAOD tools",
        default="")
    max_events = luigi.IntParameter(description="maximum number of input events per file, "
        " -1 for all events", default=50000)
    keep_and_drop_file = luigi.Parameter(description="filename with output branches to "
        "keep and drop", default="$CMT_BASE/cmt/files/keep_and_drop_branches.txt")

    # regions not supported
    region_name = None

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"

    def __init__(self, *args, **kwargs):
        super(Preprocess, self).__init__(*args, **kwargs)
        if not self.keep_and_drop_file:
            self.keep_and_drop_file = None
        else:
            if "$" in self.keep_and_drop_file:
                self.keep_and_drop_file = os.path.expandvars(self.keep_and_drop_file)
        if self.dataset.get_aux("splitting") and self.max_events != -1:
            self.max_events = self.dataset.get_aux("splitting")
        if self.max_events != -1:
            if not hasattr(self, "splitted_branches") and self.is_workflow():
                self.splitted_branches = self.build_splitted_branches()
            elif not hasattr(self, "splitted_branches"):
                self.splitted_branches = self.get_splitted_branches

    def get_n_events(self, fil):
        ROOT = import_root()
        for trial in range(10):
            try:
                f = ROOT.TFile.Open(fil)
                tree = f.Get(self.tree_name)
                nevents = tree.GetEntries()
                f.Close()
                return nevents
            except:
                print("Failed opening %s, %s/10 trials" % (fil, trial + 1))
        raise RuntimeError("Failed opening %s" % fil)

    def build_splitted_branches(self):
        if self.dataset.get_aux("splitting"):
            self.max_events = self.dataset.get_aux("splitting")
        if not os.path.exists(
                os.path.expandvars("$CMT_TMP_DIR/%s/splitted_branches_%s/%s.json" % (
                    self.config_name, self.max_events, self.dataset.name))):
            ROOT = import_root()
            files = self.dataset.get_files(
                os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name), add_prefix=False,
                check_empty=True)
            branches = []
            for ifil, fil in enumerate(files):
                nevents = -1
                fil = self.dataset.get_files(
                    os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name), index=ifil)
                print("Analyzing file %s" % fil)
                nevents = self.get_n_events(fil)
                initial_event = 0
                isplit = 0
                while initial_event < nevents:
                    max_events = min(initial_event + self.max_events, int(nevents))
                    branches.append({
                        "filenumber": ifil,
                        "split": isplit,
                        "initial_event": initial_event,
                        "max_events": max_events,
                    })
                    initial_event += self.max_events
                    isplit += 1
            with open(create_file_dir(os.path.expandvars(
                    "$CMT_TMP_DIR/%s/splitted_branches_%s/%s.json" % (
                    self.config_name, self.max_events, self.dataset.name))), "w+") as f:
                json.dump(branches, f, indent=4)
        else:
             with open(create_file_dir(os.path.expandvars(
                    "$CMT_TMP_DIR/%s/splitted_branches_%s/%s.json" % (
                    self.config_name, self.max_events, self.dataset.name)))) as f:
                branches = json.load(f)
        return branches

    @law.workflow_property
    def get_splitted_branches(self):
        return self.splitted_branches

    def create_branch_map(self):
        if self.max_events != -1:
            return len(self.splitted_branches)
        else:
            return len(self.dataset.get_files(
                os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name), add_prefix=False))

    def workflow_requires(self):
        return {"data": InputData.req(self)}

    def requires(self):
        if self.max_events == -1:
            return InputData.req(self, file_index=self.branch)
        else:
            return InputData.req(self, file_index=self.splitted_branches[self.branch]["filenumber"])

    def output(self):
        return {"data": self.local_target("data_%s.root" % self.branch),
            "stats": self.local_target("data_%s.json" % self.branch)}
        # return self.local_target("{}".format(self.input()["data"].path.split("/")[-1]))

    def get_modules(self):
        module_params = None
        if self.modules_file:
            import yaml
            from cmt.utils.yaml_utils import ordered_load
            with open(self.retrieve_file("config/{}.yaml".format(self.modules_file))) as f:
                module_params = ordered_load(f, yaml.SafeLoader)
        else:
            return []

        def _args(*_nargs, **_kwargs):
            return _nargs, _kwargs

        modules = []
        if not module_params:
            return modules
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
            modules.append(getattr(mod, name)(**kwargs)())
        return modules

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        from shutil import move
        from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
        from analysis_tools.utils import import_root

        ROOT = import_root()

        # prepare inputs and outputs
        # inp = self.input()["data"].path
        inp = self.input().path
        outp = self.output()
        d = {}
        # count events
        if self.max_events == -1:
            d["nevents"] = self.get_n_events(inp)
        else:
            d["nevents"] = (self.splitted_branches[self.branch]["max_events"]
                - self.splitted_branches[self.branch]["initial_event"])

        with open(outp["stats"].path, "w+") as f:
            json.dump(d, f, indent = 4)

        # build the full selection
        selection = self.category.get_aux("nt_selection", self.category.selection)
        # dataset_selection = self.dataset.get_aux("selection")
        # if dataset_selection and dataset_selection != "1":
            # selection = jrs(dataset_selection, selection, op="and")
        # selection = "Jet_pt > 500" # hard-coded to reduce the number of events for testing
        # selection = "(event == 265939)"
        modules = self.get_modules()

        if self.max_events == -1:
            maxEntries = None
            firstEntry = 0
            postfix = ""
            output_file = inp.split("/")[-1]
        else:
            maxEntries = self.max_events
            # maxEntries = 1
            # firstEntry = 228
            firstEntry = self.splitted_branches[self.branch]["initial_event"]
            postfix = "_%s" % self.splitted_branches[self.branch]["split"]
            output_file = ("%s." % postfix).join(inp.split("/")[-1].split("."))

        p = PostProcessor(".", [inp],
                      cut=selection,
                      modules=modules,
                      postfix=postfix,
                      outputbranchsel=self.keep_and_drop_file,
                      maxEntries=maxEntries,
                      firstEntry=firstEntry)
        p.run()
        move(output_file, outp["data"].path)


class PreprocessWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        return Preprocess.req(self, dataset_name=dataset.name, category_name=category.name)


class Categorization(PreprocessRDF):
    """
    Performs the categorization step running RDF modules and applying a post-selection

    Example command:

    ``law run Categorization --version test --category-name etau --config-name base_config \
--dataset-name tt_dl --workflow local --base-category-name base_selection \
--workers 10 --feature-modules-file features``

    :param base_category_name: category name from the PreprocessRDF requirements.
    :type base_category_name: str

    :param feature_modules_file: filename inside ``cmt/config/`` or ``../config/`` (w/o extension)
        with the RDF modules to run
    :type feature_modules_file: str

    :param skip_preprocess: whether to skip the PreprocessRDF task
    :type skip_preprocess: bool

    You can add categorization_max_events as dataset parameter, this will split the dataset output from PreprocessRDF further into different files having at most the given number of events (the last one will have less)
    """

    base_category_name = luigi.Parameter(default="base_selection", description="the name of the "
        "base category with the initial selection, default: base")
    feature_modules_file = luigi.Parameter(description="filename with RDataFrame modules to run",
        default=law.NO_STR)
    skip_preprocess = luigi.BoolParameter(default=False, description="whether to skip the "
        " PreprocessRDF task, default: False")

    # regions not supported
    region_name = None

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"

    def __init__(self, *args, **kwargs):
        super(Categorization, self).__init__(*args, **kwargs)
        self.max_events = self.dataset.get_aux("categorization_max_events", None)
        if not hasattr(self, "categorization_merging_factor") and self.is_workflow():
            self.categorization_merging_factor = get_categorization_merging_factor(self.dataset, self.category,
                dataset_input_files=fully_split_branch_map(self.config_name, self.dataset))
        elif not hasattr(self, "categorization_merging_factor"):
            self.categorization_merging_factor = self.get_categorization_merging_factor

        if sum((x is not None
                for x in [
                    self.dataset.get_aux("categorization_max_events"),
                    self.dataset.get_aux("preprocess_merging_factor"),
                    self.dataset.get_aux("event_threshold")
                ])) > 1:
            raise RuntimeError(f"Dataset {self.dataset.name} error : "
                "you can only specify one of categorization_max_events, "
                "preprocess_merging_factor, event_threshold at the same time")

        if self.dataset.get_aux("categorization_max_events") is not None and self.request_cpus > 1:
            # RDataFrame.Range is not compatible with multithreading
            raise RuntimeError(f"Dataset.categorization_max_events is not compatible with request_cpus > 1"
                f"(in dataset {self.dataset.name}), due to RDataFrame limitation")

        if self.max_events and self.categorization_merging_factor:
            raise RuntimeError(f"Dataset {self.dataset.name} error : "
                "you can only specify one of categorization_max_events and categorization_merging")

        if self.max_events is not None:
            if not hasattr(self, "splitted_branches") and self.is_workflow():
                self.splitted_branches = self.build_splitted_branches()
            elif not hasattr(self, "splitted_branches"):
                self.splitted_branches = self.get_splitted_branches

    @law.workflow_property
    def get_categorization_merging_factor(self):
        return self.categorization_merging_factor

    def get_n_events(self, fil):
        ROOT = import_root()
        for trial in range(10):
            try:
                f = ROOT.TFile.Open(fil)
                tree = f.Get(self.tree_name)
                nevents = tree.GetEntries()
                return nevents
            except:
                print("Failed opening %s, %s/10 trials" % (fil, trial + 1))
            finally:
                if 'f' in locals():
                    f.Close()
        raise RuntimeError("Failed opening %s" % fil)

    def build_splitted_branches(self):
        if not os.path.exists(
                os.path.expandvars("$CMT_TMP_DIR/%s/splitted_branches_categorization_%s/%s.json" % (
                    self.config_name, self.max_events, self.dataset.name))):
            ROOT = import_root()
            files = [target["root"].path for target in self.get_input()["data"]["collection"].targets.values()]
            branches = []
            for ifil, fil in enumerate(files):
                nevents = -1
                print("Analyzing file %s" % fil)
                nevents = self.get_n_events(fil)
                initial_event = 0
                isplit = 0
                while initial_event < nevents:
                    max_events = min(initial_event + self.max_events, int(nevents))
                    branches.append({
                        "filenumber": ifil,
                        "split": isplit,
                        "initial_event": initial_event,
                        "max_events": max_events,
                    })
                    initial_event += self.max_events
                    isplit += 1
            with open(create_file_dir(os.path.expandvars(
                    "$CMT_TMP_DIR/%s/splitted_branches_categorization_%s/%s.json" % (
                    self.config_name, self.max_events, self.dataset.name))), "w+") as f:
                json.dump(branches, f, indent=4)
        else:
             with open(create_file_dir(os.path.expandvars(
                    "$CMT_TMP_DIR/%s/splitted_branches_categorization_%s/%s.json" % (
                    self.config_name, self.max_events, self.dataset.name)))) as f:
                branches = json.load(f)
        return branches

    @law.workflow_property
    def get_splitted_branches(self):
        return self.splitted_branches

    def create_branch_map(self):
        if self.max_events:
            return len(self.splitted_branches)
        else:
            return categorization_branch_map(self.config_name, self.dataset, self.categorization_merging_factor)

    def workflow_requires(self):
        if self.skip_preprocess:
            return {"data": InputData.req(self)}
        else:
            return {"data": PreprocessRDF.vreq(self, category_name=self.base_category_name)}

    def requires(self):
        if self.max_events:
            preprocess_branch = self.splitted_branches[self.branch]["filenumber"]
            if self.skip_preprocess:
                return InputData.req(self, file_index=preprocess_branch)
            else:
                return PreprocessRDF.vreq(self, category_name=self.base_category_name,
                    branch=preprocess_branch)

        else:
            parent_branches = self.branch_data["parent_branches"]
            if self.skip_preprocess:
                return [InputData.req(self, file_index=file_index) for file_index in parent_branches]

            else:
                if len(parent_branches) == 1:
                    preprocess_kwargs = dict(branch=parent_branches[0])
                elif len(parent_branches) > 1:
                    preprocess_kwargs = dict(branches=parent_branches)

                return PreprocessRDF.vreq(self, category_name=self.base_category_name,
                    **preprocess_kwargs, _exclude=["branch"])

    def output(self):
        """
        :return: One file per input file with the tree + additional branches
        :rtype: `.root`
        """
        if self.max_events:
            branch = self.branch
        else:
            branch = get_categorization_reduced_branch(self.branch_data)

        out = {"root" : self.local_target(f"data_{self.addendum}{branch}.root")}
        if self.compute_filter_efficiency:
            out["cut_flow"] = self.local_target(f"cutflow_{self.addendum}{branch}.json")
        out = law.SiblingFileCollection(out)
        return out

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        """
        Creates one RDataFrame per input file, runs the desired RDFModules and applies a
        post-selection
        """
        from shutil import copy
        ROOT = import_root()
        ROOT.ROOT.EnableThreadSafety()
        ROOT.ROOT.EnableImplicitMT(self.request_cpus)

        # prepare outputs and inputs
        outp = self.output()

        if self.max_events:
            if self.skip_preprocess:
                input_files = [self.get_path(inp)[0]]
            else:
                input_files = [self.input()["root"].path]
        else:
            if self.skip_preprocess:
                input_files = [t[0].path for t in self.input()]
            else:
                if len(self.branch_data["parent_branches"]) == 1:
                    input_files = [self.input()["root"].path]
                else:
                    try:
                        input_files = [x["root"].path for x in self.input()["collection"].targets.values()]
                    except Exception as e:
                        raise Exception(f'Categorization : did not find file paths in task input collection {repr(self.input()["collection"].targets)}. branch_data={self.branch_data}') from e

        non_empty_input_files = []
        for in_file in input_files:
            try:
                with law.contrib.root.GuardedTFile(in_file) as file:
                    if file.IsZombie():
                        raise RuntimeError(f"Categorization : Input file for branch '{in_file}' is zombie. If it was produced "
                                            "by PreprocessRDF, try removing the file and running the task again.")

                    evts = file.Get(self.tree_name)
                    if not evts:
                        raise RuntimeError(f"Categorization : Input file for branch '{in_file}' is empty. If it was produced "
                                            "by PreprocessRDF, try removing the file and running the task again.")

                    if evts.GetEntries() > 0:
                        non_empty_input_files.append(in_file)
                    else:
                        print(f"Categorization : Removing healthy but empty input file {in_file}")

            except OSError:
                raise OSError(f"Categorization : Input file for branch '{in_file}' is corrupted or missing. If it was produced "
                               "by PreprocessRDF, try removing the file and running the task again.")

        # if files are healthy but all empty create empty output and return
        if len(non_empty_input_files) == 0:
            print(f"Categorization : All healthy but empty input files. Copying first empty input to output.")
            copy(input_files[0], outp["root"].path)
            if self.compute_filter_efficiency:
                with open(create_file_dir(self.output()["cut_flow"].path), "w+") as f:
                    json.dump({}, f, indent=4)
            return

        if len(non_empty_input_files) == 1 and not self.dataset.friend_datasets:
            # simple case : we don't need to amnually build a TTree/TChain
            df = self.RDataFrame(self.tree_name, non_empty_input_files[0],
                    allow_redefinition=self.allow_redefinition)

        else:
            tchain = ROOT.TChain(self.tree_name)
            for in_file in non_empty_input_files:
                sts = tchain.AddFile(in_file, 0)
                if not sts:
                    # this error should actually always be caught by the checks done above
                    raise RuntimeError(f"Input file for branch '{in_file}' is corrupted or missing. If it was produced "
                                        "by PreprocessRDF, try removing the file and running the task again.")


            if self.dataset.friend_datasets:
                assert (len(non_empty_input_files) == len(input_files))
                friend_tchain = ROOT.TChain(self.tree_name)
                for input_target in self.input():
                    # only one friend dataset is supported
                    assert (len(input_target) == 2)
                    # InputData output is a tuple, assume second element is the friend
                    sts = friend_tchain.AddFile(input_target[1].path, 0)
                    if not sts:
                        # this error should actually always be caught by the checks done above
                        raise RuntimeError(f"Input file for branch '{in_file}' is corrupted or missing. If it was produced "
                                            "by PreprocessRDF, try removing the file and running the task again.")

                tchain.AddFriend(friend_tchain, "friend")

            df = self.RDataFrame(tchain, allow_redefinition=self.allow_redefinition)

        # restricting number of events
        if self.max_events:
            df = df.Range(self.splitted_branches[self.branch]["initial_event"],
                self.splitted_branches[self.branch]["max_events"])

        selection = self.config.get_object_expression(self.category, self.dataset.process.isMC,
            self.systematic, self.systematic_direction)
        dataset_selection = self.dataset.get_aux("selection")
        if dataset_selection and dataset_selection != "1":
            selection = jrs(dataset_selection, selection, op="and")
        try:
            branches = list(df.GetColumnNames())
        except:
            raise ReferenceError
        feature_modules = self.get_feature_modules(self.feature_modules_file)
        if len(feature_modules) > 0:
            for module in feature_modules:
                try:
                    df, add_branches = module.run(df)
                except Exception as e:
                    print("Exception: %s. Exiting" % e)
                    sys.exit(1)
                branches += add_branches
        branches = self.get_branches_to_save(branches, self.keep_and_drop_file)
        filtered_df = df.Define("selection", selection).Filter("selection", self.category.name)

        # Save ensuring presence of TTree in output file
        snapshot_ensuring_output_tree(filtered_df, self.tree_name, create_file_dir(outp["root"].path), branches)

        if self.compute_filter_efficiency:
            report = filtered_df.Report()
            json_res = {cutReport.GetName():
                {"pass": cutReport.GetPass(), "all": cutReport.GetAll()}
                for cutReport in report.GetValue()}
            with open(create_file_dir(self.output()["cut_flow"].path), "w+") as f:
                json.dump(json_res, f, indent=4)


class CategorizationWrapper(DatasetCategorySystWrapperTask):
    """
    Wrapper task to run the Categorization task over several datasets in parallel.

    Example command:

    ``law run CategorizationWrapper --version test --category-names etau --config-name base_config \
--dataset-names tt_dl,tt_sl --Categorization-workflow htcondor --workers 20 \
--Categorization-base-category-name base_selection``
    """

    def atomic_requires(self, dataset, category, systematic, direction):
        return Categorization.req(self, dataset_name=dataset.name, category_name=category.name,
            systematic=systematic, systematic_direction=direction)


class MergeCategorization(DatasetTaskWithCategory, law.tasks.ForestMerge):
    """
    Merges the output from the Categorization or PreprocessRDF tasks in order to reduce the
    parallelization entering the plotting tasks. By default it merges into one output file,
    although a bigger number can be set with the `merging` parameter inside the dataset
    definition.

    In simulated samples, ``hadd`` is used to perform the merging. In data samples, to avoid
    skipping events due to different branches between them, ``haddnano.py`` (safer but slower)
    is used instead. In any case, the use of one method or the other can be forced by specifying
    the parameters ``--force-hadd`` and ``--force-haddnano`` respectively.

    Example command:

    ``law run MergeCategorization --version test --category-name etau \
--config-name base_config --dataset-name tt_sl --workflow local --workers 4``

    :param from_preprocess: whether it merges the output from the PreprocessRDF task (True)
        or Categorization (False, default)
    :type from_preprocess: bool

    :param force_hadd: whether to force ``hadd`` as tool to do the merging.
    :type force_hadd: bool

    :param force_haddnano: whether to force ``haddnano.py`` as tool to do the merging.
    :type force_haddnano: bool

    :param systematic: systematic to use for categorization.
    :type systematic: str

    :param systematic_direction: systematic direction to use for categorization.
    :type systematic_direction: str
    """

    from_preprocess = luigi.BoolParameter(default=False, description="whether to use as input "
        "PreprocessRDF, default: False")
    force_hadd = luigi.BoolParameter(default=False, description="whether to force hadd "
        "as tool to do the merging, default: False")
    force_haddnano = luigi.BoolParameter(default=False, description="whether to force haddnano.py "
        "as tool to do the merging, default: False")
    systematic = luigi.Parameter(default="", description="systematic to use for categorization, "
        "default: None")
    systematic_direction = luigi.Parameter(default="", description="systematic direction to use "
        "for categorization, default: None")

    # regions not supported
    region_name = None
    merge_factor = 10

    default_store = "$CMT_STORE_EOS_MERGECATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"

    def merge_workflow_requires(self):
        if not self.from_preprocess:
            return Categorization.vreq(self, _prefer_cli=["workflow"])
        else:
            return PreprocessRDF.vreq(self, _prefer_cli=["workflow"])

    def merge_requires(self, start_leaf, end_leaf):
        if not self.from_preprocess:
            return Categorization.vreq(self, workflow="local",
                branches=((start_leaf, end_leaf),), _exclude={"branch"})
        else:
            return PreprocessRDF.vreq(self, workflow="local",
                branches=((start_leaf, end_leaf),), _exclude={"branch"})

    def trace_merge_inputs(self, inputs):
        return [inp for inp in inputs["collection"].targets.values()]

    def merge_output(self):
        addendum = PreprocessRDF.get_addendum(self)
        return law.SiblingFileCollection([
            self.local_target("data_{}{}.root".format(addendum, i))
            for i in range(self.n_files_after_merging)
        ])

    def merge(self, inputs, output):
        ROOT = import_root()
        with output.localize("w") as tmp_out:
            good_inputs = []
            for inp in inputs:  # merge only files with a filled tree
                try:
                    tf = ROOT.TFile.Open(inp.path)
                except:
                    inp = inp.targets["root"]
                    tf = ROOT.TFile.Open(inp.path)

                tree = tf.Get(self.tree_name)
                # If the tree is not stored in the file (e.g. because of some issue in the production),
                # the variable 'tree' will store a TObject instead of a TTree. 
                # So if the opened TTree is not an actual TTree (i.e. its type doesn't include TTree),
                # we can consider something went wrong and raise a RuntimeError.
                if not "TTree" in str(type(tree)):
                    raise RuntimeError(f"MergeCategorization : Input file '{inp.path}' is empty. If it was produced "
                                        "by Categorization, try removing the file and running the task again.")

                # In some cases (from what I was able to identify, empty trees stored inside directories),
                # TTree.GetEntries() doesn't work, and raises a TypeError. If we get a TypeError,
                # we can assume the tree inside was empty.
                try:
                    if tree.GetEntries() > 0:
                        good_inputs.append(inp)
                    else:
                        print("MergeCategorization : File %s healthy but empty -> not used" % inp.path)
                except TypeError:
                    print("MergeCategorization : File %s healthy but empty -> not used" % inp.path)

            if len(good_inputs) != 0:
                use_hadd = self.dataset.process.isMC
                assert not(self.force_haddnano and self.force_hadd)
                if self.force_haddnano:
                    use_hadd = False
                elif self.force_hadd:
                    use_hadd = True
                if use_hadd:
                    print("Merging with hadd...")
                    law.root.hadd_task(self, good_inputs, tmp_out, local=True)
                else:
                    print("Merging with haddnano.py...")
                    cmd = "python3 %s/bin/%s/haddnano.py %s %s" % (
                        os.environ["CMSSW_BASE"], os.environ["SCRAM_ARCH"],
                        create_file_dir(tmp_out.path), " ".join([f.path for f in good_inputs]))
                    rc = call(cmd, shell=True)
            else:  # if all input files are empty, create an empty ttree as output
                tf = ROOT.TFile.Open(create_file_dir(tmp_out.path), "RECREATE")
                empty_tree = ROOT.TTree(self.tree_name, self.tree_name)
                empty_tree.Write()
                tf.Close()

class MergeCategorizationWrapper(DatasetCategorySystWrapperTask):
    """
    Wrapper task to run the MergeCategorizationWrapper task over several datasets in parallel.

    Example command:

    ``law run MergeCategorizationWrapper --version test --category-names etau \
--config-name base_config --dataset-names tt_dl,tt_sl --workers 10``
    """

    def atomic_requires(self, dataset, category, systematic, direction):
        return MergeCategorization.vreq(self, dataset_name=dataset.name,
            category_name=category.name, systematic=systematic,
            systematic_direction=direction)


class MergePreCounter(DatasetTask, law.tasks.ForestMerge):
    """
    Merges the output from the PreCounter task in order to reduce the
    parallelization entering the plotting tasks.

    :param systematic: systematic to use for categorization.
    :type systematic: str

    :param systematic_direction: systematic direction to use for categorization.
    :type systematic_direction: str

    Example command:

    ``law run MergePreCounter --version test --config-name base_config \
--dataset-name dy_high --workers 10``
    """

    systematic = luigi.Parameter(default="", description="systematic to use for categorization, "
        "default: None")
    systematic_direction = luigi.Parameter(default="", description="systematic direction to use "
        "for categorization, default: None")

    # regions not supported
    region_name = None

    merge_factor = 16

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"

    if os.getenv("CMT_STORE_EOS_PRECOUNTER"):
        default_store = "$CMT_STORE_EOS_PRECOUNTER"

    def __init__(self, *args, **kwargs):
        if os.path.exists(
                os.path.join(os.path.expandvars(self.default_store), "MergeCategorizationStats")):
            raise Exception("MergeCategorizationStats is no longer used. "
                f"Please rename {os.path.expandvars(self.default_store)}/MergeCategorizationStats folder  "
                f"to {os.path.expandvars(self.default_store)}/MergePreCounter and rerun `law index --verbose`.")
        super(MergePreCounter, self).__init__(*args, **kwargs)

    def merge_workflow_requires(self):
        return PreCounter.vreq(self, _prefer_cli=["workflow"])

    def merge_requires(self, start_leaf, end_leaf):
        return PreCounter.vreq(self, workflow="local", branches=((start_leaf, end_leaf),),
            _exclude={"branch"})

    def trace_merge_inputs(self, inputs):
        return [inp for inp in inputs["collection"].targets.values()]

    def merge_output(self):
        addendum = PreCounter.get_addendum(self)
        # Adjust addendum name for systematic variations
        if addendum != "":
            addendum = "_"+addendum[:-1]
        return self.local_target(f"stats{addendum}.json")

    def merge(self, inputs, output):
        # output content
        stats = dict(nevents=0, nweightedevents=0, filenames=[])

        # merge
        for inp in inputs:
            try:
                if "json" in inp.path:
                    _stats = inp.load(formatter="json")
                elif "root" in inp.path:
                    _stats = inp.load(formatter="root")
            except:
                print("error leading input target {}".format(inp))
                raise

            # add nevents
            if "json" in inp.path:
                stats["nevents"] += _stats["nevents"]
                stats["nweightedevents"] += _stats["nweightedevents"]
                stats["filenames"] += _stats["filenames"]
            else:
                try:
                    histo = _stats.Get("histos/events")
                    stats["nevents"] += histo.GetBinContent(1)
                    stats["nweightedevents"] += histo.GetBinContent(2)
                except:
                    stats["nevents"] += 0
                    stats["nweightedevents"] += 0

        output.parent.touch()
        output.dump(stats, indent=4, formatter="json")


class MergePreCounterWrapper(DatasetSystWrapperTask):
    """
    Wrapper task to run the MergePreCounterWrapper task over several datasets in parallel.

    Example command:

    ``law run MergePreCounterWrapper --version test --config-name base_config \
--dataset-names tt_dl,tt_sl --workers 10``
    """
    def atomic_requires(self, dataset, systematic, direction):
        return MergePreCounter.req(self, dataset_name=dataset.name,
            systematic=systematic, systematic_direction=direction)


class EventCounterDAS(DatasetTask):
    """
    Performs a counting of the events with and without applying the necessary weights.
    Weights are read from the config file.
    In case they have to be computed, RDF modules can be run.

    Example command:

    ``law run EventCounterDAS --version test  --config-name base_config --dataset-name ggf_sm``

    :param use_secondary_dataset: whether to use the dataset included in the secondary_dataset
        parameter from the dataset instead of the actual dataset
    :type use_secondary_dataset: bool
    """
    use_secondary_dataset = luigi.BoolParameter(default=False, description="whether to use "
        "secondary_dataset instead of the actual dataset or folder, default: False")

    def requires(self):
        """
        No requirements needed
        """
        return {}

    def output(self):
        """
        :return: One file for the whole dataset
        :rtype: `.json`
        """
        return self.local_target("stats.json")

    def run(self):
        """
        Asks for the numbers of events using dasgoclient and stores them in the output json file
        """
        from analysis_tools.utils import randomize
        from subprocess import call

        tmpname = randomize("tmp")
        cmd = 'dasgoclient --query=="summary dataset={}" > {}'
        if not self.use_secondary_dataset:
            assert self.dataset.dataset
            rc = call(cmd.format(self.dataset.dataset, tmpname), shell = True)
        else:
            assert self.dataset.get_aux("secondary_dataset", None)
            rc = call(cmd.format(self.dataset.get_aux("secondary_dataset"), tmpname), shell = True)
        if rc == 0:
            with open(tmpname) as f:
                d = json.load(f)
            output_d = {
                "nevents": d[0]["nevents"],
                "nweightedevents": d[0]["nevents"],
            }
        with open(create_file_dir(self.output().path), "w+") as f:
            json.dump(output_d, f, indent=4)
        os.remove(tmpname)


class EventCounterDASWrapper(DatasetSuperWrapperTask):
    """
    Wrapper task to run the EventCounterDAS task over several datasets in parallel.

    Example command:

    ``law run EventCounterDASWrapper --version test  --config-name base_config \
--dataset-names tt_dl,tt_sl --workers 2``
    """

    def atomic_requires(self, dataset):
        return EventCounterDAS.req(self, dataset_name=dataset.name)
