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
from subprocess import call
import json

import law
import luigi

from analysis_tools.utils import join_root_selection as jrs
from analysis_tools.utils import import_root, create_file_dir

from cmt.base_tasks.base import ( 
    DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, InputData,
    ConfigTaskWithCategory, SplittedTask, DatasetTask, RDFModuleTask
)


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
        )


class PreCounter(DatasetTask, law.LocalWorkflow, HTCondorWorkflow, SplittedTask, RDFModuleTask):
    """
    Performs a counting of the events with and without applying the necessary weights.
    Weights are read from the config file.
    In case they have to be computed, RDF modules can be run.

    Example command:

    ``law run PreCounter --version test  --config-name base_config --dataset-name ggf_sm \
--workflow htcondor --weights-file weight_file``

    :param weights_file: filename inside ``cmt/config/`` (w/o extension) with the RDF modules to run
    :type weights_file: str
    """

    weights_file = luigi.Parameter(description="filename with modules to run RDataFrame",
        default="")
    # regions not supported
    region_name = None

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"

    tree_name = "Events"

    def create_branch_map(self):
        """
        :return: number of files for the selected dataset
        :rtype: int
        """
        threshold = self.dataset.get_aux("event_threshold", None)
        merging_factor = self.dataset.get_aux("preprocess_merging_factor", None)
        if not threshold and not merging_factor:
            return len(self.dataset.get_files(
                os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name), add_prefix=False))
        elif threshold and not merging_factor:
            return len(self.dataset.get_file_groups(
                path_to_look=os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name),
                threshold=threshold))
        elif not threshold and merging_factor:
            nfiles = len(self.dataset.get_files(
                os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name), add_prefix=False))
            nbranches = nfiles // self.dataset.get_aux("preprocess_merging_factor")
            if nfiles % self.dataset.get_aux("preprocess_merging_factor"):
                nbranches += 1
            return nbranches
        else:
            raise ValueError("Both event_threshold and preprocess_merging_factor "
                "can't be set at once")

    def workflow_requires(self):
        """
        """
        return {"data": InputData.req(self)}

    def requires(self):
        """
        Each branch requires one input file
        """
        threshold = self.dataset.get_aux("event_threshold", None)
        merging_factor = self.dataset.get_aux("preprocess_merging_factor", None)
        if not threshold and not merging_factor:
            return InputData.req(self, file_index=self.branch)
        elif threshold and not merging_factor:
            reqs = {}
            for i in self.dataset.get_file_groups(
                    path_to_look=os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name),
                    threshold=threshold)[self.branch]:
                reqs[str(i)] = InputData.req(self, file_index=i)
            return reqs
        elif not threshold and merging_factor:
            nfiles = len(self.dataset.get_files(
                os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name), add_prefix=False))
            reqs = {}
            for i in range(merging_factor * self.branch, merging_factor * (self.branch + 1)):
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
        return self.local_target("data_%s.json" % self.branch)

    def get_input(self):
        merging_factor = self.dataset.get_aux("preprocess_merging_factor", None)
        threshold = self.dataset.get_aux("event_threshold", None)
        if not merging_factor and not threshold:
            return self.input().path
        else:
            return tuple([f.path for f in self.input().values()])

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        """
        Creates one RDataFrame per input file, runs the desired RDFModules
        and counts the number of events w/ and w/o additional weights
        """

        from shutil import copy
        ROOT = import_root()
        ROOT.ROOT.EnableImplicitMT(self.request_cpus)

        # prepare inputs and outputs
        inp = self.get_input()
        outp = self.output()
        print(inp)
        df = ROOT.RDataFrame(self.tree_name, inp)
        weight_modules = self.get_feature_modules(self.weights_file)
        if len(weight_modules) > 0:
            for module in weight_modules:
                df, _ = module.run(df)

        weights = self.config.weights.total_events_weights
        hmodel = ("", "", 1, 1, 2)
        histo_noweight = df.Define("var", "1.").Histo1D(hmodel, "var")
        if not self.dataset.process.isData:
            histo_weight = df.Define("var", "1.").Define("weight", " * ".join(weights)).Histo1D(
                hmodel, "var", "weight")
        else:
            histo_weight = df.Define("var", "1.").Histo1D(hmodel, "var")

        d = {
            "nevents": histo_noweight.Integral(),
            "nweightedevents": histo_weight.Integral(),
            "filenames": [inp]
        }
        with open(create_file_dir(self.output().path), "w+") as f:
            json.dump(d, f, indent=4)


class PreCounterWrapper(DatasetSuperWrapperTask):
    """
    Wrapper task to run the PreCounter task over several datasets in parallel.

    Example command:

    ``law run PreCounterWrapper --version test  --config-name base_config \
--dataset-names tt_dl,tt_sl --PreCounter-weights-file weight_file --workers 2``
    """
    def atomic_requires(self, dataset):
        return PreCounter.req(self, dataset_name=dataset.name)


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
    """

    modules_file = luigi.Parameter(description="filename with RDF modules", default="")
    keep_and_drop_file = luigi.Parameter(description="filename with branches to save, empty: all",
        default="")
    weights_file = None

    default_store = "$CMT_STORE_EOS_PREPROCESSING"
    default_wlcg_fs = "wlcg_fs_categorization"

    def output(self):
        """
        :return: One file per input file with the tree + additional branches
        :rtype: `.root`
        """
        return self.local_target("data_%s.root" % self.branch)

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        """
        Creates one RDataFrame per input file, applies a preselection
        and runs the desired RDFModules
        """
        from shutil import copy
        ROOT = import_root()
        ROOT.ROOT.EnableImplicitMT(self.request_cpus)

        # prepare inputs and outputs
        inp = self.get_input()
        print(inp)
        outp = self.output()
        df = ROOT.RDataFrame(self.tree_name, inp)

        selection = self.category.selection
        # dataset_selection = self.dataset.get_aux("selection")
        # if dataset_selection and dataset_selection != "1":
            # selection = jrs(dataset_selection, selection, op="and")

        if selection != "":
            filtered_df = df.Define("selection", selection).Filter("selection")
        else:
            filtered_df = df

        # filtered_df = filtered_df.Filter("event == 83362796")

        # print(filtered_df.Count().GetValue())
        # import sys
        # sys.exit()

        modules = self.get_feature_modules(self.modules_file)
        branches = list(df.GetColumnNames())
        if len(modules) > 0:
            for module in modules:
                filtered_df, add_branches = module.run(filtered_df)
                branches += add_branches
        branches = self.get_branches_to_save(branches, self.keep_and_drop_file)
        branch_list = ROOT.vector('string')()
        for branch_name in branches:
            branch_list.push_back(branch_name)
        filtered_df.Snapshot(self.tree_name, create_file_dir(outp.path), branch_list)


class PreprocessRDFWrapper(DatasetCategoryWrapperTask):
    """
    Wrapper task to run the PreprocessRDF task over several datasets in parallel.

    Example command:

    ``law run PreprocessRDFWrapper --version test  --category-name base_selection \
--config-name ul_2018 --dataset-names tt_dl,tt_sl --PreprocessRDF-workflow htcondor \
--PreprocessRDF-max-runtime 48h --PreprocessRDF-modules-file modulesrdf  --workers 10``
    """
    def atomic_requires(self, dataset, category):
        return PreprocessRDF.vreq(self, dataset_name=dataset.name, category_name=category.name)


class Preprocess(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow, SplittedTask):

    modules = luigi.DictParameter(default=None)
    modules_file = luigi.Parameter(description="filename with modules to run on nanoAOD tools",
        default="")
    max_events = luigi.IntParameter(description="maximum number of input events per file, "
        " -1 for all events", default=50000)
    keep_and_drop_file = luigi.Parameter(description="filename with output branches to "
        "keep and drop", default="$CMT_BASE/cmt/files/keep_and_drop_branches.txt")

    # regions not supported
    region_name = None

    tree_name = "Events"

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
                os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name), add_prefix=False)
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


# class Categorization(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow):
class Categorization(PreprocessRDF):
    """
    Performs the categorization step running RDF modules and applying a post-selection

    Example command:

    ``law run Categorization --version test --category-name etau --config-name base_config \
--dataset-name tt_dl --workflow local --base-category-name base_selection \
--workers 10 --feature-modules-file features``

    :param base_category_name: category name from the PreprocessRDF requirements.
    :type base_category_name: str

    :param systematic: systematic to use for categorization.
    :type systematic: str

    :param systematic_direction: systematic direction to use for categorization.
    :type systematic_direction: str

    :param feature_modules_file: filename inside ``cmt/config/`` or ``../config/`` (w/o extension)
        with the RDF modules to run
    :type feature_modules_file: str
    """
    base_category_name = luigi.Parameter(default="base_selection", description="the name of the "
        "base category with the initial selection, default: base")
    systematic = luigi.Parameter(default="", description="systematic to use for categorization, "
        "default: None")
    systematic_direction = luigi.Parameter(default="", description="systematic direction to use "
        "for categorization, default: None")
    feature_modules_file = luigi.Parameter(description="filename with modules to run RDataFrame",
        default="")

    # regions not supported
    region_name = None

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"

    tree_name = "Events"

    def __init__(self, *args, **kwargs):
        super(Categorization, self).__init__(*args, **kwargs)

    def workflow_requires(self):
        return {"data": PreprocessRDF.vreq(self, category_name=self.base_category_name)}

    def requires(self):
        return PreprocessRDF.vreq(self, category_name=self.base_category_name,
            branch=self.branch)

    def output(self):
        return {
            "data": self.local_target("data_%s.root" % self.branch),
            # "stats": self.local_target("data_%s.json" % self.branch)
        }

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        """
        Creates one RDataFrame per input file, runs the desired RDFModules and applies a
        post-selection
        """
        from shutil import copy
        ROOT = import_root()
        ROOT.ROOT.EnableImplicitMT(self.request_cpus)

        # prepare inputs and outputs
        # inp = self.input()["data"].path
        inp = self.input().path
        outp = self.output()
        tf = ROOT.TFile.Open(inp)
        try:
            tree = tf.Get(self.tree_name)
            if tree.GetEntries() > 0:
                # build the full selection
                selection = self.config.get_object_expression(self.category, self.dataset.process.isMC,
                    self.systematic, self.systematic_direction)
                dataset_selection = self.dataset.get_aux("selection")
                if dataset_selection and dataset_selection != "1":
                    selection = jrs(dataset_selection, selection, op="and")

                df = ROOT.RDataFrame(self.tree_name, inp)
                branches = list(df.GetColumnNames())
                feature_modules = self.get_feature_modules(self.feature_modules_file)

                # df = df.Filter("event == 265939")

                if len(feature_modules) > 0:
                    for module in feature_modules:
                        df, add_branches = module.run(df)
                        branches += add_branches
                branches = self.get_branches_to_save(branches, self.keep_and_drop_file)
                branch_list = ROOT.vector('string')()
                for branch_name in branches:
                    branch_list.push_back(branch_name)
                filtered_df = df.Define("selection", selection).Filter("selection")
                filtered_df.Snapshot(self.tree_name, create_file_dir(outp["data"].path), branch_list)
            else:
                tf.Close()
                copy(inp, outp["data"].path)

        except ReferenceError:  # empty ntuple
            tf.Close()
            copy(inp, outp["data"].path)
        except AttributeError:  # empty input file
            tf.Close()
            copy(inp, outp["data"].path)
        #copy(self.input()["stats"].path, outp["stats"].path)


class CategorizationWrapper(DatasetCategoryWrapperTask):
    """
    Wrapper task to run the Categorization task over several datasets in parallel.

    Example command:

    ``law run CategorizationWrapper --version test --category-names etau --config-name base_config \
--dataset-names tt_dl,tt_sl --Categorization-workflow htcondor --workers 20 \
--Categorization-base-category-name base_selection``
    """

    def atomic_requires(self, dataset, category):
        return Categorization.req(self, dataset_name=dataset.name, category_name=category.name)


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
    """

    from_preprocess = luigi.BoolParameter(default=False, description="whether to use as input "
        "PreprocessRDF, default: False")
    force_hadd = luigi.BoolParameter(default=False, description="whether to force hadd "
        "as tool to do the merging, default: False")
    force_haddnano = luigi.BoolParameter(default=False, description="whether to force haddnano.py "
        "as tool to do the merging, default: False")

    # regions not supported
    region_name = None
    tree_name = "Events"
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
		branches=range(start_leaf, end_leaf - 1), _exclude={"branch"})
        else:
            return PreprocessRDF.vreq(self, workflow="local",
		branches=range(start_leaf, end_leaf - 1), _exclude={"branch"})

    def trace_merge_inputs(self, inputs):
        if not self.from_preprocess:
            return [inp["data"] for inp in inputs["collection"].targets.values()]
        else:
            return [inp for inp in inputs["collection"].targets.values()]

    def merge_output(self):
        return law.SiblingFileCollection([
            self.local_target("data_{}.root".format(i))
            for i in range(self.n_files_after_merging)
        ])

    def merge(self, inputs, output):
        ROOT = import_root()
        # with output.localize("w") as tmp_out:
        with output.localize("w") as tmp_out:
            good_inputs = []
            for inp in inputs:  # merge only files with a filled tree
                tf = ROOT.TFile.Open(inp.path)
                try:
                    tree = tf.Get(self.tree_name)
                    if tree.GetEntries() > 0:
                        good_inputs.append(inp)
                except:
                    print("File %s not used" % inp.path)
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
            else:  # if all input files are empty, create an empty file as output
                tf = ROOT.TFile.Open(create_file_dir(tmp_out.path), "RECREATE")
                tf.Close()

class MergeCategorizationWrapper(DatasetCategoryWrapperTask):
    """
    Wrapper task to run the MergeCategorizationWrapper task over several datasets in parallel.

    Example command:

    ``law run MergeCategorizationWrapper --version test --category-names etau \
--config-name base_config --dataset-names tt_dl,tt_sl --workers 10``
    """

    def atomic_requires(self, dataset, category):
        return MergeCategorization.vreq(self, dataset_name=dataset.name, category_name=category.name)


class MergeCategorizationStats(DatasetTask, law.tasks.ForestMerge):
    """
    Merges the output from the PreCounter task in order to reduce the 
    parallelization entering the plotting tasks.

    Example command:

    ``law run MergeCategorizationStats --version test --config-name base_config \
--dataset-name dy_high --workers 10``
    """
    # regions not supported
    region_name = None

    merge_factor = 16

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"

    def merge_workflow_requires(self):
        return PreCounter.req(self, _prefer_cli=["workflow"])

    def merge_requires(self, start_leaf, end_leaf):
        return PreCounter.req(self, workflow="local", branches=range(start_leaf, end_leaf - 1),
            _exclude={"branch"})

    def trace_merge_inputs(self, inputs):
        # return [inp["data"] for inp in inputs["collection"].targets.values()]
        return [inp for inp in inputs["collection"].targets.values()]

    def merge_output(self):
        return self.local_target("stats.json")

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


class MergeCategorizationStatsWrapper(DatasetSuperWrapperTask):
    """
    Wrapper task to run the MergeCategorizationStatsWrapper task over several datasets in parallel.

    Example command:

    ``law run MergeCategorizationStatsWrapper --version test --config-name base_config \
--dataset-names tt_dl,tt_sl --workers 10``
    """
    def atomic_requires(self, dataset):
        return MergeCategorizationStats.req(self, dataset_name=dataset.name)


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
