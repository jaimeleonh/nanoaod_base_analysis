# coding: utf-8

# This python module defines the classes allowing to run the skimming task in
# the framework (merging preprocess y precounter steps in Jaime's approach).

# Written by O. Gonzalez (2023_02_18)

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

class SkimmingTaskRDF(DatasetTaskWithCategory,law.LocalWorkflow, HTCondorWorkflow, SplittedTask, RDFModuleTask):
    """
    Performs the preprocessing step applying a preselection + running RDF modules

    See requirements in :class:`.PreCounter`.

    Example command:

    ``law run SkimmingTaskRDF --version test  --category-name base_selection \
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
    weights_file = luigi.Parameter(description="filename with modules to run RDataFrame", default="")

    default_store = "$CMT_STORE_EOS_PREPROCESSING"
    default_wlcg_fs = "wlcg_fs_categorization"

    tree_name = "Events"

    def __init__ (self, *args, **kwargs):
        super(SkimmingTaskRDF, self).__init__(*args, **kwargs)
        #self.addendum = self.get_addendum()
        #self.custom_output_tag = "_%s" % self.addendum
        self.threshold = self.dataset.get_aux("event_threshold", None)
        self.merging_factor = self.dataset.get_aux("preprocess_merging_factor", None)

    def create_branch_map(self):
        """
        :return: number of files for the selected dataset
        :rtype: int
        """
        self.threshold = self.dataset.get_aux("event_threshold", None)
        self.merging_factor = self.dataset.get_aux("preprocess_merging_factor", None)
        if not self.threshold and not self.merging_factor:
            return len(self.dataset.get_files(
                os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name), add_prefix=False))
        elif self.threshold and not self.merging_factor:
            return len(self.dataset.get_file_groups(
                path_to_look=os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name),
                threshold=self.threshold))
        elif not self.threshold and self.merging_factor:
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
        return {"data": self.local_target("data_%s.root" % self.branch),
                "stats": self.local_target("data_%s.json" % self.branch)}
        # return self.local_target("{}".format(self.input()["data"].path.split("/")[-1]))

    #def get_input(self):
    #    merging_factor = self.dataset.get_aux("preprocess_merging_factor", None)
    #    threshold = self.dataset.get_aux("event_threshold", None)
    #    if not merging_factor and not threshold:
    #        return self.input().path
    #    else:
    #        return tuple([f.path for f in self.input().values()])

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

        # prepare inputs and outputs
        inp = self.get_input()
        df = ROOT.RDataFrame(self.tree_name, self.get_path(inp))

        branches = list(df.GetColumnNames())

        # Processing and storing the weights before applying the selections

        weights = self.config.weights.total_events_weights

        weight_modules = self.get_feature_modules(self.weights_file)
 
        df = df.Define('weightsign',"((genWeight<0)?-1:1)")

        if self.dataset.process.isData:
            df = df.Define('mcWeight',"1.0")
        else: # Only in MC!
            for module in weight_modules:
#                if hasattr(module,'name'):
#                    if module.name not in weights: continue  # We only run those for total weights!
                df, add_branches = module.run(df)
                branches += add_branches

            df = df.Define('mcWeight','*'.join(weights))
            branches.append('mcWeight')

        # Applying a selection from the original (or just processed) variables

        selection = self.category.selection
        if selection != "":
            filtered_df = df.Define("selection", selection).Filter("selection")
        else:
            filtered_df = df

        # Looping for the modules!

        modules = self.get_feature_modules(self.modules_file)

        for module in modules:
#            if hasattr(module,'name'):
#                if module.name in weights: continue   # These were already applied!

            filtered_df, add_branches = module.run(filtered_df)
            branches += add_branches

            # We may apply a selection after the given module!
            selection_mod = self.category.get_aux('selection_'+module.__class__.__name__,None)
            if selection_mod is not None:
                filtered_df = filtered_df.Define('selection_'+module.__class__.__name__,selection_mod).Filter('selection_'+module.__class__.__name__)

        # Creating the json... from the counts and and the sum of the weight columns

        d = {
            'nevents': df.Count().GetValue(),
            'npassingevents': filtered_df.Count().GetValue(),

            'nweightedevents': df.Sum('mcWeight').GetValue(),

            'nweightedpassingevents':filtered_df.Sum('mcWeight').GetValue(),

            'ntotalmcweight': df.Sum("weightsign").GetValue(),
            'npassingmcweight': filtered_df.Sum('weightsign').GetValue(),
            'filenames': [str(self.get_path(inp))]
        }

        with open(self.output()["stats"].path, "w+") as f:
            json.dump(d, f, indent = 4)

        # Saving the skimming Events ttre...

        branches = self.get_branches_to_save(branches, self.keep_and_drop_file)
        branch_list = ROOT.vector('string')()
        for branch_name in branches:
            branch_list.push_back(branch_name)
        # filtered_df.Snapshot(self.tree_name, create_file_dir(outp.path), branch_list)
        filtered_df.Snapshot(self.tree_name, self.output()["data"].path, branch_list)

# =======================================================================
