# coding: utf-8

"""
    Shard creation tasks.
"""

__all__ = []

import os
import law
import luigi
from collections import OrderedDict
import contextlib

from analysis_tools.utils import join_root_selection as jrs
from analysis_tools.utils import import_root, create_file_dir

from cmt.util import iter_tree
from cmt.base_tasks.base import ( 
    DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, InputData,
    ConfigTaskWithCategory
)
from cmt.base_tasks.preprocessing import PreprocessRDF, DatasetCategoryWrapperTask


split_names = ["train", "valid"]


class CreateShards(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow):
    base_category_name = luigi.Parameter(default="base_selection", description="the name of the "
            "base category with the initial selection, default: base")
    # regions not supported
    region_name = None

    tree_name = PreprocessRDF.tree_name

    default_store = "$CMT_STORE_EOS_SHARDS"
    default_wlcg_fs = "wlcg_fs_shards"

    @property
    def priority(self):
        # give bsm signal a lower task priority
        if self.process.get_aux("is_signal", False) and self.process.get_aux("is_bsm", False):
            return -10
        else:
            return 0

    def create_branch_map(self):
        return len(self.dataset.get_files(
            os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name), add_prefix=False))

    def workflow_requires(self):
        return {"data": PreprocessRDF.vreq(self, category_name=self.base_category_name)}

    def requires(self):
        return PreprocessRDF.vreq(self, category_name=self.base_category_name,
            branch=self.branch)

    def output(self):
        outputs = OrderedDict(
            (split, self.local_target("data_{}_{}.pb".format(split, self.branch)))
            for split in split_names
        )
        outputs["stats"] = self.local_target("stats_{}.json".format(self.branch))
        return outputs

    @law.decorator.notify
    @law.decorator.localize
    def run(self):
        import numpy as np
        import tensorflow as tf
        import tabulate

        np.random.seed(100 * self.branch)

        # get the splitting fractions
        splitting = self.dataset.get_aux("splitting[self.category_name]", 0.75)
        splitting = (splitting, 1. - splitting)

        # list of features to consider
        features = [
            feature for feature in self.config.features
            if not feature.has_tag("skip_shards")
        ]
        self.publish_message("found {} features to convert".format(len(features)))

        # helpers to create a tf feature
        def tf_feature(value):
            return tf.train.Feature(float_list=tf.train.FloatList(value=[value]))

        # helpers to create a tf example
        def tf_example(values):
            tf_values = {f.name: tf_feature(v) for f, v in zip(features, values)}
            example = tf.train.Example(features=tf.train.Features(feature=tf_values))
            return example

        # prepare outputs
        outputs = self.output()
        writers = [tf.io.TFRecordWriter(outputs[split].path) for split in split_names]

        # prepare input
        inp = self.input()
        self.publish_message("input file size: {}".format(law.util.human_bytes(
            inp.stat().st_size, fmt=True)))
        tfile = inp.load(formatter="root")
        tree = tfile.Get(self.tree_name)

        # when the input file is broken, create empty outputs
        broken = not bool(tree)
        if broken:
            with contextlib.nested(*writers):
                pass
            stats = {"counts": {name: 0 for name in split_names}}
            outputs["stats"].dump(stats, indent=4, formatter="json")
            return

        # get the total events
        n_total = tree.GetEntries()
        self.publish_message("found {} events ".format(n_total))

        # build the full selection
        selection = self.category.get_aux("nt_selection", self.category.selection)
        if self.dataset.get_aux("selection", "1") and self.dataset.get_aux("selection", "1") != "1":
            selection = jrs(self.dataset.x.selection, selection, op="and")

        # iterate through the tree
        expressions = [self.config.get_object_expression(feature, self.dataset.process.isMC)
            for feature in features]
        for ielem, elem in enumerate(expressions):
            while ".at" in elem:
                first_par = elem.index(".at") + len(".at")
                num_par = 1
                for i in range(first_par + 1, len(elem)):
                    if elem[i] == "(":
                        num_par += 1
                    elif elem[i] == ")":
                        num_par -= 1
                        if num_par == 0:
                            elem = elem[:first_par - len(".at")] + "[" +  elem[first_par + 1:]
                            elem = elem[:(i - len(".at"))] + "]" + elem[i - len(".at") + 1:]
                        break
            expressions[ielem] = elem

        defaults = [feature.get_aux("default", -999.) for feature in features]
        missing_values = [feature.get_aux("missing", -999.) for feature in features]
        counts = [0] * len(split_names)
        progress = self.create_progress_callback(n_total)
        with self.publish_step("converting ...", runtime=True):
            with contextlib.nested(*writers):
                n_converted = 0
                for idx, values in iter_tree(tree, expressions, defaults, selection=selection,
                        missing_values=missing_values, yield_index=True):
                    if not np.isfinite(values).all():
                        raise Exception("not all values are finite in entry {}: {}".format(
                            idx, values))
                    example = tf_example(values)

                    # add the process id
                    # example.features.feature["process_id"].float_list.value.append(self.process.id)

                    # decide which writer to use and track counts
                    r = np.random.choice(len(splitting), p=splitting)
                    writers[r].write(example.SerializeToString())
                    counts[r] += 1

                    n_converted += 1
                    progress(idx)

                tfile.Close()
                self.publish_message("converted {} events".format(n_converted))

        # save stats
        stats = {"counts": dict(zip(split_names, counts))}
        outputs["stats"].dump(stats, indent=4, formatter="json")

        # print some helpful quantities
        headers = ["Split", "Count", "Fraction / %", "File size / MB"]
        rows = []
        sum_counts = sum(counts)
        for split_name, count in zip(split_names, counts):
            rows.append([
                split_name,
                count,
                (100. * count / sum_counts) if sum_counts else "-",
                law.util.parse_bytes(outputs[split_name].stat().st_size, unit="MB"),
            ])
        table = tabulate.tabulate(rows, headers=headers, floatfmt=".2f", tablefmt="grid")
        self.publish_message(table)


class CreateShardsWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        tasks_per_job = 2
        if dataset.name == "dy":
            tasks_per_job = 5
        elif dataset.name in ("tt_sl", "tt_dl", "tth_tautau"):
            tasks_per_job = 1

        return CreateShards.req(self, dataset_name=dataset.name, category_name=category.name,
            tasks_per_job=tasks_per_job, _prefer_cli=["tasks_per_job"])

    def get_default_dataset_names(self):
        return [
            dataset.name for dataset in self.config.datasets
            if not dataset.has_tag("skip_shards")
        ]


class MergeShards(DatasetTaskWithCategory, law.tasks.ForestMerge):

    split = luigi.ChoiceParameter(default="train", choices=split_names, description="the dataset "
        "split to merge, default: train")

    # regions not supported
    region_name = None

    merge_factor = 20

    default_store = "$CMT_STORE_EOS_SHARDS"
    default_wlcg_fs = "wlcg_fs_shards"

    def merge_workflow_requires(self):
        return CreateShards.req(self, _prefer_cli=["workflow"])

    def merge_requires(self, start_leaf, end_leaf):
        return CreateShards.req(self, branch=-1, workflow="local", start_branch=start_leaf,
            end_branch=end_leaf)

    def trace_merge_inputs(self, inputs):
        return [inp[self.split] for inp in inputs["collection"].targets.values()]

    def merge_output(self):
        return law.SiblingFileCollection([
            self.local_target("data_{}_{}.pb".format(self.split, i))
            for i in range(self.n_files_after_merging)
        ])

    def merge(self, inputs, output):
        with output.localize("w") as tmp_out:
            # pb files containing tf examples are headless and can be cat'ed
            paths = [inp.path for inp in inputs]
            cmd = "cat {} > {}".format(" ".join(paths), tmp_out.path)

            # run the command
            code = law.util.interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
            if code != 0:
                raise Exception("pb file merging via cat failed")

            self.publish_message("output file size: {}".format(law.util.human_bytes(
                tmp_out.stat().st_size, fmt=True)))


class MergeShardsWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        return {
            split: MergeShards.req(self, dataset_name=dataset.name, category_name=category.name,
                split=split)
            for split in split_names
        }

    def get_default_dataset_names(self):
        return [
            dataset.name for dataset in self.config.datasets
            if not dataset.has_tag("skip_shards")
        ]
