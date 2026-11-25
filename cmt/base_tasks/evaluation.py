# coding: utf-8

"""
Evaluation tasks.
"""

__all__ = ["EvaluateTraining", "EvaluateData", "EvaluateDataWrapper", "FeatureRanking"]


import itertools
from collections import OrderedDict

import law
import luigi
from law.target.file import localize_file_targets

from cmt.base_tasks.base import ConfigTaskWithCategory, DatasetTaskWithCategory, HTCondorWorkflow, SlurmWorkflow
from cmt.base_tasks.preprocessing import MergeCategorization
from cmt.base_tasks.shards import MergeShards
from cmt.base_tasks.training import MultiSeedTrainingTask, Training
from cmt.util import evaluate_model_on_tree


class EvaluateTraining(ConfigTaskWithCategory, MultiSeedTrainingTask):
    """
    Task that evaluates the performance of the trained models on one split of the
    training, validation and test datasets.

    For every random seed, the model of the corresponding training task is loaded together
    with the merged shards of the requested split, and the resulting evaluation statistics
    are written to a json file. Regions are not supported, while composite categories are.

    :param split: dataset split to evaluate, default: "valid"
    :type split: str

    :param n_threads: number of threads to use for reading input data, default: 4
    :type n_threads: int
    """

    split = luigi.ChoiceParameter(default="valid", choices=["train", "valid", "test"],
        description="the dataset split to evaluate, default: valid")
    n_threads = luigi.IntParameter(default=4, significant=False, description="the number of "
        "threads to use for reading input data, default: 4")

    # regions not supported
    region_name = None

    allow_composite_category = True

    def requires(self):
        reqs = {
            "training": {
                seed: Training.req(self, random_seed=seed, _prefer_cli=["version"])
                for seed in self.random_seeds
            },
            "shards": {},
        }

        data_splits = ["train", "valid"] if self.split == "test" else [self.split]

        reqs["shards"][self.split] = [
            OrderedDict(
                (dataset.name, MergeShards.vreq(self, split=split, dataset_name=dataset.name,
                    category_name=category.name))
                for dataset in self.training_config.datasets
            )
            for category, split in itertools.product(self.expand_category(), data_splits)
        ]

        return reqs

    def output(self):
        return self.dynamic_target("stats_{}.json".format(self.split))

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        # get data specs and accuracy weights
        inputs = self.input()
        data_specs = self.get_data_specs(inputs["shards"])
        acc_weights = self.get_accuracy_weights(self.training_config.processes)

        # import the training functions
        from hmc.training.training import evaluate_training
        from hmc.training.util import configure_cpu
        configure_cpu(self.n_threads)

        # evaluate the model, staging the model files locally since keras loads through posix
        # paths, same for the shard files read through tf.data
        model_targets = [inp["model"] for inp in inputs["training"].values()]
        with self.publish_step("evaluate '{}' split ...".format(self.split), runtime=True):
            with localize_file_targets(model_targets, "r") as model_targets, \
                    self.localize_data_specs(data_specs) as data_specs:
                evaluate_training(data_specs[self.split], self.training_features,
                    n_threads=self.n_threads, batch_size=512, acc_weights=acc_weights,
                    process_group_ids=self.training_config.process_group_ids,
                    log_fn=self.publish_message, model_path=[t.path for t in model_targets],
                    stats_path=self.output().path)


class EvaluateData(DatasetTaskWithCategory, MultiSeedTrainingTask, law.LocalWorkflow,
        HTCondorWorkflow, SlurmWorkflow):
    """
    Workflow that applies the trained models to the events of categorized, merged datasets.

    Each branch processes one merged tree of a dataset, optionally restricted to the
    selection of the category and region. The models trained for every random seed are
    combined into a single model by averaging their predictions, and evaluated on the
    selected events. The output is a root file containing a tree named "evaluation" with
    the network predictions, stored in branches prefixed with ``dnn_``, and the observer
    features.
    """

    tree_name = "HTauTauTree"

    def store_parts(self):
        parts = super(EvaluateData, self).store_parts()
        parts["config_name"] = self.config_name
        return parts

    def create_branch_map(self):
        return self.n_files_after_merging

    def workflow_requires(self):
        return {
            "training": {
                seed: Training.req(self, random_seed=seed, _prefer_cli=["version", "config_name",
                    "data_config_names"])
                for seed in self.random_seeds
            },
            "data": MergeCategorization.vreq(self, tree_index=-1, workflow="local",
                _prefer_cli=["version", "config_name"],
                _exclude=["start_branch", "end_branch", "branches"]),
        }

    def requires(self):
        return {
            "training": {
                seed: Training.req(self, random_seed=seed, _prefer_cli=["version", "config_name",
                    "data_config_names"])
                for seed in self.random_seeds
            },
            "data": MergeCategorization.vreq(self, branch=0, tree_index=self.branch,
                workflow="local", _prefer_cli=["version", "config_name"]),
        }

    def htcondor_output_postfix(self):
        postfix = super(EvaluateData, self).htcondor_output_postfix()
        if self.region:
            postfix += "_" + self.region.name
        return postfix

    def output(self):
        postfix = ""
        if self.region:
            postfix += "_" + self.region.name
        postfix += "_" + str(self.branch)

        return self.dynamic_target("data{}.root".format(postfix))

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        from hmc.training.models import LBNLayer
        from hmc.training.util import combine_models
        from order.util import join_root_selection as jrs

        # load the models
        inputs = self.input()
        models = [
            inp["model"].load(custom_objects={"LBNLayer": LBNLayer}, formatter="tf_keras_model")
            for inp in inputs["training"].values()
        ]
        model = models[0] if len(models) == 1 else combine_models(models, mode="mean")

        # determine input features
        input_features = [
            feature for feature in self.config.features
            if feature.has_tag(self.feature_tag)
        ]

        # determine observer features
        obs_features = [
            feature for feature in self.config.features
            if feature.has_tag("eval_observer")
        ]

        # load the tree
        tfile = inputs["data"].load(formatter="root")
        tree = tfile.Get(self.tree_name)
        n_events = tree.GetEntries()
        self.publish_message("tree contains {} events".format(n_events))

        # optional selection
        selection = []
        if self.use_base_category:
            selection.append(self.category.selection)
        if self.region:
            selection.append(self.region.selection)
        if selection:
            selection = jrs(selection, op="and")
            self.publish_message("selection: {}".format(selection))
        else:
            selection = None

        # evaluate
        with self.publish_step("evaluate ...", runtime=True):
            progress_fn = self.create_progress_callback(n_events)
            rec = evaluate_model_on_tree(model, tree, self.training_config.common_processes,
                input_features, obs_features, selection=selection, create_recarray=True,
                prediction_prefix="dnn_", progress_fn=progress_fn)

        self.publish_message("evaluated {} events".format(rec.shape[0]))

        # write the output to a root file
        self.output().dump(rec, treename="evaluation", formatter="root_numpy")


class FeatureRanking(ConfigTaskWithCategory, MultiSeedTrainingTask, law.LocalWorkflow,
        HTCondorWorkflow, SlurmWorkflow):
    """
    Workflow that measures the importance of the training features using a shuffling method.

    Each branch tests one feature, either every training feature or the subset selected via
    *feature_names*: the trained models are evaluated on the requested dataset split and the
    resulting score is written to a json file named after the feature and the split.
    Regions are not supported.

    :param feature_names: features to test, all training features when empty, default: empty
    :type feature_names: list of str

    :param split: dataset split to evaluate, default: "valid"
    :type split: str

    :param n_threads: number of threads to use for reading input data, default: 4
    :type n_threads: int
    """

    feature_names = law.CSVParameter(default=(), description="features to test using the shuffling "
        "method, use all features from tag when empty, default: empty")
    split = luigi.ChoiceParameter(default="valid", choices=["train", "valid"], description="the "
        "dataset split to evaluate, default: valid")
    n_threads = luigi.IntParameter(default=4, significant=False, description="the number of "
        "threads to use for reading input data, default: 4")

    # regions not supported
    region_name = None

    def __init__(self, *args, **kwargs):
        super(FeatureRanking, self).__init__(*args, **kwargs)

        # get features to test, which all must be within the training features
        if self.feature_names:
            training_feature_names = [feature.name for feature in self.training_features]
            for name in self.feature_names:
                if name not in training_feature_names:
                    raise Exception("feature '{}' not in training features".format(name))
            self.features_to_test = [self.config.features.get(name) for name in self.feature_names]
        else:
            self.features_to_test = list(self.training_features)

    def create_branch_map(self):
        return self.features_to_test

    def workflow_requires(self):
        return {
            "training": {
                seed: Training.req(self, random_seed=seed, _prefer_cli=["version"])
                for seed in self.random_seeds
            },
            "shards": {self.split: [
                OrderedDict(
                    (dataset.name, MergeShards.vreq(self, branch=-1, workflow="local",
                        dataset_name=dataset.name, category_name=category.name,
                        _prefer_cli=["workflow"],
                        _exclude=["start_branch", "end_branch", "branches"]))
                    for dataset in self.training_config.datasets
                )
                for category in self.expand_category()
            ]}
        }

    def requires(self):
        return {
            "training": {
                seed: Training.req(self, random_seed=seed, _prefer_cli=["version"])
                for seed in self.random_seeds
            },
            "shards": {self.split: [
                OrderedDict(
                    (dataset.name, MergeShards.vreq(self, branch=-1, workflow="local",
                        dataset_name=dataset.name, category_name=category.name,
                        _exclude=["start_branch", "end_branch", "branches"]))
                    for dataset in self.training_config.datasets
                )
                for category in self.expand_category()
            ]}
        }

    def output(self):
        feature_to_test = self.branch_data
        return self.dynamic_target("score__{}__{}.json".format(feature_to_test.name, self.split))

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        # get data specs and accuracy weights
        inputs = self.input()
        data_specs = self.get_data_specs(inputs["shards"])
        acc_weights = self.get_accuracy_weights(self.training_config.processes)

        # prepare the output directory
        output = self.output()
        output.parent.touch()

        # import the training functions
        from hmc.training.training import rank_feature
        from hmc.training.util import configure_cpu
        configure_cpu(self.n_threads)

        # rank a feature, staging the model files locally since keras loads through posix paths,
        # same for the shard files read through tf.data
        feature_to_test = self.branch_data
        model_targets = [inp["model"] for inp in inputs["training"].values()]
        with self.publish_step("rank feature {} ...".format(feature_to_test.name), runtime=True):
            with localize_file_targets(model_targets, "r") as model_targets, \
                    self.localize_data_specs(data_specs) as data_specs:
                rank_feature(data_specs[self.split], self.training_features, feature_to_test,
                    n_threads=self.n_threads, batch_size=512, acc_weights=acc_weights,
                    model_path=[t.path for t in model_targets], scores_path=output.path,
                    log_fn=self.publish_message)
