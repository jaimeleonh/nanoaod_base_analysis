# coding: utf-8

"""
Training tasks.
"""

__all__ = []


from collections import OrderedDict

import six
import law
import luigi

from cmt.base_tasks.base import Task, ConfigTask, HTCondorWorkflow
from cmt.base_tasks.preprocessing import MergeShards
from cmt.util import parse_workflow_file


class TrainingTask(ConfigTask):

    training_config_name = luigi.Parameter(default="default", description="the name of the "
        "training configuration, default: default")
    training_category_name = luigi.Parameter(default="baseline_os_odd", description="the name of "
        "a category whose selection rules are applied to data to train on, default: "
        "baseline_os_odd")
    feature_tag = luigi.Parameter(default="training", description="the tag of features to use for "
        "the training, default: training")
    architecture = luigi.Parameter(default="dense:256_64_16:tanh", description="a string "
        "describing the network architecture, default: dense:256_64_16:tanh")
    loss_name = luigi.Parameter(default="wsgce", description="the name of the loss function, "
        "default: wsgce")
    l2_norm = luigi.FloatParameter(default=1e-3, description="the l2 regularization factor, "
        "default: 1e-3")
    event_weights = luigi.BoolParameter(default=False, description="whether to use event weights "
        "as per-example weights in the loss, default: False")
    learning_rate = luigi.FloatParameter(default=5e-5, description="the learning rate, default: "
        "5e-5")
    dropout_rate = luigi.FloatParameter(default=0.1, description="the dropout rate, default: 0.1")
    batch_norm = luigi.BoolParameter(default=True, description="whether batch normalization should "
        "be applied, default: True")
    batch_size = luigi.IntParameter(default=1024, description="the training batch size, default: "
        "1024")
    random_seed = luigi.IntParameter(default=1, description="random seed for weight "
        "initialization, 0 means non-deterministic, default: 1")
    min_feature_score = luigi.FloatParameter(default=law.NO_FLOAT, description="minimum score "
        "for filtering used features, requires the FeatureRanking when not empty, default: empty")

    training_id = luigi.IntParameter(default=law.NO_INT, description="when given, overwrite "
        "training parameters from the training with this branch in the training_workflow_file, "
        "default: empty")
    training_workflow_file = luigi.Parameter(default="$CMT_BASE/cmt/config/hyperopt.yml",
        significant=False, description="file containing training workflow definitions, default: "
        "$CMT_BASE/cmt/config/hyperopt.yml")

    training_hash_params = [
        "training_config_name", "training_category_name", "feature_tag", "architecture",
        "loss_name", "l2_norm", "event_weights", "learning_rate", "dropout_rate", "batch_norm",
        "batch_size", "random_seed", "min_feature_score",
    ]

    allow_composite_training_category = True

    @classmethod
    def modify_param_values(cls, params):
        if "training_workflow_file" not in params or "training_id" not in params:
            return params
        if params["training_id"] == law.NO_INT:
            return params

        branch_map = parse_workflow_file(params["training_workflow_file"])[1]

        param_names = cls.get_param_names()
        for name, value in branch_map[params["training_id"]].items():
            if name in param_names:
                params[name] = value

        params["training_id"] = law.NO_INT

        return params

    @classmethod
    def create_training_hash(cls, **kwargs):
        def fmt(key, prefix, fn):
            if key not in kwargs:
                return None
            value = fn(kwargs[key])
            if value in ("", None):
                return None
            return prefix + "_" + str(value)
        def num(n, tmpl="{}", skip_empty=False):
            if skip_empty and n in (law.NO_INT, law.NO_FLOAT):
                return None
            else:
                return tmpl.format(n).replace(".", "p")
        parts = [
            fmt("training_config_name", "CF", str),
            fmt("training_category_name", "TC", str),
            fmt("feature_tag", "FT", lambda v: v.replace("*", "X").replace("?", "Y")),
            fmt("architecture", "AR", lambda v: v.replace(":", "_")),
            fmt("loss_name", "LN", str),
            fmt("l2_norm", "L2", lambda v: num(v, "{:.2e}")),
            fmt("event_weights", "EW", int),
            fmt("learning_rate", "LR", lambda v: num(v, "{:.2e}")),
            fmt("dropout_rate", "DO", num),
            fmt("batch_norm", "BN", int),
            fmt("batch_size", "BS", num),
            fmt("random_seed", "RS", str),
            fmt("min_feature_score", "MF", lambda v: num(v, skip_empty=True)),
        ]
        return "__".join(part for part in parts if part)

    def __init__(self, *args, **kwargs):
        super(TrainingTask, self).__init__(*args, **kwargs)

        # store the training config
        self.training_config = self.config.process_training_names[self.training_config_name]

        # store the category and check for compositeness
        self.training_category = self.config.categories.get(self.training_category_name)
        # if self.training_category.x("composite", False) and \
                # not self.allow_composite_training_category:
            # raise Exception("training category '{}' is composite, prohibited by task {}".format(
                # self.training_category.name, self))

        # save training features, without minimum feature score filtering applied
        self.training_features = [
            feature for feature in self.config.features
            if feature.has_tag(self.feature_tag)
        ]

        # compute the storage hash
        self.training_hash = self.create_training_hash(**self.get_training_hash_data())

    def get_training_hash_data(self):
        return {p: getattr(self, p) for p in self.training_hash_params}

    def store_parts(self):
        parts = super(TrainingTask, self).store_parts()
        parts["training_hash"] = self.training_hash
        return parts

    def expand_training_category(self):
        # if self.training_category.x("composite", False):
            # return list(self.training_category.get_leaf_categories())
        # else:
        return [self.training_category]

    @classmethod
    def get_accuracy_weights(cls, processes):
        # obtain the weights for calculating the mean accuracy
        # strategy: combined signals should have the same weight as combined backgrounds
        signal_flags = [process.x("is_signal", False) for process in processes]
        n_signals = sum(signal_flags)
        n_backgrounds = len(processes) - n_signals
        weights = [
            1. / (n_signals if is_signal else n_backgrounds)
            for is_signal in signal_flags
        ]
        # normalize weight so that their sum is len(processes)
        f = len(processes) / sum(weights)
        return [(f * w) for w in weights]

    @classmethod
    def get_class_weights(cls, processes):
        return cls.get_accuracy_weights(processes)

    def get_data_specs(self, shards_req, configs=None, training_config_name=None):
        if configs is None:
            configs = {self.config.name: self.config}
        if training_config_name is None:
            training_config_name = self.training_config_name

        # data_specs is a nested (ordered) dictionary with the structure
        # {training_process.name: {"shards": [files], "label": int}}
        # built from shards_req which can have the following structure(s)
        # {split_name: [{dataset_name: law.FileCollection, ...}, ...}
        # or
        # {split_name: [{(config_name, dataset_name): law.FileCollection, ...}, ...}
        data_specs = {}
        for split, split_reqs in shards_req.items():
            data_specs[split] = data_spec = OrderedDict()
            for split_req in law.util.make_list(split_reqs):
                for key, coll in split_req.items():
                    # interpret the key
                    if isinstance(key, six.string_types):
                        assert(len(configs) != 1)
                        config_name = list(configs.keys())[0]
                        dataset_name = key
                    else:
                        config_name, dataset_name = key

                    # get objects
                    config = configs[config_name]
                    training_config = config.training[training_config_name]
                    process = config.datasets.get(dataset_name).processes.get_first()

                    # get the corresponding training process and the label index
                    for _label, _training_process in enumerate(training_config.processes):
                        if _training_process == process or _training_process.has_process(process):
                            training_process = _training_process
                            label = _label
                            break
                    else:
                        raise Exception("cannot determine label for events in dataset {}".format(
                            dataset_name))

                    # add to data_spec
                    data_spec.setdefault(training_process.name, {"shards": [], "label": label})
                    data_spec[training_process.name]["shards"] += [inp.path for inp in coll.targets]

        return data_specs

    def update_cross_evaluation(self, category, expanded_categories=None):
        if expanded_categories is None:
            expanded_categories = [category]

        # create a data to training category mapping
        category_mapping = OrderedDict()
        tc = self.training_category
        if category.x.is_eo:
            if not tc.x.is_eo:
                tc = self.config.helpers.get_child_eo_category(tc,
                    "odd" if category.x.is_even else "even")
            elif category.x.is_odd == tc.x.is_odd:
                tc = self.config.helpers.get_opposite_eo_category(tc)
            for c in expanded_categories:
                category_mapping[c] = tc
        else:
            if tc.x.is_eo:
                tc = self.config.helpers.get_parent_eo_category(tc)
            for c in expanded_categories:
                category_mapping[c] = self.config.helpers.get_child_eo_category(tc,
                    "odd" if c.x.is_even else "even")

        # reset the training category when changed and recompute the storage hash
        if tc != self.training_category:
            self.training_category = tc
            self.training_hash = self.create_training_hash(**{
                p: getattr(self, p) for p in self.training_hash_params
            })

        return category_mapping

    def get_tabbed_repr(self, training_features=None):
        values = [
            self.training_category_name,
            "{} ({})".format(self.feature_tag, len(training_features or self.training_features)),
            self.architecture,
            self.loss_name,
            self.l2_norm,
            self.learning_rate,
            self.batch_size,
            "TRUE" if self.event_weights else "FALSE",
            ", ".join(str(w) for w, _ in self.training_config.process_group_ids),
            self.dropout_rate,
            "TRUE" if self.batch_norm else "FALSE",
            self.random_seed,
        ]
        return "\t".join(str(v) for v in values)


class CrossTrainingWrapper(ConfigTask):

    training_id_even = luigi.IntParameter(description="id of the training on even event numbers")
    training_id_odd = luigi.IntParameter(description="id of the training on odd event numbers")

    def store_parts(self):
        parts = super(CrossTrainingWrapper, self).store_parts()
        parts["training_ids"] = "tids_{}_{}".format(self.training_id_even, self.training_id_odd)
        return parts


class MultiConfigTrainingTask(TrainingTask):
    """
    This task extends the base TrainingTask which is already a config task. The purpose of *this*
    task is to add *additional* configs only for the purpose of including more training data. This
    means that training configs, categories, feature definitions, etc., are all taken from the
    actual, *non-additional* configs. Otherwise, this would result in complex cases where configs
    might not be compatible with each other.
    """

    data_config_names = law.CSVParameter(default=(), description="names of additional config files "
        "whose training files are to be combined, default: empty")

    @classmethod
    def combine_config_names(cls, configs):
        # create a short name for base_* configs, otherwise just concatenate
        # if the order configs is important in the created name, order configs in the order you want
        if all(config.name == "base_{}".format(config.year) for config in configs):
            return "base_" + "_".join(str(config.year) for config in configs)
        else:
            return "__".join(config.name for config in configs)

    def __init__(self, *args, **kwargs):
        super(MultiConfigTrainingTask, self).__init__(*args, **kwargs)

        # store all configs, including the original one
        self.training_data_configs = OrderedDict({self.config_name: self.config})
        for name in sorted(self.data_config_names):
            if name in self.training_data_configs:
                continue
            cmt = __import__("cmt.config." + name)
            self.training_data_configs[name] = getattr(cmt.config, name).config

    def store_parts(self):
        parts = super(MultiConfigTrainingTask, self).store_parts()

        # when there are additional data confgs, replace the config name
        if len(self.training_data_configs) > 1:
            combined_config_name = self.combine_config_names(self.training_data_configs.values())
            parts["config_name"] = combined_config_name

        return parts


class MultiSeedTrainingTask(MultiConfigTrainingTask):

    random_seeds = law.CSVParameter(cls=luigi.IntParameter, default=(1,), description="random "
        "seeds, default: (1,)")
    random_seed = None

    def __init__(self, *args, **kwargs):
        super(MultiSeedTrainingTask, self).__init__(*args, **kwargs)

        # at least one seed must be set
        if not self.random_seeds:
            raise ValueError("at least one random seed is mandatory")

    def get_training_hash_data(self):
        data = super(MultiSeedTrainingTask, self).get_training_hash_data()

        # add string representation of sorted seeds
        data["random_seed"] = "_".join(str(s) for s in sorted(self.random_seeds))

        return data


class Training(MultiConfigTrainingTask):

    n_threads = luigi.IntParameter(default=4, description="the number of threads to use for "
        "reading input data and training on CPU, default: 4")
    max_steps = luigi.IntParameter(default=25000, description="the maximum number of training "
        "steps, not encoded into output paths (!), default: 25000")
    save_tensorboard = luigi.BoolParameter(default=False, description="when set, also save "
        "tensorboard infos, default: False")
    save_metrics = luigi.BoolParameter(default=True, description="when set, also save metrics of "
        "the best model, default: True")
    print_metrics = luigi.ChoiceParameter(choices=("", "plain", "tabbed"), default="",
        significant=False, description="print the training metrics when the task aleady ran and "
        "exit, but do not run any task, choices: '','plain','tabbed', default: ''")

    interactive_params = MultiConfigTrainingTask.interactive_params + ["print_metrics"]

    accepts_messages = True

    def requires(self):
        reqs = {"shards": {}}
        for split in ["train", "valid"]:
            reqs["shards"][split] = [
                OrderedDict(
                    law.util.flatten(([
                        ((config.name, dataset.name), MergeShards.vreq(self, split=split,
                            config_name=config.name, dataset_name=dataset.name,
                            category_name=category.name, _prefer_cli=["version"]))
                        # for dataset in config.training[self.training_config_name].datasets
                        for dataset in config.datasets
                        if dataset.process.name in config[self.training_config_name]
                    ] for config in self.training_data_configs.values()), flatten_tuple=False)
                )
                for category in self.expand_training_category()
            ]

        # optionally require feature ranking
        if self.min_feature_score != law.NO_FLOAT:
            # do not pass the min_feature_score here as this would result in a requirement recursion
            reqs["ranking"] = FeatureRanking.req(self, _exclude=["min_feature_score"],
                _prefer_cli=["version"])
        return reqs

    def output(self):
        targets = {"model": self.local_target("model.h5")}
        if self.save_tensorboard:
            targets["tensorboard"] = self.local_target("tensorboard", dir=True)
        if self.save_metrics:
            targets["metrics"] = self.local_target("metrics.json")

        return law.SiblingFileCollection(targets)

    def filter_features(self, ranking_input, features=None, min_feature_score=None):
        if features is None:
            features = self.training_features
        if min_feature_score is None:
            min_feature_score = self.min_feature_score
        if min_feature_score == law.NO_FLOAT:
            return features

        _features = []
        for feature in features:
            inp = ranking_input["collection"].targets[feature.name]
            scores = inp.load(formatter="json")
            # peculiarity: when the lbn is used, do not filter four-vector components
            if self.architecture.startswith("lbn") and \
                    feature.has_tag(["lbn_pt", "lbn_eta", "lbn_phi", "lbn_e"]):
                _features.append(feature)
            elif scores["mean_score"] >= min_feature_score:
                _features.append(feature)
            else:
                self.publish_message("skipping feature '{}' with mean score {}".format(
                    feature.name, scores["mean_score"]))
        return _features

    @law.decorator.notify
    @law.decorator.localize(input=False, output_kwargs={"mode": "a"})
    def run(self):
        inputs = self.input()

        # get data specs
        data_specs = self.get_data_specs(inputs["shards"], configs=self.training_data_configs)

        # get accuracy and class weights
        acc_weights = self.get_accuracy_weights(self.training_config.processes)
        class_weights = self.get_class_weights(self.training_config.processes)

        # when training with event weights, exactly one feature must be tagged training_event_weight
        ew_feature = None
        if self.event_weights:
            ew_features = [
                feature for feature in self.config.features
                if feature.has_tag("training_event_weight")
            ]
            if len(ew_features) != 1:
                raise Exception("{} features tagged 'training_event_weight', expected 1".format(
                    len(ew_features)
                ))
            ew_feature = ew_features[0]

        # special case / hotfix: training in the vr category
        # this requires to rebalance vbf and resolved events so that their effect is equal
        # (otherwise, this would favor resolved events as they are way more frequent)
        event_weight_fn = None
        if self.training_category.x("vr_weights", False):
            print("training in VR category with balancing weights")
            import tensorflow as tf

            @tf.function
            def vr_weights(labels, weight_features):
                weights = tf.zeros(labels.shape[0], dtype=tf.float32)
                vbf_mask = weight_features[:, 0] == 1
                res_mask = weight_features[:, 1] == 1
                epsilon = 1e-6
                for i in range(labels.shape[1]):
                    class_mask = labels[:, i] == 1
                    vbf_flags = tf.cast(vbf_mask & class_mask, tf.float32)
                    res_flags = tf.cast(res_mask & class_mask, tf.float32)
                    n_vbf = tf.reduce_sum(vbf_flags) + epsilon
                    n_res = tf.reduce_sum(res_flags) + epsilon
                    weights += (n_vbf + n_res) / 2. * (vbf_flags / n_vbf + res_flags / n_res)
                return weights

            event_weight_fn = (["is_vbf_cat", "is_resolved_cat"], vr_weights)

        # optionally filter training features by mininum score
        if self.min_feature_score != law.NO_FLOAT:
            training_features = self.filter_features(inputs["ranking"])
        else:
            training_features = list(self.training_features)

        # print the tabbed representation
        self.publish_message("tabbed representation:")
        self.publish_message(self.get_tabbed_repr(training_features=training_features))

        # prepare the output directory
        outputs = self.output()
        outputs.dir.touch()

        # import the training functions
        from cmt.training.training import train
        from cmt.training.util import configure_cpu
        configure_cpu(self.n_threads)

        # run the training
        with self.publish_step("running training ...", runtime=True):
            tensorboard_dir = outputs.targets["tensorboard"].path if self.save_tensorboard else None
            model_metrics_path = outputs.targets["metrics"].path if self.save_metrics else None
            step_fn = self.create_progress_callback(self.max_steps)
            train(data_specs["train"], data_specs["valid"], training_features,
                self.architecture, self.loss_name, self.learning_rate, self.batch_size,
                self.batch_norm, self.dropout_rate, self.l2_norm, event_weight_feature=ew_feature,
                event_weight_fn=event_weight_fn,
                process_group_ids=self.training_config.process_group_ids, acc_weights=acc_weights,
                class_weights=class_weights, max_steps=self.max_steps, log_every=10,
                validate_every=100, stopper_patience=15, seed=self.random_seed or None,
                n_threads=self.n_threads, model_path=outputs.targets["model"].path,
                model_metrics_path=model_metrics_path, tensorboard_dir=tensorboard_dir,
                step_fn=step_fn, stop_fn=self.stop_on_scheduler_message,
                log_fn=self.publish_message)

    def _load_metrics(self):
        outp = self.output().targets.get("metrics")
        if not outp:
            return law.util.colored("cannot access metrics when --save-metrics is not set", "red")
        elif not outp.exists():
            return law.util.colored("metrics not yet written", "yellow")
        else:
            try:
                metrics = outp.load(formatter="json")
            except:
                return law.util.colored("error reading metrics from {}".format(outp.path), "red")
            return OrderedDict((key, metrics["metrics"][key]) for key in metrics["keys"])

    def _print_metrics(self, style):
        print("print training metrics\n")

        metrics = self._load_metrics()
        if not isinstance(metrics, dict):
            print(law.util.colored(metrics, "red"))
            return

        if style == "plain":
            print("specs:")
            print(self.get_tabbed_repr())
            print("\nmetrics:")
            max_key_len = max(len(key) for key in metrics)
            for key, metric in metrics.items():
                print("{}{}: {}".format(key, " " * (max_key_len - len(key)), metric))
            pass
        elif style == "tabbed":
            print("specs:")
            print(self.get_tabbed_repr())
            print("\nmetrics:")
            print("\t".join(str(m) for m in metrics.values()))
        else:
            raise NotImplementedError("unknown style '{}'".format(style))

    def stop_on_scheduler_message(self, *args, **kwargs):
        if not self.scheduler_messages:
            return False

        while not self.scheduler_messages.empty():
            msg = self.scheduler_messages.get()

            if msg == "stop":
                msg.respond("gracefully stopping training")
                return True

            msg.respond("unknown message: {}".format(msg))

        return False


class TrainingWorkflowBase(Task, law.LocalWorkflow, HTCondorWorkflow):

    training_workflow_file = TrainingTask.training_workflow_file

    output_collection_cls = law.FileCollection

    def __init__(self, *args, **kwargs):
        super(TrainingWorkflowBase, self).__init__(*args, **kwargs)

        # store the workflow file data and already the branch map
        self.workflow_data, self._file_branch_map = parse_workflow_file(self.training_workflow_file)

    def create_branch_map(self):
        return self._file_branch_map

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


class TrainingWorkflow(TrainingWorkflowBase):

    print_metrics = Training.print_metrics

    interactive_params = TrainingWorkflowBase.interactive_params + ["print_metrics"]

    def requires(self):
        return Training.req(self, _prefer_cli=["version"], **self.matching_branch_data(Training))

    def output(self):
        return self.requires().output()

    def run(self):
        pass

    def _print_metrics(self, style):
        if self.is_workflow():
            branch_tasks = self.get_branch_tasks()
            training_tasks = [(b, task.requires()) for b, task in branch_tasks.items()]
            metrics = [task._load_metrics() for _, task in training_tasks]

            print("print metrics of {} training(s)\n".format(len(training_tasks)))

            print("specs:")
            for b, task in training_tasks:
                if style == "plain":
                    print("{}: {}".format(b, task.get_tabbed_repr()))
                elif style == "tabbed":
                    print(task.get_tabbed_repr())
                else:
                    raise NotImplementedError("unknown style '{}'".format(style))

            print("\nmetrics:")
            n_cols = 13
            n_broken = 0
            for (b, _), metrics in zip(training_tasks, metrics):
                metrics_str = None
                if isinstance(metrics, dict):
                    metrics_str = "\t".join(str(m) for m in metrics.values())
                # else:
                #     # metrics_str = "\t".join(n_cols * ["X"])
                #     # n_broken += 1
                #     metrics_str = metrics
                if style == "plain":
                    print("{}: {}".format(b, metrics_str or metrics))
                elif style == "tabbed":
                    print(metrics_str or "\t".join(n_cols * ["-"]))
                else:
                    raise NotImplementedError("unknown style '{}'".format(style))
            print("\nbroken metric files: {}".format(n_broken))
        else:
            self.requires()._print_metrics(style)


class ConvertModelToGraph(MultiSeedTrainingTask):

    def requires(self):
        return {
            seed: Training.req(self, random_seed=seed, _prefer_cli=["version"])
            for seed in self.random_seeds
        }

    def output(self):
        return {
            "pb": self.local_target("graph.pb"),
            "txt": self.local_target("graph.pb.txt"),
        }

    def run(self):
        from cmt.training.models import LBNLayer
        from cmt.training.util import combine_models

        # load the models
        models = [
            inp["model"].load(custom_objects={"LBNLayer": LBNLayer})
            for inp in self.input().values()
        ]

        # combine the models
        model = combine_models(models, mode="mean")

        # some test features for debugging purposes
        # import tensorflow as tf
        # dummy_features = [
        #     ("is_mutau", 0.),
        #     ("is_etau", 0.),
        #     ("is_tautau", 1.),
        #     ("is_2016", 1.),
        #     ("is_2017", 0.),
        #     ("is_2018", 0.),
        #     ("bjet1_pt", 48.6299362183),
        #     ("bjet1_eta", -1.80095899105),
        #     ("bjet1_phi", 0.16927665472),
        #     ("bjet1_e", 151.494415283),
        #     ("bjet1_deepflavor_b", 0.997028708458),
        #     ("bjet1_hhbtag", 1.00212621689),
        #     ("bjet2_pt", 42.9884872437),
        #     ("bjet2_eta", -0.210781753063),
        #     ("bjet2_phi", -1.27948069572),
        #     ("bjet2_e", 44.7079277039),
        #     ("bjet2_deepflavor_b", 0.426742911339),
        #     ("bjet2_hhbtag", 0.99769204855),
        #     ("ctjet1_pt", 31.4451274872),
        #     ("ctjet1_eta", -0.794705033302),
        #     ("ctjet1_phi", -0.0718769133091),
        #     ("ctjet1_e", 42.1348686218),
        #     ("ctjet1_deepflavor_b", 0.0129655441269),
        #     ("ctjet1_hhbtag", 2.1429971131e-09),
        #     ("ctjet2_pt", -1.0),
        #     ("ctjet2_eta", -5.0),
        #     ("ctjet2_phi", -4.0),
        #     ("ctjet2_e", -1.0),
        #     ("ctjet2_deepflavor_b", -1.0),
        #     ("ctjet2_hhbtag", -1.0),
        #     ("ctjet3_pt", -1.0),
        #     ("ctjet3_eta", -5.0),
        #     ("ctjet3_phi", -4.0),
        #     ("ctjet3_e", -1.0),
        #     ("ctjet3_deepflavor_b", -1.0),
        #     ("ctjet3_hhbtag", -1.0),
        #     ("fwjet1_pt", -1.0),
        #     ("fwjet1_eta", -5.0),
        #     ("fwjet1_phi", -4.0),
        #     ("fwjet1_e", -1.0),
        #     ("fwjet2_pt", -1.0),
        #     ("fwjet2_eta", -5.0),
        #     ("fwjet2_phi", -4.0),
        #     ("fwjet2_e", -1.0),
        #     ("vbfjet1_pt", 121.321395874),
        #     ("vbfjet1_eta", 2.43821716309),
        #     ("vbfjet1_phi", -2.55300831795),
        #     ("vbfjet1_e", 700.254760742),
        #     ("vbfjet1_deepflavor_b", 0.0764931738377),
        #     ("vbfjet1_hhbtag", -1.0),
        #     ("vbfjet2_pt", 47.7728424072),
        #     ("vbfjet2_eta", -2.02375650406),
        #     ("vbfjet2_phi", -2.02449274063),
        #     ("vbfjet2_e", 184.013778687),
        #     ("vbfjet2_deepflavor_b", 0.0831282660365),
        #     ("vbfjet2_hhbtag", 0.000181929775863),
        #     ("lep1_pt", 55.6019287109),
        #     ("lep1_eta", 1.10572421551),
        #     ("lep1_phi", 0.492795079947),
        #     ("lep1_e", 93.2037200928),
        #     ("lep2_pt", 114.453720093),
        #     ("lep2_eta", 0.176104769111),
        #     ("lep2_phi", 1.61304283142),
        #     ("lep2_e", 116.236434937),
        #     ("met_pt", 27.0168819427),
        #     ("met_phi", 3.92184209824),
        #     ("bh_pt", 68.7158432007),
        #     ("bh_eta", -1.53672146797),
        #     ("bh_phi", -0.500671088696),
        #     ("bh_e", 196.202346802),
        #     ("tauh_sv_pt", 159.877532959),
        #     ("tauh_sv_eta", 0.649999976158),
        #     ("tauh_sv_phi", 1.27409040928),
        #     ("tauh_sv_e", 168.309841536),
        # ]
        # vals = tf.constant([[v for _, v in dummy_features]], dtype=tf.float32)
        # print(model(vals).numpy().tolist())

        # prepare the output and dump the graph
        outputs = self.output()
        outputs["pb"].dump(model, variables_to_constants=True, formatter="tf_graph")
        outputs["txt"].dump(model, variables_to_constants=True, formatter="tf_graph")

        # test of the saved graph
        # g, s = outputs["pb"].load(create_session=True, formatter="tf_graph")
        # with g.as_default():
        #     x = g.get_tensor_by_name("input:0")
        #     y = g.get_tensor_by_name("model/output/truediv:0")
        #     out = s.run(y, {x: vals.numpy()})
        # print(out)


# trailing imports
from cmt.tasks.evaluation import FeatureRanking
