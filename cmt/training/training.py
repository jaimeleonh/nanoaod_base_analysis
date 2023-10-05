# coding: utf-8

"""
Test training script that uses keras to build the model, and bare TensorFlow for the training.
"""

__all__ = ["train", "evaluate_model", "evaluate_training", "rank_feature"]


import os
import json
from collections import OrderedDict

import numpy as np
import tensorflow as tf
import tabulate
import six
from six.moves import zip

from cmt.training.models import create_model, LBNLayer
from cmt.training.util import (
    MultiDataset, EarlyStopper, ExampleWeightScaler, print_tensorflow_info, combine_models, npy,
    calculate_confusion_matrix_t, calculate_accuracies_t, calculate_roc_curves,
    create_tensorboard_callbacks, shuffle_feature_t,
)
from cmt.util import colored


def train(data_spec_train, data_spec_valid, features, arch, loss_name, learning_rate, batch_size,
        batch_norm, dropout_rate, l2_norm, event_weight_feature=None, event_weight_fn=None,
        process_group_ids=None, acc_weights=1., class_weights=1., max_steps=20000, log_every=10,
        validate_every=50, stopper_patience=20, learning_rate_decay=(4, 0.67), seed=None,
        n_threads=4, model_path="model.h5", model_metrics_path=None, tensorboard_dir=None,
        model_factory=None, step_fn=None, stop_fn=None, log_fn=None):
    # prepare paths
    model_path = os.path.expandvars(os.path.expanduser(model_path))
    if tensorboard_dir:
        tensorboard_dir = os.path.expandvars(os.path.expanduser(tensorboard_dir))

    # log helper
    def log(msg):
        if callable(log_fn):
            log_fn(msg)
        else:
            print(msg)

    # both data_specs must have the same keys
    if tuple(data_spec_train.keys()) != tuple(data_spec_valid.keys()):
        raise Exception("diverging keys found in data_spec_train and data_spec_valid")

    # get the processes, the order is the same as in the input data
    # and should be fixed via an OrderedDict
    process_names = list(data_spec_train.keys())
    n_processes = len(data_spec_train)

    # get the feature spec
    feature_spec = OrderedDict(
        (feature.name, tf.io.FixedLenFeature(shape=1, dtype=tf.float32))
        for feature in features
    )
    n_features = len(feature_spec)

    # print a header
    print(" start training ".center(80, "-"))
    print("")
    print_tensorflow_info()
    print("")
    print("{} processes: {}".format(n_processes, ", ".join(process_names)))
    print("data specs:")
    for process_name, spec in data_spec_train.items():
        name_offset = max(len(n) for n in process_names) - len(process_name)
        print("  {}{}: label {}, shards {}".format(
            process_name, name_offset * " ", spec["label"], len(spec["shards"])))
    if event_weight_feature:
        print("example weight: {}".format(event_weight_feature.name))
    print("{} features: {}".format(n_features, ", ".join(feature_spec)))

    # prepare observer specs and indices for event weights and further observer features
    observer_spec = OrderedDict()
    event_weight_feature_index = -1
    observer_feature_indices = []

    # when event_weight_feature is set, set it as the first observer feature
    if event_weight_feature:
        event_weight_feature_index = 0
        observer_spec[event_weight_feature.name] = tf.io.FixedLenFeature(shape=1, dtype=tf.float32)

    # include other observer specs when defined by event_weight_fn (a 2-tuple containing a list of
    # required feature names and the actual function to compute weights)
    if event_weight_fn:
        for name in event_weight_fn[0]:
            if name in observer_spec:
                observer_feature_indices.append(list(observer_spec.keys()).index(name))
            else:
                observer_spec[name] = tf.io.FixedLenFeature(shape=1, dtype=tf.float32)
                observer_feature_indices.append(len(observer_spec) - 1)

    # load the datasets

    dataset_train, train_events = MultiDataset.from_specs(data_spec_train, feature_spec,
        observer_spec=observer_spec, shuffle=(10, 40960), repeat=-1, batch_size=batch_size,
        mixed=True, seed=seed, n_threads=n_threads)

    dataset_valid, valid_events = MultiDataset.from_specs(data_spec_valid, feature_spec,
        observer_spec=observer_spec, shuffle=False, repeat=1, batch_size=batch_size, mixed=False,
        seed=seed, n_threads=n_threads)

    print("----------------")
    print("Training events")
    for elem in train_events:
        print("%s: %s" % (elem, train_events[elem]))
    print("----------------")

    print("----------------")
    print("Validation events")
    for elem in valid_events:
        print("%s: %s" % (elem, valid_events[elem]))
    print("----------------")

    # helpers to add tensors and metrics to tensorboard for monitoring
    tb_log_dir = lambda kind: tensorboard_dir and os.path.join(tensorboard_dir, kind)
    tb_train_add, tb_train_flush = create_tensorboard_callbacks(tb_log_dir("train"))
    tb_valid_add, tb_valid_flush = create_tensorboard_callbacks(tb_log_dir("valid"))
    tb_validb_add, tb_validb_flush = create_tensorboard_callbacks(tb_log_dir("valid_best"))

    # create the model
    if not callable(model_factory):
        model_factory = create_model
    model, loss_funcs_t = model_factory(arch, features, process_names, loss_name,
        process_group_ids=process_group_ids, class_weights=class_weights, batch_norm=batch_norm,
        dropout_rate=dropout_rate, l2_norm=l2_norm, seed=seed)
    model.summary()

    # create a pure-graph version of the model for better performance and for saving
    @tf.function(input_signature=[
        tf.TensorSpec(shape=(None, len(feature_spec)), dtype=tf.float32),
        tf.TensorSpec(shape=(), dtype=tf.bool),
    ])
    def model_graph(features, training=tf.constant(False, dtype=tf.bool)):
        return model(features, training=training)

    # training optimizer
    learning_rate_t = tf.Variable(learning_rate, dtype=tf.float32, trainable=False)
    optimizer = tf.keras.optimizers.Adam(learning_rate_t * 100.)

    # initialize the example weight rescaler which keeps moving statistics
    # to adjust per-example, per-class weights
    example_weight_scaler = ExampleWeightScaler(n_classes=n_processes)

    # save the model initially
    model.save(model_path)

    # keep track of some metrics of the best model according to validation
    best_model_metrics = OrderedDict()

    # start the training loop
    stopper = EarlyStopper(stopper_patience, mode="max", digits=4)
    n_decays = 0
    for step, objects in enumerate(dataset_train):
        if step >= max_steps:
            log("{} steps reached, stopping training".format(max_steps))
            break

        # unpack dataset objects
        if dataset_train.n_objects == 3:
            features, labels, observer_features = objects
        else:
            features, labels = objects
            observer_features = None

        if callable(step_fn):
            step_fn(step)

        # add the graph structure to tensorboard
        if step == 0:
            tb_train_add("trace_on", graph=True)
            model_graph(features, training=False)
            tb_train_add("trace_export", "graph", step=step)
            tb_train_add("trace_off")

        # extract and rescale event weights
        event_weights = None
        if event_weight_feature:
            event_weights = observer_features[:, event_weight_feature_index]
            event_weights = example_weight_scaler(event_weights, labels, training=True)

        # use observer features to update the event weights
        if event_weight_fn:
            _event_weights = event_weight_fn[1](labels,
                tf.gather(observer_features, observer_feature_indices, axis=-1))
            if event_weights is None:
                event_weights = _event_weights
            else:
                event_weights *= _event_weights

        # do a train step
        with tf.GradientTape() as tape:
            predictions = model_graph(features, training=True)
            losses = OrderedDict(
                (key, fn(labels, predictions, example_weights=event_weights))
                for key, fn in loss_funcs_t.items()
            )
            loss = sum(losses.values())
        gradients = tape.gradient(loss, model.trainable_variables)
        optimizer.apply_gradients(zip(gradients, model.trainable_variables))

        # continue when not logging or validating
        do_log = step % log_every == 0
        do_validate = step % validate_every == 0
        if not do_log and not do_validate:
            continue

        # logging
        if do_log:
            log_performance(process_names, colored("train", "green"), step, labels, predictions,
                losses, acc_weights=acc_weights, process_group_ids=process_group_ids, log_fn=log_fn)

            # monitor stuff
            accs = calculate_accuracies_t(labels, predictions)
            acc_mean = tf.reduce_mean(accs * acc_weights)
            tb_train_add("scalar", "optimizer/learning_rate", learning_rate_t, step=step)
            tb_train_add("scalar", "loss/total", tf.reduce_mean(loss), step=step)
            for key, l in losses.items():
                tb_train_add("scalar", "loss/" + key, tf.reduce_mean(l), step=step)
            tb_train_add("scalar", "accuracy/mean", acc_mean, step=step)
            for p, acc in zip(process_names, accs):
                tb_train_add("scalar", "accuracy/" + p, acc, step=step)
            for v in model.trainable_variables:
                tb_train_add("histogram", "weight/{}".format(v.name), v, step=step)
            for v, g in zip(model.trainable_variables, gradients):
                tb_train_add("histogram", "gradient/{}".format(v.name), g, step=step)
            tb_train_flush()

        # validation
        if do_validate:
            labels, predictions, losses = evaluate_model(model_graph, dataset_valid, loss_funcs_t,
                event_weight_feature_index=event_weight_feature_index,
                observer_feature_indices=observer_feature_indices, event_weight_fn=event_weight_fn,
                example_weight_scaler=example_weight_scaler)
            loss = tf.reduce_mean(sum(losses.values()))
            accs = calculate_accuracies_t(labels, predictions)
            acc_mean = tf.reduce_mean(accs * acc_weights)

            # check if we need to stop early
            stop_custom = False
            if callable(stop_fn):
                stop_custom = stop_fn(step=step, accs=accs, acc_mean=acc_mean)
            stop_early = stopper(acc_mean)

            # check if we should save the model
            is_best = stopper.newest_is_best()
            do_save = is_best and stopper.is_full()
            if do_save:
                model.save(model_path)

            # log the performance
            postfix = []
            if do_save:
                postfix.append(colored("saved", "magenta"))
            log_ret = log_performance(process_names, "VALID", step, labels, predictions, losses,
                acc_weights=acc_weights, process_group_ids=process_group_ids, color="red",
                postfix=postfix, log_fn=log_fn)

            # store best metrics
            if is_best:
                best_model_metrics["acc_mean"] = round(float(acc_mean), 4)
                if log_ret[2] is not None:
                    group_accs = log_ret[2]
                    for i, m in enumerate(group_accs):
                        best_model_metrics["acc_group" + str(i)] = round(float(m), 3)
                for i, m in enumerate(accs):
                    best_model_metrics["acc_" + process_names[i]] = round(float(m), 3)
                if "l2" in losses:
                    best_model_metrics["l2"] = round(float(losses["l2"]), 5)
                best_model_metrics["step"] = step

            # monitor stuff
            add_funcs, flush_funcs = [tb_valid_add], [tb_valid_flush]
            if is_best:
                add_funcs.append(tb_validb_add)
                flush_funcs.append(tb_validb_flush)
            for add, flush in zip(add_funcs, flush_funcs):
                add("scalar", "loss/total", loss, step=step)
                for key, l in losses.items():
                    add("scalar", "loss/" + key, tf.reduce_mean(l), step=step)
                add("scalar", "accuracy/mean", acc_mean, step=step)
                for p, acc in zip(process_names, accs):
                    add("scalar", "accuracy/" + p, acc, step=step)
                flush()

            # try to decay the learning rate when decided to stop early
            if not stop_custom and stop_early and n_decays < learning_rate_decay[0]:
                log("no improvement in last {} steps, decay learning rate by {}".format(
                    stopper_patience * validate_every, learning_rate_decay[1]))
                learning_rate_t.assign(learning_rate_t * learning_rate_decay[1])
                n_decays += 1
                stopper.clear(preserve_best=True)
                stop_early = False

            if stop_custom:
                log("custom stopping function triggered early stopping")
                break
            elif stop_early:
                log("no improvement in last {} steps, stopping training".format(
                    stopper_patience * validate_every))
                break

    else:
        log("dataset exhausted, stopping training")

    # flush all tensorboard handles
    tb_train_flush()
    tb_valid_flush()
    tb_validb_flush()

    # store best model metrics
    if model_metrics_path:
        with open(model_metrics_path, "w") as f:
            json.dump({
                "keys": best_model_metrics.keys(),
                "metrics": best_model_metrics,
            }, f, indent=4)

    # print metrics of the best model
    log("\nbest model metrics:")
    max_key_len = max(len(key) for key in best_model_metrics)
    for key, metric in best_model_metrics.items():
        log("{}{}: {}".format(key, " " * (max_key_len - len(key)), metric))

    # print a tabbed version for easy copying into tables
    log("\ntabbed representation:")
    log("\t".join(str(m) for m in best_model_metrics.values()))


def evaluate_training(data_spec, features, batch_size=512, acc_weights=1., process_group_ids=None,
        n_threads=4, table_format="grid", model_path="model.h5", stats_path="stats.json",
        log_fn=None):
    # log helper
    def log(msg):
        if callable(log_fn):
            log_fn(msg)
        else:
            print(msg)

    # get the processes, the order is the same as in the input data
    # and should be fixed via an OrderedDict
    process_names = list(data_spec.keys())

    # get the feature spec
    feature_spec = OrderedDict(
        (feature.name, tf.io.FixedLenFeature(shape=1, dtype=tf.float32))
        for feature in features
    )

    # load the datasets
    dataset = MultiDataset.from_specs(data_spec, feature_spec, shuffle=False, repeat=1,
        batch_size=batch_size, mixed=False, n_threads=n_threads)

    # load the model(s)
    model_paths = [model_path] if isinstance(model_path, six.string_types) else model_path
    models = [tf.keras.models.load_model(path, {"LBNLayer": LBNLayer}) for path in model_paths]
    if len(models) == 1:
        model = models[0]
    else:
        model = combine_models(models, mode="mean")
    models[0].summary()

    # evaluate the model
    labels, predictions, _ = evaluate_model(model, dataset, {})

    # keep track of some stats
    stats = OrderedDict()
    stats["process_names"] = process_names
    print("Process names: {}".format(",".join(process_names)))

    #  store classification counts
    m = tf.cast(calculate_confusion_matrix_t(labels, predictions, norm=False), tf.float32)
    stats["classifications"] = {p: npy(row).tolist() for p, row in zip(process_names, m)}

    # store and print the confusion matrix
    m_normed = m / tf.reduce_sum(m, axis=1, keepdims=True)
    stats["confusion_matrix"] = {p: npy(row).tolist() for p, row in zip(process_names, m_normed)}
    headers = ["true / pred."] + process_names
    content = [[p] + npy(row).tolist() for i, (p, row) in enumerate(zip(process_names, m_normed))]
    table = tabulate.tabulate(content, headers=headers, floatfmt="06.4f", tablefmt=table_format)
    log("Confusion matrix (accuracies on diagonal):")
    log(table)

    # accuracies
    accs = npy(tf.linalg.tensor_diag_part(m / tf.reduce_sum(m, axis=1, keepdims=True)))
    acc_mean = float(tf.reduce_mean(accs * acc_weights))
    stats["accuracies"] = dict(zip(process_names, accs.tolist()))
    stats["mean_accuracy"] = acc_mean
    stats["accuracy_weights"] = acc_weights
    log("Mean accuracy: {:.4f}".format(acc_mean))

    # grouped accuracies
    if process_group_ids:
        m_grouped_v = tf.concat([
            tf.reduce_sum(tf.gather(tf.cast(m, tf.float32), ids, axis=1), axis=1, keepdims=True)
            for _, ids in process_group_ids
        ], axis=1)
        m_grouped = tf.concat([
            tf.reduce_sum(tf.gather(m_grouped_v, ids, axis=0), axis=0, keepdims=True)
            for _, ids in process_group_ids
        ], axis=0)
        m_grouped_normed = m_grouped / tf.reduce_sum(m_grouped, axis=1, keepdims=True)
        accs_grouped = tf.linalg.tensor_diag_part(m_grouped_normed)
        stats["group_accuracies"] = npy(accs_grouped).tolist()
        log("Group accuracies: " + ", ".join(
            "group{} {:.4f}".format(*tpl) for tpl in enumerate(accs_grouped)))

    # roc auc scores
    aucs, fprs, tprs = calculate_roc_curves(labels, predictions, smoothen=400, keep_ends=True)
    stats["roc_aucs"] = dict(zip(process_names, aucs))
    stats["roc_fprs"] = {p: fpr.tolist() for p, fpr in zip(process_names, fprs)}
    stats["roc_tprs"] = {p: tpr.tolist() for p, tpr in zip(process_names, tprs)}
    headers = ["Process", "ROC AUC"]
    content = [list(tpl) for tpl in zip(process_names, aucs)]
    table = tabulate.tabulate(content, headers=headers, floatfmt="06.4f", tablefmt=table_format)
    log("ROC AUC scores (process vs. rest):")
    log(table)

    # save the stats
    with open(os.path.expandvars(os.path.expanduser(stats_path)), "w") as f:
        json.dump(stats, f, indent=4)
    log("stats saved at {}".format(stats_path))


def rank_feature(data_spec, features, feature_to_test, batch_size=512, acc_weights=1.,
        n_threads=4, model_path="model.h5", scores_path="scores.json", log_fn=None):
    # log helper
    def log(msg):
        if callable(log_fn):
            log_fn(msg)
        else:
            print(msg)

    # get the processes, the order is the same as in the input data
    # and should be fixed via an OrderedDict
    process_names = list(data_spec.keys())

    # get the feature spec
    feature_spec = OrderedDict(
        (feature.name, tf.io.FixedLenFeature(shape=1, dtype=tf.float32))
        for feature in features
    )

    # load the model(s)
    model_paths = [model_path] if isinstance(model_path, six.string_types) else model_path
    models = [tf.keras.models.load_model(path, {"LBNLayer": LBNLayer}) for path in model_paths]
    if len(models) == 1:
        model = models[0]
    else:
        model = combine_models(models, mode="mean")
    models[0].summary()

    # helper to get accuracies per process and mean
    def get_accuracies(shuffle_feature_index=-1):
        dataset = MultiDataset.from_specs(data_spec, feature_spec, shuffle=False, repeat=1,
            batch_size=batch_size, mixed=False, n_threads=n_threads)
        labels, predictions, _ = evaluate_model(model, dataset, {}, shuffle_feature_index)
        accs = calculate_accuracies_t(labels, predictions)
        acc_mean = tf.reduce_mean(accs * acc_weights)
        return accs, acc_mean

    # get unchanged accuracies
    log("calculating unchanged accuracies")
    accs, acc_mean = get_accuracies()

    # get accuracies with the shuffled feature
    shuffle_feature_index = features.index(feature_to_test)
    log("calculating accuracies with shuffled feature {}, shuffle index {}".format(
        feature_to_test.name, shuffle_feature_index))
    accs_shuffled, acc_mean_shuffled = get_accuracies(shuffle_feature_index)

    # create the output structure
    scores = {
        "scores": {
            name: float((acc - acc_shuffled) / acc)
            for name, acc, acc_shuffled in zip(process_names, accs, accs_shuffled)
        },
        "mean_score": float((acc_mean - acc_mean_shuffled) / acc_mean),
    }

    # save it
    with open(os.path.expandvars(os.path.expanduser(scores_path)), "w") as f:
        json.dump(scores, f, indent=4)


def evaluate_model(model, dataset, loss_funcs_t, event_weight_feature_index=-1,
        observer_feature_indices=None, event_weight_fn=None, example_weight_scaler=None,
        shuffle_feature_index=-1):
    labels_list = []
    predictions_list = []
    loss_lists = OrderedDict((key, []) for key in loss_funcs_t)

    for objects in dataset:
        if dataset.n_objects == 3:
            features, labels, observer_features = objects
        else:
            features, labels = objects
            observer_features = None

        # extract and rescale event weights
        event_weights = None
        if event_weight_feature_index >= 0 and example_weight_scaler:
            event_weights = observer_features[:, event_weight_feature_index]
            event_weights = example_weight_scaler(event_weights, labels, training=False)

        # use observer features to update the event weights
        if observer_feature_indices and event_weight_fn:
            _event_weights = event_weight_fn[1](labels,
                tf.gather(observer_features, observer_feature_indices, axis=-1))
            if event_weights is None:
                event_weights = _event_weights
            else:
                event_weights *= _event_weights

        # maybe shuffle features across batch dimension
        if shuffle_feature_index >= 0:
            features = shuffle_feature_t(features, shuffle_feature_index)

        # get predictions
        predictions = model(features, training=False)

        # store labels and predictions for later analysis
        labels_list.append(labels)
        predictions_list.append(predictions)

        # evaluate and store particular losses
        for key, fn in loss_funcs_t.items():
            loss_lists[key].append(fn(labels, predictions, example_weights=event_weights))

    # combine objects
    labels = tf.concat(labels_list, axis=0)
    predictions = tf.concat(predictions_list, axis=0)
    losses = OrderedDict(
        (key, tf.concat(l, axis=0) if l[0].ndim > 0 else l[0])
        for key, l in loss_lists.items()
    )

    return labels, predictions, losses


# helper to print stats during training
def log_performance(process_names, step_name, step, labels, predictions, losses, acc_weights=1.,
        process_group_ids=None, postfix=None, color=None, log_fn=None):
    # loss string
    loss_str = ", ".join(
        "loss_{}: {:07.5f}".format(name, np.mean(loss))
        for name, loss in losses.items()
    )

    # accuracy string
    if len(process_names) > 2:
        m = tf.cast(calculate_confusion_matrix_t(labels, predictions, norm=False), tf.float32)
        m_normed = m / tf.reduce_sum(m, axis=1, keepdims=True)
        accs = tf.linalg.tensor_diag_part(m_normed)
        acc_mean = tf.reduce_mean(accs * acc_weights)
        accs_str = "(%s)" % " ".join("{}:{:05.3f}".format(*tpl) for tpl in zip(process_names, accs))
    else:
        accs = calculate_accuracies_t(labels, predictions)
        acc_mean = tf.reduce_mean(accs)
        accs_str = ""

    # group accuracies
    accs_grouped = None
    if process_group_ids and len(process_names) > 2:
        m_grouped_v = tf.concat([
            tf.reduce_sum(tf.gather(m, ids, axis=1), axis=1, keepdims=True)
            for _, ids in process_group_ids
        ], axis=1)
        m_grouped = tf.concat([
            tf.reduce_sum(tf.gather(m_grouped_v, ids, axis=0), axis=0, keepdims=True)
            for _, ids in process_group_ids
        ], axis=0)
        m_grouped_normed = m_grouped / tf.reduce_sum(m_grouped, axis=1, keepdims=True)
        accs_grouped = tf.linalg.tensor_diag_part(m_grouped_normed)
        accs_str += " " + " ".join("g{}:{:05.3f}".format(*tpl) for tpl in enumerate(accs_grouped))

    # create the message
    msg = "{}: step {:05d}, {}, acc: {:07.5f} {}".format(
        step_name, step, loss_str, acc_mean, accs_str)
    msg = colored(msg, color)

    # add the postfix
    if postfix:
        msg += ", " + ", ".join(postfix if isinstance(postfix, (list, tuple)) else [postfix])

    # log it
    if callable(log_fn):
        log_fn(msg)
    else:
        print(msg)

    return acc_mean, accs, accs_grouped
