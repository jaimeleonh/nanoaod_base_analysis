# coding: utf-8

"""
Utilities for model training.
"""

__all__ = [
    "MultiDataset", "EarlyStopper", "ExampleWeightScaler", "print_tensorflow_info", "configure_gpu",
    "configure_cpu", "save_graph", "load_graph", "combine_models", "npy",
    "calculate_confusion_matrix_t", "calculate_accuracies_t", "calculate_roc_curves",
    "create_tensorboard_callbacks", "smoothen_array", "shuffle_feature_t",
]


import os
import random
from collections import OrderedDict

import numpy as np
import tensorflow as tf
import sklearn.metrics
from six.moves import zip
import law


class MultiDataset(object):

    @classmethod
    def from_specs(cls, data_spec, feature_spec, observer_spec=None, shuffle=(), repeat=1,
            batch_size=512, mixed=False, seed=None, map_fn=None, filter_fn=None, n_threads=1):
        # data_spec is a dict {<key>: {"shards": strings, "label": int}}
        # TODO: mixing weights could be added as well, which is accounted for in the batch sizes

        # ensure that the labels are contiguous and starting from zero
        labels = set(spec["label"] for spec in data_spec.values())
        if labels != set(range(len(labels))):
            raise Exception("labels not contiguous or not starting from zero: {}".format(labels))

        # determine partial batch sizes per dataset so that their sum is the requested batch size
        n_datasets = len(data_spec)
        if mixed:
            batch_sizes = [len(b) for b in np.array_split(np.arange(batch_size), n_datasets)]
        else:
            batch_sizes = [batch_size] * n_datasets

        # store the number of features
        n_features = len(feature_spec)

        # when observer specs are given, add them to the feature specs (if not already there)
        # so that deserialization is shared and done only once
        if observer_spec:
            feature_spec = feature_spec.copy()
            observer_spec_indices = []
            for name, spec in observer_spec.items():
                if name in feature_spec:
                    # already used as feature, store its position
                    observer_spec_indices.append(list(feature_spec.keys()).index(name))
                else:
                    # add it and store the new position
                    feature_spec[name] = spec
                    observer_spec_indices.append(len(feature_spec) - 1)

        # function that deserializes a tf.Example from a sharded file
        # use a closure to pass the label index to do a one hot encoding
        def parse(key):
            label_index = data_spec[key]["label"]

            @tf.function
            def parse(example):
                # use label_index to do a simple one-hot encoding
                if n_datasets > 2:
                    labels = [0] * label_index + [1] + [0] * (n_datasets - label_index - 1)
                else:
                    labels = [label_index]
                labels = tf.constant(labels, dtype=tf.float32)

                # deserialize all features
                features = tf.io.parse_single_example(example, feature_spec)
                features = tf.concat([features[name] for name in feature_spec], axis=0)

                # when observer specs are given, remove them from features if necessary
                if observer_spec:
                    observer_features = tf.gather(features, observer_spec_indices, axis=-1)
                    features = features[..., :n_features]
                    return features, labels, observer_features
                else:
                    return features, labels

            return parse

        # read data with one TFRecordDataset per input dataset
        # also, prepare dataset shuffling and batching
        datasets = OrderedDict()
        nevents = OrderedDict()
        for (key, spec), batch_size in zip(data_spec.items(), batch_sizes):
            # shuffle shard files already
            shards = list(spec["shards"])
            if shuffle:
                random.seed(seed)
                random.shuffle(shards)

            # create the dataset
            dataset = tf.data.TFRecordDataset(shards, num_parallel_reads=n_threads)
            dataset = dataset.map(parse(key), num_parallel_calls=n_threads)

            nevents[key] = len([elem for elem in dataset])

            # custom mapping and filtering
            if callable(map_fn):
                dataset = dataset.map(map_fn, num_parallel_calls=n_threads)
            if callable(filter_fn):
                dataset = dataset.filter(filter_fn)

            # shuffling
            if shuffle:
                bs_buff, max_buff = shuffle
                buff = min(bs_buff * batch_size, max_buff) if batch_size > 0 else max_buff
                dataset = dataset.shuffle(buffer_size=buff, seed=seed or None,
                    reshuffle_each_iteration=True)

            # repeat
            if repeat != 1:
                dataset = dataset.repeat(repeat)

            # batching
            if batch_size > 0:
                dataset = dataset.batch(batch_size)

            datasets[key] = dataset

        return cls(datasets, mixed=mixed, n_objects=3 if observer_spec else 2), nevents

    def __init__(self, datasets, mixed=True, n_objects=2):
        super(MultiDataset, self).__init__()

        self._datasets = datasets
        self._mixed = mixed
        self._n_objects = n_objects
        self._batches_seen = 0

    @property
    def datasets(self):
        return self._datasets

    @property
    def mixed(self):
        return self._mixed

    @property
    def n_objects(self):
        return self._n_objects

    @property
    def batches_seen(self):
        return self._batches_seen

    def __iter__(self):
        self._batches_seen = 0

        # when mixing datasets, their batch sizes are adjusted by create_datasets
        # so that the concatenated batch has the requested size
        if self._mixed:
            its = [iter(dataset) for dataset in self._datasets.values()]
            while True:
                batches = []
                do_continue = False
                do_break = False
                for name, it in zip(self._datasets, its):
                    try:
                        batches.append(next(it))
                    except tf.errors.DataLossError as e:
                        print("\nDataLossError in dataset {}:\n{}\n".format(name, e))
                        do_continue = True
                        break
                    except StopIteration:
                        do_break = True
                        break

                if do_continue:
                    continue
                if do_break:
                    break

                if self._n_objects <= 0:
                    yield batches
                else:
                    yield tuple(
                        tf.concat([batch[i] for batch in batches], axis=0)
                        for i in range(self._n_objects)
                    )

                self._batches_seen += 1
        else:
            # when not mixing, just yield the features and labels sequentially for each dataset
            for name, dataset in self._datasets.items():
                it = iter(dataset)
                while True:
                    try:
                        objects = next(it)
                    except tf.errors.DataLossError as e:
                        print("\nDataLossError in dataset {}:\n{}\n".format(name, e))
                        continue
                    except StopIteration:
                        break

                    yield objects
                    self._batches_seen += 1

    def map(self, *args, **kwargs):
        for key, dataset in list(self._datasets.items()):
            self._datasets[key] = dataset.map(*args, **kwargs)

    def repeat(self, *args, **kwargs):
        for key, dataset in list(self._datasets.items()):
            self._datasets[key] = dataset.repeat(*args, **kwargs)

    def shuffle(self, *args, **kwargs):
        for key, dataset in list(self._datasets.items()):
            self._datasets[key] = dataset.shuffle(*args, **kwargs)

    def batch(self, *args, **kwargs):
        for key, dataset in list(self._datasets.items()):
            self._datasets[key] = dataset.batch(*args, **kwargs)


class EarlyStopper(object):

    modes = MIN, MAX = "min", "max"

    def __init__(self, max_size, offset=0, mode=MAX, threshold=0., digits=0):
        super(EarlyStopper, self).__init__()

        # check mode
        if mode not in self.modes:
            raise ValueError("unknown mode '{}', must be one of {}".format(mode, self.modes))

        self.max_size = max_size
        self.offset = offset
        self.mode = mode
        self.threshold = threshold
        self.digits = digits

        self.counter = 0
        self.history = []

    def __call__(self, *args, **kwargs):
        return self.check(*args, **kwargs)

    def __len__(self):
        return self.get_size()

    def get_size(self):
        return len(self.history)

    def is_full(self):
        return len(self.history) >= self.max_size

    def get_best(self):
        if not self.history:
            return None, None

        if self.mode == self.MAX:
            best_val = max(self.history)
        elif self.mode == self.MIN:
            best_val = min(self.history)
        else:
            raise NotImplementedError

        best_pos = self.history.index(best_val)

        return best_val, best_pos

    def newest_is_best(self, best_pos=None):
        if best_pos is None:
            _, best_pos = self.get_best()
        return self.history and best_pos == 0

    def oldest_is_best(self, best_pos=None):
        if best_pos is None:
            _, best_pos = self.get_best()
        return self.history and best_pos == (len(self.history) - 1)

    def clear(self, preserve_best=False, preserve_counter=True):
        best_val = None
        if preserve_best:
            best_val, _ = self.get_best()

        del self.history[:]

        if preserve_best and best_val is not None:
            self.history.append(best_val)

        if not preserve_counter:
            self.counter = 0

    def check(self, metric):
        """
        Returns *True* when the metric did not improve more than the configured treshold during the
        recorded history. The history stores newest values first.
        """
        # increment the counter
        self.counter += 1

        # when below the offset, do nothing
        if self.counter <= self.offset:
            return False

        # prepare the input metric
        metric = float(metric)
        if self.digits:
            metric = round(metric, self.digits)

        # perform the stop check only when the history is full
        if self.is_full():
            # get the best value and its position
            best_val, best_pos = self.get_best()

            # check if the new value is worse
            if self.mode == self.MAX:
                worse_than_best = (metric - self.threshold) < best_val
            elif self.mode == self.MIN:
                worse_than_best = (metric + self.threshold) > best_val
            else:
                raise NotImplementedError

            # return the stop signal, when the current value is worse than the best
            # and the best value is at the end of the history
            if worse_than_best and self.oldest_is_best(best_pos=best_pos):
                return True

            # we are not stopping, trim the history so that the new value will fit
            del self.history[self.max_size - 1:]

        # prepend the new metric
        self.history.insert(0, metric)

        return False


class ExampleWeightScaler(object):
    """
    Scales example such that they are centered around *mean* with a standard deviation of *stddev*.
    The computation of shifting and scaling parameters is adaptive and converges over time. When
    *n_classes* is a positive number, this is done separately per class as example weights might
    differ greatly between the classes which should maintain statistical independence. Class weights
    must be incorporated with a different approach.

    Note that, depending on the values of *mean* and *stddev*, event weights might become negative,
    e.g. on the left side of the scaled distribution. However, this might be acceptable when working
    with batches and summing or averaging over example losses per batch to do the backpropagation.
    """

    # algorithm types
    ROLLING = "rolling"

    def __init__(self, n_classes, mean=1., stddev=0.5, algo=ROLLING):
        super(ExampleWeightScaler, self).__init__()

        self.n_classes = int(n_classes)
        self.mean = float(mean)
        self.stddev = float(stddev)
        self.algo = algo

        # book objects to bookkeep seen examples, means and variances
        if self.per_class:
            self._n = self.n_classes * [0.]
            self._m = self.n_classes * [0.]
            self._v = self.n_classes * [0.]
        else:
            self._n = 0.
            self._m = 0.
            self._v = 0.

    @property
    def per_class(self):
        return self.n_classes > 0

    def _rolling_update(self, weights, n, m, v):
        new_n = n + weights.shape[0]
        new_m = (m * n + np.sum(weights)) / new_n
        new_v = (v * n + np.sum((weights - new_m)**2.)) / new_n
        return new_n, new_m, new_v

    def _scale(self, weights, m, v):
        return ((weights - m) / v**0.5) * self.stddev + self.mean

    def __call__(self, example_weights, labels=None, training=False):
        example_weights = npy(example_weights)

        if self.per_class:
            if labels is None:
                raise ValueError("labels must be set when scaling per class")
            labels = npy(labels)

            scaled_weights = np.ones_like(example_weights)

            flat_labels = np.argmax(labels, axis=1)
            for i in range(self.n_classes):
                mask = flat_labels == i
                if not mask.sum():
                    continue
                weights = example_weights[mask]

                # update parameters during training
                if training:
                    self._n[i], self._m[i], self._v[i] = self._rolling_update(weights, self._n[i],
                        self._m[i], self._v[i])

                # perform scaling per class
                scaled_weights[mask] = self._scale(weights, self._m[i], self._v[i])
        else:
            # update parameters during training
            if training:
                self._n, self._m, self._v = self._rolling_update(example_weights, self._n, self._m,
                    self._v)

            # perform scaling
            scaled_weights = self._scale(example_weights, self._m, self._v)

        return tf.constant(scaled_weights)


def print_tensorflow_info():
    print("TensorFlow  : {} ({})".format(tf.__version__, os.path.dirname(tf.__file__)))
    print("Cuda support: {}".format("yes" if tf.test.is_built_with_cuda() else "no"))
    print("GPUs found  : {}".format(len(tf.config.list_physical_devices("GPU"))))


def configure_gpu(gpus=None, growth=False, max_memory=None):
    for gpu in tf.config.experimental.list_physical_devices("GPU"):
        index = int(gpu.name.rsplit(":", 1)[-1])
        if gpus and index not in gpus and str(index) not in gpus:
            continue

        if growth:
            tf.config.experimental.set_memory_growth(gpu, True)
            print("{}: enable memory growth".format(gpu.name))
        elif max_memory:
            tf.config.experimental.set_virtual_device_configuration(gpu,
                [tf.config.experimental.VirtualDeviceConfiguration(memory_limit=max_memory)])
            print("{}: limit memory to {} MB".format(gpu.name, max_memory))


def configure_cpu(n_cores=1):
    if n_cores:
        tf.config.threading.set_intra_op_parallelism_threads(n_cores)
        tf.config.threading.set_inter_op_parallelism_threads(n_cores)


def save_graph(path, concrete_fn, *args, **kwargs):
    return law.tensorflow.TFGraphFormatter.dump(path, concrete_fn, *args,
        **kwargs)


def load_graph(path, create_session=None):
    return law.tensorflow.TFGraphFormatter.load(path, create_session=create_session)


def combine_models(models, mode="mean", name=None, input_name="input",
        output_name="output"):
    import tensorflow as tf

    # first, rename all models
    for i, model in enumerate(models):
        model._name += "{}_combined{}".format(model.name, i)

    # input tensor
    x = tf.keras.Input(shape=models[0].input_shape[1:], name=input_name)

    # process all outputs
    outputs = [model(x) for model in models]

    # combination layer
    if mode == "mean":
        y = tf.keras.layers.Average(name=output_name)(outputs)
    else:
        raise ValueError("unknown combination mode: {}".format(mode))

    # define and return the model
    return tf.keras.Model(inputs=x, outputs=y, name=name)


def npy(t):
    return t.numpy() if callable(getattr(t, "numpy", None)) else t


def calculate_confusion_matrix_t(labels, predictions, norm=True):
    m = tf.math.confusion_matrix(tf.argmax(labels, axis=1), tf.argmax(predictions, axis=1))

    # row-normalize
    if norm:
        m = tf.cast(m, tf.float32)
        sums = tf.reduce_sum(m, axis=1, keepdims=True)
        m /= sums

    return m


def calculate_accuracies_t(labels, predictions):
    if labels.shape[1] == 1:
        den = labels.shape[0]
        # num = tf.math.multiply(tf.math.abs(tf.math.subtract(labels, predictions)), 2)
        num = tf.math.abs(tf.math.subtract(labels, predictions))
        # num = tf.cast(num, tf.int32)
        num = tf.reduce_sum(num)
        return tf.constant([float(den - num) / den])
    # accuracies are normalized diagonal entries of the confusion matrix
    else:
        return tf.linalg.tensor_diag_part(calculate_confusion_matrix_t(labels, predictions))


def calculate_roc_curves(labels, predictions, smoothen=None, keep_ends=True):
    aucs, fprs, tprs = [], [], []

    for i in range(predictions.shape[1]):
        auc, fpr, tpr = None, np.zeros(1), np.zeros(1)
        try:
            fpr, tpr, _ = sklearn.metrics.roc_curve(labels[:, i], predictions[:, i])
            auc = sklearn.metrics.auc(fpr, tpr)
            if smoothen:
                fpr = smoothen_array(fpr, smoothen, keep_ends=keep_ends)
                tpr = smoothen_array(tpr, smoothen, keep_ends=keep_ends)
        except ValueError:
            pass
        aucs.append(auc)
        fprs.append(fpr)
        tprs.append(tpr)

    return aucs, fprs, tprs


def create_tensorboard_callbacks(log_dir):
    add = flush = lambda *args, **kwargs: None
    if log_dir:
        writer = tf.summary.create_file_writer(log_dir)
        flush = writer.flush
        def add(attr, *args, **kwargs):
            with writer.as_default():
                getattr(tf.summary, attr)(*args, **kwargs)
    return add, flush


def smoothen_array(a, n_points, keep_ends=False):
    # strategy: pad array so that it can be divided by n, reshape by n and compute the mean on the
    # new axis
    if a.ndim != 1:
        raise Exception("cannot smoothen array with {} dimensions".format(a.ndim))
    if a.shape[0] <= n_points:
        return a

    e = 2 * int(keep_ends)
    rest = (n_points - e) - ((a.shape[0] - e) % (n_points - e))
    lrest = int(round(rest * 0.5))
    rrest = rest - lrest

    inner = slice(1, -1) if keep_ends else slice(None, None)
    a_smooth = np.pad(a[inner], (lrest, rrest), mode="edge")
    a_smooth = a_smooth.reshape((n_points - e, -1)).mean(axis=-1)

    if keep_ends:
        a_ends = np.empty(n_points, dtype=a.dtype)
        a_ends[0] = a[0]
        a_ends[1:-1] = a_smooth
        a_ends[-1] = a[-1]
        a_smooth = a_ends

    return a_smooth


def shuffle_feature_t(features, feature_number):
    import sklearn.utils
    shuffled_feature = sklearn.utils.shuffle(npy(features[:, feature_number]))
    ones = np.ones(tf.shape(features))
    ones[:, feature_number] = np.zeros(tf.shape(features)[0])
    zeros = np.zeros(tf.shape(features))
    zeros[:, feature_number] = shuffled_feature
    shuffled_features = tf.convert_to_tensor(np.multiply(npy(features), ones) + zeros, tf.float32)
    return shuffled_features
