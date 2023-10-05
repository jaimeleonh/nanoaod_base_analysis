# coding: utf-8

"""
Model definitions.
"""

__all__ = ["create_model"]


import math
from collections import OrderedDict

import numpy as np
import tensorflow as tf
import tabulate
import lbn

from cmt.training.losses import (
    cross_entropy_t, symmetric_cross_entropy_t, grouped_cross_entropy_t,
    symmetric_grouped_cross_entropy_t,
)


epsilon = 1e-5

# typical inits and dropout configs per activation
activation_settings = {
    "elu": ("ELU", "he_uniform", "Dropout"),
    "relu": ("ReLU", "he_uniform", "Dropout"),
    "prelu": ("PReLU", "he_normal", "Dropout"),
    "selu": ("selu", "lecun_normal", "AlphaDropout"),
    "tanh": ("tanh", "glorot_normal", "Dropout"),
    "softmax": ("softmax", "glorot_normal", "Dropout"),
}


# helper to extract configs from an architecture string (e.g. "dense:256_128:elu")
def parse_architecture(arch):
    # split the string
    parts = arch.split(":")

    # define a helper to extract certain parts with defaults
    _no_value = object()
    def part(i, default=_no_value):
        if len(parts) <= i:
            if default == _no_value:
                raise ValueError("cannot to access mandatory architecture part {}".format(i))
            return default
        return parts[i]

    return parts, part


# model factory
def create_model(arch, features, process_names, loss_name, process_group_ids=None,
        class_weights=None, batch_norm=False, dropout_rate=0., l2_norm=0., seed=None):
    # set the seed when set
    if seed:
        np.random.seed(seed)
        tf.random.set_seed(seed)

    # store weights to be regularized
    reg_weights = []

    # create the model
    model = tf.keras.Sequential(name="cmt")

    # input layer for properly defining input name, shape and dtypes
    model.add(tf.keras.layers.InputLayer(input_shape=(len(features),), dtype=tf.float32,
        name="input"))

    # parse the architecture string
    _, part = parse_architecture(arch)

    # create the actual model
    if part(0) == "dense":
        # format: "dense:<units0_units1_...>:<activation>"
        units = [int(s) for s in part(1).split("_")]
        act = part(2)

        # get activation specific settings
        act_name, init_name, dropout_name = activation_settings[act]

        # initial batch normalization to act as an input feature scaling
        model.add(tf.keras.layers.BatchNormalization(axis=1, renorm=True, name="fs"))

        # add dense layers
        for i, n in enumerate(units):
            s = str(i)
            # order: dense layer, batch norm, activation, dropout
            model.add(tf.keras.layers.Dense(n, activation=None, use_bias=True,
                kernel_initializer=init_name, bias_initializer="zeros", name="dense" + s))
            reg_weights.append(model.layers[-1].kernel)

            if batch_norm:
                model.add(tf.keras.layers.BatchNormalization(axis=1, renorm=True, name="norm" + s))

            layer_fn = getattr(tf.keras.layers, act_name, None)
            if layer_fn:
                layer = layer_fn(name="act" + s)
            else:
                layer = tf.keras.layers.Activation(activation=act_name, name="act" + s)
            model.add(layer)

            if dropout_rate > 0:
                dropout_fn = getattr(tf.keras.layers, dropout_name)
                model.add(dropout_fn(dropout_rate, name="drop" + s, seed=seed or None))

    elif part(0) == "lbn_dense":
        # format: "lbn_dense:<lbn_particles>:<lbn_feature_set>:<units0_units1_...>:<activation>"
        lbn_particles = int(part(1))
        lbn_feature_set = part(2)
        units = [int(s) for s in part(3).split("_")]
        act = part(4)

        # determine the lbn feature names
        lbn_features = {
            "default": ["E", "pt", "eta", "phi", "m", "pair_cos"],
            "extended": ["E", "pt", "eta", "phi", "m", "gamma", "pair_cos", "pair_dr"],
            "single": ["E", "pt", "eta", "phi", "m", "gamma"],
        }[lbn_feature_set]

        # get activation specific settings
        act_name, init_name, dropout_name = activation_settings[act]

        # prepare preprocessing
        all_feature_names = [feature.name for feature in features]
        all_tags = set(sum((list(feature.tags) for feature in features), []))
        features_ext = [
            feature for feature in features
            if not feature.has_tag(["lbn_pt", "lbn_eta", "lbn_phi", "lbn_e", "lbn_aux*"])
        ]
        features_vec = []
        n_aux = len([tag for tag in all_tags if tag.startswith("lbn_aux")])
        particle_names = []
        for feature in features:
            # the order is pt, eta, phi, e, auxN
            # and pt, phi for met
            if feature.has_tag("lbn_pt"):
                particle_names.append(feature.name.rsplit("_", 1)[0])
                if feature.has_tag("lbn_met"):
                    features_vec.append([feature, 0, None, feature] + [0] * n_aux)
                else:
                    features_vec.append([feature, None, None, None] + [0] * n_aux)
            elif feature.has_tag("lbn_eta"):
                features_vec[-1][1] = feature
            elif feature.has_tag("lbn_phi"):
                features_vec[-1][2] = feature
            elif feature.has_tag("lbn_e") or feature.has_tag("lbn_m"): # FIXME
                features_vec[-1][3] = feature
            elif feature.has_tag("lbn_aux*"):
                for i in range(n_aux):
                    if feature.has_tag("lbn_aux" + str(i)):
                        features_vec[-1][4 + i] = feature
        assert(None not in sum(features_vec, []))
        indices_ext = [
            all_feature_names.index(feature.name)
            for feature in features_ext
        ]
        indices_vec = [
            [(all_feature_names.index(feature.name) if feature != 0 else -1) for feature in vec]
            for vec in features_vec
        ]
        print_lbn_features(features_vec, features_ext)

        # LBN
        lbn_layer = LBNLayer(indices_vec, indices_ext, n_particles=lbn_particles,
            boost_mode="pairs", features=lbn_features)
        model.add(lbn_layer)

        # batch normalization after lbn for feature scaling
        model.add(tf.keras.layers.BatchNormalization(axis=1, renorm=True, name="fs"))

        # add dense layers
        for i, n in enumerate(units):
            s = str(i)
            # order: dense layer, batch norm, activation, dropout
            model.add(tf.keras.layers.Dense(n, activation=None, use_bias=True,
                kernel_initializer=init_name, bias_initializer="zeros", name="dense" + s))
            reg_weights.append(model.layers[-1].kernel)

            if batch_norm:
                model.add(tf.keras.layers.BatchNormalization(axis=1, renorm=True, name="norm" + s))

            layer_fn = getattr(tf.keras.layers, act_name, None)
            if layer_fn:
                layer = layer_fn(name="act" + s)
            else:
                layer = tf.keras.layers.Activation(activation=act_name, name="act" + s)
            model.add(layer)

            if dropout_rate > 0:
                dropout_fn = getattr(tf.keras.layers, dropout_name)
                model.add(dropout_fn(dropout_rate, name="drop" + s, seed=seed or None))

    else:
        raise Exception("cannot parse architecture string '{}'".format(arch))

    # output layer
    if len(process_names) > 2:
        model.add(tf.keras.layers.Dense(len(process_names), activation="softmax", use_bias=True,
            kernel_initializer="glorot_uniform", bias_initializer="zeros", name="output"))
    else:
        model.add(tf.keras.layers.Dense(1, activation="sigmoid", use_bias=True,
            kernel_initializer="glorot_uniform", bias_initializer="zeros", name="output"))

    # prepare arguments for loss functions for better tracing
    class_weights_t = None if class_weights is None else tf.constant(class_weights)

    # helper to create a loss function with custom signature and arguments
    # (partial does not work, see https://github.com/tensorflow/tensorflow/issues/26091)
    def make_loss(func, **kwargs):
        def loss_func(labels, predictions, example_weights=None):
            return func(labels, predictions, example_weights=example_weights, **kwargs)
        return loss_func

    # define the losses that should be added in the training step to obtain the total loss
    loss_funcs_t = OrderedDict()
    if loss_name == "ce":
        loss_funcs_t[loss_name] = make_loss(cross_entropy_t)
    elif loss_name == "sce":
        loss_funcs_t[loss_name] = make_loss(symmetric_cross_entropy_t)
    elif loss_name == "wce":
        assert(class_weights_t is not None)
        loss_funcs_t[loss_name] = make_loss(cross_entropy_t, class_weights=class_weights_t)
    elif loss_name == "wsce":
        assert(class_weights_t is not None)
        loss_funcs_t[loss_name] = make_loss(symmetric_cross_entropy_t,
            class_weights=class_weights_t)
    elif loss_name == "fl":
        loss_funcs_t[loss_name] = make_loss(cross_entropy_t, focal_gamma=0.5)
    elif loss_name == "sfl":
        loss_funcs_t[loss_name] = make_loss(symmetric_cross_entropy_t, focal_gamma=0.5)
    elif loss_name == "wfl":
        assert(class_weights_t is not None)
        loss_funcs_t[loss_name] = make_loss(cross_entropy_t, focal_gamma=0.5,
            class_weights=class_weights_t)
    elif loss_name == "wsfl":
        assert(class_weights_t is not None)
        loss_funcs_t[loss_name] = make_loss(symmetric_cross_entropy_t, focal_gamma=0.5,
            class_weights=class_weights_t)
    elif loss_name == "gce":
        loss_funcs_t[loss_name] = make_loss(grouped_cross_entropy_t, group_ids=process_group_ids)
    elif loss_name == "wgce":
        assert(class_weights_t is not None)
        loss_funcs_t[loss_name] = make_loss(grouped_cross_entropy_t, group_ids=process_group_ids,
            class_weights=class_weights_t)
    elif loss_name == "gfl":
        loss_funcs_t[loss_name] = make_loss(grouped_cross_entropy_t, group_ids=process_group_ids,
            focal_gamma=0.5)
    elif loss_name == "wgfl":
        assert(class_weights_t is not None)
        loss_funcs_t[loss_name] = make_loss(grouped_cross_entropy_t, group_ids=process_group_ids,
            focal_gamma=0.5, class_weights=class_weights_t)
    elif loss_name == "sgce":
        loss_funcs_t[loss_name] = make_loss(symmetric_grouped_cross_entropy_t,
            group_ids=process_group_ids)
    elif loss_name == "wsgce":
        assert(class_weights_t is not None)
        loss_funcs_t[loss_name] = make_loss(symmetric_grouped_cross_entropy_t,
            group_ids=process_group_ids, class_weights=class_weights_t)
    elif loss_name == "sgfl":
        loss_funcs_t[loss_name] = make_loss(symmetric_grouped_cross_entropy_t,
            group_ids=process_group_ids, focal_gamma=0.5)
    elif loss_name == "wsgfl":
        assert(class_weights_t is not None)
        loss_funcs_t[loss_name] = make_loss(symmetric_grouped_cross_entropy_t,
            group_ids=process_group_ids, focal_gamma=0.5, class_weights=class_weights_t)
    else:
        raise ValueError("unknown loss: {}".format(loss_name))

    # optional l2 loss, using the same signature as what make_loss produces
    if l2_norm != 0:
        @tf.function
        def loss_l2_t(labels, predictions, example_weights=None):
            l2 = tf.add_n([tf.nn.l2_loss(w) for w in reg_weights])
            return l2_norm * l2

        loss_funcs_t["l2"] = loss_l2_t

    return model, loss_funcs_t


class LBNLayer(lbn.LBNLayer):

    def __init__(self, indices_vec, indices_ext, **kwargs):
        # get the number of inputs, i.e., total number of indices except -1
        indices_all = sum(indices_vec + [indices_ext], [])
        self._n_inputs = len(set(indices_all) - {-1})
        input_shape = (self._n_inputs,)

        # store indices
        self._indices_vec = indices_vec
        self._indices_ext = indices_ext

        # store indices with values of -1 changed to the number of inputs
        # for caching purposes of tf.gather calls in call()
        self._indices_vec_gather = [
            [(i if i >= 0 else self._n_inputs) for i in indices]
            for indices in indices_vec
        ]
        self._indices_ext_gather = [(i if i >= 0 else self._n_inputs) for i in indices_ext]

        # store names of features to build
        self._features = kwargs.pop("features", None)

        # extract kwargs that are not passed to the LBN but to the layer init
        layer_kwargs = {
            "input_shape": input_shape,
            "dtype": kwargs.pop("dtype", None),
            "dynamic": kwargs.pop("dynamic", False),
        }
        # for whatever reason, keras calls this contructor again
        # with batch_input_shape set when input_shape was accepted
        if "batch_input_shape" in kwargs:
            layer_kwargs["batch_input_shape"] = kwargs.pop("batch_input_shape")

        # create the LBN instance with the remaining arguments
        self.lbn = lbn.LBN(**kwargs)
        # test
        # self.lbn.feature_factory.DISABLE_CACHE = True

        # layer init
        tf.keras.layers.Layer.__init__(self, name=self.lbn.name,
            trainable=self.lbn.trainable, **layer_kwargs)

        # the input_shape is mandatory so we can build right away
        self.build(input_shape)

    def build(self, input_shape):
        # build the lbn
        lbn_input_shape = (len(self._indices_vec), len(self._indices_vec[0]))
        self.lbn.build(lbn_input_shape, features=self._features)

        # store references to the trainable LBN weights
        self.particle_weights = self.lbn.particle_weights
        self.restframe_weights = self.lbn.restframe_weights
        self.aux_weights = self.lbn.aux_weights

        tf.keras.layers.Layer.build(self, input_shape)

    def compute_output_shape(self, input_shape):
        return (input_shape[0], self.lbn.n_features + len(self._indices_ext))

    def get_config(self):
        config = super(LBNLayer, self).get_config()
        del config["input_shape"]
        config.update({
            "indices_vec": self._indices_vec,
            "indices_ext": self._indices_ext,
        })
        return config

    def call(self, inputs):
        # append a zero to inputs which is referred to by indices changed from -1 (see __init__)
        zero = tf.gather(inputs, [0], axis=1) * 0.
        inputs = tf.concat([inputs, zero], axis=1)

        # get lbn inputs and transform pt, eta, phi, E into E, px, py, pz
        lbn_inputs = tf.gather(inputs, self._indices_vec_gather, axis=1)
        lbn_pt = tf.maximum(lbn_inputs[:, :, 0:1], 0.)
        lbn_eta = lbn_inputs[:, :, 1:2]
        lbn_phi = lbn_inputs[:, :, 2:3]
        lbn_e = tf.maximum(lbn_inputs[:, :, 3:4], 0.)
        lbn_aux = lbn_inputs[:, :, 4:]
        lbn_px = lbn_pt * tf.cos(lbn_phi)
        lbn_py = lbn_pt * tf.sin(lbn_phi)
        lbn_pz = lbn_pt * tf.sinh(lbn_eta)
        lbn_inputs = tf.concat([lbn_e, lbn_px, lbn_py, lbn_pz, lbn_aux], axis=-1)
        lbn_features = self.lbn(lbn_inputs)

        # get external features
        ext_features = tf.gather(inputs, self._indices_ext_gather, axis=1)

        # concat and return
        combined_features = tf.concat((lbn_features, ext_features), axis=1)

        return combined_features


def print_lbn_features(features_vec, features_ext):
    # vector features
    headers = ["pt", "eta", "phi", "e"] + ["aux" + str(i) for i in range(len(features_vec[0]) - 4)]
    rows = [
        [feature.name if feature else "0" for feature in features]
        for features in features_vec
    ]
    print("")
    print("LBN vector features:")
    print(tabulate.tabulate(rows, headers=headers, tablefmt="simple", stralign="right"))

    # external features
    rows = [
        [feature.name for feature in features_ext[i * 6:(i + 1) * 6]]
        for i in range(int(math.ceil(len(features_ext) / 6.)))
    ]
    print("")
    print("LBN external features:")
    print(tabulate.tabulate(rows, tablefmt="plain", stralign="right"))
    print("")
