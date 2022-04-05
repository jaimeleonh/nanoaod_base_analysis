# coding: utf-8

"""
Collection of loss definitions.
"""

__all__ = [
    "cross_entropy_t", "symmetric_cross_entropy_t", "grouped_cross_entropy_t",
    "symmetric_grouped_cross_entropy_t",
]


import tensorflow as tf


epsilon = 1e-5


@tf.function
def cross_entropy_t(labels, predictions, focal_gamma=None, class_weights=None,
        example_weights=None):
    # get true-negative component
    predictions = tf.clip_by_value(predictions, epsilon, 1 - epsilon)
    tn = labels * tf.math.log(predictions)

    # focal loss?
    if focal_gamma is not None:
        tn *= (1 - predictions)**focal_gamma

    # convert into loss
    losses = -tn

    # apply class weights
    if class_weights is not None:
        losses *= class_weights

    # apply example weights
    if example_weights is not None:
        losses *= example_weights[:, tf.newaxis]

    # average over batch
    loss = tf.reduce_mean(losses)
    return loss


@tf.function
def symmetric_cross_entropy_t(labels, predictions, focal_gamma=None, class_weights=None,
        example_weights=None):
    # get true-negative and false-positive components
    predictions = tf.clip_by_value(predictions, epsilon, 1 - epsilon)
    tn = labels * tf.math.log(predictions)
    fp = (1 - labels) * tf.math.log(1 - predictions)

    # focal loss?
    if focal_gamma is not None:
        tn *= (1 - predictions)**focal_gamma
        fp *= predictions**focal_gamma

    # combine and balance so that tn and fp are equally important
    n_classes = labels.shape[1]
    losses = -0.5 * (tn + fp / (n_classes - 1))

    # apply class weights
    if class_weights is not None:
        losses *= class_weights

    # apply example weights
    if example_weights is not None:
        losses *= example_weights[:, tf.newaxis]

    # average over batch
    loss = tf.reduce_mean(losses)
    return loss


@tf.function
def grouped_cross_entropy_t(labels, predictions, group_ids=None, focal_gamma=None,
        class_weights=None, example_weights=None):
    assert(group_ids is not None)

    # get true-negative component
    predictions = tf.clip_by_value(predictions, epsilon, 1 - epsilon)
    tn = labels * tf.math.log(predictions)

    # focal loss?
    if focal_gamma is not None:
        tn *= (1 - predictions)**focal_gamma

    # convert into loss
    losses = -tn

    # apply class weights
    if class_weights is not None:
        losses *= class_weights

    # apply example weights
    if example_weights is not None:
        losses *= example_weights[:, tf.newaxis]

    # create grouped labels and predictions
    labels_grouped = tf.concat([
        tf.reduce_sum(tf.gather(labels, ids, axis=-1), axis=-1, keepdims=True)
        for _, ids in group_ids
    ], axis=-1)
    predictions_grouped = tf.concat([
        tf.reduce_sum(tf.gather(predictions, ids, axis=-1), axis=-1, keepdims=True)
        for _, ids in group_ids
    ], axis=-1)
    predictions_grouped = tf.clip_by_value(predictions_grouped, epsilon, 1 - epsilon)

    # grouped true-negative component
    tn_grouped = labels_grouped * tf.math.log(predictions_grouped)

    # focal loss?
    if focal_gamma is not None:
        tn_grouped *= (1 - predictions_grouped)**focal_gamma

    # convert into loss and apply group weights
    group_weights = tf.constant([w for w, _ in group_ids], tf.float32)
    losses_grouped = -tn_grouped * group_weights

    # apply example weights
    if example_weights is not None:
        losses_grouped *= example_weights[:, tf.newaxis]

    # combine losses
    loss = tf.reduce_mean(0.5 * (tf.reduce_sum(losses, axis=-1)
        + tf.reduce_sum(losses_grouped, axis=-1)))

    return loss


@tf.function
def symmetric_grouped_cross_entropy_t(labels, predictions, group_ids=None, focal_gamma=None,
        class_weights=None, example_weights=None):
    assert(group_ids is not None)

    # get true-negative and false-positive components
    predictions = tf.clip_by_value(predictions, epsilon, 1 - epsilon)
    tn = labels * tf.math.log(predictions)
    fp = (1 - labels) * tf.math.log(1 - predictions)

    # focal loss?
    if focal_gamma is not None:
        tn *= (1 - predictions)**focal_gamma
        fp *= predictions**focal_gamma

    # combine and balance so that tn and fp are equally important
    n_classes = labels.shape[1]
    losses = -0.5 * (tn + fp / (n_classes - 1))

    # apply class weights
    if class_weights is not None:
        losses *= class_weights

    # apply example weights
    if example_weights is not None:
        losses *= example_weights[:, tf.newaxis]

    # create grouped labels and predictions
    labels_grouped = tf.concat([
        tf.reduce_sum(tf.gather(labels, ids, axis=-1), axis=-1, keepdims=True)
        for _, ids in group_ids
    ], axis=-1)
    predictions_grouped = tf.concat([
        tf.reduce_sum(tf.gather(predictions, ids, axis=-1), axis=-1, keepdims=True)
        for _, ids in group_ids
    ], axis=-1)
    predictions_grouped = tf.clip_by_value(predictions_grouped, epsilon, 1 - epsilon)

    # grouped true-negative and false-positive components
    tn_grouped = labels_grouped * tf.math.log(predictions_grouped)
    fp_grouped = (1 - labels_grouped) * tf.math.log(1 - predictions_grouped)

    # focal loss?
    if focal_gamma is not None:
        tn_grouped *= (1 - predictions_grouped)**focal_gamma
        fp_grouped *= predictions_grouped**focal_gamma

    # combine and balance so that tn and fp are equally important, apply group weights
    group_weights = tf.constant([w for w, _ in group_ids], tf.float32)
    losses_grouped = -0.5 * (tn_grouped + fp_grouped) * group_weights

    # apply example weights
    if example_weights is not None:
        losses_grouped *= example_weights[:, tf.newaxis]

    # combine losses
    loss = tf.reduce_mean(0.5 * (tf.reduce_sum(losses, axis=-1)
        + tf.reduce_sum(losses_grouped, axis=-1)))

    return loss
