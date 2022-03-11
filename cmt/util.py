# coding: utf-8

"""
Helpful utilities.
"""

__all__ = [
    "import_root", "create_file_dir", "colored", "record_runtime", "linsplit", "DotDict",
    "extract_branch_names", "check_expression", "feature_expression", "get_expression_infos",
    "iter_tree", "evaluate_model_on_tree", "poisson_asym_errors", "hist_to_array", "hist_to_graph",
    "get_graph_maximum", "update_graph_values", "optimize_binning", "parse_workflow_file",
]


import os
import re
import time
import math
import contextlib
import warnings
import itertools
import operator
from collections import OrderedDict

import six
from six.moves import reduce
import law
import yaml


def import_root(batch=True):
    import ROOT
    ROOT.PyConfig.IgnoreCommandLineOptions = True
    ROOT.gROOT.SetBatch(batch)

    return ROOT


def create_file_dir(file_path):
    file_path = os.path.expandvars(os.path.expanduser(file_path))
    file_dir = os.path.dirname(file_path)
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)
    return file_path


def colored(msg, color="green", **kwargs):
    try:
        import law
    except ImportError:
        return msg

    return law.util.colored(msg, color=color, **kwargs)


@contextlib.contextmanager
def record_runtime(start_msg="recording runtime ...", end_msg="done, took {:.2f} seconds"):
    print(start_msg)
    t0 = time.time()
    try:
        yield
    finally:
        runtime = time.time() - t0
        print(end_msg.format(runtime))


def linsplit(n, l):
    lengths = []
    avg = float(n) / l
    rest = 0.
    for i in range(l):
        length = avg + rest
        length_int = int(math.floor(length))
        rest = length - length_int
        lengths.append(length_int)
    return lengths


class DotDict(dict):

    def __getattr__(self, attr):
        return self[attr]


def extract_branch_names(expression, tree, expand_aliases=True):
    """
    Parses a string or list of strings *expression* and returns the contained branch names in a
    list. *tree* must be a ROOT tree that is used to validate the found branch names. When
    *expand_aliases* is *True*, all expressions are tested for aliases and expanded accordingly.
    """
    if isinstance(expression, (list, tuple, set)):
        expressions = list(expression)
    else:
        expressions = [expression]

    all_branches = [b.GetName() for b in tree.GetListOfBranches()]
    branches = []

    while expressions:
        expression = expressions.pop(0)
        spaced = re.sub(r"(\+|\-|\*|\/|\(|\)|\[|\]|\:|\,)", " ", expression)
        parts = [s.strip() for s in spaced.split(" ") if s.strip()]
        for b in parts:
            if expand_aliases and tree.GetAlias(b):
                expressions.insert(0, tree.GetAlias(b))
                continue
            if b in all_branches and b not in branches:
                branches.append(b)

    return branches


def check_expression(expression, tree):
    """
    Checks if an *expression* is valid when evaluated on *tree*.
    """
    from plotlib.util import create_random_name
    ROOT = import_root()

    name = create_random_name("tree_expression")
    formula = ROOT.TTreeFormula(name, expression, tree)

    return formula.GetNdim() > 0


# decorator to declare a feature expression function acting on tree entries
def feature_expression(branches=None):
    def decorator(func):
        func._tree_expression = True
        func._required_branches = [] if not branches else law.util.make_list(branches)
        return func
    return decorator


def get_expression_infos(expression, delimiter=" "):
    if isinstance(expression, six.string_types):
        return False, expression
    elif getattr(expression, "_tree_expression", False):
        return True, delimiter.join(expression._required_branches)
    else:
        return False, ""


def iter_tree(tree, expressions=None, defaults=None, selection=None, missing_values=None,
        optimize_branches=True, yield_index=False, tree_fn=None):
    ROOT = import_root()

    def make_list(value):
        if isinstance(value, (list, tuple)):
            return list(value)
        else:
            return [value]

    trees = make_list(tree)
    expressions = expressions and make_list(expressions)
    defaults = defaults and make_list(defaults)

    if expressions and defaults:
        # when set, expressions and defaults must have the same size
        if len(expressions) != len(defaults):
            raise ValueError("when expressions and defaults are set, defaults ({}) must have the "
                "same size as expressions ({})".format(len(defaults), len(expressions)))

        # missing values can be a plain list of values to consider as missing, or a nested list with
        # custom missing values defined per expression
        if missing_values:
            missing_values = make_list(missing_values)
            nested_missing_values = isinstance(missing_values[0], (list, tuple, set))
            if not nested_missing_values:
                missing_values = len(expressions) * [missing_values]
            elif len(missing_values) != len(expressions):
                raise ValueError("when missing values is a nested list ({}), it must have the same "
                    "size as expressions ({})".format(len(missing_values), len(expressions)))
    else:
        missing_values = None

    def split_tree_path(path):
        tree_location = []
        while path and path != os.sep and not os.path.exists(path):
            path, basename = os.path.split(path)
            tree_location.insert(0, basename)
        if not path or path == os.sep:
            return None, None
        else:
            tree_name = os.path.join(*tree_location) if tree_location else None
            return path, tree_name

    def get_first_tree(tfile, depth_first=True):
        lookup = [tfile]
        while lookup:
            tdir = lookup.pop(0)
            for tkey in tdir.GetListOfKeys():
                name = tkey.GetName()
                tobj = tfile.Get(name)
                if isinstance(tobj, ROOT.TTree):
                    return tobj
                elif isinstance(tobj, ROOT.TDirectoryFile):
                    if depth_first:
                        lookup.insert(0, tobj)
                    else:
                        lookup.append(tobj)
        return None

    def get_value_or_default(i, v):
        if not defaults or not missing_values:
            return v
        elif v is None:
            return defaults[i]
        elif v in missing_values[i] or math.isnan(v) or math.isinf(v):
            return defaults[i]
        else:
            return v

    idx = -1
    for i, tree in enumerate(trees):
        # open the tree when a file is passed, validate it
        tfile = None
        if isinstance(tree, six.string_types):
            file_path, tree_name = split_tree_path(tree)
            if file_path is None:
                raise IOError("tree path {} does not point to a valid file".format(tree))
            tfile = ROOT.TFile(file_path)
            if tree_name:
                tree = tfile.Get(tree_name)
                if not tree:
                    raise Exception("tree {} not found in file {}".format(tree_name, file_path))
            else:
                tree = get_first_tree(tfile)
                if not tree:
                    raise Exception("file {} does not contain trees".format(file_path))
        elif not isinstance(tree, ROOT.TTree) or not tree:
            raise TypeError("invalid tree {} ".format(tree))

        # call tree_fn
        if callable(tree_fn):
            tree_fn(i, tree)

        # create a selection formula when given
        selection_formula = None
        if selection:
            selection_formula = ROOT.TTreeFormula("selection_{}".format(i), selection, tree)

        # create expression formulas when given, create callable expression funcs
        expression_objects = []
        if expressions:
            for j, expression in enumerate(expressions):
                if isinstance(expression, six.string_types):
                    f = ROOT.TTreeFormula("expression_{}_{}".format(i, j), expression, tree)
                    expression_objects.append(f)
                elif callable(expression):
                    expression_objects.append(expression)
                else:
                    TypeError("unknown expression type: {}".format(expression))

        # when optimizing branches, determine names of branches to load and set their status
        if expressions and optimize_branches:
            all_expressions = [get_expression_infos(expression)[1] for expression in expressions]
            if selection:
                all_expressions.append(selection)
            read_branches = extract_branch_names(all_expressions, tree)
            tree.SetBranchStatus("*", False)
            for name in read_branches:
                tree.SetBranchStatus(name, True)

        # disable root warnings that show up when evaluating an expression the first time
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)

            # start iterating
            for entry in tree:
                idx += 1

                # evaluate the selection if any
                if selection:
                    if selection_formula.GetNdim() == 0:
                        raise Exception("selection formula could not be evaluated, branches or "
                            "leaf values possibly missing: {}".format(selection))
                    elif not selection_formula.EvalInstanceLD():
                        continue

                # get the payload to yield, i.e., the entry itself or expression values in same order
                if expressions:
                    payload = []
                    for j, f in enumerate(expression_objects):
                        if isinstance(f, ROOT.TTreeFormula):
                            v = f.EvalInstanceLD() if f.GetNdim() > 0 else None
                        else:
                            v = f(entry)
                        payload.append(get_value_or_default(j, v))
                else:
                    payload = entry

                yield (idx, payload) if yield_index else payload


def evaluate_model_on_tree(model, tree, training_processes, input_features, obs_features=None,
        selection=None, batch_sizes=1024, create_recarray=False, add_maximum=False,
        prediction_prefix="dnn_", progress_fn=None):
    import numpy as np

    obs_features = obs_features or []

    # prepare tree iteration
    expressions = [feature.expression for feature in obs_features + input_features]
    defaults = [feature.x.default for feature in obs_features + input_features]
    missing_values = [feature.x.missing for feature in obs_features + input_features]
    obs_values = []
    predictions = []

    # evaluate
    batch = []
    for i, values in enumerate(iter_tree(tree, expressions=expressions, defaults=defaults,
            selection=selection, missing_values=missing_values)):
        if callable(progress_fn):
            progress_fn(i)

        # strip observer and input features
        obs_values.append(values[:len(obs_features)])
        batch.append(values[len(obs_features):])

        # batched inferences
        if len(batch) < 1024:
            continue
        predictions.append(model(np.array(batch), training=False))
        del batch[:]

    # drain the batch
    if batch:
        predictions.append(model(np.array(batch), training=False))

    # handle empty predictions in case of an empty tree or a too tight selection
    if obs_values and predictions:
        predictions = np.concatenate(predictions, axis=0).reshape((-1, len(training_processes)))
        obs_values = np.concatenate(obs_values, axis=0).reshape((-1, len(obs_features)))
    else:
        predictions = np.array([], dtype=np.float32).reshape((0, len(training_processes)))
        obs_values = np.array([], dtype=np.float32).reshape((0, len(obs_features)))

    # when not creating a recarray, return here
    if not create_recarray:
        return (predictions, obs_values) if obs_features else predictions

    # prepare data and dtype for recarray
    data = [obs_values, predictions]
    dtype = [
        (feature.expression, np.float32)
        for feature in obs_features
    ] + [
        (prediction_prefix + process.name, np.float32)
        for process in training_processes
    ]

    # add maximum column?
    if add_maximum:
        data.append(np.max(predictions, axis=-1, keepdims=True))
        dtype.append((prediction_prefix + "maximum", np.float32))

    # create the rec array
    data = np.concatenate(data, axis=1)
    rec = np.core.records.fromarrays(data.transpose(), dtype=dtype)

    return rec


def poisson_asym_errors(v):
    """
    Returns asymmetric poisson errors for a value *v* in a tuple (up, down) following the Garwoord
    prescription (1936). For more info, see
    https://twiki.cern.ch/twiki/bin/viewauth/CMS/PoissonErrorBars and
    https://root.cern.ch/doc/master/TH1_8cxx_source.html#l08537.
    """
    ROOT = import_root()

    v_int = int(v)
    alpha = 1. - 0.682689492

    err_up = ROOT.Math.gamma_quantile_c(alpha / 2., v_int + 1, 1) - v
    err_down = 0. if v == 0 else (v - ROOT.Math.gamma_quantile(alpha / 2, v_int, 1.))

    return err_up, err_down


def hist_to_array(hist, remove_zeros=False, errors=False, asymm=True, overflow=True,
        underflow=True, edges=False):
    import numpy as np
    ROOT = import_root()

    # determine the dtype based on the hist type
    if isinstance(hist, ROOT.TH1D):
        dtype = np.float64
    elif isinstance(hist, ROOT.TH1F):
        dtype = np.float32
    elif isinstance(hist, ROOT.TH1I):
        dtype = np.int32
    else:
        dtype = np.float32

    # number of bins
    n_bins = hist.GetNbinsX()

    # bin range
    start = 0 if underflow else 1
    stop = (n_bins + 1) if overflow else n_bins
    bins = list(range(start, stop + 1))

    # remove zeros
    if remove_zeros:
        bins = [i for i in bins if hist.GetBinContent(i) != 0]

    # create either x values (bin centers) or bin edges
    if edges:
        x = [hist.GetBinLowEdge(i) for i in bins]
        x.append(hist.GetXaxis().GetXmax())
    else:
        x = [hist.GetBinCenter(i) for i in bins]
    x = np.array(x, dtype=dtype)

    # create y values
    y = np.array([hist.GetBinContent(i) for i in bins], dtype=dtype)

    if not errors:
        return x, y

    if not asymm:
        err = np.array([hist.GetBinError(i) for i in bins], dtype=dtype)
        return x, y, err

    else:
        err_up = np.array([hist.GetBinErrorUp(i) for i in bins], dtype=dtype)
        err_down = np.array([hist.GetBinErrorLow(i) for i in bins], dtype=dtype)
        return x, y, err_up, err_down


def hist_to_graph(hist, remove_zeros=False, errors=False, asymm=True, attrs=None, props=None, *args,
        **kwargs):
    import numpy as np
    ROOT = import_root()

    # convert to numpy arrays
    arrays = hist_to_array(hist, remove_zeros=remove_zeros, errors=errors, asymm=asymm, *args,
        **kwargs)
    x, y, errs = arrays[0], arrays[1], arrays[2:]
    zeros = np.zeros_like(x)

    # create the graph
    if not errors:
        graph = ROOT.TGraph(len(x), x, y)
    elif not asymm:
        graph = ROOT.TGraphErrors(len(x), x, y, zeros, errs)
    else:
        err_up, err_down = errs
        graph = ROOT.TGraphAsymmErrors(len(x), x, y, zeros, zeros, err_down, err_up)

    # copy attributes
    if attrs:
        for attr in attrs:
            setattr(graph, attr, getattr(hist, attr, None))

    # copy properties
    if props:
        for prop in props:
            getter = getattr(hist, "Get" + prop, None)
            setter = getattr(graph, "Set" + prop, None)
            if callable(getter) and callable(setter):
                setter(getter())

    return graph


def get_graph_maximum(graph, errors=False):
    ROOT = import_root()

    x, y = ROOT.Double(), ROOT.Double()
    y_max = None
    for i in range(graph.GetN()):
        graph.GetPoint(i, x, y)
        y_ = y + (graph.GetErrorYhigh(i) if errors else 0.)
        if y_max is None or y_ > y_max:
            y_max = y_

    return y_max


def update_graph_values(graph, update_fn):
    ROOT = import_root()

    x, y = ROOT.Double(), ROOT.Double()
    for i in range(graph.GetN()):
        graph.GetPoint(i, x, y)
        new_values = update_fn(i, x, y)
        if new_values:
            graph.SetPoint(i, *new_values)


def optimize_binning(full_edges, s_vals, b_vals, s_errs, b_errs, n_start_bins, n_min_bins, y_low,
        y_high, x_min=None, x_max=None, callback=None, silent=False):
    import numpy as np

    # defaults
    if s_errs is None:
        s_errs = np.zeros_like(s_vals)
    if b_errs is None:
        b_errs = np.zeros_like(b_vals)

    # some input checks
    # assert(s_vals.sum() > 0)
    assert(b_vals.sum() > 0)
    assert(len(s_vals) == len(full_edges) - 1)
    assert(len(b_vals) == len(full_edges) - 1)
    assert(s_errs.shape == s_vals.shape)
    assert(b_errs.shape == b_vals.shape)
    assert(n_start_bins >= n_min_bins)
    assert(y_low <= y_high)
    _mini_bin_widths = full_edges[1:] - full_edges[:-1]
    assert(_mini_bin_widths.max() - _mini_bin_widths.min() < 1e-6)

    # helpers
    def raise_min_bins():
        if silent:
            return None
        else:
            raise Exception("bin contents insufficient for n_min_bins {} and y_low {}: {}".format(
                n_min_bins, y_low, s_vals.sum() + b_vals.sum()))

    def select_vals(vals, start, stop):
        vals = np.array(vals)
        vals[start] = vals[:start + 1].sum()
        vals[stop - 1] = vals[stop - 1:].sum()
        return vals[start:stop]

    def select_errs(errs, start, stop):
        errs = np.array(errs)
        errs[start] = (errs[:start + 1]**2.).sum()**0.5
        errs[stop - 1] = (errs[stop - 1:]**2.).sum()**0.5
        return errs[start:stop]

    def sizes_to_edges(sizes):
        return full_edges[[0] + np.cumsum(sizes).tolist()]

    # when x_min or x_max are "auto", auto detect the centrally populated range
    vals = s_vals + b_vals
    if x_min == "auto":
        x_min = full_edges[np.argwhere(vals > 0).reshape(-1)[0]]
    if x_max == "auto":
        x_max = full_edges[np.argwhere(vals > 0).reshape(-1)[-1] + 1]

    # x_min and x_max define the approximate range of optimized edges to return, so when they are
    # set, find the outer most approximate edges and limit all arrays
    start, stop = 0, len(s_vals)
    if x_min is not None:
        start = int(np.argwhere(full_edges <= x_min).reshape(-1)[-1])
    if x_max is not None:
        stop = int(np.argwhere(full_edges >= x_max).reshape(-1)[0])
    full_edges = full_edges[start:stop + 1]
    s_vals, s_errs = select_vals(s_vals, start, stop), select_errs(s_errs, start, stop)
    b_vals, b_errs = select_vals(b_vals, start, stop), select_errs(b_errs, start, stop)

    # recompute things
    vals = s_vals + b_vals
    itg = vals.sum()
    # errs = (s_errs**2. + b_errs**2.)**0.5

    # detect early when the bin contents are insufficient to fill n_min_bins with at least y_low
    if itg < n_min_bins * y_low:
        # special case: when a single bin is allowed, return two appropriate edges
        if n_min_bins == 1:
            edges = np.array([full_edges[0], full_edges[-1]], dtype=np.float32)
            if callable(callback):
                callback(edges)
            return edges
        return raise_min_bins()

    # start with the requested number of bins and an even binning
    # for easier handling, keep track of bin widths ("sizes" below) in units of bins defined by
    # full_edges ("mini bins"), e.g. if bin 0 has a size 5, it combines the first 5 mini bins
    n_bins = n_start_bins
    sizes = None
    bin_changes = None
    xxx = 0
    while True:
        xxx += 1
        if sizes is None:
            sizes = linsplit(len(s_vals), n_bins)
            bin_changes = np.zeros((n_bins,), dtype=np.int32)
            print("start from even binning with {} bins".format(colored(n_bins, "green")))
            if callable(callback):
                callback(sizes_to_edges(sizes))

        # get bin contents and errors
        split_points = np.cumsum(sizes)[:-1]
        binned_vals = np.array([sum(s) for s in np.split(vals, split_points)])
        # binned_errs = np.array([sum(s**2.)**0.5 for s in np.split(errs, split_points)])
        # binned_rels = binned_errs / binned_vals
        # binned_rels[np.isnan(binned_rels)] = 0.

        # identify bins that are below y_low / above y_high
        low_bins = np.argwhere(binned_vals < y_low).reshape(-1)
        high_bins = np.argwhere(binned_vals >= y_high).reshape(-1)

        # stop when there are no low bins
        if not len(low_bins):
            break

        # when there are no high bins with size > 1 to extract bin contents from
        # reduce the number of bins and start over
        high_bin_sizes = np.array([sizes[b] for b in high_bins])
        if not len(high_bins) or (high_bin_sizes == 1).all():
            n_bins -= 1
            if n_bins >= n_min_bins:
                print("reducing n_bins to {}".format(n_bins))
                sizes = None
                continue
            else:
                from IPython import embed; embed()
                return raise_min_bins()

        # find the low bin with the smallest content, select the outermost in case of multiple ones
        smallest_low_bins = np.argwhere(binned_vals == binned_vals.min()).reshape(-1)
        dst_bin = sorted(smallest_low_bins, key=lambda i: abs(i - 0.5 * (n_bins - 1)))[-1]

        # # find the widest high bin, select the one with the largest content in case of multiple ones
        # widest_high_bins = high_bins[high_bin_sizes == high_bin_sizes.max()]
        # src_bin = sorted(widest_high_bins, key=lambda i: binned_vals[i])[-1]

        # find the high bin that has been adjusted the least times, in case of multiple ones, select
        # the one with the largest content
        high_bin_changes = bin_changes[high_bins]
        least_changed_high_bins = high_bins[high_bin_changes == high_bin_changes.min()]
        src_bin = sorted(least_changed_high_bins, key=lambda i: binned_vals[i])[-1]

        # reduce the size of the widest high bin and increase that of the smallest low bin
        sizes[src_bin] -= 1
        sizes[dst_bin] += 1

    # convert sizes back into optimized edges
    edges = sizes_to_edges(sizes)

    # call the callback one last time
    if callable(callback):
        callback(edges)

    return edges


def parse_workflow_file(workflow_file):
    workflow_file = os.path.expandvars(os.path.expanduser(workflow_file))

    with open(workflow_file, "r") as f:
        workflow_file_data = yaml.safe_load(f)

    workflow_data = workflow_file_data["training_workflow"]
    defaults = workflow_data["defaults"]
    branches = workflow_data["branches"]

    training_data = {}

    # loop through branch data in a special order to be able to resolve references
    ids = list(branches.keys())
    ids.sort(key=lambda i: i if isinstance(i, six.integer_types) else int(i.split("-", 1)[0]))
    for b in ids:
        data = branches[b]

        # does data contain a reference?
        ref_branch = data.pop("ref", None)
        ref_data = {}
        if ref_branch is not None:
            if ref_branch not in training_data:
                raise ReferenceError("branch {} referred by {} not existing yet".format(
                    ref_branch, b))
            ref_data = training_data[ref_branch]

        if isinstance(b, six.integer_types):
            # when b is an integer, data refers to a single point
            training_data[b] = law.util.merge_dicts(defaults, ref_data, data)
        else:
            # otherwise, it is a range expression (both start and end included) and the data
            # contains lists for some parameters
            # first, parse the branch range
            m = re.match(r"^(\d+)-(\d+)$", b)
            if not m:
                raise ValueError("invalid branch range {}".format(b))
            start, stop = int(m.group(1)), int(m.group(2)) + 1
            n = stop - start
            # split parameters into fixed ones and those that are scanned
            fixed_params, scan_params = {}, OrderedDict()
            for key in sorted(data.keys()):
                value = data[key]
                (scan_params if isinstance(value, list) else fixed_params)[key] = value
            # verify the number of combinations
            m = reduce(operator.mul, [len(values) for values in scan_params.values()], 1)
            if m != n:
                raise Exception("wrong branch range {} for {} combinations, should be {}-{}".format(
                    b, m, start, start + m - 1))
            # build combinations, add fixed params, and insert into branch map
            for i, values in enumerate(itertools.product(*scan_params.values())):
                _data = dict(zip(scan_params.keys(), values))
                training_data[start + i] = law.util.merge_dicts(defaults, ref_data, fixed_params,
                    _data)

    return workflow_data, training_data
