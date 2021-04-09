"""Machine Learning"""

import importlib
import numpy as np
import pandas as pd
import json
from jsonschema import validate
from sklearn.pipeline import make_pipeline
from timeflux.core.node import Node
from timeflux.core.exceptions import ValidationError, WorkerInterrupt
from timeflux.helpers.background import Task
from timeflux.helpers.port import make_event, match_events, get_meta
from timeflux.helpers.clock import now, min_time, max_time

# Statuses
IDLE = 0
ACCUMULATING = 1
FITTING = 2
READY = 3


class Pipeline(Node):
    """Fit, transform and predict.

    Training on continuous data is always unsupervised.
    Training on epoched data can either be supervised or unsupervised.

    If fit is `False`, input events are ignored, and initital training is not performed.
    Automatically set to False if mode is either 'fit_predict' or fit_transform'.
    Automatically set to True if mode is either 'predict', 'predict_proba' or 'predict_log_proba'.

    Attributes:
        i (Port): Continuous data input, expects DataFrame.
        i_* (Port): Epoched data input, expects DataFrame.
        i_training (Port): Continuous training data input, expects DataFrame.
        i_training_* (Port): Epoched training data input, expects DataFrame.
        i_events (Port): Event input, expects DataFrame.
        o (Port): Continuous data output, provides DataFrame.
        o_* (Port): Epoched data output, provides DataFrame.
        o_events (Port): Event output, provides DataFrame.

    Args:
        steps (dict): Pipeline steps and settings
        fit (bool):
        mode ('predict'|'predict_proba'|'predict_log_proba'|'transform'|'fit_predict'|'fit_transform'):
        meta_label (str|tuple|None):
        event_start_accumulation (str):
        event_stop_accumulation (str):
        event_start_training (str):
        event_reset (str):
        buffer_size (str):
        passthrough (bool):
        resample (bool):
        resample_direction ('right'|'left'|'both'):
        resample_rate (None|float):
        model: Load a pickle model - NOT IMPLEMENTED
        cv: Cross-validation - NOT IMPLEMENTED

    """

    def __init__(
        self,
        steps,
        fit=True,
        mode="predict",
        meta_label=("epoch", "context", "target"),
        event_start_accumulation="accumulation_starts",
        event_stop_accumulation="accumulation_stops",
        event_start_training="training_starts",
        event_reset=None,
        buffer_size="5s",
        passthrough=False,
        resample=False,
        resample_direction="right",
        resample_rate=None,
        model=None,
        cv=None,
    ):

        # TODO: validation
        # TODO: model loading from file
        # TODO: cross-validation
        # TODO: provide more context for errors
        self.fit = fit
        self.mode = mode
        self.meta_label = meta_label
        self.event_start_accumulation = event_start_accumulation
        self.event_stop_accumulation = event_stop_accumulation
        self.event_start_training = event_start_training
        self.event_reset = event_reset
        self.passthrough = passthrough
        self.resample = resample
        self.resample_direction = resample_direction
        self.resample_rate = resample_rate
        self._buffer_size = pd.Timedelta(buffer_size)
        self._make_pipeline(steps)
        self._reset()

    def update(self):

        # Let's get ready
        self._clear()

        # Reset
        if self.event_reset:
            matches = match_events(self.i_events, self.event_reset)
            if matches is not None:
                self.logger.debug("Reset")
                if self._status == FITTING:
                    self._task.stop()
                self._reset()

        # Are we dealing with continuous data or epochs?
        if self._dimensions is None:
            port_name = "i_training" if self.fit else "i"
            if getattr(self, port_name).ready():
                self._dimensions = 2
            elif len(list(self.iterate(port_name + "_*"))) > 0:
                self._dimensions = 3

        # Set the accumulation boundaries
        if self._accumulation_start is None:
            matches = match_events(self.i_events, self.event_start_accumulation)
            if matches is not None:
                self._accumulation_start = matches.index.values[0]
                self._status = ACCUMULATING
                self.logger.debug("Start accumulation")
        if self._accumulation_stop is None:
            matches = match_events(self.i_events, self.event_stop_accumulation)
            if matches is not None:
                self._accumulation_stop = matches.index.values[0]
                self.logger.debug("Stop accumulation")

        # Always buffer a few seconds, in case the start event is coming late
        if self._status == IDLE:
            start = (now() - self._buffer_size).to_datetime64()
            stop = max_time()
            self._accumulate(start, stop)

        # Accumulate between boundaries
        if self._status == ACCUMULATING:
            start = self._accumulation_start
            stop = self._accumulation_stop if self._accumulation_stop else max_time()
            self._accumulate(start, stop)

        # Should we start fitting the model?
        if self._status < FITTING:
            if match_events(self.i_events, self.event_start_training) is not None:
                self._status = FITTING
                self.logger.debug("Start training")
                self._task = Task(
                    self._pipeline, "fit", self._X_train, self._y_train
                ).start()

        # Is the model ready?
        if self._status == FITTING:
            status = self._task.status()
            if status:
                if status["success"]:
                    self._pipeline = status["instance"]
                    self._status = READY
                    self.logger.debug(f"Model fitted in {status['time']} seconds")
                    # TODO: this can potentially be overwritten in _send()
                    self.o_events.data = make_event("ready")
                else:
                    self.logger.error(
                        f"An error occured while fitting: {status['exception'].args[0]}"
                    )
                    self.logger.debug(
                        "\nTraceback (most recent call last):\n"
                        + "".join(status["traceback"])
                    )
                    raise WorkerInterrupt()

        # Run the pipeline
        if self._status == READY:
            self._receive()
            if self._X is not None:
                args = [self._X]
                if self.mode.startswith("fit"):
                    args.append(self._y)
                # TODO: optionally loop through epochs instead of sending them all at once
                self._out = getattr(self._pipeline, self.mode)(*args)

        # Set output streams
        self._send()

    def terminate(self):

        # Kill the fit subprocess
        if self._task is not None:
            self._task.stop()

    def _reset(self):

        self._X_train = None
        self._y_train = None
        self._X_train_indices = np.array([], dtype=np.datetime64)
        self._accumulation_start = None
        self._accumulation_stop = None
        self._dimensions = None
        self._shape = ()
        self._task = None
        if self.mode.startswith("fit"):
            self.fit = False
        elif self.mode.startswith("predict"):
            self.fit = True
        if self.fit:
            self._status = IDLE
        else:
            self._status = READY

    def _clear(self):

        self._X = None
        self._y = None
        self._X_indices = []
        self._X_columns = []
        self._X_meta = None
        self._out = None

    def _make_pipeline(self, steps):

        schema = {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "properties": {
                    "module": {"type": "string"},
                    "class": {"type": "string"},
                    "args": {"type": "object"},
                },
                "required": ["module", "class"],
            },
        }
        try:
            validate(instance=steps, schema=schema)
        except Exception as error:
            raise ValidationError("steps", error.message)
        pipeline = []
        for step in steps:
            try:
                args = step["args"] if "args" in step else {}
                m = importlib.import_module(step["module"])
                c = getattr(m, step["class"])
                i = c(**args)
                pipeline.append(i)
            except ImportError as error:
                raise ValidationError("steps", f"could not import '{step['module']}'")
            except AttributeError as error:
                raise ValidationError(
                    "steps", f"could not find class '{step['class']}'"
                )
            except TypeError as error:
                raise ValidationError(
                    "steps",
                    f"could not instantiate class '{step['class']}' with the given params",
                )
        # TODO: memory and verbose args
        self._pipeline = make_pipeline(*pipeline, memory=None, verbose=False)

    def _accumulate(self, start, stop):

        # Do nothing if no fitting required
        if not self.fit:
            return

        # Set defaults
        indices = np.array([], dtype=np.datetime64)

        # Accumulate continuous data
        if self._dimensions == 2:
            if self.i_training.ready():
                data = self.i_training.data
                mask = (data.index >= start) & (data.index < stop)
                data = data[mask]
                if not data.empty:
                    if self._X_train is None:
                        self._X_train = data.values
                        self._shape = self._X_train.shape[1]
                        indices = data.index.values
                    else:
                        if data.shape[1] == self._shape:
                            self._X_train = np.vstack((self._X_train, data.values))
                            indices = data.index.values
                        else:
                            self.logger.warning("Invalid shape")

        # Accumulate epoched data
        if self._dimensions == 3:
            for _, _, port in self.iterate("i_training_*"):
                if port.ready():
                    index = port.data.index.values[0]
                    if index >= start and index < stop:
                        data = port.data.values
                        label = get_meta(port, self.meta_label)
                        if self._shape and (data.shape != self._shape):
                            self.logger.warning("Invalid shape")
                            continue
                        if self.meta_label is not None and label is None:
                            self.logger.warning("Invalid label")
                            continue
                        if self._X_train is None:
                            self._X_train = np.array([data])
                            self._shape = self._X_train.shape[1:]
                        else:
                            self._X_train = np.vstack((self._X_train, [data]))
                        indices = np.append(indices, index)
                        if label is not None:
                            if self._y_train is None:
                                self._y_train = np.array([label])
                            else:
                                self._y_train = np.append(self._y_train, [label])

        # Store indices
        if indices.size != 0:
            self._X_train_indices = np.append(self._X_train_indices, indices)

        # Trim
        if self._X_train is not None:
            mask = (self._X_train_indices >= start) & (self._X_train_indices < stop)
            self._X_train = self._X_train[mask]
            self._X_train_indices = self._X_train_indices[mask]
            if self._y_train is not None:
                self._y_train = self._y_train[mask]

    def _receive(self):

        # Continuous data
        if self._dimensions == 2:
            if self.i.ready():
                if not self._X_columns:
                    self._X_columns = list(self.i.data.columns)
                if self._shape and (self.i.data.shape[1] != self._shape):
                    self.logger.warning("Invalid shape")
                else:
                    self._X = self.i.data.values
                    self._X_indices = self.i.data.index.values
                    self._X_meta = self.i.meta

        # Epochs
        if self._dimensions == 3:
            for name, _, port in self.iterate("i_*"):
                if port.ready() and "training" not in name and "events" not in name:
                    data = port.data.values
                    meta = port.meta
                    indices = port.data.index.values
                    label = get_meta(port, self.meta_label)
                    if not self._X_columns:
                        self._X_columns = list(port.data.columns)
                    if self._shape and (data.shape != self._shape):
                        self.logger.warning("Invalid shape")
                        continue
                    if not self.fit and self.meta_label is not None and label is None:
                        self.logger.warning("Invalid label")
                        continue
                    if self._X is None:
                        self._X = []
                    if self._y is None and label is not None:
                        self._y = []
                    if self._X_meta is None:
                        self._X_meta = []
                    self._X.append(data)
                    self._X_indices.append(indices)
                    self._X_meta.append(meta)
                    if label is not None:
                        self._y.append(label)

    def _send(self):

        # Passthrough
        if self._status < READY and self.passthrough:
            inputs = []
            for _, suffix, port in self.iterate("i*"):
                if not suffix.startswith("_training") and not suffix.startswith(
                    "_events"
                ):
                    inputs.append((suffix, port))
            for suffix, src_port in inputs:
                dst_port = getattr(self, "o" + suffix)
                dst_port.data = src_port.data
                dst_port.meta = src_port.meta

        # Model
        if self._out is not None:
            if "predict" in self.mode:
                # Send events
                if len(self._X_indices) == len(self._out):
                    # TODO: skip JSON serialization?
                    data = [
                        [self.mode, json.dumps({"result": self._np_to_native(result)})]
                        for result in self._out
                    ]
                    times = (
                        self._X_indices
                        if self._dimensions == 2
                        else np.asarray(self._X_indices)[:, 0]
                    )  # Keep the first timestamp of each epoch
                    names = ["label", "data"]
                    meta = (
                        self._X_meta
                        if self._dimensions == 2
                        else {"epochs": self._X_meta}
                    )  # port.meta should always be an object
                    self.o_events.set(data, times, names, meta)
                else:
                    self.logger.warning(
                        "Number of predictions inconsistent with input length"
                    )
            else:
                # Send data
                if self._dimensions == 2:
                    try:
                        self.o.data = self._reindex(
                            self._out, self._X_indices, self._X_columns
                        )
                        self.o.meta = self._X_meta
                    except Exception as e:
                        self.logger.warning(getattr(e, "message", repr(e)))
                if self._dimensions == 3:
                    if len(self._X_indices) == len(self._out):
                        for i, (data, times) in enumerate(
                            zip(self._out, self._X_indices)
                        ):
                            try:
                                getattr(self, "o_" + str(i)).data = self._reindex(
                                    data, times, self._X_columns
                                )
                                getattr(self, "o_" + str(i)).meta = self._X_meta[i]
                            except Exception as e:
                                self.logger.warning(getattr(e, "message", repr(e)))
                    else:
                        self.logger.warning(
                            "Number of transforms inconsistent with number of epochs"
                        )

    def _np_to_native(self, data):
        """Convert numpy scalars and objects to native types."""
        return getattr(data, "tolist", lambda: data)()

    def _reindex(self, data, times, columns):

        if len(data) != len(times):

            if self.resample:
                # Resample at a specific frequency
                kwargs = {"periods": len(data)}
                if self.resample_rate is None:
                    kwargs["freq"] = pd.infer_freq(times)
                    kwargs["freq"] = pd.tseries.frequencies.to_offset(kwargs["freq"])
                else:
                    kwargs["freq"] = pd.DateOffset(seconds=1 / self.resample_rate)
                if self.resample_direction == "right":
                    kwargs["start"] = times[0]
                elif self.resample_direction == "left":
                    kwargs["end"] = times[-1]
                else:

                    def middle(a):
                        return int(np.ceil(len(a) / 2)) - 1

                    kwargs["start"] = times[middle(times)] - (
                        middle(data) * kwargs["freq"]
                    )
                times = pd.date_range(**kwargs)

            else:
                # Linearly arange between first and last
                times = pd.date_range(start=times[0], end=times[-1], periods=len(data))

        return pd.DataFrame(data, times, columns)
