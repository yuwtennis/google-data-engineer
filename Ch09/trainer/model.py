""" Necessary tools for training """
import dataclasses
import logging
import pprint
import uuid
from enum import Enum
from pathlib import Path
from typing import Tuple, Any, Dict, List

import numpy as np
import tensorflow as tf
from tensorflow.python.data.ops.map_op import _MapDataset
from tensorflow.python.feature_column.feature_column_v2 import (
    NumericColumn, CategoricalColumn, IndicatorColumn)
from tensorflow.python.keras.callbacks import History
import hypertune

UUID = uuid.uuid1().hex

CSV_COLUMNS = (
    'ontime,'
    'dep_delay,'
    'taxiout,'
    'distance,'
    'avg_dep_delay,'
    'avg_arr_delay,'
    'carrier,'
    'dep_lat,'
    'dep_lon,'
    'arr_lat,'
    'arr_lon,'
    'origin,'
    'dest'
)

LABEL_COLUMN = 'ontime'

DEFAULTS = [
    [0.0], [0.0], [0.0], [0.0], [0.0], [0.0],
    ['na'], [0.0], [0.0], [0.0], [0.0], ['na'], ['na']
]

REAL_COLS = ('dep_delay,'
             'taxiout,'
             'distance,'
             'avg_dep_delay,'
             'avg_arr_delay,'
             'dep_lat,'
             'dep_lon,'
             'arr_lat,'
             'arr_lon')

CARRIER_VOCAB_LIST = ('AS,'
                      'VX,'
                      'F9,'
                      'UA,'
                      'US,'
                      'WN,'
                      'HA,'
                      'EV,'
                      'MQ,'
                      'DL,'
                      '00,'
                      '86,'
                      'NK,'
                      'AA')

CHECK_POINT_PATH = f"checkpoints/{UUID}/flights.cpt"
MODEL_PATH = f'export/{UUID}'
LOGGER = logging.getLogger(__name__)

class ModelType(Enum):
    """ Enum class defining Model types"""
    LINEAR = 'linear'
    EMBEDDINGS = 'embeddings'


def read_dataset(
        filename: str,
        batch_size: int = 512,
        mode: tf.estimator.ModeKeys = tf.estimator.ModeKeys.TRAIN,
        truncate: int = 0
)  -> _MapDataset:
    """

    :param filename:
    :param mode:
    :param batch_size:
    :param truncate:
    :return:
    """
    def pop_features_and_label(features: Dict[str, Any]) -> Tuple[dict[str, Any], Any]:
        label = features.pop(LABEL_COLUMN)

        return features, label

    dataset = tf.data.experimental.make_csv_dataset(
        filename,
        batch_size,
        header=False,
        column_names=CSV_COLUMNS.split(','),
        column_defaults=DEFAULTS)

    dataset = dataset.map(pop_features_and_label)
    if mode == tf.estimator.ModeKeys.TRAIN:
        dataset = dataset.repeat()
        dataset = dataset.shuffle(batch_size * 10)
    dataset = dataset.prefetch(1)
    if truncate > 0:
        dataset = dataset.take(truncate)

    return dataset

def find_label_avg(dataset: _MapDataset):
    """

    :param dataset:
    :return:
    """
    labels = dataset.map(lambda x,y: y)
    count, total = labels.reduce((0.0, 0.0), lambda state, y: (state[0]+1.0, state[1]+y))
    LOGGER.info(total/count)


def get_feature_columns() -> Tuple[Dict[str, NumericColumn], Dict[str, CategoricalColumn]]:
    """
    Define data structure of features so that the model can understand

    See
    https://developers.googleblog.com/en/introducing-tensorflow-feature-columns/

    :return:
    """
    real = {
        col_name: tf.feature_column.numeric_column(col_name)
            for col_name in REAL_COLS.split(',')
    }

    sparse = {
        'carrier': tf.feature_column.categorical_column_with_vocabulary_list(
            'carrier',
            vocabulary_list=CARRIER_VOCAB_LIST.split(',')),
        'origin': tf.feature_column.categorical_column_with_hash_bucket(
            'origin',
            hash_bucket_size=1000
        ),
        'dest': tf.feature_column.categorical_column_with_hash_bucket(
            'dest',
            hash_bucket_size=1000
        )
    }

    return real, sparse

def get_inputs(
        real: Dict[str, Any],
        sparse: Dict[str, Any],
        model_type: ModelType,
        num_of_buckets: int) -> Dict[str, tf.keras.layers.Input]:
    """
    Instantiate Keras tensors

    See
    https://keras.io/api/layers/core_layers/input/

    :param num_of_buckets:
    :param model_type:
    :param real:
    :param sparse:
    :return:
    """
    inputs = {
        col_name: tf.keras.layers.Input(name=col_name, shape=(), dtype='float32')
        for col_name in real.keys()
    }

    inputs.update({
        col_name: tf.keras.layers.Input(name=col_name, shape=(), dtype='string')
        for col_name in sparse.keys()
    })

    LOGGER.info("Keys are %s", inputs.keys())

    if model_type == ModelType.EMBEDDINGS:
        LOGGER.info("With embeddings")

        latbuckets = np.linspace(20.0, 50.0, num_of_buckets).tolist() # USA
        lonbuckets = np.linspace(-120.0, -70.0, num_of_buckets).tolist() # USA
        disc = {}
        disc.update({
            f'd_{key}': tf.feature_column.bucketized_column(real[key], latbuckets)
            for key in ['dep_lat', 'arr_lat']
        })

        disc.update({
            f'd_{key}': tf.feature_column.bucketized_column(real[key], lonbuckets)
            for key in ['dep_lon', 'arr_lon']
        })

        # Cross columns that make sense in combination
        sparse['dep_loc'] = tf.feature_column.crossed_column(
            [disc['d_dep_lat'], disc['d_dep_lon']], num_of_buckets * num_of_buckets)

        sparse['arr_loc'] = tf.feature_column.crossed_column(
            [disc['d_arr_lat'], disc['d_arr_lon']], num_of_buckets * num_of_buckets)

        sparse['dep_arr'] = tf.feature_column.crossed_column(
            [sparse['dep_loc'], sparse['arr_loc']], num_of_buckets ** 4)

        # Embed all the sparse columns
        embed = {
            f'embed_{colname}': tf.feature_column.embedding_column(col, 10)
                for colname, col in sparse.items()
        }

        real.update(embed)

    return inputs

def calc_rmse(y_true: tf.Tensor, y_pred: tf.Tensor) -> tf.Tensor:
    """
    Calculate Root Mean Square Error

    :param y_true: Actual Tensor
    :param y_pred: Predicted Tensor
    :return: Tensor including the RMSE with the same size, type and sparsity as original tensor
    """
    return tf.sqrt(tf.reduce_mean((tf.square(y_pred - y_true))))

def one_hot_encode(sparse: Dict[str, CategoricalColumn]) -> Dict[str, IndicatorColumn]:
    """

    :param sparse:
    :return:
    """
    return {col: tf.feature_column.indicator_column(val) for col, val in sparse.items()}

class ModelFactory:
    """ Create new model using input parameters """

    @staticmethod
    def new_linear_classifier(
            inputs: Dict[str, tf.keras.layers.Input],
            real_list: List[NumericColumn],
            sparse_list: List[IndicatorColumn]) -> tf.keras.Model:
        """
        Prepare single NN layer with sigmoid activation function

        :param real_list:
        :param sparse_list:
        :param inputs: Keras tensors
        :return:
        """
        # Single NN layer
        both = tf.keras.layers.DenseFeatures(
            real_list + sparse_list, name='features'
        )(inputs)

        # Invoke activation function
        output = tf.keras.layers.Dense(
            1, activation='sigmoid', name='pred'
        )(both)

        model = tf.keras.Model(inputs, output)
        model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy', calc_rmse])

        return model

    @staticmethod
    def new_wide_and_deep_classifier(
            inputs: Dict[str, tf.keras.layers.Input],
            linear_feature_columns: List[NumericColumn],
            dnn_feature_columns: List[IndicatorColumn],
            dnn_hidden_units: List[int]
    ):
        """

        :param inputs:
        :param linear_feature_columns:
        :param dnn_feature_columns:
        :param dnn_hidden_units:
        :return:
        """
        # TODO Check if keras supports adjusting learning rate
        deep = tf.keras.layers.DenseFeatures(
            dnn_feature_columns, name='deep_inputs')(inputs)
        layers = [int(x) for x in dnn_hidden_units]
        for layerno, numnodes in enumerate(layers):
            deep = tf.keras.layers.Dense(numnodes, activation='relu', name=f'dnn_{layerno+1}')(deep)
        wide = tf.keras.layers.DenseFeatures(linear_feature_columns, name='wide_inputs')(inputs)
        both = tf.keras.layers.concatenate([deep, wide], name='both')
        output = tf.keras.layers.Dense(1, activation='sigmoid', name='pred')(both)
        model = tf.keras.Model(inputs, output)
        model.compile(optimizer='adam',
                      loss='binary_crossentropy',
                      metrics=['accuracy', calc_rmse])

        return model

BATCH_CUT_OFF_SIZE = 10000

@dataclasses.dataclass
class TrainParams:
    """
    Defines training parameters

    :ivar num_of_examples: Group of samples in a batch when read by make_csv_dataset
    :ivar train_batch_size: Number of examples for an epoch for training
    :ivar num_of_epochs: Number of epochs
    :ivar eval_batch_size: Number of examples for an epoch for validation
    :ivar num_of_eval_examples: Number of neurons for each layer
    """
    # pylint: disable=R0902
    num_of_examples: int
    train_batch_size: int
    num_of_buckets: int
    model_type: ModelType
    dnn_hidden_units: List[int]

    # Automatically set
    num_of_epochs: int = dataclasses.field(init=False)
    eval_batch_size: int = dataclasses.field(init=False)
    num_of_eval_examples: int = dataclasses.field(init=False)

    def __post_init__(self):
        if self.num_of_examples < BATCH_CUT_OFF_SIZE:
            self.num_of_epochs = 2
            self.train_batch_size = 3
            self.eval_batch_size = 100
            self.num_of_eval_examples = 5 * self.eval_batch_size
        else:
            self.num_of_epochs = 10
            self.eval_batch_size = 10000
            self.num_of_eval_examples = 0

@dataclasses.dataclass
class HyperParameterTuningMetric:
    """ Hyperparameter tuning """
    metric_name: str
    metric_val: Any

class TrainJobs:
    """ Do training """
    @staticmethod
    def train(
            train_dataset: _MapDataset,
            eval_dataset: _MapDataset,
            model: tf.keras.Model,
            output_dir: str,
            tp: TrainParams
            ) -> Tuple[History, tf.keras.Model]:
        """
        Train the model and print the evaluation.

        :param tp:
        :param output_dir: Output directory to store the trained model
        :param train_dataset: Training dataset
        :param eval_dataset: Validation dataset
        :param model: Model to train
        :return:
        """

        p = Path(f'{output_dir}/{CHECK_POINT_PATH}')
        LOGGER.info("Save check points to %s", str(p.absolute()))
        cp_callback = tf.keras.callbacks.ModelCheckpoint(
            str(p.absolute()),
            save_weights_only=True,
            verbose=1)

        history: History = model.fit(
            train_dataset,
            validation_data=eval_dataset,
            epochs=tp.num_of_epochs,
            steps_per_epoch=tp.train_batch_size,
            validation_steps=10,
            callbacks=[cp_callback]
        )

        LOGGER.info("Result %s", pprint.pformat(history.history))
        return history, model

    @staticmethod
    def eval(history: History) -> HyperParameterTuningMetric:
        """
        Evaluate and tune the hyperparameter

        :param history:
        :return: final rmse
        """

        final_rmse: float = history.history['val_calc_rmse'][-1]
        LOGGER.info('Final RMSE (Validation Calculated RMSE) = %s', final_rmse)

        return HyperParameterTuningMetric(metric_name="rmse", metric_val=final_rmse)

    @staticmethod
    def save(model: tf.keras.Model, output_dir: str = '.'):
        """
        Write out model to the filesystem

        :param model:
        :param output_dir:
        :return:
        """
        p = Path(f'{output_dir}/{MODEL_PATH}')

        LOGGER.info('Exporting to %s', str(p))
        tf.saved_model.save(model, str(p.absolute()))

    @staticmethod
    def hp_tuning(metric: HyperParameterTuningMetric) -> None:
        """

        :param metric:
        :return:
        """
        hpt = hypertune.HyperTune()
        hpt.report_hyperparameter_tuning_metric(
            hyperparameter_metric_tag=metric.metric_name,
            metric_value=metric.metric_val,
            global_step=1
        )
