import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple, Any, Dict

import tensorflow as tf
from tensorflow.python.data.ops.map_op import _MapDataset
from tensorflow.python.feature_column.feature_column_v2 import NumericColumn, CategoricalColumn
from tensorflow.python.keras.callbacks import History

CSV_COLUMNS = (
    'ontime,dep_delay,taxiout,distance,avg_dep_delay,avg_arr_delay' +
    'carrier,dep_lat,dep_lon,arr_lat,arr_lon,origin,dest'
).split(',')

LABEL_COLUMN = 'ontime'

DEFAULTS = [
    [0.0], [0.0], [0.0], [0.0], [0.0], [0.0],
    ['na'], [0.0], [0.0], [0.0], [0.0], ['na'], ['na']
]

CHECK_POINT_PATH = f"{str(Path('.').cwd())}/checkpoints/flights.cpt"
MODEL_PATH = f'export/{datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")}'

LOGGER = logging.getLogger(__name__)

def read_dataset(
        filename: str,
        mode: tf.estimator.ModeKeys = tf.estimator.ModeKeys.EVAL,
        batch_size: int = 512,
        truncate: int = None
)  -> _MapDataset:
    """

    :param filename:
    :param mode:
    :param batch_size:
    :param truncate:
    :return:
    """
    def pop_features_and_label(features: Dict[str, Any]) -> Tuple[dict[str, Any], Any]:
        print(features)
        label = features.pop(LABEL_COLUMN)

        return features, label

    dataset = tf.data.experimental.make_csv_dataset(
        filename,
        batch_size,
        header=False,
        column_names=CSV_COLUMNS,
        column_defaults=DEFAULTS)

    dataset = dataset.map(pop_features_and_label)
    if mode == tf.estimator.ModeKeys.TRAIN:
        dataset = dataset.repeat()
        dataset = dataset.shuffle(batch_size * 10)
    dataset = dataset.prefetch(1)
    if truncate is not None:
        dataset = dataset.take(truncate)

    return dataset

def find_label_avg(dataset: _MapDataset):
    """

    :param dataset:
    :return:
    """
    labels = dataset.map(lambda x,y: y)
    count, total = labels.reduce((0.0, 0.0), lambda state, y: (state[0]+1.0, state[1]+y))
    print(total/count)


def get_feature_columns() -> Tuple[Dict[str, NumericColumn], Dict[str, CategoricalColumn]]:
    """
    Define data structure of features so that the model can understand

    See
    https://developers.googleblog.com/en/introducing-tensorflow-feature-columns/

    :return:
    """
    real = {
        col_name: tf.feature_column.numeric_column(col_name)
            for col_name in 'dep_delay,taxiout,distance,avg_dep_delay,avg_arr_delay,dep_lat,dep_lon,arr_lat,arr_lon'
                                .split(',')
    }

    sparse = {
        'carrier': tf.feature_column.categorical_column_with_vocabulary_list(
            'carrier',
            vocabulary_list='AS,VX,F9,UA,US,WN,HA,EV,MQ,DL,00,86,NK,AA').split(','),
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

def get_inputs(real: Dict[str, Any], sparse: Dict[str, Any]) -> Dict[str, tf.keras.layers.Input]:
    """
    Instantiate Keras tensors

    See
    https://keras.io/api/layers/core_layers/input/

    :param real:
    :param sparse:
    :return:
    """
    inputs = {
        col_name: tf.keras.layers.Input(name=col_name, shape=(), dtype='float32') for col_name in real.keys()
    }

    inputs.update({
        col_name: tf.keras.layers.Input(name=col_name, shape=(), dtype='string') for col_name in sparse.keys()
    })

    return inputs

def calc_rmse(y_true: tf.Tensor, y_pred: tf.Tensor) -> tf.Tensor:
    """
    Calculate Root Mean Square Error

    :param y_true: Actual Tensor
    :param y_pred: Predicted Tensor
    :return: Tensor including the RMSE with the same size, type and sparsity as original tensor
    """
    return tf.sqrt(tf.reduce_mean((tf.square(y_pred - y_true))))

class ModelFactory:
    """

    """

    @staticmethod
    def new_linear_classifier(
            inputs: Dict[str, tf.keras.layers.Input],
            sparse: Dict[str, NumericColumn],
            real: Dict[str, CategoricalColumn]) -> tf.keras.Model:
        """

        :param inputs: Keras tensors
        :param sparse: Tensorflow NumericColumns
        :param real: Tensorflow CategoricalColumns
        :return:
        """
        both = tf.keras.layers.DenseFeatures(
            list(sparse) + list(real), name='features'
        )(inputs)

        output = tf.keras.layers.Dense(
            1, activation='sigmoid', name='pred'
        )

        model = tf.keras.Model(inputs, output)
        model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy', calc_rmse])

        return model

class TrainJobs:

    @staticmethod
    def train(
            train_dataset: _MapDataset,
            eval_dataset: _MapDataset,
            model: tf.keras.Model,
            num_of_epochs: int,
            steps_per_epoch: int,
            output_dir: str = '.',
            ) -> Tuple[History, tf.keras.Model]:
        """
        Train the model and print the evaluation.

        :param output_dir:
        :param train_dataset:
        :param eval_dataset:
        :param model:
        :param num_of_epochs:
        :param steps_per_epoch:
        :return:
        """

        p = Path(f'{output_dir}/{CHECK_POINT_PATH}')
        cp_callback = tf.keras.callbacks.ModelCheckpoint(
            str(p.absolute()),
            save_weights_only=True,
            verbose=1)

        history: History = model.fit(
            train_dataset,
            validation_data=eval_dataset,
            epochs=num_of_epochs,
            steps_per_epoch=steps_per_epoch,
            validation_steps=10,
            callbacks=[cp_callback]
        )

        return history, model

    @staticmethod
    def eval(history: History) -> None:
        """
        Evaluate and tune the hyperparameter

        :param history:
        :return:
        """
        final_rmse = history.history['val_rmse'][-1]
        LOGGER.info('Final RMSE = %s', final_rmse)

        # TODO Hyperparameter tuning

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
