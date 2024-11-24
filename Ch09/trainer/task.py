""" Entry point module """
import argparse
import logging
import os
from typing import Dict

from tensorflow.keras.layers import Input
from tensorflow.estimator import ModeKeys
from tensorflow.python.data.ops.map_op import _MapDataset
from tensorflow.python.feature_column.feature_column_v2 import NumericColumn, IndicatorColumn

from trainer.model import ModelType, UUID
from . import model

def run_train(
        inputs: Dict[str, Input],
        real: Dict[str, NumericColumn],
        sparse: Dict[str, IndicatorColumn],
        tds: _MapDataset,
        eds: _MapDataset,
        output_dir: str,
        tp: model.TrainParams) -> None:
    # pylint: disable=R0913
    # pylint: disable=R0917
    """
    Run training

    :param inputs:
    :param real:
    :param sparse:
    :param tds: Training dataset
    :param eds: Evaluation dataset
    :param output_dir:
    :param tp: Train params
    :return:
    """
    # Prepare model
    md = model.ModelFactory.new_linear_classifier(
            inputs,
            list(real.values()),
            list(sparse.values())) if tp.model_type == ModelType.LINEAR \
        else (
        model.ModelFactory.new_wide_and_deep_classifier(
            inputs,
            list(real.values()),
            list(sparse.values()),
            tp.dnn_hidden_units))

    # Train
    h, tm = model.TrainJobs.train(tds, eds, md, output_dir, tp)

    # Eval
    hp_metric = model.TrainJobs.eval(h)

    # Save
    model.TrainJobs.save(tm, output_dir)

    # Hyperparameter tuning
    model.TrainJobs.hp_tuning(hp_metric)


def main():
    """ Main function """
    parser = argparse.ArgumentParser()

    parser.add_argument('--traindata',
                        help='Training data file(s)',
                        type=str,
                        required=True)

    parser.add_argument('--evaldata',
                        help='Eval data file(s)',
                        type=str,
                        required=True)

    parser.add_argument('--output_dir',
                        help='Parent directory to output artifacts',
                        type=str,
                        default=os.getcwd(),
                        required=False)

    parser.add_argument('--num_of_examples',
                        help='Batches',
                        type=int,
                        default=1000000,
                        required=False)

    parser.add_argument('--train_batch_size',
                        help='Batches',
                        type=int,
                        default=64,
                        required=False)

    parser.add_argument('--num_of_buckets',
                        help='Number of bins to span for lon and lat when doing embedding',
                        type=int,
                        default=5,
                        required=False)

    # TODO Something suitable interface compatible with gcloud ai create
    parser.add_argument('--dnn_hidden_units',
                        help='Architecture of DNN part of wide-and-deep network',
                        type=int,
                        default=[32,4],
                        nargs="*",
                        required=False)

    parser.add_argument('--func',
                        help='Function name to generate model. '
                             'Available values are linear and embeddings',
                        type=str,
                        default='linear',
                        required=False)

    parser.add_argument('--truncate',
                        help='Number of lines to take from csv file. Mainly for testing purpose.',
                        type=int,
                        default=0,
                        required=False)

    args = parser.parse_args()
    arguments = args.__dict__
    traindata = arguments.pop('traindata')
    evaldata = arguments.pop('evaldata')
    output_dir = arguments.pop('output_dir')

    # Set logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info('Start training UUID: %s', UUID)

    tp = model.TrainParams(
        num_of_examples=arguments.pop('num_of_examples'),
        train_batch_size=arguments.pop('train_batch_size'),
        model_type=model.ModelType(arguments.pop('func')),
        num_of_buckets=arguments.pop('num_of_buckets'),
        dnn_hidden_units=arguments.pop('dnn_hidden_units').split("-"))

    # Prepare dataset
    tds = model.read_dataset(
        traindata, tp.train_batch_size, ModeKeys.TRAIN, arguments.pop('truncate'))
    eds = model.read_dataset(
        evaldata, tp.eval_batch_size, ModeKeys.EVAL, tp.num_of_eval_examples)

    # Prepare features columns
    real, sparse = model.get_feature_columns()
    inputs = model.get_inputs(
        real,
        sparse,
        tp.model_type,
        tp.num_of_buckets
    )

    # One hot encode the categorical column
    sparse = model.one_hot_encode(sparse)

    run_train(inputs, real, sparse, tds, eds, output_dir, tp)

if __name__ == "__main__":
    main()
