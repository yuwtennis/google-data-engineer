""" Entry point module """
import argparse
import logging
import os

from tensorflow.estimator import ModeKeys
from tensorflow.python.data.ops.map_op import _MapDataset

from trainer import model


def wf_linear_classification(
        tds: _MapDataset,
        eds: _MapDataset,
        output_dir: str,
        tp: model.TrainParams) -> None:
    """

    :param tp:
    :param tds:
    :param eds:
    :param output_dir:
    :return:
    """
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

    # Prepare model
    md = model.ModelFactory.new_linear_classifier(
        inputs, list(real.values()), list(sparse.values()))

    # Train
    h, tm = model.TrainJobs.train(tds, eds, md, output_dir, tp)

    # Eval
    model.TrainJobs.eval(h)

    # Save
    model.TrainJobs.save(tm, output_dir)

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
    logging.basicConfig(level=logging.ERROR)

    tp = model.TrainParams(
        num_of_examples=arguments.pop('num_of_examples'),
        train_batch_size=arguments.pop('train_batch_size'),
        model_type=model.ModelType(arguments.pop('func')),
        num_of_buckets=arguments.pop('num_of_buckets'))

    # Prepare dataset
    tds = model.read_dataset(
        traindata, tp.train_batch_size, ModeKeys.TRAIN, arguments.pop('truncate'))
    eds = model.read_dataset(
        evaldata, tp.eval_batch_size, ModeKeys.EVAL, tp.num_of_eval_examples)

    wf_linear_classification(tds, eds, output_dir, tp)

if __name__ == "__main__":
    main()
