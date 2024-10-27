import argparse
import dataclasses
import logging
import os

from tensorflow.estimator import ModeKeys
from tensorflow.python.data.ops.map_op import _MapDataset

import model
from model import ModelFactory, TrainJobs


def wf_linear_classification(
        tds: _MapDataset,
        eds: _MapDataset,
        output_dir: str,
        tp: model.TrainParams) -> None:
    """

    :param train_params:
    :param tds:
    :param eds:
    :param output_dir:
    :return:
    """
    # Prepare features columns
    real, sparse = model.get_feature_columns()
    inputs = model.get_inputs(real, sparse)

    # One hot encode the categorical column
    sparse = model.one_hot_encode(sparse)

    # Prepare model
    md = ModelFactory.new_linear_classifier(inputs, list(real.values()), list(sparse.values()))

    # Train
    h, tm = TrainJobs.train(tds, eds, md, output_dir, tp)

    # Eval
    TrainJobs.eval(h)

    # Save
    TrainJobs.save(tm, output_dir)

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--traindata',
                        help='Training data file(s)',
                        required=True)

    parser.add_argument('--evaldata',
                        help='Eval data file(s)',
                        required=True)

    parser.add_argument('--output_dir',
                        help='Parent directory to output artifacts',
                        default=os.getcwd(),
                        required=False)

    parser.add_argument('--num_of_examples',
                        help='Batches',
                        default=1000000,
                        required=False)

    parser.add_argument('--train_batch_size',
                        help='Batches',
                        default=64,
                        required=False)

    args = parser.parse_args()
    arguments = args.__dict__
    traindata = arguments.pop('traindata')
    evaldata = arguments.pop('evaldata')
    output_dir = arguments.pop('output_dir')

    # Set logging
    logging.basicConfig(level=logging.INFO)

    tp = model.TrainParams(
        num_of_examples=arguments.pop('num_of_examples'),
        train_batch_size=arguments.pop('train_batch_size'))

    # Prepare dataset
    tds = model.read_dataset(traindata, tp.train_batch_size)
    eds = model.read_dataset(evaldata, tp.eval_batch_size, ModeKeys.EVAL, tp.num_of_eval_examples)

    wf_linear_classification(tds, eds, output_dir, tp)

if __name__ == "__main__":
    main()



