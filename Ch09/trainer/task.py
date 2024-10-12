import argparse
import logging
import os

from tensorflow.python.data.ops.map_op import _MapDataset

import model
from model import ModelFactory, TrainJobs

def wf_linear_classification(
        tds: _MapDataset,
        eds: _MapDataset,
        output_dir: str) -> None:
    """

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
    h, tm = TrainJobs.train(tds, eds, md,  1, 1, output_dir)

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

    args = parser.parse_args()
    arguments = args.__dict__
    traindata = arguments.pop('traindata')
    evaldata = arguments.pop('evaldata')
    output_dir = arguments.pop('output_dir')

    # Set logging
    logging.basicConfig(level=logging.INFO)

    # Prepare dataset
    tds = model.read_dataset(traindata, truncate=5)
    eds = model.read_dataset(evaldata, truncate=5)

    wf_linear_classification(tds, eds, output_dir)

if __name__ == "__main__":
    main()



