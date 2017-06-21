#! /usr/bin/env python

import pandas as pd
import numpy as np
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog='aggregate_rescaling_limits',
        description=('')
    )
    parser.add_argument(
        '-i', '--input_files', type=str, nargs='+', required=True,
        help='list of input files to be aggregated (.pkl)'
    )
    parser.add_argument(
        '-o', '--output_file', type=str, default='aggregated_limits.pkl',
        help='filename for output file (.pkl)'
    )

    return(parser.parse_args())


def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_


def main(args):

    rescaling_limits_list = []
    for index, filename in enumerate(args.input_files):
        rescaling_limits_list.append(
            pd.read_pickle(filename)
        )
    rescaling_limits = pd.concat(rescaling_limits_list)

    grouped = rescaling_limits.groupby('control')
    columns = grouped['lower_limit', 'upper_limit']
    aggregated_limits = columns.agg([
        np.mean, np.min, np.max,
        percentile(10), percentile(40),
        percentile(60), percentile(80)
    ])

    print aggregated_limits

    aggregated_limits.to_pickle(args.output_file)

    return


if __name__ == '__main__':
    arguments = parse_arguments()
    main(arguments)
