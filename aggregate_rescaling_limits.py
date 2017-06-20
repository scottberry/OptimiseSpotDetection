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
        help='list of input files to be aggregated (.csv)'
    )
    parser.add_argument(
        '-o', '--output_file', type=str, default='aggregated_limits.csv',
        help='filename for output file (.csv)'
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
            pd.read_csv(filename, encoding='utf-8')
        )
    rescaling_limits = pd.concat(rescaling_limits_list)

    grouped = rescaling_limits.groupby('control')
    columns = grouped['lower_limit', 'upper_limit']
    aggregated_limits = columns.agg([
        np.mean, np.min, np.max,
        percentile(10), percentile(40),
        percentile(60), percentile(80)
    ])

    aggregated_limits.to_csv(args.output_file, encoding='utf-8')

    return


if __name__ == '__main__':
    arguments = parse_arguments()
    main(arguments)
