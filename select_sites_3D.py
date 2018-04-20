import argparse
import numpy as np
import pandas as pd
import itertools
from tmclient import TmClient


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog='get_intensity_extrema',
        description=('Accesses images from TissueMAPS instance and '
                     'writes a pkl file of channel image intensity extrema.')
    )
    parser.add_argument(
        '-v', '--verbosity', action='count', default=0,
        help='increase logging verbosity'
    )
    parser.add_argument(
        '-H', '--host', default='app.tissuemaps.org',
        help='name of TissueMAPS server host'
    )
    parser.add_argument(
        '-P', '--port', type=int, default=80,
        help='number of the port to which the server listens (default: 80)'
    )
    parser.add_argument(
        '-u', '--user', dest='username', required=True,
        help='name of TissueMAPS user'
    )
    parser.add_argument(
        '--password', required=True,
        help='password of TissueMAPS user'
    )
    parser.add_argument(
        '-e', '--experiment', required=True,
        help='experiment name'
    )
    parser.add_argument(
        '-p', '--plate', type=str, default='plate01',
        help='plate name'
    )
    parser.add_argument(
        '--negative_wells', type=str, nargs='+', required=True,
        help='wells of negative control (as list)'
    )
    parser.add_argument(
        '--positive_wells', type=str, nargs='+', required=True,
        help='wells of positive control (as list)'
    )
    parser.add_argument(
        '-n', '--number_sites', dest='n_sites', type=int, required=True,
        help='number of randomly selected sites'
    )
    parser.add_argument(
        '-o', '--output_file', type=str, required=True,
        help='filename for output file (.pkl)'
    )

    return(parser.parse_args())


def main(args):

    tmaps_api = TmClient(
        host=args.host,
        port=args.port,
        experiment_name=args.experiment,
        username=args.username,
        password=args.password
    )

    negative = pd.DataFrame({
        'control': 'negative',
        'well': args.negative_wells
    })

    positive = pd.DataFrame({
        'control': 'positive',
        'well': args.positive_wells
    })

    rescaling_limits = negative.append(positive)

    rescaling_limits = rescaling_limits.merge(
        get_site_dimensions(
            df=rescaling_limits,
            plate_name=args.plate,
            client=tmaps_api
        )
    )

    rescaling_limits = rescaling_limits.merge(
        select_random_sites(
            df=rescaling_limits,
            n_sites=args.n_sites
        ), how='outer'
    )

    rescaling_limits.to_pickle(args.output_file)
    return


def get_site_dimensions(df, client, plate_name):
    dimensions = pd.DataFrame()
    for index, row in df.iterrows():
        well = client.get_sites(plate_name=plate_name, well_name=row['well'])
        max_x = 0
        max_y = 0
        for s in range(len(well)):
            max_x = well[s]['x'] if well[s]['x'] > max_x else max_x
            max_y = well[s]['y'] if well[s]['y'] > max_y else max_y

        dimensions = dimensions.append(
            pd.DataFrame({
                'well': row['well'],
                'n_site_x': int(max_x),
                'n_site_y': int(max_y)
            }, index=[index])
        )
    return dimensions


def select_random_sites(df, n_sites):
    selection = pd.DataFrame()
    for index, row in df.iterrows():
        all_sites = list(
            itertools.product(
                range(row['n_site_x']),
                range(row['n_site_y'])
            )
        )
        selected_sites_x = [all_sites[i][0] for i in list(
            np.random.choice(len(all_sites), n_sites)
        )]
        selected_sites_y = [all_sites[i][1] for i in list(
            np.random.choice(len(all_sites), n_sites)
        )]
        selection = selection.append(
            pd.DataFrame({
                'well': row['well'],
                'site_x': selected_sites_x,
                'site_y': selected_sites_y
            })
        )
    return selection


def get_extrema_of_sites(df, client, channel_name, plate_name, lower_percentile=1.0, upper_percentile=99.5):
    extrema = pd.DataFrame()
    for index, row in df.iterrows():
        image = client.download_channel_image(
            channel_name=channel_name,
            plate_name=plate_name,
            well_name=row['well'],
            well_pos_y=row['site_y'],
            well_pos_x=row['site_x'],
            correct=True
        )
        extrema = extrema.append(
            pd.DataFrame({
                'well': row['well'],
                'site_x': row['site_x'],
                'site_y': row['site_y'],
                'lower_limit': np.percentile(image, lower_percentile),
                'upper_limit': np.percentile(image, upper_percentile)
            }, index=[index])
        )
    return extrema


if __name__ == '__main__':
    arguments = parse_arguments()
    main(arguments)
