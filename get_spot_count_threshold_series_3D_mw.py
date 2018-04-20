from __future__ import print_function, absolute_import
import matlab_wrapper
import pandas as pd
import numpy as np
from tmclient import TmClient
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog='get_spot_count_threshold_series',
        description=('Uses ObjByFilter.m to detect spots for a series of'
                     'thresholds. Images analysed are taken from the '
                     'input csv file which is generated by '
                     'get_intensity_extrema.py. Writes results as a '
                     'csv file.')
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
        '--input_batch_file', type=str, required=True,
        help='filename for batch input file (.pkl)'
    )
    parser.add_argument(
        '-o', '--output_file', type=str, required=True,
        help='filename for output file (.csv)'
    )
    parser.add_argument(
        '-t', '--thresholds', default=[0.02, 0.04, 0.02],
        nargs=3, metavar=('start', 'end', 'step'),
        type=float, help='specify a range of thresholds'
    )
    parser.add_argument(
        '--hard_rescaling', default=[120.0, 120.0, 500.0, 500.0],
        nargs=4, type=float,
        help='specify hard rescaling thresholds'
    )

    return(parser.parse_args())


def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_


def segment_cells(dapi,se):
    from jtmodules import smooth, fill, filter, threshold_manual, label, register_objects, segment_secondary

    dapi_smooth = smooth.main(dapi, 'gaussian', 3,plot=False)
    nuclei = threshold_manual.main(
        image=dapi_smooth.smoothed_image,
        threshold=115
    )
    nuclei = fill.main(nuclei.mask, plot=False)
    nuclei = filter.main(
        mask=nuclei.filled_mask,
        feature='area',
        lower_threshold=2000,
        upper_threshold=None,
        plot=False
    )
    nuclei = label.main(
        mask=nuclei.filtered_mask
    )
    nuclei = register_objects.main(
        nuclei.label_image
    )
    se_smooth = smooth.main(se, 'bilateral', 7)
    cells = segment_secondary.main(
        nuclei.objects,se_smooth.smoothed_image,
        contrast_threshold=3,
        min_threshold=116,
        max_threshold=120)

    return cells.secondary_label_image


def main(args):

    tmaps_api = TmClient(
        host=args.host,
        port=args.port,
        experiment_name=args.experiment,
        username=args.username,
        password=args.password
    )

    matlab = matlab_wrapper.MatlabSession(options='-nojvm')
    matlab.eval("addpath('~/repositories/JtLibrary/matlab/jtlibrary/')")

    # read rescaling_limits and aggregate by control
    selected_sites = pd.read_pickle(args.input_batch_file)

    # set options for ObjByFilter
    matlab.eval("op = cpsub.fspecialCP3D('3D LoG, Raj', 4.0, 1.0, 3.0);")
    detection_thresholds = np.arange(
        args.thresholds[0],
        args.thresholds[1],
        args.thresholds[2])
    matlab.workspace.iImgLimes = [0.01, 0.995]

    matlab.workspace.min_of_min = float(args.hard_rescaling[0])
    matlab.workspace.max_of_min = float(args.hard_rescaling[1])
    matlab.workspace.min_of_max = float(args.hard_rescaling[2])
    matlab.workspace.max_of_max = float(args.hard_rescaling[3])

    channels = tmaps_api.get_channels()
    sites = tmaps_api.get_sites()
    z_depth = len(channels[0]['layers'])

    spot_count = pd.DataFrame()
    for index, row in selected_sites.iterrows():

        dapi = tmaps_api.download_channel_image(
            channel_name='DAPI',
            plate_name=args.plate,
            well_name=row['well'],
            well_pos_y=row['site_y'],
            well_pos_x=row['site_x'],
            correct=False
        )

        se = tmaps_api.download_channel_image(
            channel_name='SE',
            plate_name=args.plate,
            well_name=row['well'],
            well_pos_y=row['site_y'],
            well_pos_x=row['site_x'],
            correct=False
        )

        cells = segment_cells(dapi, se)
        n_cells = np.max(cells)

        fish3D = np.zeros(
            (sites[0]['height'],sites[0]['width'],z_depth),
            dtype=np.uint16
        )

        for z in range(0,z_depth):
            fish = tmaps_api.download_channel_image(
                channel_name='FISH',
                plate_name=args.plate,
                well_name=row['well'],
                well_pos_y=row['site_y'],
                well_pos_x=row['site_x'],
                correct=False,
                zplane=z
            )
            fish[cells == 0] = 0
            fish3D[:,:,z] = fish

        matlab.workspace.fish3D = fish3D.tolist()

        for threshold in np.nditer(detection_thresholds):

            '''Note: second returned argument from ObjByFilter.m
            is matlab CC object (as a python dict) which stores the
            NumObjects attribute. This is the calculated spot count
            '''

            matlab.workspace.threshold = float(threshold)

            matlab.eval(
                "[ObjCount SegCC] = cpsub.ObjByFilter(double(fish3D)," +
                " op," +
                " threshold, iImgLimes," +
                "[min_of_min, max_of_min, min_of_max, max_of_max]," +
                " [], false, [], []);"
            )

            n_spots = matlab.get('ObjCount')
            spots_per_cell = (
                n_spots / float(n_cells) if n_cells > 0 else None
            )

            spot_count = spot_count.append(
                pd.DataFrame({
                    'rescaling_limit_1': args.hard_rescaling[0],
                    'rescaling_limit_2': args.hard_rescaling[1],
                    'rescaling_limit_3': args.hard_rescaling[2],
                    'rescaling_limit_4': args.hard_rescaling[3],
                    'threshold': threshold,
                    'well': row['well'],
                    'site_x': row['site_x'],
                    'site_y': row['site_y'],
                    'mean_spot_count_per_cell': spots_per_cell
                }, index=[index])
            )

    spot_count = selected_sites.merge(spot_count)
    spot_count.to_csv(args.output_file, encoding='utf-8', index=False)

    return


if __name__ == '__main__':
    arguments = parse_arguments()
    main(arguments)
