import sys

import os
from os.path import basename

import gc3libs
from gc3libs.cmdline import SessionBasedScript
from gc3libs import Application
from gc3libs.quantity import GB
from gc3libs.workflow import StagedTaskCollection, ParallelTaskCollection

if __name__ == '__main__':
    from optimise_spot_detection_3D import OptimiseSpotDetection3DScript
    OptimiseSpotDetection3DScript().run()


class OptimiseSpotDetection3DScript(SessionBasedScript):
    '''
    Script to scan a range of spot detection thresholds and calculate
    spot count per image for positive and negative controls
    '''

    def __init__(self):
        super(OptimiseSpotDetection3DScript, self).__init__(version='1.0')

    def setup_args(self):
        self.add_param('--host', type=str, help=('TissueMAPS host address'))
        self.add_param('--username', type=str, help=('TissueMAPS username'))
        self.add_param('--password', type=str, help=('TissueMAPS password'))
        self.add_param('--experiment', type=str,
                       help=('TissueMAPS experiment name'))
        self.add_param('--plate', type=str, help=('Plate name'))
        self.add_param('--positive_wells', nargs='+', help=('Postive wells'))
        self.add_param('--negative_wells', nargs='+', help=('Negative wells'))
        self.add_param('--thresholds', nargs=3, default=[0.02, 0.04, 0.02],
                       type=float, help=('Thresholds to test'))
        self.add_param('--hard_rescaling', nargs=4,
                       default=[120.0, 120.0, 1000.0, 1000.0],
                       type=float, help=('Hard rescaling thresholds'))
        self.add_param('--n_sites', type=int,
                       help=('Batch size: number of images per well'))
        self.add_param('--n_batches', type=int, help=('Number of batches'))

    def new_tasks(self, extra):
        apps = [OptimiseSpotDetectionPipeline(self.params)]
        return apps


class OptimiseSpotDetectionPipeline(StagedTaskCollection):
    '''
    Define the pipeline for optimisation of spot_detection batches
    '''

    def __init__(self, params):
        self.params = params
        StagedTaskCollection.__init__(self, output_dir='')

    # Get intensity extrema
    def stage0(self):
        return SelectSitesParallel(
            self.params.host,
            self.params.username,
            self.params.password,
            self.params.experiment,
            self.params.negative_wells,
            self.params.positive_wells,
            self.params.plate,
            self.params.n_sites,
            self.params.n_batches
        )

    # Perform spot detection
    def stage1(self):
        return GetSpotCountThresholdSeries3DParallel(
            self.params.host,
            self.params.username,
            self.params.password,
            self.params.experiment,
            self.params.plate,
            self.params.thresholds,
            self.params.n_batches,
            self.params.hard_rescaling
        )

    # Aggregate spot detection
    def stage2(self):
        return AggregateSpotCountThresholdSeriesApp(
            self.params.n_batches,
            self.params.experiment
        )


class SelectSitesParallel(ParallelTaskCollection):
    '''
    Run n_batches instances of GetIntensityExtremaApp in parallel
    '''

    def __init__(self, host, username, password, experiment,
                 negative_wells, positive_wells, plate,
                 n_sites, n_batches):
        task_list = []
        for batch_id in range(n_batches):
            task_list.append(
                GetSites3DApp(
                    host, username, password, experiment,
                    negative_wells, positive_wells, plate,
                    n_sites, batch_id
                )
            )
        ParallelTaskCollection.__init__(self, task_list, output_dir='')


class GetSites3DApp(Application):
    '''
    Get sites for a batch of images and write as python pickle
    '''

    def __init__(self, host, username, password, experiment,
                 negative_wells, positive_wells, plate,
                 n_sites, batch_id):
        out = 'selected_sites_{num:03d}'.format(num=batch_id)
        out_dir = os.path.join(experiment, out)
        Application.__init__(
            self,
            arguments=[
                'python',
                'select_sites_3D.py',
                '--host', host,
                '--user', username,
                '--password', password,
                '--experiment', experiment,
                '--plate', plate,
                '--negative_wells', ' '.join(negative_wells),
                '--positive_wells', ' '.join(positive_wells),
                '--number_sites', n_sites,
                '--output_file', out + '.pkl'],
            inputs=['select_sites_3D.py'],
            outputs=[out + '.pkl'],
            output_dir=out_dir,
            stdout='stdout.txt',
            stderr='stderr.txt',
            requested_memory=3 * GB)


class GetSpotCountThresholdSeries3DParallel(ParallelTaskCollection):
    '''
    Run n_batches instances of GetSpotCountThresholdSeriesApp in parallel
    '''

    def __init__(self, host, username, password, experiment,
                 plate, thresholds, n_batches, hard_rescaling):
        task_list = []
        for batch_id in range(n_batches):
            input_batch_file = os.path.join(
                os.getcwd(),
                experiment,
                'selected_sites_{num:03d}'.format(num=batch_id),
                'selected_sites_{num:03d}.pkl'.format(num=batch_id)
            )
            task_list.append(
                GetSpotCountThresholdSeries3DApp(
                    host, username, password, experiment,
                    plate, input_batch_file,
                    thresholds,
                    batch_id, hard_rescaling
                )
            )
        ParallelTaskCollection.__init__(self, task_list, output_dir='')


class GetSpotCountThresholdSeries3DApp(Application):
    '''
    Get spot count for a series of thresholds
    '''

    def __init__(self, host, username, password, experiment,
                 plate, input_batch_file,
                 thresholds, batch_id, hard_rescaling):

        out = 'spot_count_{num:03d}'.format(num=batch_id)
        output_dir = os.path.join(experiment, out)
        Application.__init__(
            self,
            arguments=[
                'python',
                'get_spot_count_threshold_series_3D_mw.py',
                '--host', host,
                '--user', username,
                '--password', password,
                '--experiment', experiment,
                '--thresholds'] + thresholds + [
                '--hard_rescaling'] + hard_rescaling + [
                '--plate', plate,
                '--input_batch_file', input_batch_file,
                '--output_file', out + '.csv'],
            inputs=[input_batch_file,
                    'get_spot_count_threshold_series_3D_mw.py'],
            outputs=[out + '.csv'],
            output_dir=output_dir,
            stdout='stdout.txt',
            stderr='stderr.txt',
            requested_memory=7 * GB
        )


class AggregateSpotCountThresholdSeriesApp(Application):
    '''
    Aggregate spot count results into a single csv file
    '''

    def __init__(self, n_batches, experiment):

        input_filepath_list = []
        out = 'aggregated_spot_count.csv'
        output_dir = os.path.join(experiment, 'aggregated_spot_count')

        for batch_id in range(n_batches):
            input_filepath_list.append(
                os.path.join(
                    os.getcwd(),
                    experiment,
                    'spot_count_{num:03d}'.format(num=batch_id),
                    'spot_count_{num:03d}.csv'.format(num=batch_id)
                )
            )

        Application.__init__(
            self,
            arguments=['./concatenate_csv.sh'] + input_filepath_list,
            inputs=['concatenate_csv.sh'] + input_filepath_list,
            outputs=[out],
            output_dir=output_dir,
            stdout=out,
            stderr='stderr.txt',
            requested_memory=1 * GB
        )
