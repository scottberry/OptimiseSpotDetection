#! /usr/bin/env python
import sys

import os
from os.path import basename

import gc3libs
from gc3libs.cmdline import SessionBasedScript
from gc3libs import Application
from gc3libs.quantity import GB
from gc3libs.workflow import StagedTaskCollection, ParallelTaskCollection

if __name__ == '__main__':
    from optimise_spot_detection import OptimiseSpotDetectionScript
    OptimiseSpotDetectionScript().run()


class OptimiseSpotDetectionScript(SessionBasedScript):
    '''
    Script to scan a range of spot detection thresholds and calculate
    spot count per image for positive and negative controls
    '''

    def __init__(self):
        super(OptimiseSpotDetectionScript, self).__init__(version='1.0')

    def setup_args(self):
        self.add_param('--host', type=str, help=('TissueMAPS host address'))
        self.add_param('--username', type=str, help=('TissueMAPS username'))
        self.add_param('--password', type=str, help=('TissueMAPS password'))
        self.add_param('--experiment', type=str, help=('TissueMAPS experiment name'))
        self.add_param('--plate', type=str, help=('Plate name'))
        self.add_param('--channel', type=str, help=('Channel name'))
        self.add_param('--positive_wells', nargs='+', help=('Postive wells'))
        self.add_param('--negative_wells', nargs='+', help=('Negative wells'))
        self.add_param('--thresholds', nargs=3, default=[0.02, 0.04, 0.02],
                       type=float, help=('Thresholds to test'))
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
        return GetIntensityExtremaParallel(
            self.params.host,
            self.params.username,
            self.params.password,
            self.params.experiment,
            self.params.negative_wells,
            self.params.positive_wells,
            self.params.plate,
            self.params.channel,
            self.params.n_sites,
            self.params.n_batches
        )

    # Collect results and aggregate
    def stage1(self):
        return AggregateRescalingLimitsApp(
            self.params.n_batches,
            self.params.experiment
        )

    # Perform spot detection
    def stage2(self):
        return GetSpotCountThresholdSeriesParallel(
            self.params.host,
            self.params.username,
            self.params.password,
            self.params.experiment,
            self.params.plate,
            self.params.channel,
            self.params.thresholds,
            self.params.n_batches
        )


class GetIntensityExtremaParallel(ParallelTaskCollection):
    '''
    Run n_batches instances of GetIntensityExtremaApp in parallel
    '''

    def __init__(self, host, username, password, experiment,
                 negative_wells, positive_wells, plate, channel,
                 n_sites, n_batches):
        task_list = []
        for batch_id in range(n_batches):
            task_list.append(
                GetIntensityExtremaApp(
                    host, username, password, experiment,
                    negative_wells, positive_wells, plate, channel,
                    n_sites, batch_id
                )
            )
        ParallelTaskCollection.__init__(self, task_list, output_dir='')


class GetIntensityExtremaApp(Application):
    '''
    Get intensity extrema for a batch of images and write as python pickle
    '''

    def __init__(self, host, username, password, experiment,
                 negative_wells, positive_wells, plate, channel,
                 n_sites, batch_id):
        out = 'intensity_extrema_{num:03d}'.format(num=batch_id)
        out_dir = experiment + '_' + out
        Application.__init__(
            self,
            arguments=[
                './get_intensity_extrema.py',
                '--host', host,
                '--user', username,
                '--password', password,
                '--experiment', experiment,
                '--plate', plate,
                '--channel', channel,
                '--negative_wells', ' '.join(negative_wells),
                '--positive_wells', ' '.join(positive_wells),
                '--number_sites', n_sites,
                '--output_file', out + '.pkl'],
            inputs=['get_intensity_extrema.py'],
            outputs=[out + '.pkl'],
            output_dir=out_dir,
            stdout='stdout.txt',
            stderr='stderr.txt',
            requested_memory=1 * GB)


class AggregateRescalingLimitsApp(Application):
    '''
    Aggregate batches of results from GetIntensityExtremaApp
    '''

    def __init__(self, n_batches, experiment):
        input_list_filepath = []
        for batch_id in range(n_batches):
            input_list_filepath.append(
                os.path.join(
                    os.getcwd(),
                    experiment + '_intensity_extrema_{num:03d}'.format(num=batch_id),
                    'intensity_extrema_{num:03d}.pkl'.format(num=batch_id)
                )
            )
        input_list_filepath_exec = input_list_filepath[:]
        input_list_filepath_exec.append('aggregate_rescaling_limits.py')

        Application.__init__(
            self,
            arguments=[
                './aggregate_rescaling_limits.py',
                '--input_files'] + input_list_filepath + [
                '--output_file', 'aggregated_rescaling_limits.pkl'],
            inputs=input_list_filepath_exec,
            outputs=['aggregated_rescaling_limits.pkl'],
            output_dir=experiment + '_aggregated_extrema',
            stdout='stdout.txt',
            stderr='stderr.txt',
            requested_memory=1 * GB)


class GetSpotCountThresholdSeriesParallel(ParallelTaskCollection):
    '''
    Run n_batches instances of GetSpotCountThresholdSeriesApp in parallel
    '''

    def __init__(self, host, username, password, experiment,
                 plate, channel, thresholds, n_batches):
        task_list = []
        input_aggregate_file = os.path.join(
            os.getcwd(),
            experiment + '_aggregated_extrema',
            'aggregated_rescaling_limits.pkl'
        )
        for batch_id in range(n_batches):
            input_batch_file = os.path.join(
                os.getcwd(),
                experiment +
                '_intensity_extrema_{num:03d}'.format(num=batch_id),
                'intensity_extrema_{num:03d}.pkl'.format(num=batch_id)
            )
            task_list.append(
                GetSpotCountThresholdSeriesApp(
                    host, username, password, experiment,
                    plate, channel, input_batch_file,
                    input_aggregate_file, thresholds, batch_id
                )
            )
        ParallelTaskCollection.__init__(self, task_list, output_dir='')


class GetSpotCountThresholdSeriesApp(Application):
    '''
    Get spot count for a series of thresholds
    '''

    def __init__(self, host, username, password, experiment,
                 plate, channel, input_batch_file, input_aggregate_file,
                 thresholds, batch_id):

        out = 'spot_count_{num:03d}'.format(num=batch_id)
        Application.__init__(
            self,
            arguments=[
                './get_spot_count_threshold_series.py',
                '--host', host,
                '--user', username,
                '--password', password,
                '--experiment', experiment,
                '--plate', plate,
                '--channel', channel,
                '--input_batch_file', input_batch_file,
                '--input_aggregate_file', input_aggregate_file,
                '--output_file', out + '.csv'],
            inputs=[input_aggregate_file,
                    input_batch_file,
                    'get_spot_count_threshold_series.py'],
            outputs=[out + '.csv'],
            output_dir=experiment + '_' + out,
            stdout='stdout.txt',
            stderr='stderr.txt',
            requested_memory=1 * GB
        )
