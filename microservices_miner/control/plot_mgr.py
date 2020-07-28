# (C) Copyright IBM Corporation 2018
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from datetime import datetime
from matplotlib.pyplot import figure
import os
import numpy as np
from typing import List, Tuple
plt.rcParams.update({'font.size': 10})


class PlotMgr:
    DPI = 1200
    FIGURES_DIR = 'figures'
    DEFECT_DENSITY_CSV_FILE = 'defect_density.csv'
    WIDTH = 8
    HEIGHT = 4

    def __init__(self, db_path: str):
        self.db_path = db_path

    @staticmethod
    def _get_full_path(filename):
        """

        Parameters
        ----------
        filename

        Returns
        -------

        """
        base_dir = os.getenv('BASE_DIR')
        full_path = os.path.join(base_dir, PlotMgr.FIGURES_DIR, filename)
        return full_path

    @staticmethod
    def plot_modifications_over_time(df, col_name):
        """

        Parameters
        ----------
        df: pd.Dataframe
        col_name: str

        Returns
        -------

        """
        df.dropna(axis=0, inplace=True)
        fig = figure(num=None, figsize=(PlotMgr.WIDTH, PlotMgr.HEIGHT), dpi=PlotMgr.DPI, facecolor='w', edgecolor='k')
        ax = plt.gca()
        ax.yaxis.grid(True)
        sns.set_style("whitegrid")
        g = sns.lineplot(x="date", y="changes", ci='sd', data=df)
        filename = 'metric-{}.pdf'.format(col_name)
        full_path = PlotMgr._get_full_path(filename)
        plt.tight_layout()
        fig.savefig(full_path, dpi=PlotMgr.DPI, bbox_inches='tight')
        plt.close()
        print('Saved figure: {}'.format(full_path))

    @staticmethod
    def bar_plot(df, x, y, xtickslabels, y_label, x_label, filename):
        """

        Parameters
        ----------
        df: pd.DataFrame
        x: str
        y: str
        xtickslabels
        y_label
        x_label
        filename

        Returns
        -------

        """

        fig = figure(num=None, figsize=(PlotMgr.WIDTH, PlotMgr.HEIGHT), dpi=PlotMgr.DPI, facecolor='w', edgecolor='k')
        ax = plt.gca()
        ax.yaxis.grid(True)
        sns.set_style("whitegrid")
        g = sns.barplot(y=y,  # Y-axis - values for boxplot
                        x=x,
                        data=df,
                        palette='pastel')
        full_path = PlotMgr._get_full_path(filename)
        g.set(ylabel=y_label)
        g.set(xlabel=x_label)
        g.set(xticklabels=xtickslabels)
        plt.tight_layout()
        fig.savefig(full_path, dpi=PlotMgr.DPI, bbox_inches='tight')
        plt.close()
        print('Saved figure: {}'.format(full_path))

    @staticmethod
    def plot_loc_growth_over_time(data: dict, time_bins: Tuple, group_by):
        """

        Parameters
        ----------
        data: dict
        time_bins: List
        group_by: str

        Returns
        -------

        """
        fig = figure(num=None, figsize=(PlotMgr.WIDTH, PlotMgr.HEIGHT), dpi=PlotMgr.DPI, facecolor='w', edgecolor='k')
        ax = plt.gca()
        ax.yaxis.grid(True)
        all = list()
        for service_name, loc_list in data.items():
            plt.plot(time_bins[1:], loc_list, label=service_name)
            all.append(loc_list)
        # median_list = np.median(all, axis=0)

        # plt.plot(time_bins[1:], median_list, label='median', linestyle='dashed')
        # plt.legend(loc='best')
        plt.xlabel('time')
        plt.ylabel('LOC')
        plt.yscale('log')

        # plt.show()
        # plt.legend(bbox_to_anchor=(1.45, 0.5), loc="center right")
        filename = 'loc-growth-groupby-{}.pdf'.format(group_by)
        full_path = PlotMgr._get_full_path(filename)
        plt.tight_layout()
        fig.savefig(full_path, dpi=PlotMgr.DPI, bbox_inches='tight')
        plt.close()
        print('Saved figure: {}'.format(full_path))

    @staticmethod
    def plot_loc_per_operation(df):
        """

        Parameters
        ----------
        df

        Returns
        -------

        """
        df.dropna(axis=0, inplace=True)
        fig = figure(num=None, figsize=(PlotMgr.WIDTH, PlotMgr.HEIGHT), dpi=PlotMgr.DPI, facecolor='w', edgecolor='k')
        ax = plt.gca()
        ax.yaxis.grid(True)
        sns.set_style("whitegrid")
        g = sns.lineplot(x="date", y="locperoperation", hue='service', data=df, legend=False)
        g.set(ylabel='LOC per operation')
        filename = 'loc-per-operation.pdf'
        full_path = PlotMgr._get_full_path(filename)
        plt.yscale('log', basey=2)
        plt.tight_layout()
        fig.savefig(full_path, dpi=PlotMgr.DPI, bbox_inches='tight')
        plt.close()
        print('Saved figure: {}'.format(full_path))

    @staticmethod
    def plot_loc_per_operation_summary(df):
        """

        Parameters
        ----------
        df: pd.DataFrame

        Returns
        -------
        None
        """
        # remove NA
        df.dropna(axis=0, inplace=True)
        # cast column to numeric
        df['loc_operation'] = pd.to_numeric(df['loc_operation'])
        fig = figure(num=None, figsize=(PlotMgr.WIDTH, PlotMgr.HEIGHT), dpi=PlotMgr.DPI, facecolor='w', edgecolor='k')
        ax = plt.gca()
        ax.yaxis.grid(True)
        sns.set_style("whitegrid")
        df.to_csv(PlotMgr._get_full_path('loc_per_operation.csv'))
        prefix = 'loc-per-operation-group'
        g = sns.lineplot(x="date", y="loc_operation", ci='sd', data=df, legend=False, marker='o')
        g = sns.lineplot(x="date", y="loc_operation", ci=None, data=df, legend=False, marker='*', estimator='median')
        median_df = df.groupby(by='date').median()
        median_df.to_csv(PlotMgr._get_full_path('loc_operation_median.csv'))
        average_df = df.groupby(by='date').mean()
        average_df.to_csv(PlotMgr._get_full_path('loc_operation_average.csv'))
        g.set(ylabel='LOC per operation')
        # g.set_xticklabels(years)
        filename = '{}.pdf'.format(prefix)
        plt.tight_layout()
        full_path = PlotMgr._get_full_path(filename)
        fig.legend(labels=['average', 'median'])
        fig.savefig(full_path, dpi=PlotMgr.DPI, bbox_inches='tight')
        plt.close()
        print('Saved figure: {}'.format(full_path))

    @staticmethod
    def plot_change_per_loc_over_time(data, time_bins, group_by):
        """

        plots the number of changes divided by the number of LOC

        Parameters
        ----------
        data: dict
        time_bins: list
        group_by: str

        Returns
        -------

        """
        fig = figure(num=None, figsize=(PlotMgr.WIDTH, PlotMgr.HEIGHT), dpi=PlotMgr.DPI, facecolor='w', edgecolor='k')
        ax = plt.gca()
        ax.grid(True)
        avg = list()
        x = sorted(time_bins[1:])
        for service_name, change_per_loc_list in data.items():
            avg.append(change_per_loc_list)
            plt.scatter(x, change_per_loc_list, label=service_name)
        avg_list = np.nanmean(avg, axis=0)
        # median_list = np.nanmedian(avg, axis=0)
        # plt.legend(loc='best')
        plt.plot(x, avg_list)
        # plt.show()
        # plt.yscale('log')
        filename = 'changes-per-loc-group-by-{}.pdf'.format(group_by)
        # fig = plt.figure(figsize=(6, 4))
        plt.xlabel('time')
        plt.ylabel('changes/LOC')
        # fig = plt.figure(figsize=(5,4))
        full_path = PlotMgr._get_full_path(filename)
        plt.tight_layout()
        fig.savefig(full_path, dpi=PlotMgr.DPI, bbox_inches='tight')
        plt.close()
        print('Saved figure: {}'.format(full_path))

    @staticmethod
    def plot_stats_change_per_loc_over_time(data, time_bins, group_by):
        """

        plots the number of changes divided by the number of LOC

        Parameters
        ----------
        data: dict
        time_bins: list
        group_by: str

        Returns
        -------

        """
        fig = figure(num=None, figsize=(PlotMgr.WIDTH, PlotMgr.HEIGHT), dpi=PlotMgr.DPI, facecolor='w', edgecolor='k')
        ax = plt.gca()
        ax.yaxis.grid()

        x = sorted(time_bins[1:])
        changes = list()
        services = list()
        dates = list()
        for service_name, change_per_loc_list in data.items():
            changes.extend(change_per_loc_list)
            services.extend([service_name] * len(change_per_loc_list))
            dates.extend(x)
        df = pd.DataFrame(data={'service': services, 'changes_per_loc': changes, 'date': dates})
        filename = 'changes-per-loc-group-by-{}.pdf'.format(group_by)
        full_path = PlotMgr._get_full_path(filename)
        plt.tight_layout()
        g = sns.lineplot(x="date", y="changes_per_loc", ci='sd', data=df, legend=False, marker='o')
        g = sns.lineplot(x="date", y="changes_per_loc", ci=None, data=df, legend=False, marker='*', estimator='median')
        median_df = df.groupby(by='date').median()
        median_df.to_csv(PlotMgr._get_full_path('median_changes_per_loc.csv'))
        average_df = df.groupby(by='date').mean()
        average_df.to_csv(PlotMgr._get_full_path('average_changes_per_loc.csv'))
        g.set(ylabel='Number of changes per LOC')
        fig.legend(labels=['average','median'])
        plt.tight_layout()
        fig.savefig(full_path, dpi=PlotMgr.DPI, bbox_inches='tight')
        plt.close()
        print('Saved figure: {}'.format(full_path))

    @staticmethod
    def plot_stats_loc_over_time(data, time_bins, group_by):
        """

        Parameters
        ----------
        data
        time_bins
        group_by

        Returns
        -------

        """
        fig = figure(num=None, figsize=(PlotMgr.WIDTH, PlotMgr.HEIGHT), dpi=PlotMgr.DPI, facecolor='w', edgecolor='k')
        ax = plt.gca()
        ax.yaxis.grid()

        x = sorted(time_bins[1:])
        loc = list()
        services = list()
        dates = list()
        for service_name, loc_aux in data.items():
            loc.extend(loc_aux)
            services.extend([service_name] * len(loc_aux))
            dates.extend(x)

        df = pd.DataFrame(data={'service': services, 'LOC': loc, 'date': dates})
        df = df[df.LOC != 0]
        df.to_csv(PlotMgr._get_full_path('loc_growth_per_service.csv'), sep=',')
        median_df = df.groupby(by='date').median()
        print('median')
        print(median_df)

        median_df.to_csv(PlotMgr._get_full_path('loc_growth_median.csv'), sep=',')
        mean_df = df.groupby(by='date').mean()
        print('mean')
        print(mean_df)

        std_df = df.groupby(by='date').std()
        print('standard deviation')
        print(std_df)

        mean_df.to_csv(PlotMgr._get_full_path('loc_growth_mean.csv'), sep=',')
        filename = 'loc-growth-stats-{}.pdf'.format(group_by)
        full_path = PlotMgr._get_full_path(filename)
        plt.tight_layout()
        g = sns.lineplot(x="date", y="LOC", ci='sd', data=df, legend=False, marker='o')
        g = sns.lineplot(x="date", y="LOC", ci=None, data=df, legend=False, marker='*', estimator='median')
        g.set(ylabel='LOC per microservice')
        fig.legend(labels=['average','median'])
        plt.tight_layout()
        fig.savefig(full_path, dpi=PlotMgr.DPI, bbox_inches='tight')
        plt.close()
        print('Saved figure: {}'.format(full_path))

    @staticmethod
    def scatter_plot(df, x, y, hue, filename):
        """

        Parameters
        ----------
        df

        Returns
        -------

        """
        fig = figure(num=None, figsize=(PlotMgr.WIDTH, PlotMgr.HEIGHT), dpi=PlotMgr.DPI, facecolor='w', edgecolor='k')
        g = sns.scatterplot(x=x, y=y, hue=hue, data=df, legend=False)
        g.set(ylabel='Coupling')
        # plt.xscale('log')
        plt.tight_layout()
        full_path = PlotMgr._get_full_path(filename)
        fig.savefig(full_path, dpi=PlotMgr.DPI, bbox_inches='tight')
        plt.close()
        print('Saved figure: {}'.format(full_path))

    @staticmethod
    def reg_plot(df, x, y, ylabel, xlabel, filename):
        """

        Parameters
        ----------
        df

        Returns
        -------

        """
        fig = figure(num=None, figsize=(PlotMgr.WIDTH, PlotMgr.HEIGHT), dpi=PlotMgr.DPI, facecolor='w', edgecolor='k')
        g = sns.regplot(x=x, y=y, data=df)
        g.set(ylabel=ylabel, xlabel=xlabel)
        plt.tight_layout()
        full_path = PlotMgr._get_full_path(filename)
        fig.savefig(full_path, dpi=PlotMgr.DPI, bbox_inches='tight')
        plt.close()
        print('Saved figure: {}'.format(full_path))

    @staticmethod
    def plot_coupling(df: pd.DataFrame, prefix):
        """

        plots coupling using scatterplot

        Parameters
        ----------
        df: pd.DataFrame
        prefix: str

        Returns
        -------

        """

        # remove NA
        df.dropna(axis=0, inplace=True)
        index = df[df.service == 'UI'].index
        df.drop(index, inplace=True)
        # cast column to numeric
        # df['loc_operation'] = pd.to_numeric(df['loc_operation'])
        fig = figure(num=None, figsize=(PlotMgr.WIDTH, PlotMgr.HEIGHT), dpi=PlotMgr.DPI, facecolor='w', edgecolor='k')
        ax = plt.gca()
        ax.yaxis.grid(True)
        # sns.set_style("whitegrid")
        col_name = 'coupling'
        g = sns.lineplot(x="date", y=col_name, ci='sd', data=df, legend=False, marker='o')
        g = sns.lineplot(x="date", y=col_name, ci=None, data=df, legend=False, marker='*', estimator='median')
        median_df = df.groupby(by='date').median()
        median_df.to_csv(PlotMgr._get_full_path('median_{}.csv'.format(prefix)))
        average_df = df.groupby(by='date').mean()
        average_df.to_csv(PlotMgr._get_full_path('average_{}.csv'.format(prefix)))
        g.set(ylabel='Coupling')
        filename = '{}.pdf'.format(prefix)
        plt.tight_layout()
        full_path = PlotMgr._get_full_path(filename)
        fig.legend(labels=['average', 'median'])
        fig.savefig(full_path, dpi=PlotMgr.DPI, bbox_inches='tight')
        plt.close()
        print('Saved figure: {}'.format(full_path))

    @staticmethod
    def lineplot(df, hue, x, y, coupling_type):
        fig = figure(num=None, figsize=(PlotMgr.WIDTH+4, PlotMgr.HEIGHT), dpi=PlotMgr.DPI, facecolor='w', edgecolor='k')
        ax = plt.gca()
        ax.yaxis.grid(True)
        g = sns.lineplot(x=x, y=y, data=df, hue=hue)
        # xticks_label = df['date'].tolist()
        # g.set_xticklabels(xticks_label)
        full_path = PlotMgr._get_full_path('{}_changed_services.png'.format(coupling_type))
        fig.savefig(full_path, dpi=PlotMgr.DPI, bbox_inches='tight')
        print('Saved: {}'.format(full_path))
        plt.close()

    @staticmethod
    def plot_microservices_size(dates, microservices_size):
        """


        Parameters
        ----------
        dates: list of datetime
        microservices_size: list of int
        Returns
        -------

        """
        fig, ax = plt.subplots(figsize=(PlotMgr.WIDTH, PlotMgr.HEIGHT))
        ax = plt.gca()
        ax.grid(True)
        y_ticks = np.arange(min(microservices_size), max(microservices_size) + 1, 3.0)
        plt.yticks(y_ticks)
        sns.set(font_scale=3)
        sns.lineplot(x=dates, y=microservices_size, marker='o')
        plt.tight_layout()
        filename = 'num_microservices_over_time.pdf'
        full_path = PlotMgr._get_full_path(filename)
        fig.savefig(full_path, dpi=PlotMgr.DPI)
        plt.close()
        print('Saved figure: {}'.format(full_path))

    @staticmethod
    def boxplot_time_to_repair(df):
        """

        Parameters
        ----------
        df: pd.DataFrame

        Returns
        -------

        """
        fig, ax = plt.subplots(figsize=(PlotMgr.WIDTH, PlotMgr.HEIGHT))
        # df = pd.DataFrame(data={'time_to_repair': time_to_repair})
        sns.catplot(kind='box',  # Boxplot
                    y='time_to_repair',  # Y-axis - values for boxplot
                    hue='closed_at',
                    data=df,  # Dataframe
                    height=8,  # Figure size (x100px)
                    aspect=1.5,  # Width = size * aspect
                    whis=1.5,
                    showfliers=False,
                    fliersize=0,
                    palette='pastel',
                    legend_out=False)  # Make legend inside the plot

        filename = 'boxplot_time_to_repair.pdf'
        full_path = PlotMgr._get_full_path(filename)
        fig.savefig(full_path, dpi=PlotMgr.DPI)
        plt.close()
        print('Saved figure: {}'.format(full_path))

    @staticmethod
    def plot_bugs_vs_changes(df, filename):
        """

        Parameters
        ----------
        df: pd.DataFrame
        Returns
        -------

        """

        df.plot()

        # plt.show()
        full_path = PlotMgr._get_full_path(filename)
        plt.savefig(full_path, dpi=PlotMgr.DPI, bbox_inches='tight')
        print('Saved figure: {}'.format(full_path))
        plt.close()

    @staticmethod
    def plot_time_to_repair_over_time(df, x, y):
        """
        Plot a boxplot on the relation between time to repair bugs (in days) over the time

        Parameters
        ----------
        df: pd.DataFrame
        x: str
        y: str

        Returns
        -------

        """
        fig = figure(num=None, figsize=(PlotMgr.WIDTH, PlotMgr.HEIGHT), dpi=PlotMgr.DPI, facecolor='w', edgecolor='k')

        ax = sns.boxplot(x=x, y=y, data=df, showfliers=False, palette='pastel')
        ax.set(ylabel='number of days to repair', xlabel='year')
        ax.yaxis.grid(True)
        filename = 'boxplot-repair-time.pdf'
        full_path = PlotMgr._get_full_path(filename)
        fig.savefig(full_path, dpi=PlotMgr.DPI, bbox_inches='tight')

        plt.close()
        print('Saved fig: {}'.format(full_path))

    @staticmethod
    def plot_loc_time_to_repair(df):
        """

        Parameters
        ----------
        df: pd.DataFrame

        Returns
        -------

        """
        fig, ax = plt.subplots(figsize=(PlotMgr.WIDTH, PlotMgr.HEIGHT))
        df.dropna(axis=0, inplace=True)
        sns.regplot(x='num_loc', y='num_bugs', data=df)
        plt.tight_layout()
        filename = 'regplot_LOC_bugs.pdf'
        full_path = PlotMgr._get_full_path(filename)
        fig.savefig(full_path, dpi=PlotMgr.DPI)
        plt.close()
        print('Saved figure: {}'.format(full_path))