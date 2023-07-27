#!/usr/bin/env python3
from __future__ import print_function
import datetime
import json
from pathlib import Path
from typing import Union
import requests
import os
import argparse
import logging
from googleapiclient.discovery import build
from google.oauth2 import service_account
from dateutil import parser as dateparser, tz


DT = datetime.datetime


class Google(object):
    """
    Class for connecting to API and retreiving longs
    """

    # These applications will be collected by default
    # DEFAULT_APPLICATIONS = ['login', 'drive', 'admin', 'user_accounts', 'chat', 'calendar', 'token']

    DEFAULT_APPLICATIONS = ["chrome", "admin", "access_transparency", "context_aware_access", "gplus", "data_studio", "mobile", "groups_enterprise",
                            "calendar", "chat", "gcp", "drive", "groups", "keep", "meet", "jamboard", "login", "token", "rules", "saml", "user_accounts"]

    def __init__(self, **kwargs):
        self.SERVICE_ACCOUNT_FILE = kwargs['creds_path']
        self.delegated_creds = kwargs['delegated_creds']
        self.output_path = kwargs['output_path']
        self.app_list = kwargs['apps']
        self.update = kwargs['update']
        self.overwrite = kwargs['overwrite']

        # Create output path if required
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)

        # Connect to Google API
        self.service = self.google_session()

    @staticmethod
    def get_application_list():
        """ 
        Returns a list of valid applicationName parameters for the activities.list() API method 
        Note: this is the complete list of valid options, and some may not be valid on particular accounts.
        """
        r = requests.get(
            'https://admin.googleapis.com/$discovery/rest?version=reports_v1')
        return r.json()['resources']['activities']['methods']['list']['parameters']['applicationName']['enum']

    @staticmethod
    def _check_recent_date(log_file_path):
        """
        Opens an existing log file to find the datetime of the most recent record
        """
        return_date = None
        if os.path.exists(log_file_path):
            with open(log_file_path, 'r') as f:
                for line in f.readlines():
                    json_obj = json.loads(line)
                    line_datetime = dateparser.parse(json_obj['id']['time'])
                    if not return_date:
                        return_date = line_datetime
                    elif return_date < line_datetime:
                        return_date = line_datetime
        return return_date

    def google_session(self):
        """
        Establish connection to Google Workspace.
        """
        SCOPES = ['https://www.googleapis.com/auth/admin.reports.audit.readonly',
                  'https://www.googleapis.com/auth/apps.alerts']
        creds = service_account.Credentials.from_service_account_file(
            self.SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        delegated_credentials = creds.with_subject(self.delegated_creds)

        service = build('admin', 'reports_v1',
                        credentials=delegated_credentials)

        return service

    def get_logs(self, from_date=None, to_date=None):
        """ 
        Collect all logs from specified applications
        """

        total_saved, total_found = 0, 0

        for app in self.app_list:

            # Define output file name
            folder = f"{self.output_path}/{from_date}_{to_date}"

            if not Path(folder).exists():
                logging.info(f"Creating log path: {folder}")
                Path(folder).mkdir(parents=True, exist_ok=True)

            output_file = f"{folder}/{app}_logs.json"

            # Get most recent log entry date (if required)
            if self.update:
                from_date = self._check_recent_date(output_file) or from_date

            saved, found = self._get_activity_logs(
                app,
                output_file=output_file,
                overwrite=self.overwrite,
                start_time=from_date,
                end_time=to_date
            )
            logging.info(f"Saved {saved} of {found} entries for {app}")
            total_saved += saved
            total_found += found

        logging.info(f"TOTAL: Saved {total_saved} of {total_found} records.")

    def _get_activity_logs(self, application_name, output_file, overwrite=False, start_time=None, end_time=None):
        """ Collect activitiy logs from the specified application """

        page_token = None
        output_count = 0
        total_records = 0

        while True:
            # Call the Admin SDK Reports API
            try:
                results = self.service.activities().list(
                    userKey='all', applicationName=application_name, pageToken=page_token, startTime=start_time, endTime=end_time).execute()
            except TypeError as e:
                logging.error(
                    f"Error collecting logs for {application_name}: {e}")
                return False, False

            page_token = results.get('nextPageToken', "")

            activities = results.get('items', [])

            if activities:
                total_records += len(activities)
                with open(output_file, 'w' if overwrite else 'a') as output:

                    # Loop through activities in reverse order (so latest events are at the end)
                    for entry in activities[::-1]:
                        # TODO: See if we can speed this up to prevent looping through all activities

                        # Output this record
                        json_formatted_str = json.dumps(entry)
                        output.write(f"{json_formatted_str}\n")
                        output_count += 1

            if not page_token:
                break

        return output_count, total_records


def get_start_of_the_day(day: DT, fmt: str = '%Y-%m-%dT%H:%M:%SZ'):
    return datetime.datetime.combine(day, datetime.time.min)


def get_end_of_the_day(day: DT, fmt: str = '%Y-%m-%dT%H:%M:%SZ'):
    return datetime.datetime.combine(day, datetime.time.max)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='This script will fetch Google Workspace logs.')

    # Configure with single config file
    parser.add_argument('--config', '-c', required=False, default='config.json',
                        help="Configuration file containing required arguments")

    # Or parse arguments separately
    parser.add_argument('--creds-path', required=False,
                        help=".json credential file for the service account.")
    parser.add_argument('--delegated-creds', required=False,
                        help="Principal name of the service account")
    parser.add_argument('--output-path', '-o', required=False,
                        help="Folder to save downloaded logs")
    parser.add_argument('--apps', '-a', required=False, default=','.join(Google.DEFAULT_APPLICATIONS),
                        help="Comma separated list of applications whose logs will be downloaded. "
                        "Or 'all' to attempt to download all available logs")

    parser.add_argument('--start-time', required=False, default=None,
                        type=str, help="Start collecting from date (RFC3339 format)")
    parser.add_argument('--end-time', required=False, default=None,
                        type=str, help="Collect until date (RFC3339 format)")
    parser.add_argument('--daily', required=False, action="store_true",
                        help="Split requests by day.")

    # Update/overwrite behaviour
    parser.add_argument('--update', '-u', required=False, action="store_true",
                        help="Update existing log files (if present). This will only save new log records.")
    parser.add_argument('--overwrite', required=False, action="store_true",
                        help="Overwrite existing log files (if present), with all available (or requested) log records.")

    # Logging/output levels
    parser.add_argument('--quiet', '-q', dest="log_level", action='store_const',
                        const=logging.ERROR, default=logging.INFO,
                        help="Prevent all output except errors")
    parser.add_argument('--debug', '-v', dest="log_level", action='store_const',
                        const=logging.DEBUG, default=logging.INFO,
                        help="Show debug/verbose output.")

    args = parser.parse_args()

    # Setup logging
    FORMAT = '%(asctime)s %(levelname)-8s %(message)s'
    logging.basicConfig(format=FORMAT, level=args.log_level)

    # Load values from config file, but don't overwrite arguments specified separately
    # (i.e. args passed at the command line overwrite values stored in config file)
    if args.config and os.path.exists(args.config):
        with open(args.config) as json_data_file:
            config = json.load(json_data_file)
        for key in config:
            if key not in args or not vars(args)[key]:
                vars(args)[key] = config[key]

    # Convert apps argument to list
    if args.apps.strip().lower() == 'all':
        args.apps = Google.get_application_list()
    elif args.apps:
        args.apps = [a.strip().lower() for a in args.apps.split(',')]

    # DEBUG: Show combined arguments to be used
    logging.debug(args)

    # Connect to Google API
    google = Google(**vars(args))
    if args.daily:
        fmt = '%Y-%m-%dT%H:%M:%SZ'
        start_time = datetime.datetime.strptime(args.start_time, fmt)
        end_time = datetime.datetime.strptime(args.end_time, fmt)

        while start_time < end_time:
            end_of_day = get_end_of_the_day(start_time)

            if end_of_day > end_time:
                end_of_day = end_time


            from_date = datetime.datetime.strftime(start_time, fmt)
            to_date = datetime.datetime.strftime(end_of_day, fmt)

            # Collect logs for specified app
            logging.info(
                f"Collecting logs from {from_date} -> {to_date}")

            google.get_logs(
                from_date=datetime.datetime.strftime(start_time, fmt),
                to_date=datetime.datetime.strftime(end_of_day, fmt),
            )
            start_time = start_time + datetime.timedelta(days=1)
            start_time = get_start_of_the_day(start_time)
    else:
        google.get_logs(args.from_date)
