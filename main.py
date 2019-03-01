#!/usr/bin/env python3

import io
import locale
import os
import socket
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree
import yaml
import zipfile

from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

locale.setlocale(locale.LC_ALL, 'C')

scriptdir = Path(__file__).resolve().parent
logdir = scriptdir / 'logs'
configpath = scriptdir / 'config.yaml'
lastrunpath = scriptdir / 'lastrun'
utilpath = scriptdir / 'idevsutil_dedup'

with configpath.open(mode='rt', encoding='utf-8') as configfile:
    config = yaml.safe_load(configfile)

if not logdir.is_dir():
    logdir.mkdir()

# Time to connect to the local network
time.sleep(300)

if not utilpath.is_file():
    print('Downloading idevsutil_dedup binary...')
    out = subprocess.check_output(['curl', 'https://www.idrivedownloads.com/downloads/linux/download-options/IDrive_linux_64bit.zip'])

    with zipfile.ZipFile(io.BytesIO(out)) as zipfile:
        Path(zipfile.extract('IDrive_linux_64bit/idevsutil_dedup')).rename(utilpath)
        Path('IDrive_linux_64bit').rmdir()
        utilpath.chmod(0o755)


def run_util(*args):
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8') as passfile:
        passfile.write(config['password'])
        passfile.flush()

        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8') as keyfile:
            keyfile.write(config['encryption_key'])
            keyfile.flush()

            cmd = [str(utilpath), '--password-file=' + passfile.name, '--pvt-key=' + keyfile.name]
            cmd.extend(args)
            return subprocess.check_output(cmd).decode('utf-8')


def run_util_tree(*args):
    out = run_util(*args)

    pos = out.find('<tree')
    if pos != -1:
        out = out[pos:]

    return xml.etree.ElementTree.fromstring(out)


def run_util_items(*args):
    out = run_util(*args)

    items = list()

    for line in out.splitlines():
        if line.startswith('<item'):
            items.append(xml.etree.ElementTree.fromstring(line))

    return items


srvaddr = run_util_tree('--getServerAddress', config['username']).get('cmdUtilityServerIP')
print('Using IDrive server {}...'.format(srvaddr))

for device in run_util_items('--list-device', '{}@{}::home/'.format(config['username'], srvaddr)):
    if device.get('nick_name') == config['device_name']:
        device_id = '5c0b' + device.get('device_id') + '4b5z'
        print('Using device ID {} ({})...'.format(device_id, config['device_name']))
        break
else:
    sys.exit('Failed to map device name {} to device ID.'.format(config['device_name']))


def upload_files(files):
    global logfile, files_considered_for_backup, files_backed_up_now, files_already_present, files_failed_to_backup

    print('Uploading batch of {} files...'.format(len(files)))
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8') as listfile:
        for path in files:
            listfile.write(str(path))
            listfile.write('\n')
        listfile.flush()
        files.clear()

        items = run_util_items(
            '--xml-output', '--type',
            '--device-id=' + device_id,
            '--files-from=' + listfile.name, '--relative',
            '/', '{}@{}::home/'.format(config['username'], srvaddr)
        )

        last_total_transfer_size = 0

        for item in items:
            if item.get('per') != '100%':
                continue

            file_name = item.get('fname')
            transfer_type = item.get('trf_type')
            transfer_rate = item.get('rate_trf')
            total_transfer_size = int(item.get('tottrf_sz'))

            transfer_size = total_transfer_size - last_total_transfer_size
            last_total_transfer_size = total_transfer_size

            files_considered_for_backup += 1

            if transfer_type == 'FULL' or transfer_type == 'INCREMENTAL':
                size = transfer_size
                unit = 'B'

                if size > 1024:
                    size /= 1024
                    unit = 'kB'

                if size > 1024:
                    size /= 1024
                    unit = 'MB'

                if size > 1024:
                    size /= 1024
                    unit = 'GB'

                logfile.write('Transferred {:.1f} {} at {} to backup file /{}.\n'.format(size, unit, transfer_rate, file_name))
                files_backed_up_now += 1
            elif transfer_type == 'FILE IN SYNC':
                files_already_present += 1
            else:
                logfile.write('Failed to backup file: {}\n'.format(xml.etree.ElementTree.tostring(item)))
                files_failed_to_backup += 1


def run_backup():
    global logfile, files_considered_for_backup, files_backed_up_now, files_already_present, files_failed_to_backup

    starttime = datetime.now()

    logpath = logdir / '{:%Y-%m-%dT%H:%M:%S}.log'.format(starttime)
    logfile = logpath.open('wt', buffering=1, encoding='utf-8')
    files_considered_for_backup = 0
    files_backed_up_now = 0
    files_already_present = 0
    files_failed_to_backup = 0

    logfile.write('Starting backup from {} to {} ({}) on {:%c}...\n'.format(socket.gethostname(), config['device_name'], device_id, starttime))

    includes = list(map(Path, config['includes']))
    excludes = list(map(Path, config['excludes']))
    files = list()

    while len(includes) > 0:
        try:
          include = includes.pop().resolve()
        except FileNotFoundError:
            logfile.write('Skipping path {} as it appears to be a broken symbolic link...\n'.format(include))

        for exclude in excludes:
            if exclude == include or exclude in include.parents:
                logfile.write('Skipping path {} due to exclude {}...\n'.format(include, exclude))
                break
        else:
            if include.is_file():
                files.append(include)

                if len(files) == 1000:
                    upload_files(files)
            elif include.is_dir():
                try:
                    includes.extend(include.iterdir())
                except FileNotFoundError:
                    logfile.write('Skipping path {} as it was removed while the backup was running...\n'.format(include))
            else:
                logfile.write('Skipping path {} as it is neither a file nor a directory...\n'.format(include))

    if len(files) > 0:
        upload_files(files)

    endtime = datetime.now()

    if files_failed_to_backup == 0:
        logfile.write('Finished successful backup of {} files at {:%c}.\n'.format(files_backed_up_now, endtime))
    else:
        logfile.write('Finished incomplete backup of {} files with {} files missing at {:%c}.\n'.format(files_backed_up_now, files_failed_to_backup, endtime))

    quota = run_util_tree('--get-quota', '{}@{}::home/'.format(config['username'], srvaddr))
    quota_used = int(quota.get('usedquota')) >> 30
    quota_total = int(quota.get('totalquota')) >> 30

    summary = """
Summary:
Backup start time: {starttime:%c}
Backup end time : {endtime:%c}
Files considered for backup: {files_considered_for_backup}
Files backed up now: {files_backed_up_now}
Files already present in your account: {files_already_present}
Files failed to backup: {files_failed_to_backup}
Quota used: {quota_used} GB out of {quota_total} GB
""".format(
        starttime=starttime, endtime=endtime,
        files_considered_for_backup=files_considered_for_backup,
        files_backed_up_now=files_backed_up_now,
        files_already_present=files_already_present,
        files_failed_to_backup=files_failed_to_backup,
        quota_used=quota_used, quota_total=quota_total,
    )

    logfile.write(summary)
    logfile.close()

    data = urlencode({
        'username': config['username'],
        'password': config['password'],
        'to_email': config['notify_email'],
        'subject': 'Successful backup summary' if files_failed_to_backup == 0 else 'Incomplete backup summary ({} out of {})'.format(files_failed_to_backup, files_considered_for_backup),
        'content': summary,
    })
    subprocess.check_call(['curl', '-s', '-d', data, 'https://webdav.ibackup.com/cgi-bin/Notify_email_ibl'], stdout=subprocess.DEVNULL)


while True:
    try:
        with lastrunpath.open(mode='rt', encoding='ascii') as lastrunfile:
            lastrun = datetime.fromtimestamp(float(lastrunfile.read()))
    except FileNotFoundError:
        lastrun = None

    interval = timedelta(seconds=config['interval'])
    now = datetime.now()

    if lastrun is not None and lastrun + interval > now:
        time.sleep(300)
        continue

    with lastrunpath.open(mode='wt', encoding='ascii') as lastrunfile:
        lastrunfile.write(str(now.timestamp()))

    try:
        print('Starting backup on {:%c}...'.format(now))
        run_backup()
        print('Completed backup on {:%c}.'.format(datetime.now()))
    except Exception as exception:
        print('Backup failed due to {}: {}'.format(type(exception).__name__, exception))
