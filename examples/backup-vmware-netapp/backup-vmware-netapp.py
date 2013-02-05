#!/opt/virtualenv/admintools/bin/python

import argparse
import functools
import pysphere
import re
import socket
import sys
import time
import yaml

# Load NetApp API wrapper:
sys.path.append('/opt/netapp-manageability-sdk-4.1/lib/python/NetApp')
import Ontap


def clean_vm_snaps(servers, vms_by_ds, datastore, skip_vms = [],
                   dry_run = False):
    """Given dict of VMs by datastore, clean VM snaps on server:datastore."""

    for vmx in vms_by_ds[datastore].keys():
        if skip_vms.count(vmx):
            continue
        v_print("Removing snapshot on %s..." % vmx, 3)
        if not dry_run:
            try:
                vm = servers[vms_by_ds[datastore][vmx]].get_vm_by_path(vmx)
                vm.delete_named_snapshot('backup')
            except pysphere.resources.vi_exception.VIException:
                v_print("Failed to remove snapshot on %s!" % vmx, 1)
                v_print('Manual snapshot deletion may be required.', 2)
                pass # continue on
        v_print('done.', 3)


def set_mode_from_args():
    """
    Parse command line arguments.

    Return v_print, which sets output verbosity level.
    """

    parser = argparse.ArgumentParser(
        description='Coordinate VMware and NetApp backups.')
    parser.add_argument('-n', '--noexec', action='store_true',
                        help='Dry run - do not create any snapshots')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='Reduce output to only errors')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Produce verbose output')
    args = parser.parse_args()

    if args.verbose:
        v_print = functools.partial(verbose_print, verbosity=3)
    elif args.quiet:
        v_print = functools.partial(verbose_print, verbosity=1)
    else:
        v_print = functools.partial(verbose_print, verbosity=2)

    if args.noexec:
        dry_run = True
    else:
        dry_run = False

    return (v_print, dry_run)


def verbose_print(output, level, verbosity=2):
    """
    Print output (or not) depending on level of verbosity.

    verbosity level 3 prints all messages
    verbosity level 2 prints only warning or error messages
    verbosity level 1 prints only error messages
    """

    if level <= verbosity:
        now = time.strftime("%b %d %H:%M:%S", time.localtime())
        print now, output


if __name__ == '__main__':
    """
    Coordinate VMware snapshots with NetApp SnapVault snapshots.

    Required NetApp role permissions: login-http-admin,
    api-system-get-version, api-snapvault-primary-initiate-snapshot-create,
    api-volume-list-info, api-snapvault-secondary-initiate-snapshot-create

    Required vSphere role permissions:
    -Virtual machine: State: Create snapshot
    -Virtual machine: State: Remove snapshot
    """

    # Parse arguments:
    (v_print, dry_run) = set_mode_from_args()

    # Gather authentication info

    f = open('/opt/ops-scripts/etc/vmware-netapp-backup-auth.yaml')
    auth = yaml.load(f.read())
    f.close()

    # Read VMware datastore to NetApp volume mapping configuration:

    f = open('/opt/ops-scripts/etc/vmware-netapp-backup-config.yaml')
    config = yaml.load(f.read())
    f.close()

    # Connect to NetApp filers:
    filers = {}
    for filer in auth['filers']:
        v_print("Connecting to filer %s" % filer['hostname'], 3)
        filers[filer['hostname']] = Ontap.Filer(filer['hostname'], 
                                                filer['user'],
                                                filer['passwd'])

    # Connect to all vSphere instances:
    servers = {}
    for vcenter in auth['vcenter']:
        vc = pysphere.VIServer()
        v_print("Connecting to vCenter host %s" % vcenter['hostname'], 3)
        try:
            vc.connect(vcenter['hostname'], vcenter['user'], vcenter['passwd'])
        except socket.error:
            v_print("Could not connect to vCenter host %s" %
                    vcenter['hostname'], 1)
            continue
        servers[vcenter['hostname']] = vc

    # Generate dict of VMs by datastore:
    vms_by_ds = {}
    for vchost in servers.keys():

        v_print("Cataloging vCenter host %s" % vchost, 3)
        server = servers[vchost]
        vmlist = server.get_registered_vms()

        for vmx in vmlist:
            match = re.search(r'^\[(.*)\] (\S+)$', vmx)
            if match:
                datastore = match.groups()[0]
                if not vms_by_ds.has_key(datastore):
                    vms_by_ds[datastore] = {}
                vms_by_ds[datastore][vmx] = vchost

    #
    # Do backups per datastore to optimize performance
    #
    
    for datastore in config['datastores']:

        v_print("Snapping contents of %s" % datastore['name'], 3)

        # Track off VMs - don't try to snap them:
        off_vms = []

        if not vms_by_ds.has_key(datastore['name']):
            v_print("No VMs found in %s" % datastore['name'], 3)
            continue

        #
        # Take VMware VM-level snapshots
        #
        
        for vmx in vms_by_ds[datastore['name']].keys():

            try:
                vm = servers[vms_by_ds[datastore['name']][vmx]].get_vm_by_path(vmx)
            except pysphere.resources.vi_exception.VIException as e:
                v_print("Failed to get_vm with %s!" % vmx, 1)
                v_print("Exception detail: %s" % e, 1)
                v_print("Expected VM to be on host %s" %
                        vms_by_ds[datastore['name']][vmx], 2)
                v_print("%s will not get a VMware snapshot." % vmx, 2)
                del vms_by_ds[datastore['name']][vmx]
                continue

            # Skip off VMs:
            if vm.get_status() == 'POWERED OFF':
                v_print("%s is off - will not take VMware snap" % vmx, 3)
                off_vms.append(vmx)
                continue
            
            v_print("Snapping %s..." % vmx, 3)
            if not dry_run:
                try:
                    vm.create_snapshot('backup', memory=False, quiesce=True)
                except pysphere.resources.vi_exception.VIException:
                    v_print("Failed to snap %s!" % vmx, 1)
                    pass # bravely march on
                v_print('done.', 3)

        #
        # Take NetApp snapshot
        #

        try:
            pri_vol = filers[datastore['primary']].get_volume(
                datastore['pri_vol'])
            v_print("Snapping %s..." % datastore['pri_vol'], 3)
            if not dry_run:
                pri_vol.snapvault_primary_snap('sv_daily')
            v_print('done.', 3)
        except Ontap.OntapApiException as e:
            v_print('FAILURE: Exiting due to OntapApiException', 1)

            # Clean up snapshots on the way out the door:
            clean_vm_snaps(servers, vms_by_ds, datastore['name'],
                           off_vms)

            v_print("Code: %s - %s" % (e.errno, e.reason), 2)
            raise
        except:
            raise

        #
        # Remove VMware VM-level snapshots
        #

        clean_vm_snaps(servers, vms_by_ds, datastore['name'], off_vms,
                       dry_run)

    #
    # Send NetApp snapshots to SnapVault secondary, where configured
    #
    
    for datastore in config['datastores']:
        if datastore.has_key('secondary'):
            sec_vol = filers[datastore['secondary']].get_volume(
                datastore['sec_vol'])
            if sec_vol is False:
                v_print("Secondary volume %s does not exist on %s!" %
                        (datastore['sec_vol'],
                         filers[datastore['secondary']].name),
                        1)
                continue
            v_print("Initiating transfer to %s" % datastore['sec_vol'], 3)
            if not dry_run:
                sec_vol.snapvault_secondary_snap('sv_daily')
            v_print('done.', 3)
            if not dry_run:
                v_print('Sleeping for 600 seconds.', 3)
                time.sleep(600)
