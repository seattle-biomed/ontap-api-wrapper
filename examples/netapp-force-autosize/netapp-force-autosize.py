#!/opt/virtualenv/admintools/bin/python

import re
import sys
import yaml

# Load NetApp API wrapper:
sys.path.append('/opt/netapp-manageability-sdk-4.1/lib/python/NetApp')
import Ontap


def size_in_kb(string_size):
    """Convert string_size with optional (k,m,g,t) suffix to gigabytes."""

    # Remove last character from string:
    suffix = string_size[-1]
    value = int(string_size[:-1])

    if suffix == 't':
        scale_factor = 1024 * 1024 * 1024
    elif suffix == 'g':
        scale_factor = 1024 * 1024
    elif suffix == 'm':
        scale_factor = 1024
    elif suffix == 'k':
        scale_factor = 1

    return value * scale_factor

if __name__ == '__main__':
    """
    If Filer is warning/critical on FlexVol space, attempt to remediate.

    Autosize rules and increments are used, if they exist.  This can be
    thought of as a more-aggressively triggered autosize operation.

    Required NetApp role permissions: login-http-admin, api-snmp-get,
    api-snapshot-get-reserve, api-system-get-version,
    api-volume-autosize-get, api-volume-size
    """

    # Gather authentication info
    f = open('/opt/ops-scripts/etc/netapp-space-check.yaml')
    auth = yaml.load(f.read())
    f.close()

    # Connect to NetApp filers:
    filers = []
    for filer in auth['filers']:
        filers.append(Ontap.Filer(filer['hostname'],
                                  filer['user'],
                                  filer['passwd']))

    for filer in filers:
        try:
            message = filer.get_fs_status_msg().rstrip()
        except Ontap.OntapApiException as e:
            print "Failed with error: %s: %s" % (filer.name, e.reason)
            raise

        if message == 'All volumes have adequate space.':
            continue

        print message

        vol_path = re.match(r'^/vol/(\S+)\s', message).groups()[0]

        try:
            v = filer.get_volume(vol_path)
            v_size = size_in_kb(v.get_size())
            as_incr = v.get_autosize_increment()
            as_max = v.get_autosize_max_size()

            if v_size < as_max:
                if v_size + as_incr > as_max:
                    print "Resize /vol/%s to %iKB" % (v.name, as_max)
                    v.set_size(str(as_max))
                else:
                    print "Grow /vol/%s by %iKB to %iKB" % (v.name, as_incr,
                                                            v_size + as_incr)
                    v.set_size("+%ik" % as_incr)
        except Ontap.OntapApiException as e:
            print "Failed with error: %s: %s" % (v.filer.name, e.reason)
            continue
