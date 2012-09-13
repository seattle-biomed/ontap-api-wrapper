import re
import sys

from NaElement import NaElement
from NaServer import NaServer

class OntapApiException(Exception):
    """Expose errors surfaced in the NetApp API as exceptions."""

    def __init__(self, errno, reason):
        self.errno = errno
        self.reason = reason


class OntapException(Exception):
    """Exception for syntax errors passed to API calls."""

    def __init__(self, reason):
        self.reason = reason


class Filer:
    """A NetApp filer."""

    def __init__(self, hostname, user, passwd):
        self.api = NaServer(hostname, 1, 3)
        self.api.set_style('LOGIN')
        self.api.set_admin_user(user, passwd)
        self.api.set_transport_type('HTTPS')

        self.name = hostname
        out = self.invoke('system-get-version')
        self.version = out.child_get_string('version')

    def create_volume(self, name, aggr, size):
        v = FlexVol(self, name)
        v.create(aggr, size)
        return v

    def flexshare_disable(self):
        """Equivalent to 'priority off' on the CLI."""

        self.invoke('priority-disable')

    def flexshare_enable(self):
        """Equivalent to 'priority on' on the CLI."""

        self.invoke('priority-enable')

    def flexshare_is_enabled(self):
        """Return boolean representing whether FlexShare is enabled."""

        out = self.invoke('priority-list-info')

        if out.child_get_string('status') == 'on':
            return True
        else:
            return False

    def get_cifs_homedirs(self):
        """
        Equivalent to 'cifs homedir' on the CLI.

        Return an array of cifs home directory paths.
        """

        out = self.invoke('cifs-homedir-paths-get')

        if out.has_children():
            homedirs = []
            for d in out.child_get('homedir-paths').children_get():
                homedirs.append(d.element['content'])
            return homedirs
        else:
            return []

    def get_export(self, export_path):
        """
        Return an Export object representing NFS share at export_path.

        If export does not exist, return False.
        """

        if self.has_export(export_path):
            return(Export(self, export_path))
        else:
            return False

    def get_exports(self):
        """Return a list of Export objects of filer's configured NFS shares."""

        out = self.invoke('nfs-exportfs-list-rules')

        exports = []

        for export in out.child_get('rules').children_get():
            path = export.child_get_string('pathname')
            exports.append(Export(self, path))

        return exports                         

    def get_fs_status_msg(self):
        """Return a string containing the file system status message."""

        return self.get_oid('.1.3.6.1.4.1.789.1.5.7.2.0')

    def get_oid(self, oid):
        """Return a generic OID from the NetApp SNMP MIB."""

        out = self.invoke('snmp-get', 'object-id', oid)
        return out.child_get_string('value')

    def get_root_name(self):
        """Return a string containing the Filer's root volume's name."""

        out = self.invoke('aggr-get-root-name')
        return out.child_get_string('root-volume')

    def get_share(self, name):
        """
        Return a Share object representing the existing CIFS share of name.

        If share does not exist, return False.
        """

        if self.has_share(name):
            return(Share(self, name))
        else:
            return False   
    
    def get_shares(self):
        """Return a list of Share objects containing filer's CIFS exports."""

        out = self.invoke_cli('cifs', 'shares')

        # Pattern of output is two header lines, followed by each share name
        # starting at the left-hand side of the output.  Regexp accounts
        # For share name being able to include whitespace and other
        # characters - match is anchored on first "/" following whitespace,
        # which is presumed to be the start of the mount point.

        output = out.child_get('cli-output').element['content'].splitlines()

        share_pattern = re.compile(r'^([a-zA-Z].*\S)\s+\/')

        shares = []
        
        for line in output[2:]:
            m = share_pattern.match(line)
            if m:
                shares.append(Share(self, m.groups()[0]))

        return shares

    def get_option(self, name):
        """Equivalent to 'options <name>' on the CLI."""

        out = self.invoke('options-get', 'name', name)
        return out.child_get_string('value')

    def get_volume(self, name):
        """Return FlexVol object of existing vol 'name'; else return False."""

        if self.has_volume(name):
            return(FlexVol(self, name))
        else:
            return False

    def get_volumes(self):
        """Retun a list of FlexVol objects that exist on filer."""

        volumes = []

        out = self.invoke('volume-list-info')
        for volume in out.child_get('volumes').children_get():
            name = volume.child_get_string('name')
            volumes.append(FlexVol(self, name))

        return volumes

    def has_export(self, path):
        """Check if filer has NFS export name; return boolean."""

        export = Export(self, path)
        return export.configured()

    def has_share(self, name):
        """Check if filer has CIFS share name; return boolean."""

        share = Share(self, name)
        return share.configured()

    def has_volume(self, name):
        """Check if filer has FlexVol name; return boolean."""
        
        try:
            self.invoke('volume-list-info', 'volume', name)
        except OntapApiException as e:
            if e.errno == '13040':
                return False
            else:
                raise
        return True

    def invoke(self, *args):
        out = self.api.invoke(*args)
        if out.results_status() == 'failed':
            raise OntapApiException(out.results_errno(), out.results_reason())
        return out

    def invoke_cli(self, *cli_args):
        """
        Call the unsupported/undocumented system-cli API.

        args is a tuple of arguments that, joined with spaces, would represent
        the command line if executing in the CLI.
        """

        args = NaElement('args')
        for arg in cli_args:
            args.child_add(NaElement('arg', arg))

        cli = NaElement('system-cli')
        cli.child_add(args)
        out = self.api.invoke_elem(cli)
        if out.results_status() == 'failed':
            raise OntapApiException(out.results_errno(), out.results_reason())
        return out

    def invoke_elem(self, naelement):
        """Call the NetApp API using an NaElement."""

        out = self.api.invoke_elem(naelement)
        if out.results_status() == 'failed':
            raise OntapApiException(out.results_errno(), out.results_reason())
        return out

    def set_cifs_homedirs(self, homedirs):
        """Set the list of CIFS home directory paths for the filer."""

        homedir_paths = NaElement('homedir-paths')

        for d in homedirs:
            homedir_paths.child_add(NaElement('homedir-path-info', d))

        chps = NaElement('cifs-homedir-paths-set')
        chps.child_add(homedir_paths)
        self.invoke_elem(chps)

    def set_option(self, option, value):
        """Equivalent to 'options <option> <value>' on the CLI."""

        self.invoke('options-set', 'name', option, 'value', value)

    def _xmltree_to_dict(self, out, int_values=(), key='name', value='value'):
        """Convert thinly-veiled XML from ONTAP API to a dict."""
        options = {}

        for option in out.child_get('options').children_get():
            name = option.child_get_string(key)
            if name in int_values:
                options[name] = option.child_get_int(value)
            else:
                options[name] = option.child_get_string(value)

        return options

    def _xmltree_to_list(self, nae, outer_name, inner_name):
        """
        Return list converted from ONTAP API NaElement 'nae'.

        nae - NaElement from ONTAP API
        outer_name - outer 'child' of NaElement
        inner_name - inner 'child' of NaElement

        """

        out_list = []
        if nae.child_get(outer_name):
            for item in nae.child_get(outer_name).children_get():
                inner_val = item.child_get_string(inner_name)
                if inner_val is not None:
                    out_list.append(inner_val)

        return out_list


class Aggr:
    """An aggregate on a NetApp filer."""

    def __init__(self, filer, name):
        self.filer = filer
        self.name = name
        

class Export:
    """An NFS export on a NetApp Filer."""

    def __init__(self, filer, path):
        self.filer = filer
        self.path = path

    def configured(self):
        """
        Determine if export at self.path has been configured on filer.

        Return boolean.
        """

        if self._get_rules():
            return True
        else:
            return False

    def create_rule(self, nosuid=True, root_hosts = [], ro_hosts = [],
                    rw_hosts = [], sec_flavor = 'sys'):
        """
        Create new exportfs rule for an NFS share.

        This method follows the semantics of the NetApp API for
        default values, namely: 'By default, if no 'read-only' or
        'read-write' hosts are given, then 'read-write' [access is
        granted to all hosts].'
        """

        # Parse arguments:
        if nosuid:
            nosuid_val = 'true'
        else:
            nosuid_val = 'false'

        #
        # Construct NaElement tree:
        #

        rule_info = NaElement('exports-rule-info')
        rule_info.child_add(NaElement('nosuid', nosuid_val))
        rule_info.child_add(NaElement('pathname', self.path))

        host_lists = { 'root': root_hosts,
                       'read-only': ro_hosts,
                       'read-write': rw_hosts }

        for elem in host_lists:
            if len(host_lists[elem]) > 0:
                nae = NaElement(elem)
                for host in host_lists[elem]:
                    ehi = NaElement('exports-hostname-info')
                    ehi.child_add(NaElement('name', host))
                    nae.child_add(ehi)
                rule_info.child_add(nae)

        nfs_export = NaElement('nfs-exportfs-append-rules')
        nfs_export.child_add(NaElement('persistent', 'true'))
        rules = NaElement('rules')
        rules.child_add(rule_info)
        nfs_export.child_add(rules)

        # Execute rule change:
        self.filer.invoke_elem(nfs_export)

    def delete_rule(self):
        """Remove the exportfs rule for a share."""

        #
        # Construct NaElement tree:
        #
        
        pathname_info = NaElement('pathname-info')
        pathname_info.child_add(NaElement('name', self.path))

        pathnames = NaElement('pathnames')
        pathnames.child_add(pathname_info)

        elem = NaElement('nfs-exportfs-delete-rules')
        elem.child_add(NaElement('persistent', 'true'))
        elem.child_add(pathnames)

        # Execute it:
        self.filer.invoke_elem(elem)

    def get_nosuid(self):
        """
        Return boolean reflecting nosuid setting on export.

        From ONTAP API docs on nosuid setting: 'If true, causes the
        server file system to silently ignore any attempt to enable
        the setuid or setgid mode bits. Default value is false.'

        If export does not exist, return an empty string.
        """

        rules = self._get_rules()
        if rules:
            if rules.child_get('nosuid'):
                if rules.child_get_string('nosuid') == 'true':
                    return True
                else:
                    return False
            else:
                return False
        else:
            return ''

    def get_ro_hosts(self):
        """
        Return list of hosts permitted read-only access.

        If export does not exist, return an empty list.
        """

        rules = self._get_rules()
        if rules:
            return self.filer._xmltree_to_list(rules, 'read-only', 'name')
        else:
            return []

    def get_rw_hosts(self):
        """Return list of hosts permitted read/write access."""

        rules = self._get_rules()
        return self.filer._xmltree_to_list(rules, 'read-write', 'name')

    def get_root_hosts(self):
        """Return list of hosts permitted root access."""

        rules = self._get_rules()
        return self.filer._xmltree_to_list(rules, 'root', 'name')

    def get_sec_flavor(self):
        """Return the security 'flavor' of the NFS export."""

        rules = self._get_rules()
        return rules.child_get('sec-flavor').child_get(
            'sec-flavor-info').child_get_string('flavor')

    def modify_rule(self, nosuid=True, root_hosts = [], ro_hosts = [],
                    rw_hosts = [], sec_flavor = 'sys'):
        """
        Change the exportfs rule for an NFS share.

        This method follows the semantics of the NetApp API for
        default values, namely: 'By default, if no 'read-only' or
        'read-write' hosts are given, then 'read-write' [access is
        granted to all hosts].'

        The exportfs rule must already exist before calling this method, or
        an exception will be thrown.
        """

        # Parse arguments:
        if nosuid:
            nosuid_val = 'true'
        else:
            nosuid_val = 'false'

        #
        # Construct NaElement tree:
        #

        rule_info = NaElement('exports-rule-info')
        rule_info.child_add(NaElement('nosuid', nosuid_val))
        rule_info.child_add(NaElement('pathname', self.path))

        host_lists = { 'root': root_hosts,
                       'read-only': ro_hosts,
                       'read-write': rw_hosts }

        for elem in host_lists:
            if len(host_lists[elem]) > 0:
                nae = NaElement(elem)
                for host in host_lists[elem]:
                    ehi = NaElement('exports-hostname-info')
                    ehi.child_add(NaElement('name', host))
                    nae.child_add(ehi)
                rule_info.child_add(nae)

        nfs_export = NaElement('nfs-exportfs-modify-rule')
        nfs_export.child_add(NaElement('persistent', 'true'))
        rule = NaElement('rule')
        rule.child_add(rule_info)
        nfs_export.child_add(rule)

        # Execute rule change:
        self.filer.invoke_elem(nfs_export)

    def _get_rules(self):
        """
        Return an NaElement containing the 'exports-rule-info'.

        If there is no 'exports-rule-info', return False.
        """

        out = self.filer.invoke('nfs-exportfs-list-rules',
                                'pathname', self.path)
        try:
            return out.child_get('rules').child_get('exports-rule-info')
        except AttributeError:
            return False
        else:
            raise

class FlexVol:
    """A FlexVol on a NetApp Filer."""

    def __init__(self, filer, name):
        self.filer = filer

        m = re.match('^/vol/(.+)$', name)
        if m:
            name = m.groups()[0]

        self.name = name
        self.path = '/vol/' + name
    
    def create(self, aggr, size):
        self.filer.invoke('volume-create',
                          'volume', self.name,
                          'containing-aggr-name', aggr,
                          'size', size)

    def autosize_is_enabled(self):
        out = self.filer.invoke('volume-autosize-get', 'volume', self.name)
        if out.child_get_string('is-enabled') == 'true':
            return True
        else:
            return False


    def sis_is_enabled(self):
        try:
            out = self.filer.invoke('sis-status', 'path', self.path)
        except OntapApiException as e:
            if e.errno == '13001':
                return False
            else:
                raise
            
        state = out.child_get('sis-object').child_get('dense-status').child_get_string('state') 
        if state == 'Enabled':
            return True
        else:
            return False

    def del_sv_pri_snap_sched(self, schedule_name):
        """Delete a SnapVault primary snapshot schedule."""

        self.filer.invoke('snapvault-primary-delete-snapshot-schedule',
                          'schedule-name', schedule_name,
                          'volume-name', self.name)

    def del_sv_sec_snap_sched(self, schedule_name):
        """Delete a SnapVault secondary snapshot schedule."""

        self.filer.invoke('snapvault-secondary-delete-snapshot-schedule',
                          'schedule-name', schedule_name,
                          'volume-name', self.name)
        
    def get_autosize_increment(self):
        out = self.filer.invoke('volume-autosize-get', 'volume', self.name)
        return out.child_get_int('increment-size')


    def get_autosize_increment_gb(self):
        """
        Return the vol autosize increment rounded to the nearest gigabyte.

        Value is returned as a string, suffixed with a 'g' to match Data
        ONTAP conventions.
        """
        
        kb = self.get_autosize_increment()
        return str(int(round(kb / 1024. / 1024.))) + 'g'

    def get_autosize_max_size(self):
        out = self.filer.invoke('volume-autosize-get', 'volume', self.name)
        return out.child_get_int('maximum-size')

    def get_autosize_max_size_gb(self):
        """
        Return the vol autosize maximum size rounded to the nearest gigabyte.

        Value is returned as a string, suffixed with a 'g' to match Data
        ONTAP conventions.
        """

        kb = self.get_autosize_max_size()
        return str(int(round(kb / 1024. / 1024.))) + 'g'
    
    def get_df(self):
        """
        Return an array containing space used, available and total space.

        Values are returned as integers, representing bytes.  Note
        that values for total space are after snapshot reserve (if
        any), similar to how 'df' works on the CLI.
        """

        out = self.filer.invoke('volume-list-info', 'volume', self.name)
        used = out.child_get('volumes').child_get(
            'volume-info').child_get_int('size-used')
        avail = out.child_get('volumes').child_get(
            'volume-info').child_get_int('size-available')
        total = out.child_get('volumes').child_get(
            'volume-info').child_get_int('size-total')
        return([used, avail, total])

    def get_options(self):
        """Equivalent to: vol options <self.name>

        Returns a dict comprised of the volume's options.  Note that the API
        returns options beyond what 'vol options' returns in the ONTAP
        CLI."""
        
        out = self.filer.invoke('volume-options-list-info',
                                'volume', self.name)

        # option values that should be integers; the rest are strings:
        int_values = ('fractional_reserve', 'maxdirsize',
                      'max_write_alloc_blocks', 'raidsize', 'resyncsnaptime')

        return self.filer._xmltree_to_dict(out, int_values)

    def get_priority_cache_policy(self):
        """Return the FlexShare cache policy for the volume."""

        try:
            out = self.filer.invoke('priority-list-info-volume',
                                    'volume', self.name)
        except OntapApiException as e:
            # If volume doesn't have a priority schedule, it is default:
            if e.reason == 'unable to find volume' and e.errno == '2':
                return 'default'
            else:
                raise

        pri_vol = out.child_get('priority-volume').child_get(
            'priority-volume.info')
        return pri_vol.child_get_string('cache-policy')

    def get_security_style(self):
        """Return the security stle (unix, ntfs, mixed) of the volume."""

        out = self.filer.invoke('qtree-list', 'volume', self.name)

        for qtree in out.child_get('qtrees').children_get():
            if qtree.child_get_string('qtree') == '':
                return qtree.child_get_string('security-style')

    def get_sis_state(self):
        """Get deduplication state; return 'Enabled' or 'Disabled'."""
        try:
            out = self.filer.invoke('sis-status', 'path', self.path)
        except OntapApiException as e:
            if e.errno == '13001':
                return 'Disabled'
            else:
                raise
            
        return out.child_get('sis-object').child_get('dense-status').child_get_string('state') 


    def get_size(self):
        out = self.filer.invoke('volume-size', 'volume', self.name)
        return out.child_get_string('volume-size')

    def get_snap_autodelete(self):
        """Equivalent to: 'snap autodelete <self.name> show'

        Returns a dict consisting of the snapshot autodelete options."""

        out = self.filer.invoke('snapshot-autodelete-list-info',
                                'volume', self.name)

        # option values that should be integers; the rest are strings:
        int_values = ('target_free_space')

        return self.filer._xmltree_to_dict(out, int_values, key='option-name',
                                   value='option-value')

    def get_snap_reserve(self):
        """Equivalent to: snap reserve <self.name>"""
        
        out = self.filer.invoke('snapshot-get-reserve', 'volume', self.name)
        return out.child_get_int('percent-reserved')

    def get_snap_sched(self):
        """
        Closest equivalent: snap sched <self.name>

        Return a dict with the following key-value pairs:

        days - The number of snapshots taken daily to keep on line.
        hours - The number of snapshots taken hourly to keep on line.
        minutes - The number of snapshots taken minutely to keep on line.
        weeks - The number of snapshots taken weekly to keep on line.
        which-hours - Comma separated string of the hours at which the hourly
                      snapshots are created.
        which-minutes - Comma separated string of the minutes at which the
                        minutely snapshots are created.
        """

        out = self.filer.invoke('snapshot-get-schedule', 'volume', self.name)

        sched = {}

        for retention in ('days', 'hours', 'minutes', 'weeks'):
            sched[retention] = out.child_get_int(retention)

        for t in ('which-hours', 'which-minutes'):
            sched[t] = out.child_get_string(t)

        return sched

    def get_sv_pri_snap_sched(self):
        """
        Return the snapvault primary snapshot schedule as an array of dicts.

        Roughly equivalent to:
        snapvault snap sched <self.name>

        Each dict in the returned array contains the following keys:
        schedule-name - Name of the snap schedule
        retention-count - Number of snapshots retained by this schedule
        days-of-week - Days of the week schedule will run
        hours-of-day - Hours of the day schedule will run, default 0
        """

        out = self.filer.invoke(
            'snapvault-primary-snapshot-schedule-list-info',
            'volume-name', self.name)

        scheds = {}

        for schedxml in out.child_get('snapshot-schedules').children_get():
            sched = {}
            name = schedxml.child_get_string('schedule-name')
            sched['retention-count'] = schedxml.child_get_int(
                'retention-count')
            schedinfo = schedxml.child_get('schedule').child_get(
                'snapvault-schedule-info')
            sched['days-of-week'] = schedinfo.child_get_string('days-of-week')
            sched['hours-of-day'] = schedinfo.child_get_string('hours-of-day')
            scheds[name] = sched

        return scheds

    def get_sv_sec_snap_sched(self):
        """
        Return the snapvault secondary snapshot schedule as an array of dicts.

        Roughly equivalent to:
        snapvault snap sched <self.name>

        Each dict in the returned array contains the following keys:
        schedule-name - Name of the snap schedule
        retention-count - Number of snapshots retained by this schedule
        days-of-week - Days of the week schedule will run
        hours-of-day - Hours of the day schedule will run, default 0
        is-auto-update - boolean - Schedule initiates xfer before snap create?
        """

        out = self.filer.invoke(
            'snapvault-secondary-snapshot-schedule-list-info',
            'volume-name', self.name)

        scheds = {}

        for schedxml in out.child_get('snapshot-schedules').children_get():
            sched = {}
            name = schedxml.child_get_string('schedule-name')
            sched['retention-count'] = schedxml.child_get_int(
                'retention-count')
            schedinfo = schedxml.child_get('schedule').child_get(
                'snapvault-schedule-info')
            sched['days-of-week'] = schedinfo.child_get_string('days-of-week')
            sched['hours-of-day'] = schedinfo.child_get_string('hours-of-day')
            iau = schedxml.child_get_string('is-auto-update')
            if iau == 'true':
                sched['is-auto-update'] = True
            else:
                sched['is-auto-update'] = False
            scheds[name] = sched

        return scheds        

    def has_snap(self, snap_name):
        """Return boolean of whether FlexVol has snapshot 'snap_name'."""

        out = self.filer.invoke('snapshot-list-info',
                                'target-name', self.name,
                                'target-type', 'volume')

        for s in out.child_get('snapshots').children_get():
            if s.child_get_string('name') == snap_name:
                return True

        return False

    def set_autosize_state(self,
                           enabled,
                           increment_size = False,
                           maximum_size = False):
        """
        Enable, disable or configure autosize for a FlexVol.

        Arguments:
        enabled -- Boolean: Turn autosize on or off
        increment_size -- Increment size for growing FlexVol (string)
        maximum_size -- Limit to which FlexVol will grow (string)

        increment_size and maximum_size may be suffixed with a 'k', 'm', 'g' or
        't' to indicate KB, MB, GB or TB, respectively.  If there is no suffix,
        the values are treated as being in KB.
        """
        
        if enabled:
            self.filer.invoke('volume-autosize-set',
                              'volume', self.name,
                              'is-enabled', 'true')
        else:
            self.filer.invoke('volume-autosize-set',
                              'volume', self.name,
                              'is-enabled', 'false')

        if increment_size:
            self.filer.invoke('volume-autosize-set',
                              'volume', self.name,
                              'increment-size', increment_size)

        if maximum_size:
            self.filer.invoke('volume-autosize-set',
                              'volume', self.name,
                              'maximum-size', maximum_size)

    def set_priority_cache_policy(self, policy):
        """CLI equivalent: 'priority set volume <self.name> cache=<policy>'"""

        self.filer.invoke('priority-set-volume',
                          'volume', self.name,
                          'cache-policy', policy)
                           
    def set_option(self, option_name, value):
        self.filer.invoke('volume-set-option',
                          'option-name', option_name,
                          'option-value', value,
                          'volume', self.name)

    def set_security_style(self, style):
        self.filer.invoke_cli('qtree', 'security', self.path, style)

    def set_sis_state(self, state):
        if state == 'enabled' or state == 'Enabled':
            self.filer.invoke('sis-enable', 'path', self.path)
        elif state == 'disabled' or state == 'Disabled':
            self.filer.invoke('sis-disable', 'path', self.path)
        else:
            raise OntapException('Unknown sis state.')

    def set_size(self, size):
        """
        Set a FlexVol's capacity according to argument size.

        Argument size is a string that follows the same semantics as
        the underlying ONTAP API: 'Specify the flexible volume's new
        size using the following format: [+|-]< number > k|m|g|t] If a
        leading '+' or '-' appears, it indicates that the given
        flexible volume's size is to be increased or decreased
        (respectively) by the indicated amount, else the amount is the
        absolute size to set. The optional trailing 'k', 'm', 'g', and
        't' indicates the desired units, namely 'kilobytes',
        'megabytes', 'gigabytes', and 'terabytes' (respectively). If
        the trailing unit character doesn't appear, then < number > is
        interpreted as the number of kilobytes desired.'
        """
        
        self.filer.invoke('volume-size',
                          'new-size', size,
                          'volume', self.name)

    def set_snap_autodelete_option(self, option_name, value):
        """Equivalent to 'snap autodelete <self.name> <option_name> <value>'

        If option_name is 'state', then definition is equivalent to:

        'snap autodelete <value>'

        where '<value>' is 'on' or 'off'."""

        self.filer.invoke('snapshot-autodelete-set-option',
                          'option-name', option_name,
                          'option-value', value,
                          'volume', self.name)

    def set_snap_reserve(self, percent):
        """Equivalent to: snap reserve <self.name> <percent>"""

        self.filer.invoke('snapshot-set-reserve',
                          'volume', self.name,
                          'percentage', percent)

    def set_snap_sched(self, days=0, hours=0, minutes=0, weeks=0,
                       which_hours=' ', which_minutes=' '):
        """
        Closest equivalent: snap sched <self.name> ...

        Arguments:

        days - The number of snapshots taken daily to keep on line.
        hours - The number of snapshots taken hourly to keep on line.
        minutes - The number of snapshots taken minutely to keep on line.
        weeks - The number of snapshots taken weekly to keep on line.
        which_hours - Comma-separated string of the hours at which the hourly
                      snapshots are created.
        which_minutes - Comma-separated string of the minutes at which the
                        minutely snapshots are created.
        """

        self.filer.invoke('snapshot-set-schedule',
                          'days', days,
                          'hours', hours,
                          'minutes', minutes,
                          'weeks', weeks,
                          'which-hours', which_hours,
                          'which-minutes', which_minutes,
                          'volume', self.name)

    def set_sv_pri_snap_sched(self, sched, retention_ct, dow = 'mon-sun',
                              hod = '0'):
        """
        Set the SnapVault snapshot schedule on a SnapVault primary.

        sched - SnapVault schedule's name
        retention_ct - Number of snapshots to be retained
        dow - Days of week on which the schedule will run
        hod - Hours of day on whcih the schedule will run
        """

        nae = NaElement('snapvault-primary-set-snapshot-schedule')

        snap_sched = NaElement('snapshot-schedule')

        spssi = NaElement('snapvault-primary-snapshot-schedule-info')
        spssi.child_add(NaElement('retention-count', int(retention_ct)))
        spssi.child_add(NaElement('schedule-name', sched))
        spssi.child_add(NaElement('volume-name', self.name))
        
        sched_info = NaElement('snapvault-schedule-info')
        sched_info.child_add(NaElement('days-of-week', dow))
        sched_info.child_add(NaElement('hours-of_day', str(hod)))

        sched = NaElement('schedule')
        sched.child_add(sched_info)

        spssi.child_add(sched)

        snap_sched.child_add(spssi)
        nae.child_add(snap_sched)

        self.filer.invoke_elem(nae)

    def set_sv_sec_snap_sched(self, sched, auto_update, retention_ct,
                              dow = 'mon-sun',
                              hod = '0'):
        """
        Set the SnapVault snapshot schedule on a SnapVault secondary.

        sched - SnapVault schedule's name
        retention_ct - Number of snapshots to be retained
        dow - Days of week on which the schedule will run
        hod - Hours of day on whcih the schedule will run
        """

        nae = NaElement('snapvault-secondary-set-snapshot-schedule')

        snap_sched = NaElement('snapshot-schedule')

        ssssi = NaElement('snapvault-secondary-snapshot-schedule-info')

        if auto_update:
            ssssi.child_add(NaElement('is-auto-update', 'true'))
        else:
            ssssi.child_add(NaElement('is-auto-update', 'false'))

        ssssi.child_add(NaElement('retention-count', int(retention_ct)))
        ssssi.child_add(NaElement('schedule-name', sched))
        ssssi.child_add(NaElement('volume-name', self.name))
        
        sched_info = NaElement('snapvault-schedule-info')
        sched_info.child_add(NaElement('days-of-week', dow))
        sched_info.child_add(NaElement('hours-of_day', str(hod)))

        sched = NaElement('schedule')
        sched.child_add(sched_info)

        ssssi.child_add(sched)

        snap_sched.child_add(ssssi)
        nae.child_add(snap_sched)

        self.filer.invoke_elem(nae)

    def snapshot_create(self, snap_name):
        """Equivalent to 'snap create <self.name> <snap_name>'."""

        self.filer.invoke('snapshot-create',
                          'volume', self.name,
                          'snapshot', snap_name)

    def snapshot_delete(self, snap_name):
        """Equivalent to 'snap delete <self.name> <snap_name>'."""

        self.filer.invoke('snapshot-delete',
                          'volume', self.name,
                          'snapshot', snap_name)

    def snapshot_rename(self, current_name, new_name):
        """Equivalent to 'snap rename <self.name> <current_name> <new_name>'"""

        self.filer.invoke('snapshot-rename',
                          'volume', self.name,
                          'current-name', current_name,
                          'new-name', new_name)

    def snapvault_primary_snap(self, schedule):
        """
        Equivalent to 'snapvault snap create <self.name> <schedule>'

        Can only be run on SnapVault primary.
        """
        
        self.filer.invoke('snapvault-primary-initiate-snapshot-create',
                          'volume-name', self.name,
                           'schedule-name', schedule)

    def snapvault_secondary_snap(self, schedule):
        """
        Equivalent to 'snapvault snap create <self.name> <schedule>'

        Can only be run on SnapVault secondary.
        """
        
        self.filer.invoke('snapvault-secondary-initiate-snapshot-create',
                          'volume-name', self.name,
                          'schedule-name', schedule)
        

class Share:
    """A CIFS share on a NetApp filer."""

    def __init__(self, filer, name):
        self.filer = filer
        self.name = name

    def configured(self):
        """
        Determind if a share named self.name has been configured on filer.

        Return boolean.
        """

        output = self._get_cifs_share()
        if re.match('^No share is matching that name\.', output):
            return False
        else:
            return True

    def create(self, mount_point, description=False, forcegroup=False,
               dir_umask=False, file_umask=False, umask=False):
        """Equivalent to 'cifs shares -add mount_point' on the CLI."""

        command = ['cifs', 'shares', '-add', self.name, mount_point]
        if description:
            command.append('-comment')
            command.append(description)
        if forcegroup:
            command.append('-forcegroup')
            command.append(forcegroup)
        if dir_umask:
            command.append('-dir_umask')
            command.append(dir_umask)
        if file_umask:
            command.append('-file_umask')
            command.append(file_umask)
        if umask:
            command.append('-umask')
            command.append(umask)

        self.filer.invoke_cli(*command)

    def del_access(self, user):
        """CLI equivalent to 'cifs access -delete self.name <user>'."""

        out = self.filer.invoke_cli('cifs', 'access', '-delete', self.name,
                                    user)

    def get_access(self):
        """Return a dict containing the ACLs for a share."""

        output = self._get_cifs_share()
        acl_lines = output.splitlines()[1:]

        acls = {}
        for line in acl_lines:
            if re.match(r'^\s+\.\.\.', line):
                continue # It's an option line, not an ACL line
            m = re.match(r'^\s+(.*) / (Full Control|Change|Read)$', line)
            if m:
                acls[m.groups()[0]] = m.groups()[1]

        return acls

    def get_description(self):
        """
        Return a share's description.

        If the description is not set, return False.
        """

        config = self._get_cifs_share().splitlines()[0]
        m = re.match(r'^(.*\S)\s+(/\S*)\s+(.*)$', config)
        if m:
            return m.groups()[2]
        else:
            return False

    def get_dir_umask(self):
        """
        Return the 'dir_umask' setting for a share.

        If 'dir_umask' is not set, return False.
        """

        pattern = re.compile(r'^\s+\.\.\. dir_umask=(.*)$')
        return self._get_option(pattern)

    def get_file_umask(self):
        """
        Return the 'file_umask' setting for a share.

        If 'file_umask' is not set, return False.
        """

        pattern = re.compile(r'^\s+\.\.\. file_umask=(.*)$')
        return self._get_option(pattern)

    def get_forcegroup(self):
        """
        Return the 'forcegroup' setting for a share.

        If 'forcegroup' is not set, return False.
        """

        pattern = re.compile(r'^\s+\.\.\. forcegroup=(.*)$')
        return self._get_option(pattern)

    def get_mount_point(self):
        """Return a share's mount point."""

        config = self._get_cifs_share().splitlines()[0]
        m = re.match(r'^(.*\S)\s+(/\S*)\s+(.*)$', config)
        return m.groups()[1]

    def get_umask(self):
        """
        Return the 'umask' setting for a share.

        If 'umask' is not set, return False.
        """

        pattern = re.compile(r'^\s+\.\.\. umask=(.*)$')
        return self._get_option(pattern)

    def modify(self, description=False, forcegroup=False, dir_umask=False,
               file_umask=False, umask=False):
        """Equivalend to 'cifs shares -change ...' on the CLI."""

        command = ['cifs', 'shares', '-change', self.name]

        if description:
            command.append('-comment')
            command.append(description)
        if forcegroup:
            command.append('-forcegroup')
            command.append(forcegroup)
        if dir_umask:
            command.append('-dir_umask')
            command.append(dir_umask)
        if file_umask:
            command.append('-file_umask')
            command.append(file_umask)
        if umask:
            command.append('-umask')
            command.append(umask)

        self.filer.invoke_cli(*command)

    def set_access(self, user, rights):
        """CLI equivalent to 'cifs access share <user> <rights>'."""

        out = self.filer.invoke_cli('cifs', 'access', self.name, user, rights)

    def _get_cifs_share(self):
        """
        Return the raw CLI output from 'cifs shares <self.name>'.

        The first two header lines are stripped.
        """
        
        out = self.filer.invoke_cli('cifs', 'shares', self.name)

        output = out.child_get('cli-output').element['content'].splitlines()

        return '\n'.join(output[2:])

    def _get_option(self, pattern):
        """
        Search the _get_cifs_share output for a CIFS option and return it.

        If option is not set, return False.
        """

        output = self._get_cifs_share()
        option_lines = output.splitlines()[1:]

        for line in option_lines:
            m = pattern.match(line)
            if m:
                return m.groups()[0]
        return False
