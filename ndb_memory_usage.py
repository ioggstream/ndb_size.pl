#!/usr/bin/python
#
# Evaluate memory usage of ndb cluster
#
# Author: Roberto Polli <rpolli@babel.it>
# License: AGPLv3
#
# TODO use argparse to customize output
from subprocess import Popen, PIPE
import sys

progname = sys.argv[0]
unit_allowed = ['B','K','M','G']

def usage():
    print("%s: node_id unit\n\n Evaluate the memory usage of an ndb node.\nAllowed units are %s" % (
          progname, unit_allowed))
    exit(1)
#
# Ndb attributes which impact on memory
#   with a factor
#
mem_attributes_factor = {
    'MaxNoOfAttributes': 200,                  # 200bytes for each attribute
    'MaxNoOfTables': 20 * 1 << 10,             # 20KB per table
    'MaxNoOfOrderedIndexes': 10 * 1 << 10,     # 10KB per Index
    'MaxNoOfUniqueHashIndexes': 15 * 1 << 10,  # 15KB per HashIndex
    'MaxNoOfConcurrentOperations': 1 << 10,    # 1K per Operation
}


#
# Ndb attributes involving memory
#
mem_attributes = """MaxNoOfAttributes
MaxNoOfTables
MaxNoOfOrderedIndexes
MaxNoOfUniqueHashIndexes
MaxNoOfConcurrentOperations
TransactionBufferMemory
IndexMemory
DataMemory
UndoIndexBuffer
UndoDataBuffer
RedoBuffer
LongMessageBuffer
DiskPageBufferMemory
SharedGlobalMemory
BackupMemory""".split('\n')

#
# Ndb attributes involving IO
#
disk_attributes = """NoOfFragmentLogFiles
NoOfFragmentLogParts
FragmentLogFileSize
NoOfReplicas
DiskCheckpointSpeed
TimeBetweenGlobalCheckpoints
TimeBetweenLocalCheckpoints
DiskSyncSize""".split('\n')

all_attributes = """Arbitration
ArbitrationRank
BackupDataDir
BackupLogBufferSize
BackupMaxWriteSize
BackupMemory
BackupReportFrequency
BatchSize
BatchSizePerLocalScan
CompressedLCP
CrashOnCorruptedTuple
DataDir
DataMemory
DiskCheckpointSpeed
DiskIOThreadPool
Diskless
DiskPageBufferMemory
DiskSyncSize
EventLogBufferSize
FileSystemPath
FileSystemPathDataFiles
FragmentLogFileSize
HeartbeatIntervalDbApi
HeartbeatIntervalDbDb
HeartbeatOrder
HeartbeatThreadPriority
HostName
IndexMemory
IndexStatAutoUpdate
IndexStatSaveScale
IndexStatTriggerScale
InitialLogfileGroup
InitialNoOfOpenFiles
LateAlloc
LockMaintThreadsToCPU
LockPagesInMainMemory
LogDestination
LogLevelCheckpoint
LogLevelConnection
LogLevelError
LogLevelShutdown
LongMessageBuffer
MaxAllocate
MaxLCPStartDelay
MaxNoOfAttributes
MaxNoOfConcurrentIndexOperations
MaxNoOfConcurrentOperations
MaxNoOfConcurrentScans
MaxNoOfConcurrentSubOperations
MaxNoOfExecutionThreads
MaxNoOfFiredTriggers
MaxNoOfLocalOperations
MaxNoOfOrderedIndexes
MaxNoOfSavedEvents
MaxNoOfSavedMessages
MaxNoOfSubscriptions
MaxNoOfTables
MaxNoOfUniqueHashIndexes
MaxParallelScansPerFragment
MaxScanBatchSize
MaxStartFailRetries
NodeId
NoOfFragmentLogFiles
NoOfFragmentLogParts
NoOfReplicas
ODirect
PortNumber
RedoBuffer
RedoOverCommitLimit
SchedulerExecutionTimer
ServerPort
SharedGlobalMemory
StartFailureTimeout
StartPartialTimeout
ThreadConfig
TimeBetweenEpochsTimeout
TimeBetweenGlobalCheckpoints
TimeBetweenLocalCheckpoints
TimeBetweenWatchDogCheckInitial
TotalSendBufferMemory
TransactionBufferMemory
TransactionInactiveTimeout
UndoDataBuffer
UndoIndexBuffer""".split('\n')


def get_unit(unit):
    """Return the size in Byte of the given unit"""
    return {
        'B': 1,
        'K': 1 << 10,
        'M': 1 << 20,
        'G': 1 << 30
    }[unit.upper()]


def memory_occupation(conf, factor='B'):
    """return a dict with memory occupation in
       the given unit.
    """
    factor = get_unit(factor)
    return dict((k, conf[k] * mem_attributes_factor.get(k, 1) / factor) for k in mem_attributes)


def disk_report(conf, factor='M', thruput=1):
    """Some disk reports based on:
         conf - configuration
         factor - unit to use
         thruput - writes in KBps
    """
    factor = get_unit(factor)
    # a reducible product and sum function
    f_product = lambda prod, y: prod * conf.get(y, 1)
    f_sum = lambda tot, y: tot + conf.get(y, 0)
    
    redo_log_size = reduce(f_product, ['NoOfFragmentLogParts', 'NoOfFragmentLogFiles', 'FragmentLogFileSize'], 1)
    # A LCP is a complete dump of the whole DataMemory
    #  its duration depends on disk speed
    checkpoint_duration = conf.get('DataMemory') / conf.get('DiskCheckpointSpeed')

    # REDO logs should buffer thruput incoming
    #  between 2 LCP
    redo_expected_size = thruput * checkpoint_duration * 3
    ret = {
        'RedoLogSize': redo_log_size / factor,
        'LocalCheckpointSize': 4 * (1 << conf.get('TimeBetweenLocalCheckpoints')) / factor,
        'DiskSpeed': conf.get('DiskCheckpointSpeed'),
        'RedoShield': redo_log_size / conf.get('DiskCheckpointSpeed'),
        'LocalCheckPointDuration': checkpoint_duration,		
        'GCPSizeFactor': thruput * conf.get('TimeBetweenGlobalCheckpoints') / 1000 ,		# A LCP is a complete dump of the whole DataMemory
        'RedoExpectedSize': redo_expected_size
    }
    return ret


def main(node_id, unit='M'):
    assert unit in unit_allowed, "Bad unit value: expected one in %s. Was: %s " %  (unit_allowed,unit)
    # prepare some variables
    conf_values = []
    cmd = ["ndb_config", "--nodeid=%s" % node_id, "-q", ",".join(
        all_attributes)]

    # FIXME check for errors!
    print "executing: %s " % cmd
    ret = Popen(cmd, stdout=PIPE, close_fds=True)
    ret = ret.stdout.read()

    # Support just one node output
    #  use integer value as possible

    # TODO support multiple nodes splitting
    #      by " ", then by ","
    for x in ret.split(","):
        # convert to integer where possible
        try:
            conf_values.append(long(x))
        except ValueError:
            conf_values.append(x)

    # Put configuration in a dictionary
    conf = dict(zip(all_attributes, conf_values))

    # Evaluate memory usage
    mem_usage = memory_occupation(conf, factor=unit)

    # Print it nicely
    print "Memory occupation per variable (real value in parens)"
    for k, v in mem_usage.iteritems():
        print "%33s: %s (%s)" % (k, v, conf.get(k))
    print "%33s: %s" % (
        'Total in MB', sum(v for (k, v) in mem_usage.iteritems())
    )

    print "Other variables"
    for k in set(all_attributes).difference(mem_attributes):
        print "%33s: %s" % (k, conf[k])

    print ("Disk report\n%s" % disk_report(conf, factor=unit))

if __name__ == '__main__':
    try:
        node_id, unit = sys.argv[1], sys.argv[2]
        main(node_id, unit)
    except AssertionError:
        raise
    except:
        usage()
