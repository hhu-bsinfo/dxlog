# DXLog: Fast object logging, reorganization and recovery.

DXLog is developed by the [operating systems group](http://www.cs.hhu.de/en/research-groups/operating-systems.html)
of the department of computer science of the Heinrich-Heine-University
Düsseldorf. DXLog is stand-alone and can be used with existing Java applications
but is also part of the distributed in-memory key-value store [DXRAM](https://github.com/hhu-bsinfo/dxram).

DXLog is ... TODO

# Important
DXLog is a research project that's under development. We do not
recommend using the system in a production environment or with
production data without having an external backup. Expect to encounter
bugs. However, we are looking forward to bug reports and code
contributions.

# Features
* A novel two-stage logging approach enabling fast recovery and providing high throughput while being memory efficient
* A highly concurrent log cleaning concept designed for handling many small data objects
* A fast parallel recovery of servers storing hundreds of millions of small data objects
* Optimized for SSDs

# Architecture
The architecture of DXLog is thoroughly described in the following publications:
* Logging: [High Throughput Log-based Replication for Many Small In-memory Objects](https://coconucos.cs.uni-duesseldorf.de/forschung/pubs/2016/ICPADS16.pdf)
* Recovery: [Fast Parallel Recovery of Many Small In-memory Objects](https://coconucos.cs.uni-duesseldorf.de/forschung/pubs/2017/ICPADS17.pdf)
* More details: [DXRAM's Fault-Tolerance Mechanisms Meet High Speed I/O Devices](https://arxiv.org/abs/1807.03562)

For an overview of all threads and their dependencies refer to [Threads](https://github.com/hhu-bsinfo/dxlog/blob/development/doc/threads.pdf).

Special classes:
* BackupRangeCatalog: This class collects all backup ranges and enables adding/removing backup ranges. A backup range contains 
a secondary log buffer and a version buffer which both control access to their log (secondary log or version log). For instance, to access
a secondary, the BackupRangeCatalog is used to get the corresponding secondary log buffer which provides access to the secondary log.
* Scheduler: This class allows communication between threads of different packages (e.g., the WriterThread triggers the ReorganizationThread to clean a secondary log).
* DirectByteBufferWrapper: Depending on the hard drive access we need different ByteBuffers (heap for RandomAccessFile, direct otherwise)
and access information (the array or address). This class wraps a ByteBuffer and the access information. Furthermore, the ByteBuffer
is created when a DirectByteBufferWrapper is instantiated. Created direct ByteBuffers are always page-aligned.
* WriteBufferTests: This class provides tests for the WriteBuffer. To enable the tests, execute DXLog with assertions ("-ea").


# How to Build and Run
## Requirements
DXLog requires Java 1.8 to run on a Linux distribution of your choice
(MacOSX might work as well but is not supported, officially).

## Building
The script *build.sh* bootstraps our build system which is using gradle to build DXLog. The build output is located
in *build/dist* either as directory (dxlog) or zip-package (dxlog.zip).

## LogThroughputTest: Logging and Recovery Benchmark
The dxlog jar-file contains a built in benchmark that can be run to evaluate the performance of DXLog locally on a
single node.

Deploy the build output to your cluster and run DXLog by executing the script *dxlog* in the *bin* subfolder:
```
./bin/dxlog ./config/dxlog.json
```

If there is no configuration file, it will create one with default values before starting the benchmark.

The hard drive access can be configured to use either a RandomAccessFile accessing files in your file system (directory
configurable, see "Usage information"), O_Direct bypassing the kernel's page cache or by writing to and reading from
a raw device. Using a raw device requires several steps for preparation:
1. Use an empty partition
2. If executed in nspawn container: add "--capability=CAP_SYS_MODULE --bind-ro=/lib/modules" to systemd-nspawn command in boot script
3. Get root access
4. mkdir /dev/raw
5. cd /dev/raw/
6. mknod raw1 c 162 1
7. modprobe raw
8. If /dev/raw/rawctl was not created: mknod /dev/raw/rawctl c 162 0
9. raw /dev/raw/raw1 /dev/*empty partition*
10. Execute DXLog as root user ("sudo -P" for nfs)

Usage information:
```
Args: <config_file> <log directory> <backup range size> <chunk count> <chunk size> <batch size> <workload> <number of updates> <enable recovery> <use recovery dummy>
  config_file: Path to the config file to use (e.g. ./config/dxlog.json). Creates new config with default value if file does not exist
  log directory: Path of directory to store logs in
  backup range size: The size of a backup range (half the size of a secondary log)
  chunk count: The number of chunks to log
  chunk size: The size of the chunks
  batch size: The number of chunks logged in a batch (minor impact on logging)
  workload: Workload to execute
     none: Finish after logging phase
     sequential: Update chunks in sequential order
     random: Update chunks randomly
     zipf: Update chunks according to zipf distribution
     hotncold: Update chunks according to hot-and-cold distribution
  number of updates: The number of updates
  enable recovery: True if all logged chunks should be recovered from disk after the update phase
  use recovery dummy: False to store all recovered chunks in DXMem, True to avoid writing to memory
```

For example, to run workload random, log 100000 chunks, update 1000000 chunks, 32 byte chunks size, run the following command:
```
./bin/dxlog ./config/dxlog.json /media/ssd/dxram_log/ 268435456 100000 32 10 random 1000000 true false
```

__When using this benchmark for evaluation make sure logs from previous runs are removed from disk and space is freed:__
```
rm /media/ssd/dxram_log/* && sudo fstrim -v /media/ssd/ && sleep 2 && ./bin/dxlog ...
```

# License

Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf,
Institute of Computer Science, Department Operating Systems.
Licensed under the [GNU General Public License](LICENSE).
