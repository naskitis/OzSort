External merge sort for 100-byte fixed-length keys, written in C.

Winner of the 2009 Indy Penny Sort and Joule Sort Competition, and the 2010 Penny Sort Indy Competition,
found at www.sortbenchmark.org.

Author:  Dr. Nikolas Askitis.
Email:   askitisn@gmail.com
Github:  https://github.com/naskitis  

To compile:
----------------------------------
make;


To run:
----------------------------------

Visit www.sortbenchmark.org to download the dataset generation tool for Indy (the version used during 2009 and 2010 benchmarks). 
Should generate 100-byte records: 10 byte key followed by 90 byte payload.

./naskitis_non_commercial_ozsort_stage_one <file-name-to-sort>  // this file will be overwritten with sorted runs. 
./naskitis_non_commercial_ozsort_stage_one <file-name-to-sort> <file-name-to-write-out-to>

example:

./naskitis_non_commercial_ozsort_stage_one dataset  
./naskitis_non_commercial_ozsort_stage_one dataset dataset.sorted 

You need a 64-bit Linux machine with at least 4GB of RAM to run this 
software, plus lots of disk space (and preferably a RAID-0 HDD setup) with custom Kernel
and TLB configurations to achieve high-performance.
