# fusepy-flatfs
A FUSE-python (using fuse-py) implementation of a flat file system. Allows maintaining a directory hirarchy in the mounted drive, while actually keeping a flat (single directory with many files) structure on the disk. The hirarchy information is kept in an embedded DB file on disk. I use it to backup a directory structure to S3 since S3 has an object key size limit and directory names count.

Currently implemented using Oracle Berkeley DB.

In ubuntu: 
```bash
sudo apt-get install python-bsddb3
```


fuse-py takes a serious performance toll in filesystem actions. Actual read/write speed to file once open isn't effected.

To mount:
```bash
python ./flatfs.py flat_dir mount_point
```
